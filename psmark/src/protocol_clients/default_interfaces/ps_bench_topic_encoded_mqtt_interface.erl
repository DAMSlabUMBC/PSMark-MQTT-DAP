-module(ps_bench_topic_encoded_mqtt_interface).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

%% public
-export([start_link/3]).

%% exported for eunit
-export([purpose_scoped_filters/2]).

%% gen_server callbacks
-export([init/1, handle_call/3,
         handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(ScenarioName, ClientName0, OwnerPid) ->
    RegName = ps_bench_utils:convert_to_atom(ClientName0),
    gen_server:start_link({local, RegName}, ?MODULE, {ScenarioName, RegName, OwnerPid}, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({ScenarioName, RegName, OwnerPid}) ->
    ClientIdBin = ps_bench_utils:convert_to_binary(RegName),
    {ok, PurposeMap} = ps_bench_config_manager:fetch_mqtt_device_purposes(),
    {ok, VariantCount} = ps_bench_config_manager:fetch_mqtt_purpose_variant_count(),
    {ok, FanoutModel} = ps_bench_config_manager:fetch_mqtt_purpose_fanout_model(),
    {ok, NodePurpose} = resolve_node_purpose(),
    {ok, RotationWindowMs} = ps_bench_config_manager:fetch_mqtt_purpose_rotation_window_ms(),
    {ok, TogglePurpose} = ps_bench_config_manager:fetch_mqtt_rotation_toggle_purpose(),
    {ok, NodeRole} = ps_bench_config_manager:fetch_role_for_this_node(),
    {ok, #{scenario_name => ScenarioName, client_name => ClientIdBin, reg_name => RegName, client_pid => 0, owner_pid => OwnerPid, connected => false, first_start => true, purpose_map => PurposeMap, variant_count => VariantCount, fanout_model => FanoutModel, node_purpose => NodePurpose,
           rotation_window_ms => RotationWindowMs, rotation_toggle => TogglePurpose, node_role => NodeRole,
           sub_topics => [], applied => [], published => #{}, op_seq => 0}}.

%% Per-node consumer purpose for the topic-encoded baseline (paper SS2.2 /
%% SS7.3: one subscriber per purpose, mirroring the DAP cells). Reuses the
%% scenario's node_subscription_purposes map. Nodes not listed (publisher
%% nodes, and every node in pre-existing scenarios without the key) keep the
%% legacy subscribe-to-everything behavior.
resolve_node_purpose() ->
    {ok, NodeSPs} = ps_bench_config_manager:fetch_mqtt_node_subscription_purposes(),
    case ps_bench_config_manager:fetch_node_name() of
        {ok, NodeName} ->
            case proplists:get_value(NodeName, NodeSPs) of
                undefined -> {ok, undefined};
                SP -> {ok, ps_bench_utils:convert_to_binary(SP)}
            end;
        _ ->
            {ok, undefined}
    end.

handle_call(connect, _From, State = #{first_start := FirstStart}) ->
    do_connect(FirstStart, State);

handle_call(connect_clean, _From, State) ->
    do_connect(true, State);

handle_call(reconnect, _From, State) ->
    do_connect(false, State);

handle_call({subscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid, connected := Connected, node_purpose := undefined}) when Connected == true ->
    %% Unscoped nodes keep the wildcard topics they were given.
    _ = emqtt:subscribe(ClientPid, Properties, Topics),
    {reply, ok, State};

handle_call({subscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid, client_name := ClientIdBin, connected := Connected}) when Connected == true ->
    %% Purpose-scoped consumers subscribe only to their purpose's encoded topics,
    %% and to the broadcast operation requests.
    Purposes = sub_purposes(State),
    record_sp(ClientIdBin, Purposes),
    %% One SUBSCRIBE: with the toggled purpose removed, the filters may be empty.
    _ = emqtt:subscribe(ClientPid, Properties, filters_for(Purposes, Topics) ++ [{?LEGACY_OP_REQUEST_TOPIC, [{qos, 1}]}]),
    {reply, ok, State#{sub_topics := Topics, applied := Purposes}};

handle_call({subscribe, _Properties, _Topics}, _From, State = #{connected := Connected}) when Connected == false ->
    {reply, ok, State};

handle_call({publish, Properties, Topic, _Seq, Payload, PubOpts},
            _From,
            State = #{client_pid := ClientPid, client_name := ClientIdBin, connected := Connected, published := Published})
  when is_binary(Topic), is_binary(Payload) ->
    case Connected of
        true ->
            AllPurposeList = pub_purposes(Topic, State),
            case maps:get(Topic, Published, undefined) of
                AllPurposeList -> ok;
                _ -> ps_bench_store:record_dap_event(erlang:system_time(nanosecond), ClientIdBin, mp_register, Topic,
                                                     #{detail => join(AllPurposeList, <<"|">>, <<>>)})
            end,

            %% Topic-based purpose amplification: publish the same logical message
            %% under several purpose-topic labels
            lists:foreach(
              fun(Label) ->
                  PurposeBin = ps_bench_utils:convert_to_binary(Label),
                  EncodedTopic = <<Topic/binary, "/[", PurposeBin/binary, "]">>,
                  %% Prepend system time so payload matches decode_seq_header/1
                  TimeNs = erlang:system_time(nanosecond),
                  Payload1 = <<TimeNs:64/unsigned, Payload/binary>>,
                  emqtt:publish(ClientPid, EncodedTopic, Properties, Payload1, PubOpts)
              end,
              AllPurposeList),
            {reply, {ok, published}, State#{published := Published#{Topic => AllPurposeList}}};
        false ->
            %% Not connected; swallow to keep orchestration alive
            {reply, {ok, not_connected}, State}
    end;

handle_call({unsubscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid}) ->
    {ok, _Props, _ReasonCodes} = emqtt:unsubscribe(ClientPid, Properties, Topics),
    {reply, ok, State};

handle_call(disconnect, _From, State = #{client_pid := ClientPid, connected := Connected}) ->
    case Connected of
        true ->
            ok = emqtt:disconnect(ClientPid),
            {reply, ok, State#{connected := false}};
        false ->
            {reply, ok, State}
    end;

handle_call(op_issue_ready, _From, State = #{connected := Connected, published := Published}) ->
    {reply, Connected andalso map_size(Published) > 0, State};

handle_call(stop, _From, State) ->
    {reply, ok, State};

handle_call(_, _, State) ->
    {reply, ok, State}.

%% Dynamic purpose toggle: a subscriber subscribes to a newly added purpose's
%% topics and unsubscribes from a removed one's. Publishers toggle per publish.
handle_cast(rotate_purpose, State = #{connected := true, client_pid := ClientPid, client_name := ClientIdBin,
                                      node_purpose := NodePurpose, sub_topics := Topics, applied := Applied})
  when NodePurpose =/= undefined, Topics =/= [] ->
    case sub_purposes(State) of
        Applied ->
            {noreply, State};
        Purposes ->
            record_sp(ClientIdBin, Purposes),
            case filters_for(Purposes -- Applied, Topics) of
                [] -> ok;
                Add -> _ = emqtt:subscribe(ClientPid, #{}, Add)
            end,
            case [F || {F, _} <- filters_for(Applied -- Purposes, Topics)] of
                [] -> ok;
                Remove -> _ = emqtt:unsubscribe(ClientPid, #{}, Remove)
            end,
            {noreply, State#{applied := Purposes}}
    end;

%% An operation over stock MQTT (paper Observation 2, option i): broadcast to every
%% subscriber, which answers on the requester's response topic.
handle_cast({issue_operation, OpType}, State = #{connected := true, client_pid := ClientPid, client_name := ClientIdBin,
                                                 published := Published, op_seq := Seq}) ->
    OpId = <<ClientIdBin/binary, "-", (integer_to_binary(Seq + 1))/binary>>,
    OpTFs = join(maps:keys(Published), <<",">>, <<"*">>),
    OpPFs = join(lists:usort(lists:append(maps:values(Published))), <<",">>, <<"*">>),
    Response = <<?LEGACY_OP_RESPONSE_TOPIC/binary, "/", ClientIdBin/binary>>,
    _ = emqtt:subscribe(ClientPid, #{}, [{Response, [{qos, 1}]}]),
    ps_bench_store:record_dap_event(erlang:system_time(nanosecond), ClientIdBin, op_issue, OpTFs,
                                    #{op_type => OpType, op_id => OpId, detail => OpPFs}),
    _ = emqtt:publish(ClientPid, ?LEGACY_OP_REQUEST_TOPIC,
                      #{'User-Property' => [{?DAP_KEY_OPTYPE, OpType}, {?DAP_KEY_OPID, OpId}, {?DAP_KEY_OPTFS, OpTFs},
                                            {?DAP_KEY_OPPFS, OpPFs}, {?DAP_KEY_CLIENTID, ClientIdBin}],
                        'Response-Topic' => Response},
                      <<>>, [{qos, 1}]),
    {noreply, State#{op_seq := Seq + 1}};

handle_cast({respond_to_op, Props}, State = #{connected := true, client_pid := ClientPid, client_name := ClientIdBin}) ->
    case Props of
        #{'Response-Topic' := Response} ->
            UPs = maps:get('User-Property', Props, []),
            Get = fun(K) -> case lists:keyfind(K, 1, UPs) of {K, V} -> V; false -> <<>> end end,
            _ = emqtt:publish(ClientPid, Response,
                              #{'User-Property' => [{?DAP_KEY_STATUS, <<"Success">>}, {?DAP_KEY_OPID, Get(?DAP_KEY_OPID)},
                                                    {?DAP_KEY_OPTYPE, Get(?DAP_KEY_OPTYPE)}, {?DAP_KEY_CLIENTID, ClientIdBin}]},
                              <<>>, [{qos, 1}]);
        _ ->
            ok
    end,
    {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    case Info of
        {'EXIT', _Pid, Reason} ->
            handle_exception(Reason, State);
        _ ->
            ps_bench_utils:log_message("Recieved unknown info: ~p", [Info]),
            {noreply, State}
    end.

handle_exception(Reason, State) ->
    case Reason of
        {shutdown, econnrefused} ->
            ps_bench_utils:log_message("ERROR: MQTT Broker refused connection. Ensure connection parameters are correct"),
            {kill, Reason};
        normal ->
            {noreply, State};
        _ ->
            ps_bench_utils:log_message("ERROR: Recieved termination signal: ~p", [Reason]),
            {noreply, State}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

start_client_link(ClientName, CleanStart, OwnerPid) ->
    {ok, BrokerIP, BrokerPort} = ps_bench_config_manager:fetch_mqtt_broker_information(),
    {ok, Protocol} = ps_bench_config_manager:fetch_protocol_type(),

    PropList = [
        {host, BrokerIP},
        {port, BrokerPort},
        {clientid, ClientName},
        {clean_start, CleanStart},
        {properties, #{'Session-Expiry-Interval' => ?DAP_SESSION_EXPIRY_SECS}},
        {msg_handler, #{disconnected => fun(Reason) -> disconnect_event(OwnerPid, Reason, ClientName) end,
                        publish => fun(Msg) -> publish_event(OwnerPid, Msg, ClientName) end}}
    ],

    process_flag(trap_exit, true),
    case Protocol of
        ?MQTT_V5_PROTOCOL ->
            FullPropList = PropList ++ [{proto_ver, v5}],
            ok = ensure_emqtt_started(),
            emqtt:start_link(FullPropList);
        ?MQTT_V311_PROTOCOL ->
            FullPropList = PropList ++ [{proto_ver, v3}],
            ok = ensure_emqtt_started(),
            emqtt:start_link(FullPropList)
    end.

do_connect(CleanStart, State = #{client_name := ClientName, owner_pid := OwnerPid, connected := Connected}) ->
    case Connected of
        false ->
            case start_client_link(ClientName, CleanStart, OwnerPid) of
                {ok, NewClientPid} ->
                    case emqtt:connect(NewClientPid) of
                        {ok, Properties} ->
                            connect_event(OwnerPid, Properties, ClientName),
                            {reply, {ok, new_connection},
                             State#{client_pid := NewClientPid, connected := true, first_start := false}};
                        {error, Reason} ->
                            ps_bench_utils:log_message("MQTT connect failed for ~s with reason ~p", [ClientName, Reason]),
                            {reply, {error, Reason}, State}
                    end;
                {error, Reason} ->
                    ps_bench_utils:log_message("MQTT client start_link failed (~p): ~p", [ClientName, Reason]),
                    {reply, {error, Reason}, State};
                Res ->
                    ps_bench_utils:log_message("Error ~p", [Res])
            end;
        true ->
            {reply, {ok, already_connected}, State}
    end.

connect_event(OwnerPid, _Properties, ClientName) ->
    TimeNs = erlang:system_time(nanosecond),
    OwnerPid ! {?CONNECTED_MSG, {TimeNs}, ClientName},
    ok.

disconnect_event(OwnerPid, Reason, ClientName) ->
    TimeNs = erlang:system_time(nanosecond),
    OwnerPid ! {?DISCONNECTED_MSG, {TimeNs, Reason}, ClientName},
    ok.

publish_event(OwnerPid, Msg = #{topic := Topic, payload := Payload}, ClientName) ->
    %% 4-tuple incl. properties to match the shared PUBLISH_RECV contract.
    Props = maps:get(properties, Msg, undefined),
    TimeNs = erlang:system_time(nanosecond),
    OwnerPid ! {?PUBLISH_RECV_MSG, {TimeNs, Topic, Payload, Props}, ClientName},
    ok.

ensure_emqtt_started() ->
    case application:ensure_all_started(emqtt) of
        {ok, _} -> ok;
        {error, {emqtt, {already_started, _}}} -> ok;
        {error, Reason} -> exit({emqtt_not_started, Reason})
    end.

%% Rewrite the adapter's wildcard subscriptions into this consumer's
%% purpose-encoded topic filters. Publishes are encoded as
%% <topic>/[<purpose>] (see the publish clause), so the wildcard
%% ps_bench/device/qos_<N>/# becomes ps_bench/device/qos_<N>/+/[<variant>]
%% - one filter per decomposed purpose variant ('+' spans the device type;
%% braces in the purpose, e.g. the scaling suffixes, expand to one filter per
%% variant exactly like the publisher fan-out). undefined = unscoped node:
%% topics pass through untouched. Non-wildcard topics also pass through.
purpose_scoped_filters(undefined, Topics) ->
    Topics;
purpose_scoped_filters(PurposeBin, Topics) when is_binary(PurposeBin) ->
    filters_for(ps_bench_utils:decompose_purpose_filter(binary_to_list(PurposeBin)), Topics).

filters_for(Variants, Topics) ->
    lists:flatmap(
      fun({TopicBin, Opts}) ->
          case strip_trailing_hash(TopicBin) of
              {ok, Prefix} ->
                  [{<<Prefix/binary, "+/[", (ps_bench_utils:convert_to_binary(V))/binary, "]">>, Opts}
                   || V <- Variants];
              nomatch ->
                  [{TopicBin, Opts}]
          end
      end, Topics).

%% Split "<prefix>/#" into {ok, <<"<prefix>/">>}; nomatch for anything else.
strip_trailing_hash(TopicBin) when byte_size(TopicBin) > 1 ->
    PrefixSize = byte_size(TopicBin) - 1,
    case TopicBin of
        <<Prefix:PrefixSize/binary, "#">> -> {ok, Prefix};
        _ -> nomatch
    end;
strip_trailing_hash(_) ->
    nomatch.

%% The subscriber's purposes now: its node SP, with the toggle purpose added or
%% removed for the current window on a subscriber node (as the DAP interface does).
sub_purposes(#{node_purpose := NodePurpose} = State) ->
    toggled(ps_bench_utils:decompose_purpose_filter(binary_to_list(NodePurpose)),
            maps:get(node_role, State) =:= ?DEPLOYMENT_ROLE_SUBSCRIBER, State).

%% The purposes a publish on Topic goes out under now.
pub_purposes(Topic, State = #{purpose_map := PurposeMap}) ->
    Base = lists:flatmap(fun(X) -> ps_bench_utils:decompose_purpose_filter(X) end, purposes_for_topic(Topic, PurposeMap)),
    toggled(Base, true, State).

toggled(Base, true, #{rotation_window_ms := W, rotation_toggle := Toggle}) when W > 0, is_binary(Toggle) ->
    Tokens = [ps_bench_utils:convert_to_binary(P) || P <- Base] -- [Toggle],
    Filter = ps_bench_utils:rotation_toggle_filter(erlang:system_time(millisecond), W, Tokens, Toggle),
    ps_bench_utils:decompose_purpose_filter(binary_to_list(Filter));
toggled(Base, _, _State) ->
    Base.

record_sp(ClientIdBin, Purposes) ->
    ps_bench_store:record_dap_event(erlang:system_time(nanosecond), ClientIdBin, sp_subscribe, <<>>,
                                    #{detail => join(Purposes, <<"|">>, <<>>)}).

join([], _Sep, Default) -> Default;
join(Items, Sep, _Default) ->
    iolist_to_binary(lists:join(Sep, [ps_bench_utils:convert_to_binary(I) || I <- Items])).

%% Look up the publisher purpose for the topic's device-type suffix.
%% Topics follow ps_bench/device/qos_<N>/<device_type>. With no mapping
%% configured the encoded topic uses a default token so the shape is
%% still <topic>/[<purpose>].
purposes_for_topic(Topic, PurposeMap) ->
    case binary:split(Topic, <<"/">>, [global]) of
        [_, _, _, DeviceTypeBin] when PurposeMap =/= [] ->
            DeviceType = binary_to_atom(DeviceTypeBin, utf8),
            case proplists:get_value(DeviceType, PurposeMap) of
                undefined -> ["p1"];
                [C | _] = P when is_integer(C) -> [P];
                P -> P
            end;
        _ ->
            ["p1"]
    end.

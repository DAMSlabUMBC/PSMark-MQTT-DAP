-module(ps_bench_topic_encoded_mqtt_interface).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

%% public
-export([start_link/3]).

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
    {ok, #{scenario_name => ScenarioName, client_name => ClientIdBin, reg_name => RegName, client_pid => 0, owner_pid => OwnerPid, connected => false, first_start => true, purpose_map => PurposeMap, variant_count => VariantCount, fanout_model => FanoutModel}}.

handle_call(connect, _From, State = #{first_start := FirstStart}) ->
    do_connect(FirstStart, State);

handle_call(connect_clean, _From, State) ->
    do_connect(false, State);

handle_call(reconnect, _From, State) ->
    do_connect(true, State);

handle_call({subscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid, connected := Connected}) when Connected == true ->
    _ = emqtt:subscribe(ClientPid, Properties, Topics),
    {reply, ok, State};

handle_call({subscribe, _Properties, _Topics}, _From, State = #{connected := Connected}) when Connected == false ->
    {reply, ok, State};

handle_call({publish, Properties, Topic, _Seq, Payload, PubOpts},
            _From,
            State = #{client_pid := ClientPid, connected := Connected, purpose_map := PurposeMap, variant_count := VariantCount, fanout_model := FanoutModel})
  when is_binary(Topic), is_binary(Payload) ->
    case Connected of
        true ->
            PurposeList = purposes_for_topic(Topic, PurposeMap),
            %% Decompose purpose list into discrete purposes to send
            AllPurposeList = lists:flatmap(fun(X) -> ps_bench_utils:decompose_purpose_filter(X) end, PurposeList),

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
            {reply, {ok, published}, State};
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

handle_call(stop, _From, State) ->
    {reply, ok, State};

handle_call(_, _, State) ->
    {reply, ok, State}.

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

publish_event(OwnerPid, _Msg = #{topic := Topic, payload := Payload}, ClientName) ->
    TimeNs = erlang:system_time(nanosecond),
    OwnerPid ! {?PUBLISH_RECV_MSG, {TimeNs, Topic, Payload}, ClientName},
    ok.

ensure_emqtt_started() ->
    case application:ensure_all_started(emqtt) of
        {ok, _} -> ok;
        {error, {emqtt, {already_started, _}}} -> ok;
        {error, Reason} -> exit({emqtt_not_started, Reason})
    end.

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
                P -> P
            end;
        _ ->
            ["p1"]
    end.

-module(ps_bench_dap_mqtt_interface).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

%% public
-export([start_link/3]).

%% ===== PLACEHOLDER per-node subscription purpose (DAP-SP) =====
%% Christian's test-case section will define the real node -> SP mapping. Until
%% then this stand-in maps the DAP deployment's node names to a subscription
%% purpose. Nodes not listed fall back to "*" (match-any) so every existing
%% scenario keeps its previous wildcard behavior. A value may be a single filter
%% or a '|'-joined multi-filter; the broker delivers when the SP matches ANY one
%% of the message-purpose filters (see purpose_filter_mp_matches_sp in the broker).
-define(PLACEHOLDER_NODE_SP, [
    {runner_qa,          <<"quality/assurance">>},
    {runner_maintenance, <<"maintenance/predictive|maintenance/routine">>},
    {runner_regulatory,  <<"regulatory/reporting">>},
    {runner_ops_finance, <<"operations/forecast">>}
]).
-define(DEFAULT_NODE_SP, <<"*">>).

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
    SubPurpose = resolve_node_sp(),
    {ok, #{scenario_name => ScenarioName, client_name => ClientIdBin, reg_name => RegName, client_pid => 0, owner_pid => OwnerPid, connected => false, first_start => true, registered_topics => #{}, purpose_map => PurposeMap, sub_purpose => SubPurpose}}.

%% Resolve this node's subscription purpose (DAP-SP) from the placeholder map.
resolve_node_sp() ->
    case ps_bench_config_manager:fetch_node_name() of
        {ok, NodeName} ->
            proplists:get_value(NodeName, ?PLACEHOLDER_NODE_SP, ?DEFAULT_NODE_SP);
        _ ->
            ?DEFAULT_NODE_SP
    end.

handle_call(connect, _From, State = #{first_start := FirstStart}) ->
    % If not specifically told, we start clean by default, then preserve old sessions on reconnects
    do_connect(FirstStart, State);

handle_call(connect_clean, _From, State) ->
    % Force clean connect
    do_connect(false, State);

handle_call(reconnect, _From, State) ->
    % Force restablishment of session
    % By the MQTT standard, this just starts a clean session if none existed previously
    do_connect(true, State);

handle_call({subscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid, connected := Connected, sub_purpose := SubPurpose}) when Connected == true ->
    DapProperties = Properties#{'User-Property' => [{<<"DAP-SP">>, SubPurpose}]},
    _ = emqtt:subscribe(ClientPid, DapProperties, Topics),
    {reply, ok, State};

handle_call({subscribe, _Properties, _Topics}, _From, State = #{connected := Connected}) when Connected == false ->
    % Do nothing if not connected
    {reply, ok, State};

handle_call({publish, Properties, Topic, Seq, Payload, PubOpts},
            _From,
            State = #{client_pid := ClientPid, client_name := ClientIdBin, connected := Connected, registered_topics := Regs, purpose_map := PurposeMap})
  when is_binary(Topic), is_binary(Payload) ->
    case Connected of
        true ->
            PurposeBin = purpose_for_topic(Topic, PurposeMap),
            NewRegs = ensure_mp_registered(ClientPid, ClientIdBin, Topic, PurposeBin, Regs),
            DapProperties = Properties#{
                'User-Property' => [
                    {<<"DAP-ClientID">>, ClientIdBin},
                    {<<"DAP-Allow">>, <<"1">>}
                ],
                'Correlation-Data' => <<Seq:64/unsigned>>
            },
            %% Prepend system time time so payload matches decode_seq_header/1
            TimeNs = erlang:system_time(nanosecond),
            Payload1 = <<TimeNs:64/unsigned, Payload/binary>>,
            emqtt:publish(ClientPid, Topic, DapProperties, Payload1, PubOpts),
            {reply, {ok, published}, State#{registered_topics := NewRegs}};
        false ->
            %% Not connected; swallow to keep orchestration alive
            {reply, {ok, not_connected}, State}
    end;

handle_call({unsubscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid}) ->
    % Subscribe with the on Topic with Options
    {ok, _Props, _ReasonCodes} = emqtt:unsubscribe(ClientPid, Properties, Topics),
    {reply, ok, State};

handle_call(disconnect, _From, State = #{client_pid := ClientPid, connected := Connected}) ->
    % Disconnect from MQTT broker if connected
    case Connected of
        true ->
            % For now, we assume the properties are accepted as requested
            ok = emqtt:disconnect(ClientPid),
            {reply, ok, State#{connected := false, registered_topics := #{}}};
        false ->
            % Not connected don't do anything
            {reply, ok, State}
    end;

handle_call(stop, _From, State) ->
    % Do nothing
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
            % Do nothing
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

    % Configure properties
    PropList = [
        {host, BrokerIP},
        {port, BrokerPort},
        {clientid, ClientName},
        {clean_start, CleanStart},
        {msg_handler, #{disconnected => fun(Reason) -> disconnect_event(OwnerPid, Reason, ClientName) end,
                        publish => fun(Msg) -> publish_event(OwnerPid, Msg, ClientName) end}}
    ],

    % Start the client process
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

% Helper function to handle clean starts and reconnects
do_connect(CleanStart, State = #{client_name := ClientName, owner_pid := OwnerPid, connected := Connected}) ->
    case Connected of
        false ->
            case start_client_link(ClientName, CleanStart, OwnerPid) of
                {ok, NewClientPid} ->
                    case emqtt:connect(NewClientPid) of
                        {ok, Properties} ->
                            connect_event(OwnerPid, Properties, ClientName),
                            {reply, {ok, new_connection},
                             State#{client_pid := NewClientPid, connected := true, first_start := false, registered_topics := #{}}};
                        {error, Reason} ->
                            ps_bench_utils:log_message("MQTT connect failed for ~s with reason ~p", [ClientName, Reason]),
                            % Keep the server alive; report the error upward
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
    % Forward required event information to the adapter
    TimeNs = erlang:system_time(nanosecond),
    OwnerPid ! {?CONNECTED_MSG, {TimeNs}, ClientName},
    ok.

disconnect_event(OwnerPid, Reason, ClientName) ->
    % Forward required event information to the adapter
    TimeNs = erlang:system_time(nanosecond),
    OwnerPid ! {?DISCONNECTED_MSG, {TimeNs, Reason}, ClientName},
    ok.

publish_event(OwnerPid, _Msg = #{topic := Topic, payload := Payload}, ClientName) ->
    % Forward required event information to the adapter
    TimeNs = erlang:system_time(nanosecond),
    OwnerPid ! {?PUBLISH_RECV_MSG, {TimeNs, Topic, Payload}, ClientName},
    ok.

ensure_emqtt_started() ->
    case application:ensure_all_started(emqtt) of
        {ok, _} -> ok;
        {error, {emqtt, {already_started, _}}} -> ok;
        {error, Reason} -> exit({emqtt_not_started, Reason})
    end.

ensure_mp_registered(ClientPid, ClientIdBin, Topic, PurposeBin, Regs) ->
    case maps:is_key(Topic, Regs) of
        true ->
            Regs;
        false ->
            MpValue = <<PurposeBin/binary, ":", Topic/binary>>,
            RegProperties = #{'User-Property' => [
                {<<"DAP-ClientID">>, ClientIdBin},
                {<<"DAP-MP">>, MpValue},
                {<<"DAP-Allow">>, <<"1">>}
            ]},
            emqtt:publish(ClientPid, <<"$MP_REG">>, RegProperties, <<>>, [{qos, 0}]),
            Regs#{Topic => true}
    end.

%% Derive the publisher purpose from the device-type suffix of the topic.
%% Topics produced by PSMark follow the form ps_bench/device/qos_<N>/<device_type>.
%% A missing or empty map falls back to the wildcard purpose so pre-existing
%% scenarios keep working without a device_purposes block.
purpose_for_topic(Topic, PurposeMap) ->
    case binary:split(Topic, <<"/">>, [global]) of
        [_, _, _, DeviceTypeBin] when PurposeMap =/= [] ->
            DeviceType = binary_to_atom(DeviceTypeBin, utf8),
            case proplists:get_value(DeviceType, PurposeMap) of
                undefined -> <<"*">>;
                P when is_list(P) -> list_to_binary(P);
                P when is_binary(P) -> P;
                P when is_atom(P) -> atom_to_binary(P, utf8)
            end;
        _ ->
            <<"*">>
    end.

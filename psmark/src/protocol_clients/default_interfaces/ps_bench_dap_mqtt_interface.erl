-module(ps_bench_dap_mqtt_interface).
-behaviour(gen_server).

-include("ps_bench_config.hrl").

%% public
-export([start_link/3]).

%% ===== Per-node subscription purpose (DAP-SP) =====
%% The real node -> SP mapping lives in each scenario's protocol_config under
%% node_subscription_purposes (paper SS2.2 consumer grouping; see the
%% experiments/DAP/Smoke scenarios). A value may be a single filter or a
%% '|'-joined multi-filter; the broker delivers when the MP permits every purpose
%% of the SP. Nodes not listed fall back to "*", which only an MP of "*" permits.
-define(DEFAULT_NODE_SP, <<"*">>).

%% ===== Dynamic-purpose add/remove toggle (paper SS7.3) =====
%% Each window ALTERNATES between ADDING and REMOVING a single purpose - the
%% scenario's rotation_toggle_purpose - on top of each client's REAL filter:
%% a publisher toggles it on the MP derived from its device_purposes entry, a
%% subscriber-role node on its node_subscription_purposes DAP-SP. Both derive
%% the add/remove decision from the SAME clock window
%% (ps_bench_utils:rotation_toggle_filter/4), which keeps them in LOCKSTEP.
%% The window (ms) is the scenario config purpose_rotation_window_ms
%% (10000/5000/1000 for the paper's 10/5/1 s cells); 0 disables it.
%%
%% When the toggle purpose is part of a client's base filter (e.g. toggling
%% maintenance/routine on the Maintenance consumer), "removed" windows strip it
%% - real consent-withdrawal semantics; "added" windows restore it. Publisher
%% nodes' legacy subscription clients (not in the SP map) never toggle their SP,
%% so their match-any/default behavior is preserved.

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
    {ok, RotationWindowMs} = ps_bench_config_manager:fetch_mqtt_purpose_rotation_window_ms(),
    {ok, TogglePurpose} = ps_bench_config_manager:fetch_mqtt_rotation_toggle_purpose(),
    {ok, NodeRole} = ps_bench_config_manager:fetch_role_for_this_node(),
    SubPurpose = resolve_node_sp(),
    {ok, #{scenario_name => ScenarioName, client_name => ClientIdBin, reg_name => RegName,
           client_pid => 0, owner_pid => OwnerPid, connected => false, first_start => true,
           %% registered_topics maps Topic -> last-registered purpose binary so a
           %% rotation that changes the purpose forces a fresh MP registration.
           registered_topics => #{}, purpose_map => PurposeMap,
           sub_purpose => SubPurpose,
           node_role => NodeRole,
           rotation_window_ms => RotationWindowMs,
           rotation_toggle => TogglePurpose,
           sub_topics => [],          %% topics this client is subscribed to (for re-subscribe)
           applied_sp => undefined}}. %% last DAP-SP actually sent (avoids redundant re-subscribe)

%% Resolve this node's subscription purpose (DAP-SP) from the scenario's
%% node_subscription_purposes map; unmapped nodes get match-any.
resolve_node_sp() ->
    {ok, NodeSPs} = ps_bench_config_manager:fetch_mqtt_node_subscription_purposes(),
    case ps_bench_config_manager:fetch_node_name() of
        {ok, NodeName} ->
            case proplists:get_value(NodeName, NodeSPs) of
                undefined -> ?DEFAULT_NODE_SP;
                SP -> ps_bench_utils:convert_to_binary(SP)
            end;
        _ ->
            ?DEFAULT_NODE_SP
    end.

handle_call(connect, _From, State = #{first_start := FirstStart}) ->
    % If not specifically told, we start clean by default, then preserve old sessions on reconnects
    do_connect(FirstStart, State);

handle_call(connect_clean, _From, State) ->
    % Force clean connect
    do_connect(true, State);

handle_call(reconnect, _From, State) ->
    % Force restablishment of session
    % By the MQTT standard, this just starts a clean session if none existed previously
    do_connect(false, State);

handle_call({subscribe, Properties, Topics}, _From, State = #{client_pid := ClientPid, client_name := ClientIdBin, connected := Connected}) when Connected == true ->
    SP = effective_sp(State),
    DapProperties = Properties#{'User-Property' => [{?DAP_KEY_SP, SP}]},
    record_sp(ClientIdBin, SP),
    _ = emqtt:subscribe(ClientPid, DapProperties, Topics),
    %% Also listen on this subscriber's operation-request inbox (OP_REQ/<id>) so
    %% the broker can forward scoped operations about the data it received. The
    %% inbox is an op-system topic (SP-exempt); we still attach DAP-SP=DAP_OP by
    %% convention.
    _ = subscribe_op_request_inbox(ClientPid, ClientIdBin),
    %% Remember the topics + applied SP so the rotation timer can re-subscribe.
    {reply, ok, State#{sub_topics := Topics, applied_sp := SP}};

handle_call({subscribe, _Properties, _Topics}, _From, State = #{connected := Connected}) when Connected == false ->
    % Do nothing if not connected
    {reply, ok, State};

handle_call({publish, Properties, Topic, Seq, Payload, PubOpts},
            _From,
            State = #{client_pid := ClientPid, client_name := ClientIdBin, connected := Connected, registered_topics := Regs})
  when is_binary(Topic), is_binary(Payload) ->
    case Connected of
        true ->
            PurposeBin = effective_pub_purpose(Topic, State),
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

%% Ready to issue operations once connected with an MP registered, i.e. after publishing.
handle_call(op_issue_ready, _From, State = #{connected := Connected, registered_topics := Regs}) ->
    {reply, Connected andalso map_size(Regs) > 0, State};

handle_call(stop, _From, State) ->
    % Do nothing
    {reply, ok, State};

handle_call(_, _, State) ->
    {reply, ok, State}.

%% Dynamic purpose toggle tick (driven by the scenario manager's timer).
%% A publisher re-registers its MPs with the current toggled MP filter; a
%% subscriber re-subscribes with the current toggled SP filter. Both derive the
%% add/remove decision from the same clock window, so they stay in lockstep.
handle_cast(rotate_purpose, State = #{rotation_window_ms := W}) when W > 0 ->
    State1 = rotate_subscriptions(State),
    State2 = rotate_registrations(State1),
    {noreply, State2};

%% Issue an operation scoped to THIS publisher's own data. OpTFs/OpPFs are the
%% topics and purposes this client has registered MPs for - the same per-issuer
%% scoping proven on the Python side. Submitted to $OP_SYS; the broker forwards
%% it to the subscribers that received this publisher's matching data.
handle_cast({issue_operation, OpType}, State = #{connected := true, client_pid := ClientPid,
                                                 client_name := ClientIdBin, registered_topics := Regs}) ->
    OpTFs = join_binaries(maps:keys(Regs), <<",">>, ?DAP_ALLOW_ALL_FILTER),
    OpPFs = join_binaries(lists:usort(maps:values(Regs)), <<",">>, ?DAP_ALLOW_ALL_FILTER),
    %% Listen for the broker's status notification about our own requests. QoS 1
    %% and a kept session deliver it at least once, also across a disconnect (paper 6.2).
    Notif = <<(?DAP_TOPIC_OP_NOTIF)/binary, "/", ClientIdBin/binary>>,
    _ = emqtt:subscribe(ClientPid, #{'User-Property' => [{?DAP_KEY_SP, ?DAP_OP_PURPOSE}]},
                        [{Notif, [{qos, 1}]}]),
    OpProps = #{'User-Property' => [
                    {?DAP_KEY_OPTYPE, OpType},
                    {?DAP_KEY_OPTFS, OpTFs},
                    {?DAP_KEY_OPPFS, OpPFs},
                    {?DAP_KEY_CLIENTID, ClientIdBin},
                    {?DAP_KEY_CONSENT, <<"1">>}
                ],
                'Response-Topic' => Notif},
    ps_bench_store:record_dap_event(erlang:system_time(nanosecond), ClientIdBin, op_issue, OpTFs,
                                    #{op_type => OpType, detail => OpPFs}),
    emqtt:publish(ClientPid, ?DAP_TOPIC_OSYS, OpProps, <<>>, [{qos, 0}]),
    {noreply, State};
handle_cast({issue_operation, _OpType}, State) ->
    %% not connected yet; drop quietly to keep orchestration alive
    {noreply, State};

%% Respond to a broker-forwarded operation request (subscriber side). Echo the
%% broker's numeric DAP-OpId and the DAP-OpType so the status correlates, and
%% report Success on $OP_SYS; the broker relays it to the requester's OP_NOTIF.
handle_cast({respond_to_op, Props}, State = #{connected := true, client_pid := ClientPid,
                                              client_name := ClientIdBin}) ->
    OpType = prop_value(?DAP_KEY_OPTYPE, Props, <<>>),
    OpId = prop_value(?DAP_KEY_OPID, Props, undefined),
    Base = [
        {?DAP_KEY_STATUS, <<"Success">>},
        {?DAP_KEY_OPTYPE, OpType},
        {?DAP_KEY_CLIENTID, ClientIdBin},
        {?DAP_KEY_CONSENT, <<"1">>}
    ],
    UserProps = case OpId of
                    undefined -> Base;
                    _ -> [{?DAP_KEY_OPID, OpId} | Base]
                end,
    emqtt:publish(ClientPid, ?DAP_TOPIC_OSYS, #{'User-Property' => UserProps}, <<>>, [{qos, 0}]),
    {noreply, State};
handle_cast({respond_to_op, _Props}, State) ->
    {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.

%% Record the SP a subscription is about to be sent with, for residual-error analysis.
record_sp(ClientIdBin, SP) ->
    ps_bench_store:record_dap_event(erlang:system_time(nanosecond), ClientIdBin, sp_subscribe, <<>>,
                                    #{detail => SP}).

%% Subscribe a client to its OP_REQ/<id> operation-request inbox.
subscribe_op_request_inbox(ClientPid, ClientIdBin) ->
    Inbox = <<(?DAP_TOPIC_OP_REQ)/binary, "/", ClientIdBin/binary>>,
    emqtt:subscribe(ClientPid, #{'User-Property' => [{?DAP_KEY_SP, ?DAP_OP_PURPOSE}]},
                    [{Inbox, [{qos, 1}]}]).

%% Join a list of binaries with Sep, or return Default for the empty list.
join_binaries([], _Sep, Default) -> Default;
join_binaries([First | Rest], Sep, _Default) ->
    lists:foldl(fun(B, Acc) -> <<Acc/binary, Sep/binary, B/binary>> end, First, Rest).

%% Read a user-property value (binary) from an emqtt properties map.
prop_value(Key, Props, Default) when is_map(Props) ->
    case maps:get('User-Property', Props, []) of
        UPs when is_list(UPs) ->
            case lists:keyfind(Key, 1, UPs) of
                {Key, V} -> V;
                false -> Default
            end;
        _ ->
            Default
    end;
prop_value(_Key, _Props, Default) ->
    Default.

%% Re-subscribe with the current rotation SP (subscriber clients only).
rotate_subscriptions(State = #{connected := true, client_pid := ClientPid, client_name := ClientIdBin,
                               sub_topics := Topics, applied_sp := Applied})
  when Topics =/= [] ->
    SP = effective_sp(State),
    case SP =:= Applied of
        true ->
            State;
        false ->
            DapProperties = #{'User-Property' => [{<<"DAP-SP">>, SP}]},
            record_sp(ClientIdBin, SP),
            _ = emqtt:subscribe(ClientPid, DapProperties, Topics),
            State#{applied_sp := SP}
    end;
rotate_subscriptions(State) ->
    State.

%% Re-register all known MPs with the current toggled MP filter (publisher
%% clients only). ensure_mp_registered re-publishes only when the filter actually
%% changed, so this is a cheap no-op within a window and a real re-registration
%% at each add/remove boundary. Each topic toggles over its OWN device-purpose
%% base, so multi-purpose devices keep their other purposes throughout.
rotate_registrations(State = #{connected := true, client_pid := ClientPid,
                               client_name := ClientIdBin, registered_topics := Regs})
  when map_size(Regs) > 0 ->
    NewRegs = maps:fold(
                fun(Topic, _Old, Acc) ->
                    Filter = effective_pub_purpose(Topic, State),
                    ensure_mp_registered(ClientPid, ClientIdBin, Topic, Filter, Acc)
                end, Regs, Regs),
    State#{registered_topics := NewRegs};
rotate_registrations(State) ->
    State.

%% Active toggled filter ('|'-joined binary) for "now" over BaseTokens, or
%% undefined when rotation is off (window 0 or no toggle purpose configured).
%% The toggle purpose is stripped from the base first so "removed" windows
%% genuinely remove it when the base already contains it (consent withdrawal),
%% and "added" windows carry base + toggle.
current_rotation_filter(W, Toggle, BaseTokens) when W > 0, is_binary(Toggle) ->
    NowMs = erlang:system_time(millisecond),
    ps_bench_utils:rotation_toggle_filter(NowMs, W, BaseTokens -- [Toggle], Toggle);
current_rotation_filter(_W, _Toggle, _BaseTokens) ->
    undefined.

%% Effective subscription purpose: subscriber-role nodes toggle over their real
%% per-node SP when rotation is on; everyone else (and rotation-off runs) keeps
%% the static per-node SP. Publisher nodes' legacy subscription clients are
%% excluded so a toggled window can never widen their match-any SP.
effective_sp(#{node_role := subscriber, rotation_window_ms := W,
               rotation_toggle := Toggle, sub_purpose := SubPurpose}) ->
    case current_rotation_filter(W, Toggle, filter_tokens(SubPurpose)) of
        undefined -> SubPurpose;
        F -> F
    end;
effective_sp(#{sub_purpose := SubPurpose}) ->
    SubPurpose.

%% Effective publish (MP) purpose: the device's real purpose tokens, toggled
%% when rotation is on, statically '|'-joined otherwise.
effective_pub_purpose(Topic, #{rotation_window_ms := W, rotation_toggle := Toggle,
                               purpose_map := PurposeMap}) ->
    BaseTokens = mp_tokens_for_topic(Topic, PurposeMap),
    case current_rotation_filter(W, Toggle, BaseTokens) of
        undefined -> join_binaries(BaseTokens, <<"|">>, ?DAP_ALLOW_ALL_FILTER);
        F -> F
    end.

%% Plain '|'-token list (binaries) for a filter binary: '|' split plus {a,b}
%% brace expansion via decompose_purpose_filter. "*" passes through as ["*"].
filter_tokens(FilterBin) when is_binary(FilterBin) ->
    [list_to_binary(T)
     || T <- ps_bench_utils:decompose_purpose_filter(binary_to_list(FilterBin))].

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
        %% Keep the session across a device's simulated disconnects, so operation
        %% requests and notifications sent meanwhile are delivered on reconnect.
        {properties, #{'Session-Expiry-Interval' => ?DAP_SESSION_EXPIRY_SECS}},
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

publish_event(OwnerPid, Msg = #{topic := Topic, payload := Payload}, ClientName) ->
    % Forward required event information to the adapter, now including the MQTT
    % properties so DAP operation requests/notifications can be recognized.
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

%% Register the topic's MP, (re-)publishing to $MP_REG whenever the desired
%% purpose differs from the one last registered for this topic. Keying the cache
%% on the purpose (not just presence) is what lets a rotation switch the MP - the
%% broker overwrites the topic's purpose filter on each fresh registration.
ensure_mp_registered(ClientPid, ClientIdBin, Topic, PurposeBin, Regs) ->
    case maps:get(Topic, Regs, undefined) of
        PurposeBin ->
            %% already registered with exactly this purpose
            Regs;
        _ ->
            MpValue = <<PurposeBin/binary, ":", Topic/binary>>,
            ps_bench_store:record_dap_event(erlang:system_time(nanosecond), ClientIdBin, mp_register, Topic,
                                            #{detail => PurposeBin}),
            RegProperties = #{'User-Property' => [
                {<<"DAP-ClientID">>, ClientIdBin},
                {<<"DAP-MP">>, MpValue},
                {<<"DAP-Allow">>, <<"1">>}
            ]},
            emqtt:publish(ClientPid, <<"$MP_REG">>, RegProperties, <<>>, [{qos, 0}]),
            Regs#{Topic => PurposeBin}
    end.

%% Derive the publisher MP token list from the device-type suffix of the topic.
%% Topics produced by PSMark follow the form ps_bench/device/qos_<N>/<device_type>.
%% Each device_purposes entry may be a single purpose or a list of purposes, and
%% each purpose may carry '|' multi-filters or {a,b} braces - the broker stores
%% MPs RAW (no brace expansion on $MP_REG), so braces MUST be expanded here.
%% A missing or empty map falls back to the wildcard purpose so pre-existing
%% scenarios keep working without a device_purposes block.
mp_tokens_for_topic(Topic, PurposeMap) ->
    case binary:split(Topic, <<"/">>, [global]) of
        [_, _, _, DeviceTypeBin] when PurposeMap =/= [] ->
            DeviceType = binary_to_atom(DeviceTypeBin, utf8),
            case proplists:get_value(DeviceType, PurposeMap) of
                undefined -> [?DAP_ALLOW_ALL_FILTER];
                Entry -> purpose_entry_tokens(Entry)
            end;
        _ ->
            [?DAP_ALLOW_ALL_FILTER]
    end.

%% Normalize a device_purposes entry to a deduped, order-preserving token list.
%% Entry shapes: "purpose" (charlist), <<"purpose">>, an atom, or a LIST of
%% those (multi-purpose device). A bare charlist is told apart from a list of
%% purposes by its integer head.
purpose_entry_tokens(Entry) when is_binary(Entry) ->
    dedupe_tokens(filter_tokens(Entry));
purpose_entry_tokens(Entry) when is_atom(Entry) ->
    purpose_entry_tokens(atom_to_binary(Entry, utf8));
purpose_entry_tokens([C | _] = Entry) when is_integer(C) ->
    purpose_entry_tokens(list_to_binary(Entry));
purpose_entry_tokens(Entries) when is_list(Entries), Entries =/= [] ->
    dedupe_tokens(lists:append(
        [filter_tokens(ps_bench_utils:convert_to_binary(E)) || E <- Entries]));
purpose_entry_tokens(_) ->
    [?DAP_ALLOW_ALL_FILTER].

%% Order-preserving dedupe (keeps the first occurrence of each token).
dedupe_tokens([]) -> [];
dedupe_tokens([T | Rest]) -> [T | dedupe_tokens([X || X <- Rest, X =/= T])].

%% Part 4 integration proof: operations from PSMark publisher nodes.
%%
%% Phase A (DAP op round-trip, against the DAP broker):
%%   * subscriber runs the FULL production stack (adapter + DAP interface), so the
%%     adapter's op-request routing and the interface's op response are exercised;
%%   * publisher (direct interface) publishes data the subscriber receives, then
%%     issues a HISTORY operation scoped to its OWN topic/purpose.
%%   The broker forwards the op to the relevant subscriber (the one that got the
%%   publisher's matching data); the subscriber emits a Success response; the
%%   broker relays it to the requester's OP_NOTIF inbox. We assert the publisher
%%   receives a DAP-Status=Success notification - which can ONLY happen if the
%%   subscriber that received the data actually responded.
%%
%% Phase B (non-DAP baseline, against a plain broker):
%%   The PUBLISH_RECV contract now carries MQTT properties (a 4-tuple), which
%%   touches the non-DAP interface + adapter too. We run the plain default MQTT
%%   interface through the adapter and confirm normal publish/receive still works
%%   (recv recorded) - i.e. the contract change did not break the baseline.
%%
%% Run (DAP broker on 18840, plain broker on 18841):
%%   erlc -I include -o integration_tests integration_tests/ps_bench_operations_itest.erl
%%   erl -noshell -pa _build/default/lib/*/ebin -pa integration_tests \
%%       -eval 'ps_bench_operations_itest:main()'
-module(ps_bench_operations_itest).

-export([main/0, start/2, stop/1]).

-define(TEST_APP, ps_bench_operations_itest_app).
-define(DIR, "/tmp/ps_bench_operations_itest").
-define(DAP_PORT, 18840).
-define(PLAIN_PORT, 18841).

start(_Type, _Args) ->
    ok = ps_bench_config_manager:load_config_from_env_vars(),
    {ok, spawn(fun() -> receive stop -> ok end end)}.

stop(_) -> ok.

main() ->
    try
        setup_config_files(),
        load_config_via_app(),
        ok = ps_bench_store:initialize_node_storage(),

        APhase = phase_a_dap_operations(),
        BPhase = phase_b_non_dap_baseline(),

        io:format("~n==== PART 4 RESULTS ====~n"),
        io:format("  Phase A (DAP op round-trip):~n"),
        io:format("    OP_NOTIF statuses received by requester: ~p~n", [maps:get(statuses, APhase)]),
        io:format("    subscriber responded Success (round-trip complete): ~p~n", [maps:get(ok, APhase)]),
        io:format("  Phase B (non-DAP baseline through the 4-tuple contract):~n"),
        io:format("    recv events recorded: ~p  ->  non-DAP still works: ~p~n",
                  [maps:get(recv, BPhase), maps:get(ok, BPhase)]),

        case maps:get(ok, APhase) andalso maps:get(ok, BPhase) of
            true  -> io:format("~nPART 4 PASSED~n"), halt(0);
            false -> io:format("~nPART 4 FAILED~n"), halt(1)
        end
    catch
        Class:Reason:Stack ->
            io:format("~nPART 4 CRASHED: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(2)
    end.

%%--------------------------------------------------------------------
%% Phase A: DAP operation round-trip.
%%--------------------------------------------------------------------
phase_a_dap_operations() ->
    application:set_env(?TEST_APP, selected_scenario, p4_scn),
    application:set_env(?TEST_APP, node_name, p4_node),
    drain(),

    Topic = <<"ps_bench/device/qos_0/p4dev">>,

    %% Subscriber: full production stack (adapter + DAP interface).
    {ok, SubAdapter} = ps_bench_erlang_mqtt_adapter:start_link(p4_scn, ps_bench_dap_mqtt_interface, "p4_sub", subscriber),
    ok = gen_server:call(SubAdapter, connect),
    ok = gen_server:call(SubAdapter, subscribe),
    timer:sleep(400),

    %% Publisher: direct interface so this harness sees its OP_NOTIF inbox.
    {ok, _Pub} = ps_bench_dap_mqtt_interface:start_link(p4_scn, p4_pub, self()),
    {ok, _} = gen_server:call(p4_pub, connect),
    timer:sleep(200),

    %% Publish data the subscriber receives -> makes the subscriber "relevant".
    {ok, published} = gen_server:call(p4_pub, {publish, #{}, Topic, 1, <<"data">>, [{qos, 0}]}),
    timer:sleep(500),

    %% Issue a HISTORY op scoped to the publisher's own topic/purpose.
    gen_server:cast(p4_pub, {issue_operation, <<"HISTORY">>}),
    timer:sleep(1500),

    Statuses = collect_op_statuses([]),

    catch gen_server:call(p4_pub, disconnect),
    catch gen_server:call(SubAdapter, disconnect),
    timer:sleep(200),

    Ok = lists:member(<<"Success">>, Statuses),
    #{statuses => Statuses, ok => Ok}.

%% Collect DAP-Status values from OP_NOTIF messages delivered to the requester.
collect_op_statuses(Acc) ->
    receive
        {publish_recv, {_T, Topic, _P, Props}, _CN} ->
            case is_op_notif(Topic) of
                true ->
                    St = prop(<<"DAP-Status">>, Props),
                    collect_op_statuses(case St of undefined -> Acc; _ -> [St | Acc] end);
                false ->
                    collect_op_statuses(Acc)
            end;
        _Other ->
            collect_op_statuses(Acc)
    after 200 ->
        lists:reverse(Acc)
    end.

is_op_notif(<<"OP_NOTIF/", _/binary>>) -> true;
is_op_notif(_) -> false.

prop(Key, Props) when is_map(Props) ->
    case maps:get('User-Property', Props, []) of
        UPs when is_list(UPs) ->
            case lists:keyfind(Key, 1, UPs) of
                {Key, V} -> V;
                false -> undefined
            end;
        _ -> undefined
    end;
prop(_Key, _Props) -> undefined.

%%--------------------------------------------------------------------
%% Phase B: non-DAP baseline through the new 4-tuple contract.
%%--------------------------------------------------------------------
phase_b_non_dap_baseline() ->
    application:set_env(?TEST_APP, selected_scenario, p4b_scn),
    application:set_env(?TEST_APP, node_name, p4b_node),

    Topic = <<"ps_bench/device/qos_0/p4bdev">>,
    Before = recv_count(p4b_node),

    {ok, SubAdapter} = ps_bench_erlang_mqtt_adapter:start_link(p4b_scn, ps_bench_default_mqtt_interface, "p4b_sub", subscriber),
    {ok, PubAdapter} = ps_bench_erlang_mqtt_adapter:start_link(p4b_scn, ps_bench_default_mqtt_interface, "p4b_pub", p4bdev),
    ok = gen_server:call(SubAdapter, connect),
    ok = gen_server:call(PubAdapter, connect),
    ok = gen_server:call(SubAdapter, subscribe),
    timer:sleep(400),

    %% Publish a few messages through the adapter -> interface -> plain broker.
    [ gen_server:call(PubAdapter, {publish, Topic, N, <<"payload">>, [{qos, 0}]}) || N <- lists:seq(1, 5) ],
    timer:sleep(600),

    catch gen_server:call(SubAdapter, disconnect),
    catch gen_server:call(PubAdapter, disconnect),
    timer:sleep(200),

    Recv = recv_count(p4b_node) - Before,
    #{recv => Recv, ok => Recv > 0}.

recv_count(NodeName) ->
    length([E || E <- safe_recv(), element(1, E) =:= NodeName]).

safe_recv() ->
    case catch ps_bench_store:fetch_recv_events() of
        L when is_list(L) -> L;
        _ -> []
    end.

drain() ->
    receive _ -> drain() after 0 -> ok end.

%%--------------------------------------------------------------------
load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, p4_node),
    application:set_env(?TEST_APP, device_definitions_directory, ?DIR ++ "/devices"),
    application:set_env(?TEST_APP, deployment_definitions_directory, ?DIR ++ "/deployments"),
    application:set_env(?TEST_APP, scenario_definitions_directory, ?DIR ++ "/scenarios"),
    application:set_env(?TEST_APP, selected_scenario, p4_scn),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark operations itest"}, {vsn, "1"},
                {modules, [?MODULE]}, {registered, []},
                {applications, [kernel, stdlib]},
                {mod, {?MODULE, []}}]},
    ok = application:load(AppSpec),
    ok = application:start(?TEST_APP).

setup_config_files() ->
    DevDir = ?DIR ++ "/devices",
    DepDir = ?DIR ++ "/deployments",
    ScnDir = ?DIR ++ "/scenarios",
    [ok = filelib:ensure_dir(D ++ "/x") || D <- [DevDir, DepDir, ScnDir]],

    Dev = fun(Type) ->
        "[{type, " ++ Type ++ "},\n"
        " {publication_frequency_ms, 500},\n"
        " {payload_bytes_mean, 16.0},\n"
        " {payload_bytes_variance, 0.0},\n"
        " {disconnect_check_period_ms, 0},\n"
        " {disconnect_chance_pct, 0.0},\n"
        " {reconnect_check_period_ms, 0},\n"
        " {reconnect_chance_pct, 0.0}].\n"
    end,
    ok = file:write_file(DevDir ++ "/p4dev.device", Dev("p4dev")),
    ok = file:write_file(DevDir ++ "/p4bdev.device", Dev("p4bdev")),

    Deployment =
        "[{name, p4_dep},\n"
        " {nodes, [{p4_node, [{devices, [{p4dev, 1}]}]},\n"
        "          {p4b_node, [{devices, [{p4bdev, 1}]}]}]}].\n",
    ok = file:write_file(DepDir ++ "/p4.deployment", Deployment),

    %% DAP scenario (broker on DAP_PORT)
    DapScn =
        "[{name, p4_scn},\n"
        " {duration, {30, seconds}},\n"
        " {protocol, mqttv5},\n"
        " {deployment_name, p4_dep},\n"
        " {hosts, [{p4_node, [{hostname, \"127.0.0.1\"}]}]},\n"
        " {protocol_config,\n"
        "  [{client_interface_module, ps_bench_dap_mqtt_interface},\n"
        "   {broker, \"127.0.0.1\"},\n"
        "   {port, " ++ integer_to_list(?DAP_PORT) ++ "},\n"
        "   {qos, [{default_qos, 0}]},\n"
        "   {device_purposes, [{p4dev, \"quality/assurance\"}]}]},\n"
        " {metric_config,\n"
        "  [{output_dir, \"" ?DIR "/results\"},\n"
        "   {hw_stats_poll_period_ms, 1000},\n"
        "   {metric_plugins, [{ps_bench_throughput_calc_plugin, erlang}]}]}].\n",
    ok = file:write_file(ScnDir ++ "/p4.scenario", DapScn),

    %% Plain (non-DAP) scenario (broker on PLAIN_PORT, default interface)
    PlainScn =
        "[{name, p4b_scn},\n"
        " {duration, {30, seconds}},\n"
        " {protocol, mqttv5},\n"
        " {deployment_name, p4_dep},\n"
        " {hosts, [{p4b_node, [{hostname, \"127.0.0.1\"}]}]},\n"
        " {protocol_config,\n"
        "  [{client_interface_module, ps_bench_default_mqtt_interface},\n"
        "   {broker, \"127.0.0.1\"},\n"
        "   {port, " ++ integer_to_list(?PLAIN_PORT) ++ "},\n"
        "   {qos, [{default_qos, 0}]}]},\n"
        " {metric_config,\n"
        "  [{output_dir, \"" ?DIR "/results\"},\n"
        "   {hw_stats_poll_period_ms, 1000},\n"
        "   {metric_plugins, [{ps_bench_throughput_calc_plugin, erlang}]}]}].\n",
    ok = file:write_file(ScnDir ++ "/p4b.scenario", PlainScn),
    ok.

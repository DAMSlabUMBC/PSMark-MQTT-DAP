%% Part 3 integration proof (production timer path): the scenario_manager
%% rotation timer drives lockstep rotation through the FULL stack.
%%
%% Runs the real scenario_manager lifecycle for a single publisher node (which
%% also self-subscribes) with rotation enabled. The publisher device loop
%% rotates its MP on its own; the subscription client has NO publish loop, so it
%% can only rotate its SP when the scenario_manager timer casts rotate_purpose
%% to it (scenario_manager -> adapter -> interface). If that timer path were
%% broken, the subscriber would stick on its first-window SP and self-delivery
%% would drop to zero after the first window.
%%
%% Proves: self-delivery is sustained across >= 3 rotation windows, i.e. the
%% production timer actually fires and rotates both sides in lockstep.
%%
%% Run (broker must be up on BROKER_PORT, DAP build):
%%   erlc -I include -o integration_tests integration_tests/ps_bench_rotation_timer_itest.erl
%%   erl -noshell -pa _build/default/lib/*/ebin -pa integration_tests \
%%       -eval 'ps_bench_rotation_timer_itest:main()'
-module(ps_bench_rotation_timer_itest).

-export([main/0, start/2, stop/1]).

-define(TEST_APP, ps_bench_rotation_timer_itest_app).
-define(BROKER_HOST, "127.0.0.1").
-define(BROKER_PORT, 18840).
-define(DIR, "/tmp/ps_bench_rotation_timer_itest").
-define(WINDOW_MS, 1500).
-define(NODE, rot_tnode).

start(_Type, _Args) ->
    ok = ps_bench_config_manager:load_config_from_env_vars(),
    {ok, spawn(fun() -> receive stop -> ok end end)}.

stop(_) -> ok.

main() ->
    try
        setup_config_files(),
        load_config_via_app(),
        ok = ps_bench_store:initialize_node_storage(),
        {ok, _Sup} = ps_bench_client_sup:start_link(),

        application:set_env(?TEST_APP, node_name, ?NODE),
        ok = ps_bench_scenario_manager:initialize_scenario(),
        ps_bench_scenario_manager:run_scenario(),
        {ok, DurationMs} = ps_bench_config_manager:fetch_scenario_duration(),
        timer:sleep(DurationMs + 3000),
        _ = ps_bench_scenario_manager:clean_up_scenario(),
        timer:sleep(300),

        %% Bucket received messages by the absolute rotation window they arrived
        %% in (element 7 of the recv tuple is t_recv_ns).
        Recvs = [E || E <- ps_bench_store:fetch_recv_events(), element(1, E) =:= ?NODE],
        Windows = lists:usort([ (element(7, E) div 1000000) div ?WINDOW_MS || E <- Recvs ]),
        NWindows = length(Windows),

        io:format("~n==== PART 3 (production timer) RESULTS ====~n"),
        io:format("  total self-delivered messages: ~p~n", [length(Recvs)]),
        io:format("  distinct rotation windows with delivery: ~p~n", [NWindows]),

        Ok = NWindows >= 3,
        io:format("  self-delivery sustained across >= 3 rotation windows: ~p~n", [Ok]),
        case Ok of
            true  -> io:format("~nPART 3 (timer) PASSED~n"), halt(0);
            false -> io:format("~nPART 3 (timer) FAILED~n"), halt(1)
        end
    catch
        Class:Reason:Stack ->
            io:format("~nPART 3 (timer) CRASHED: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(2)
    end.

%%--------------------------------------------------------------------
load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, ?NODE),
    application:set_env(?TEST_APP, device_definitions_directory, ?DIR ++ "/devices"),
    application:set_env(?TEST_APP, deployment_definitions_directory, ?DIR ++ "/deployments"),
    application:set_env(?TEST_APP, scenario_definitions_directory, ?DIR ++ "/scenarios"),
    application:set_env(?TEST_APP, selected_scenario, rot_tscn),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark rotation timer itest"}, {vsn, "1"},
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

    Device =
        "[{type, rot_dev},\n"
        " {publication_frequency_ms, 150},\n"
        " {payload_bytes_mean, 16.0},\n"
        " {payload_bytes_variance, 0.0},\n"
        " {disconnect_check_period_ms, 0},\n"
        " {disconnect_chance_pct, 0.0},\n"
        " {reconnect_check_period_ms, 0},\n"
        " {reconnect_chance_pct, 0.0}].\n",
    ok = file:write_file(DevDir ++ "/rot_dev.device", Device),

    Deployment =
        "[{name, rot_tdep},\n"
        " {nodes, [{rot_tnode, [{devices, [{rot_dev, 1}]}]}]}].\n",
    ok = file:write_file(DepDir ++ "/rot_t.deployment", Deployment),

    Scenario =
        "[{name, rot_tscn},\n"
        " {duration, {7, seconds}},\n"
        " {protocol, mqttv5},\n"
        " {deployment_name, rot_tdep},\n"
        " {hosts, [{rot_tnode, [{hostname, \"127.0.0.1\"}]}]},\n"
        " {protocol_config,\n"
        "  [{client_interface_module, ps_bench_dap_mqtt_interface},\n"
        "   {broker, \"" ?BROKER_HOST "\"},\n"
        "   {port, " ++ integer_to_list(?BROKER_PORT) ++ "},\n"
        "   {qos, [{default_qos, 0}]},\n"
        "   {purpose_rotation_window_ms, " ++ integer_to_list(?WINDOW_MS) ++ "}]},\n"
        " {metric_config,\n"
        "  [{output_dir, \"" ?DIR "/results\"},\n"
        "   {hw_stats_poll_period_ms, 1000},\n"
        "   {metric_plugins, [{ps_bench_throughput_calc_plugin, erlang}]}]}].\n",
    ok = file:write_file(ScnDir ++ "/rot_t.scenario", Scenario),
    ok.

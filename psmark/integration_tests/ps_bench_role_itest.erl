%% Part 1 integration proof: subscriber-only nodes via {role, subscriber}.
%%
%% Drives the REAL config loader/parser and the REAL scenario_manager lifecycle
%% against a live DAP broker. Proves:
%%   * a subscriber-only node starts NO publisher device clients (device_clients
%%     persistent_term key is absent) and a subscription client only;
%%   * a publisher node keeps the legacy publisher+subscriber behavior, actually
%%     publishes, and (subscribing to itself) actually receives its own data;
%%   * the full lifecycle (initialize -> run -> clean_up) completes for BOTH
%%     roles with no crash / no badkey.
%%
%% Run (broker must be up on BROKER_PORT, DAP build):
%%   erlc -I include -o integration_tests integration_tests/ps_bench_role_itest.erl
%%   erl -noshell -pa _build/default/lib/*/ebin -pa integration_tests \
%%       -eval 'ps_bench_role_itest:main()'
-module(ps_bench_role_itest).

-export([main/0, start/2, stop/1]).

-define(TEST_APP, ps_bench_itest_app).
-define(BROKER_HOST, "127.0.0.1").
-define(BROKER_PORT, 18840).
-define(DIR, "/tmp/ps_bench_itest").

%% application start/2 callback: runs inside the app master so that
%% application:get_application/0 (used by the config manager) resolves.
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

        SubRes = run_node_phase(sub_node, subscriber),
        PubRes = run_node_phase(pub_node, publisher),

        io:format("~n==== PART 1 RESULTS ====~n"),
        print_phase(SubRes),
        print_phase(PubRes),

        Ok = phase_ok(SubRes) andalso phase_ok(PubRes),
        case Ok of
            true  -> io:format("~nPART 1 PASSED~n"), halt(0);
            false -> io:format("~nPART 1 FAILED~n"), halt(1)
        end
    catch
        Class:Reason:Stack ->
            io:format("~nPART 1 CRASHED: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(2)
    end.

%%--------------------------------------------------------------------
%% Per-node phase
%%--------------------------------------------------------------------
run_node_phase(NodeName, ExpectedRole) ->
    application:set_env(?TEST_APP, node_name, NodeName),

    PubBefore = count_pub(NodeName),
    RecvBefore = count_recv(NodeName),

    %% --- initialize (builds clients, connects, subscribes) ---
    ok = ps_bench_scenario_manager:initialize_scenario(),

    HasDev = persistent_term_has(device_clients),
    HasSub = persistent_term_has(subscription_client),
    NumDev = case HasDev of
                 true  -> length(persistent_term:get({ps_bench_scenario_manager, device_clients}));
                 false -> 0
             end,

    %% --- run for the scenario duration; run_scenario auto-stops itself ---
    ps_bench_scenario_manager:run_scenario(),
    {ok, DurationMs} = ps_bench_config_manager:fetch_scenario_duration(),
    timer:sleep(DurationMs + 3000),

    %% --- clean up (tears down whatever client classes this role has) ---
    %% (return value is the persistent_term:erase result, not necessarily 'ok')
    _ = ps_bench_scenario_manager:clean_up_scenario(),
    timer:sleep(300),

    PubDelta = count_pub(NodeName) - PubBefore,
    RecvDelta = count_recv(NodeName) - RecvBefore,

    #{node => NodeName,
      expected_role => ExpectedRole,
      has_device_clients => HasDev,
      has_subscription_client => HasSub,
      num_device_clients => NumDev,
      pub_events => PubDelta,
      recv_events => RecvDelta,
      keys_cleaned => (not persistent_term_has(device_clients))
                      andalso (not persistent_term_has(subscription_client))}.

%% Expectations per role.
phase_ok(#{expected_role := subscriber} = R) ->
    %% subscriber-only: no publishers, has a subscriber, never published,
    %% lifecycle completed (we got here), keys cleaned up.
    (R =:= R)
        andalso (maps:get(has_device_clients, R) =:= false)
        andalso (maps:get(has_subscription_client, R) =:= true)
        andalso (maps:get(num_device_clients, R) =:= 0)
        andalso (maps:get(pub_events, R) =:= 0)
        andalso (maps:get(keys_cleaned, R) =:= true);
phase_ok(#{expected_role := publisher} = R) ->
    %% publisher: has publishers AND a subscriber, actually published, and
    %% (subscribing to itself) actually received -> real end-to-end delivery.
    (maps:get(has_device_clients, R) =:= true)
        andalso (maps:get(has_subscription_client, R) =:= true)
        andalso (maps:get(num_device_clients, R) > 0)
        andalso (maps:get(pub_events, R) > 0)
        andalso (maps:get(recv_events, R) > 0)
        andalso (maps:get(keys_cleaned, R) =:= true).

print_phase(R) ->
    io:format("  node=~p role=~p~n", [maps:get(node, R), maps:get(expected_role, R)]),
    io:format("    has_device_clients=~p num=~p has_subscription_client=~p~n",
              [maps:get(has_device_clients, R), maps:get(num_device_clients, R),
               maps:get(has_subscription_client, R)]),
    io:format("    pub_events=~p recv_events=~p keys_cleaned=~p  => ~s~n",
              [maps:get(pub_events, R), maps:get(recv_events, R),
               maps:get(keys_cleaned, R),
               case phase_ok(R) of true -> "OK"; false -> "FAIL" end]).

%%--------------------------------------------------------------------
%% Store helpers
%%--------------------------------------------------------------------
count_pub(NodeName) ->
    length([E || E <- safe_fetch(fun ps_bench_store:fetch_publish_events/0),
                 element(1, E) =:= NodeName]).

count_recv(NodeName) ->
    length([E || E <- safe_fetch(fun ps_bench_store:fetch_recv_events/0),
                 element(1, E) =:= NodeName]).

safe_fetch(F) ->
    case catch F() of
        L when is_list(L) -> L;
        _ -> []
    end.

persistent_term_has(Key) ->
    case catch persistent_term:get({ps_bench_scenario_manager, Key}) of
        {'EXIT', _} -> false;
        _ -> true
    end.

%%--------------------------------------------------------------------
%% Config setup (real files, parsed by the real loader)
%%--------------------------------------------------------------------
load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, pub_node),
    application:set_env(?TEST_APP, device_definitions_directory, ?DIR ++ "/devices"),
    application:set_env(?TEST_APP, deployment_definitions_directory, ?DIR ++ "/deployments"),
    application:set_env(?TEST_APP, scenario_definitions_directory, ?DIR ++ "/scenarios"),
    application:set_env(?TEST_APP, selected_scenario, role_itest),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark role itest"}, {vsn, "1"},
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
        "[{type, test_sensor},\n"
        " {publication_frequency_ms, 200},\n"
        " {payload_bytes_mean, 64.0},\n"
        " {payload_bytes_variance, 0.0},\n"
        " {disconnect_check_period_ms, 0},\n"
        " {disconnect_chance_pct, 0.0},\n"
        " {reconnect_check_period_ms, 0},\n"
        " {reconnect_chance_pct, 0.0}].\n",
    ok = file:write_file(DevDir ++ "/test_sensor.device", Device),

    Deployment =
        "[{name, role_itest},\n"
        " {nodes,\n"
        "  [{pub_node, [{devices, [{test_sensor, 2}]}]},\n"
        "   {sub_node, [{devices, []}, {role, subscriber}]}]}].\n",
    ok = file:write_file(DepDir ++ "/role_itest.deployment", Deployment),

    Scenario =
        "[{name, role_itest},\n"
        " {duration, {3, seconds}},\n"
        " {protocol, mqttv5},\n"
        " {deployment_name, role_itest},\n"
        " {hosts, [{pub_node, [{hostname, \"127.0.0.1\"}]},\n"
        "          {sub_node, [{hostname, \"127.0.0.1\"}]}]},\n"
        " {protocol_config,\n"
        "  [{client_interface_module, ps_bench_dap_mqtt_interface},\n"
        "   {broker, \"" ?BROKER_HOST "\"},\n"
        "   {port, " ++ integer_to_list(?BROKER_PORT) ++ "},\n"
        "   {qos, [{default_qos, 0}]},\n"
        "   {device_purposes, [{test_sensor, \"quality/assurance\"}]}]},\n"
        " {metric_config,\n"
        "  [{output_dir, \"" ?DIR "/results\"},\n"
        "   {hw_stats_poll_period_ms, 1000},\n"
        "   {metric_plugins, [{ps_bench_throughput_calc_plugin, erlang}]}]}].\n",
    ok = file:write_file(ScnDir ++ "/role_itest.scenario", Scenario),
    ok.

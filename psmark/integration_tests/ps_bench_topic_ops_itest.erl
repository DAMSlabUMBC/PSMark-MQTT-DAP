%% Stock-MQTT baseline (topic-encoded interface) against a plain broker:
%%   * a broadcast operation reaches the subscriber, and its answer reaches the
%%     requester (paper Observation 2, option i);
%%   * the purpose toggle subscribes to and unsubscribes from the toggled purpose's
%%     topic, so data published under that purpose only arrives in added windows.
%%
%% Run (plain broker on 18841):
%%   erlc -I include -o integration_tests integration_tests/ps_bench_topic_ops_itest.erl
%%   erl -noshell -pa _build/default/lib/*/ebin -pa integration_tests \
%%       -eval 'ps_bench_topic_ops_itest:main()'
-module(ps_bench_topic_ops_itest).

-export([main/0, start/2, stop/1]).

-include("ps_bench_config.hrl").

-define(TEST_APP, ps_bench_topic_ops_itest_app).
-define(DIR, "/tmp/ps_bench_topic_ops_itest").
-define(PORT, 18841).
-define(WINDOW_MS, 1500).
-define(N_WINDOWS, 6).
-define(EDGE_GUARD_MS, 300).
-define(T_QA, <<"ps_bench/device/qos_0/qdev">>).
-define(T_TOG, <<"ps_bench/device/qos_0/tdev">>).

start(_Type, _Args) ->
    ok = ps_bench_config_manager:load_config_from_env_vars(),
    {ok, spawn(fun() -> receive stop -> ok end end)}.

stop(_) -> ok.

main() ->
    try
        setup_config_files(),
        load_config_via_app(),
        ok = ps_bench_store:initialize_node_storage(),
        [{ok, _} = ps_bench_topic_encoded_mqtt_interface:start_link(to_scn, C, self()) || C <- [to_sub, to_p1, to_p2]],
        [{ok, _} = gen_server:call(C, connect) || C <- [to_sub, to_p1, to_p2]],
        ok = gen_server:call(to_sub, {subscribe, #{}, [{<<"ps_bench/device/qos_0/#">>, [{qos, 0}]}]}),
        timer:sleep(300),

        OpOk = run_operation(),
        {Added, RemovedHits} = run_toggle(),
        ToggleOk = length(Added) >= 2 andalso RemovedHits =:= 0,

        io:format("~n==== TOPIC-ENCODED RESULTS ====~n"),
        io:format("  broadcast operation answered: ~p~n", [OpOk]),
        io:format("  toggled-purpose data in ~p added windows, ~p deliveries in removed windows (expect 0)~n",
                  [length(Added), RemovedHits]),
        case OpOk andalso ToggleOk of
            true -> io:format("~nTOPIC OPS ITEST PASSED~n"), halt(0);
            false -> io:format("~nTOPIC OPS ITEST FAILED~n"), halt(1)
        end
    catch
        Class:Reason:Stack ->
            io:format("~nTOPIC OPS ITEST CRASHED: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(2)
    end.

run_operation() ->
    {ok, published} = gen_server:call(to_p1, {publish, #{}, ?T_QA, 1, <<"d">>, [{qos, 0}]}),
    timer:sleep(300),
    gen_server:cast(to_p1, {issue_operation, <<"HISTORY">>}),
    case wait_for(fun(Topic, _) -> Topic =:= ?LEGACY_OP_REQUEST_TOPIC end, 2000) of
        {ok, Props} ->
            gen_server:cast(to_sub, {respond_to_op, Props}),
            wait_for(fun(Topic, P) -> Topic =:= <<?LEGACY_OP_RESPONSE_TOPIC/binary, "/to_p1">>
                                          andalso prop(?DAP_KEY_STATUS, P) =:= <<"Success">>
                                          andalso prop(?DAP_KEY_CLIENTID, P) =:= <<"to_sub">> end, 2000) =/= timeout;
        timeout ->
            false
    end.

run_toggle() ->
    drain(),
    align_to_window_boundary(),
    Deadline = erlang:monotonic_time(millisecond) + ?N_WINDOWS * ?WINDOW_MS - ?EDGE_GUARD_MS,
    toggle_loop(Deadline, {[], 0}).

toggle_loop(Deadline, Acc) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true ->
            collect(Acc);
        false ->
            gen_server:cast(to_sub, rotate_purpose),
            timer:sleep(20),
            case in_edge_guard() of
                true -> ok;
                false -> {ok, published} = gen_server:call(to_p2, {publish, #{}, ?T_TOG, 1, <<"d">>, [{qos, 0}]})
            end,
            timer:sleep(100),
            toggle_loop(Deadline, collect(Acc))
    end.

%% Bucket each delivery of the toggled publisher's data by the window it arrived in.
collect({Added, Removed}) ->
    receive
        {publish_recv, {_T, <<"ps_bench/device/qos_0/tdev/", _/binary>>, _P, _Props}, _C} ->
            Now = erlang:system_time(millisecond),
            Win = ps_bench_utils:rotation_index(Now, ?WINDOW_MS),
            case ps_bench_utils:rotation_toggle_added(Now, ?WINDOW_MS) of
                true -> collect({lists:usort([Win | Added]), Removed});
                false -> collect({Added, Removed + 1})
            end;
        _Other ->
            collect({Added, Removed})
    after 0 ->
        {Added, Removed}
    end.

in_edge_guard() ->
    Now = erlang:system_time(millisecond),
    Into = Now rem ?WINDOW_MS,
    Into < ?EDGE_GUARD_MS orelse Into > ?WINDOW_MS - ?EDGE_GUARD_MS.

align_to_window_boundary() ->
    Now = erlang:system_time(millisecond),
    timer:sleep(?WINDOW_MS - (Now rem ?WINDOW_MS) + 5).

wait_for(Pred, TimeoutMs) ->
    receive
        {publish_recv, {_T, Topic, _Payload, Props}, _ClientName} ->
            case Pred(Topic, Props) of
                true -> {ok, Props};
                false -> wait_for(Pred, TimeoutMs)
            end;
        _Other ->
            wait_for(Pred, TimeoutMs)
    after TimeoutMs ->
        timeout
    end.

drain() ->
    receive _ -> drain() after 0 -> ok end.

prop(Key, Props) when is_map(Props) ->
    case lists:keyfind(Key, 1, maps:get('User-Property', Props, [])) of
        {Key, V} -> V;
        false -> undefined
    end;
prop(_Key, _Props) -> undefined.

load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, to_node),
    application:set_env(?TEST_APP, device_definitions_directory, ?DIR ++ "/devices"),
    application:set_env(?TEST_APP, deployment_definitions_directory, ?DIR ++ "/deployments"),
    application:set_env(?TEST_APP, scenario_definitions_directory, ?DIR ++ "/scenarios"),
    application:set_env(?TEST_APP, selected_scenario, to_scn),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark topic ops itest"}, {vsn, "1"},
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
    ok = file:write_file(DevDir ++ "/qdev.device", Dev("qdev")),
    ok = file:write_file(DevDir ++ "/tdev.device", Dev("tdev")),
    ok = file:write_file(DepDir ++ "/to.deployment",
        "[{name, to_dep},\n"
        " {nodes, [{to_node, [{devices, [{qdev, 1}, {tdev, 1}]}, {role, subscriber}]}]}].\n"),
    ok = file:write_file(ScnDir ++ "/to.scenario",
        "[{name, to_scn},\n"
        " {duration, {30, seconds}},\n"
        " {protocol, mqttv5},\n"
        " {deployment_name, to_dep},\n"
        " {hosts, [{to_node, [{hostname, \"127.0.0.1\"}]}]},\n"
        " {protocol_config,\n"
        "  [{client_interface_module, ps_bench_topic_encoded_mqtt_interface},\n"
        "   {broker, \"127.0.0.1\"},\n"
        "   {port, " ++ integer_to_list(?PORT) ++ "},\n"
        "   {qos, [{default_qos, 0}]},\n"
        "   {purpose_rotation_window_ms, " ++ integer_to_list(?WINDOW_MS) ++ "},\n"
        "   {rotation_toggle_purpose, \"maintenance/routine\"},\n"
        "   {device_purposes, [{qdev, \"quality/assurance\"}, {tdev, \"maintenance/predictive\"}]},\n"
        "   {node_subscription_purposes, [{to_node, \"quality/assurance\"}]}]},\n"
        " {metric_config,\n"
        "  [{output_dir, \"" ?DIR "/results\"},\n"
        "   {hw_stats_poll_period_ms, 1000},\n"
        "   {metric_plugins, [{ps_bench_throughput_calc_plugin, erlang}]}]}].\n"),
    ok.

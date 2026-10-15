%% Part 2 integration proof: per-node DAP-SP replaces the hardcoded "*".
%%
%% Drives the REAL ps_bench_dap_mqtt_interface against a live DAP broker. The
%% subscriber runs on node 'runner_maintenance', whose SP (node_subscription_purposes)
%% is the collection "maintenance/predictive|maintenance/routine". Three publishers
%% register message purposes maintenance/{predictive,routine}, maintenance/predictive
%% and quality/assurance and publish data.
%%
%% Proves, under the paper's rule that the MP must permit every purpose of the SP:
%%   * data whose MP permits both purposes of the SP is delivered;
%%   * data whose MP permits only one of them (maintenance/predictive) is not;
%%   * data whose MP permits neither (quality/assurance) is not.
%% The exclusions are only possible if the real per-node SP is on the wire.
%%
%% Run (broker must be up on BROKER_PORT, DAP build):
%%   erlc -I include -o integration_tests integration_tests/ps_bench_sp_itest.erl
%%   erl -noshell -pa _build/default/lib/*/ebin -pa integration_tests \
%%       -eval 'ps_bench_sp_itest:main()'
-module(ps_bench_sp_itest).

-export([main/0, start/2, stop/1]).

-define(TEST_APP, ps_bench_sp_itest_app).
-define(BROKER_HOST, "127.0.0.1").
-define(BROKER_PORT, 18840).
-define(DIR, "/tmp/ps_bench_sp_itest").
-define(SUB_NODE, runner_maintenance).

-define(T_BOTH, <<"ps_bench/device/qos_0/dev_both">>).
-define(T_PRED, <<"ps_bench/device/qos_0/dev_pred">>).
-define(T_QA,   <<"ps_bench/device/qos_0/dev_qa">>).

start(_Type, _Args) ->
    ok = ps_bench_config_manager:load_config_from_env_vars(),
    {ok, spawn(fun() -> receive stop -> ok end end)}.

stop(_) -> ok.

main() ->
    try
        setup_config_files(),
        load_config_via_app(),

        %% Subscriber on runner_maintenance -> multi-filter SP.
        {ok, _} = ps_bench_dap_mqtt_interface:start_link(p2_scn, p2_sub, self()),
        {ok, _} = ps_bench_dap_mqtt_interface:start_link(p2_scn, p2_pub_pred, self()),
        {ok, _} = ps_bench_dap_mqtt_interface:start_link(p2_scn, p2_pub_both, self()),
        {ok, _} = ps_bench_dap_mqtt_interface:start_link(p2_scn, p2_pub_qa, self()),

        {ok, _} = gen_server:call(p2_sub, connect),
        {ok, _} = gen_server:call(p2_pub_pred, connect),
        {ok, _} = gen_server:call(p2_pub_both, connect),
        {ok, _} = gen_server:call(p2_pub_qa, connect),
        timer:sleep(300),

        %% Subscribe with the per-node SP (handler injects DAP-SP from state).
        ok = gen_server:call(p2_sub, {subscribe, #{}, [{<<"ps_bench/device/qos_0/#">>, [{qos, 0}]}]}),
        timer:sleep(400),

        %% Publishers register MP (from device_purposes) and publish.
        publish(p2_pub_both, ?T_BOTH),
        publish(p2_pub_pred, ?T_PRED),
        publish(p2_pub_qa,   ?T_QA),
        timer:sleep(800),

        Recv = drain_recv([]),
        GotBoth = lists:member(?T_BOTH, Recv),
        GotPred = lists:member(?T_PRED, Recv),
        GotQA   = lists:member(?T_QA, Recv),

        io:format("~n==== PART 2 RESULTS ====~n"),
        io:format("  subscriber node=~p  SP=~p~n",
                  [?SUB_NODE, <<"maintenance/predictive|maintenance/routine">>]),
        io:format("  received topics: ~p~n", [Recv]),
        io:format("  maintenance/{predictive,routine} delivered (permits both): ~p~n", [GotBoth]),
        io:format("  maintenance/predictive           delivered (permits one): ~p (expect false)~n", [GotPred]),
        io:format("  quality/assurance                delivered (permits none): ~p (expect false)~n", [GotQA]),

        Ok = GotBoth andalso (not GotPred) andalso (not GotQA),
        case Ok of
            true  -> io:format("~nPART 2 PASSED~n"), halt(0);
            false -> io:format("~nPART 2 FAILED~n"), halt(1)
        end
    catch
        Class:Reason:Stack ->
            io:format("~nPART 2 CRASHED: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(2)
    end.

publish(Client, Topic) ->
    {ok, published} = gen_server:call(Client, {publish, #{}, Topic, 1, <<"payload">>, [{qos, 0}]}),
    ok.

drain_recv(Acc) ->
    receive
        {publish_recv, {_TimeNs, Topic, _Payload, _Props}, _ClientName} ->
            drain_recv([Topic | Acc]);
        _Other ->
            drain_recv(Acc)
    after 200 ->
        lists:reverse(Acc)
    end.

%%--------------------------------------------------------------------
load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, ?SUB_NODE),
    application:set_env(?TEST_APP, device_definitions_directory, ?DIR ++ "/devices"),
    application:set_env(?TEST_APP, deployment_definitions_directory, ?DIR ++ "/deployments"),
    application:set_env(?TEST_APP, scenario_definitions_directory, ?DIR ++ "/scenarios"),
    application:set_env(?TEST_APP, selected_scenario, p2_scn),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark sp itest"}, {vsn, "1"},
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
        " {publication_frequency_ms, 1000},\n"
        " {payload_bytes_mean, 32.0},\n"
        " {payload_bytes_variance, 0.0},\n"
        " {disconnect_check_period_ms, 0},\n"
        " {disconnect_chance_pct, 0.0},\n"
        " {reconnect_check_period_ms, 0},\n"
        " {reconnect_chance_pct, 0.0}].\n"
    end,
    ok = file:write_file(DevDir ++ "/dev_pred.device", Dev("dev_pred")),
    ok = file:write_file(DevDir ++ "/dev_both.device", Dev("dev_both")),
    ok = file:write_file(DevDir ++ "/dev_qa.device", Dev("dev_qa")),

    Deployment =
        "[{name, p2_dep},\n"
        " {nodes, [{runner_maintenance, [{devices, [{dev_pred, 1}]}]}]}].\n",
    ok = file:write_file(DepDir ++ "/p2.deployment", Deployment),

    Scenario =
        "[{name, p2_scn},\n"
        " {duration, {3, seconds}},\n"
        " {protocol, mqttv5},\n"
        " {deployment_name, p2_dep},\n"
        " {hosts, [{runner_maintenance, [{hostname, \"127.0.0.1\"}]}]},\n"
        " {protocol_config,\n"
        "  [{client_interface_module, ps_bench_dap_mqtt_interface},\n"
        "   {broker, \"" ?BROKER_HOST "\"},\n"
        "   {port, " ++ integer_to_list(?BROKER_PORT) ++ "},\n"
        "   {qos, [{default_qos, 0}]},\n"
        "   {device_purposes, [{dev_both, \"maintenance/{predictive,routine}\"},\n"
        "                      {dev_pred, \"maintenance/predictive\"},\n"
        "                      {dev_qa,   \"quality/assurance\"}]},\n"
        "   {node_subscription_purposes,\n"
        "    [{runner_maintenance, \"maintenance/predictive|maintenance/routine\"}]}]},\n"
        " {metric_config,\n"
        "  [{output_dir, \"" ?DIR "/results\"},\n"
        "   {hw_stats_poll_period_ms, 1000},\n"
        "   {metric_plugins, [{ps_bench_throughput_calc_plugin, erlang}]}]}].\n",
    ok = file:write_file(ScnDir ++ "/p2.scenario", Scenario),
    ok.

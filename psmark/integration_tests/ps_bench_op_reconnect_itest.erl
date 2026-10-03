%% An operation's final notification reaches a requester that was disconnected when
%% the broker sent it (paper 6.2: requests and notifications are delivered at least
%% once). The requester issues a HISTORY, disconnects before the subscriber answers,
%% and reconnects; the broker's Success must arrive after the reconnect.
%%
%% Run (DAP broker with use_metadata_operation_support on 18840):
%%   erlc -I include -o integration_tests integration_tests/ps_bench_op_reconnect_itest.erl
%%   erl -noshell -pa _build/default/lib/*/ebin -pa integration_tests \
%%       -eval 'ps_bench_op_reconnect_itest:main()'
-module(ps_bench_op_reconnect_itest).

-export([main/0, start/2, stop/1]).

-define(TEST_APP, ps_bench_op_reconnect_itest_app).
-define(DIR, "/tmp/ps_bench_op_reconnect_itest").
-define(DAP_PORT, 18840).
-define(TOPIC, <<"ps_bench/device/qos_0/rcdev">>).

start(_Type, _Args) ->
    ok = ps_bench_config_manager:load_config_from_env_vars(),
    {ok, spawn(fun() -> receive stop -> ok end end)}.

stop(_) -> ok.

main() ->
    try
        setup_config_files(),
        load_config_via_app(),
        ok = ps_bench_store:initialize_node_storage(),
        Result = run(),
        io:format("~n==== RESULTS ====~n~p~n", [Result]),
        case maps:get(ok, Result) of
            true -> io:format("~nOP RECONNECT ITEST PASSED~n"), halt(0);
            false -> io:format("~nOP RECONNECT ITEST FAILED~n"), halt(1)
        end
    catch
        Class:Reason:Stack ->
            io:format("~nOP RECONNECT ITEST CRASHED: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(2)
    end.

run() ->
    {ok, _} = ps_bench_dap_mqtt_interface:start_link(rc_scn, rc_sub, self()),
    {ok, _} = ps_bench_dap_mqtt_interface:start_link(rc_scn, rc_pub, self()),
    {ok, _} = gen_server:call(rc_sub, connect),
    {ok, _} = gen_server:call(rc_pub, connect),
    ok = gen_server:call(rc_sub, {subscribe, #{}, [{<<"ps_bench/device/qos_0/#">>, [{qos, 0}]}]}),
    timer:sleep(300),

    {ok, published} = gen_server:call(rc_pub, {publish, #{}, ?TOPIC, 1, <<"data">>, [{qos, 0}]}),
    timer:sleep(300),

    gen_server:cast(rc_pub, {issue_operation, <<"HISTORY">>}),
    {ok, ReqProps} = wait_for(fun(Topic, _Props) -> is_prefix(<<"OP_REQ/">>, Topic) end, 2000),
    {ok, _} = wait_for(fun(Topic, Props) -> is_prefix(<<"OP_NOTIF/">>, Topic)
                                            andalso prop(<<"DAP-Status">>, Props) =:= <<"Pending">> end, 2000),

    %% The requester goes away; only then does the subscriber answer.
    ok = gen_server:call(rc_pub, disconnect),
    timer:sleep(200),
    gen_server:cast(rc_sub, {respond_to_op, ReqProps}),
    timer:sleep(500),

    {ok, _} = gen_server:call(rc_pub, reconnect),
    Final = wait_for(fun(Topic, Props) -> is_prefix(<<"OP_NOTIF/">>, Topic)
                                          andalso prop(<<"DAP-ClientID">>, Props) =:= <<"Broker">>
                                          andalso prop(<<"DAP-Status">>, Props) =:= <<"Success">> end, 2000),
    catch gen_server:call(rc_pub, disconnect),
    catch gen_server:call(rc_sub, disconnect),
    #{final_after_reconnect => Final =/= timeout, ok => Final =/= timeout}.

%% The properties of the first message matching Pred, or timeout.
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

is_prefix(Prefix, Bin) ->
    binary:longest_common_prefix([Prefix, Bin]) =:= byte_size(Prefix).

prop(Key, Props) when is_map(Props) ->
    case lists:keyfind(Key, 1, maps:get('User-Property', Props, [])) of
        {Key, V} -> V;
        false -> undefined
    end;
prop(_Key, _Props) -> undefined.

load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, rc_node),
    application:set_env(?TEST_APP, device_definitions_directory, ?DIR ++ "/devices"),
    application:set_env(?TEST_APP, deployment_definitions_directory, ?DIR ++ "/deployments"),
    application:set_env(?TEST_APP, scenario_definitions_directory, ?DIR ++ "/scenarios"),
    application:set_env(?TEST_APP, selected_scenario, rc_scn),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark op reconnect itest"}, {vsn, "1"},
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
    ok = file:write_file(DevDir ++ "/rcdev.device",
        "[{type, rcdev},\n"
        " {publication_frequency_ms, 500},\n"
        " {payload_bytes_mean, 16.0},\n"
        " {payload_bytes_variance, 0.0},\n"
        " {disconnect_check_period_ms, 0},\n"
        " {disconnect_chance_pct, 0.0},\n"
        " {reconnect_check_period_ms, 0},\n"
        " {reconnect_chance_pct, 0.0}].\n"),
    ok = file:write_file(DepDir ++ "/rc.deployment",
        "[{name, rc_dep},\n"
        " {nodes, [{rc_node, [{devices, [{rcdev, 1}]}]}]}].\n"),
    ok = file:write_file(ScnDir ++ "/rc.scenario",
        "[{name, rc_scn},\n"
        " {duration, {30, seconds}},\n"
        " {protocol, mqttv5},\n"
        " {deployment_name, rc_dep},\n"
        " {hosts, [{rc_node, [{hostname, \"127.0.0.1\"}]}]},\n"
        " {protocol_config,\n"
        "  [{client_interface_module, ps_bench_dap_mqtt_interface},\n"
        "   {broker, \"127.0.0.1\"},\n"
        "   {port, " ++ integer_to_list(?DAP_PORT) ++ "},\n"
        "   {qos, [{default_qos, 0}]},\n"
        "   {device_purposes, [{rcdev, \"quality/assurance\"}]},\n"
        "   {node_subscription_purposes, [{rc_node, \"quality/assurance\"}]}]},\n"
        " {metric_config,\n"
        "  [{output_dir, \"" ?DIR "/results\"},\n"
        "   {hw_stats_poll_period_ms, 1000},\n"
        "   {metric_plugins, [{ps_bench_throughput_calc_plugin, erlang}]}]}].\n"),
    ok.

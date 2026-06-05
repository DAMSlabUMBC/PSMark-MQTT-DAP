%% Floors x devices topology load proof.
%%
%% Loads the committed floor topology templates through the REAL config loader
%% (ps_bench_config_manager:load_config_from_env_vars/0 -> load_config/3) and
%% asserts, for each of Christian's four cells, that the deployment maps onto
%% floors-as-nodes with the right per-floor device count AND a subscriber-only
%% node:
%%
%%   floor_40x1 -> 1 floor node (40 devices) + 1 subscriber node
%%   floor_80x1 -> 1 floor node (80 devices) + 1 subscriber node
%%   floor_40x2 -> 2 floor nodes (40 devices each) + 1 subscriber node
%%   floor_80x2 -> 2 floor nodes (80 devices each) + 1 subscriber node
%%
%% Each floor node defaults to the publisher role; the aggregator carries
%% {role, subscriber}, exercising the Part 1 role plumbing end to end through the
%% real loader. No broker needed: this is a config-parse + structure proof.
%%
%% Run:
%%   erlc -I include -o integration_tests integration_tests/ps_bench_topology_itest.erl
%%   erl -noshell -pa _build/default/lib/*/ebin -pa integration_tests \
%%       -eval 'ps_bench_topology_itest:main()'
-module(ps_bench_topology_itest).

-export([main/0, start/2, stop/1, check_cell/1, floor_sensor_count/1]).

-define(TEST_APP, ps_bench_topology_itest_app).
-define(ROOT, "/home/nsamson/mqtt-dap-paper/PSMark-MQTT-DAP/psmark/configs/builtin-test-suites").

start(_Type, _Args) ->
    ok = ps_bench_config_manager:load_config_from_env_vars(),
    {ok, spawn(fun() -> receive stop -> ok end end)}.

stop(_) -> ok.

main() ->
    try
        load_config_via_app(),

        Cells = [
            {floor_40x1, [floor1],          40, [floor1, aggregator]},
            {floor_80x1, [floor1],          80, [floor1, aggregator]},
            {floor_40x2, [floor1, floor2],  40, [floor1, floor2, aggregator]},
            {floor_80x2, [floor1, floor2],  80, [floor1, floor2, aggregator]}
        ],

        io:format("~n==== FLOORS x DEVICES TOPOLOGY RESULTS ====~n"),
        Results = [check_cell(C) || C <- Cells],

        case lists:all(fun(X) -> X end, Results) of
            true  -> io:format("~nTOPOLOGY PASSED~n"), halt(0);
            false -> io:format("~nTOPOLOGY FAILED~n"), halt(1)
        end
    catch
        Class:Reason:Stack ->
            io:format("~nTOPOLOGY CRASHED: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(2)
    end.

check_cell({Scenario, FloorNodes, DevPerFloor, ExpectedNodes}) ->
    application:set_env(?TEST_APP, selected_scenario, Scenario),

    {ok, Nodes} = ps_bench_config_manager:fetch_node_name_list(),
    NodesOk = lists:sort(Nodes) =:= lists:sort(ExpectedNodes),

    %% Each floor node defaults to publisher and carries DevPerFloor floor_sensors.
    FloorOk = lists:all(
        fun(FN) ->
            {ok, publisher} =:= ps_bench_config_manager:fetch_role_for_node(FN)
            andalso floor_sensor_count(FN) =:= DevPerFloor
        end, FloorNodes),

    %% The aggregator is a subscriber-only node with no publisher devices.
    {ok, AggRole} = ps_bench_config_manager:fetch_role_for_node(aggregator),
    AggOk = (AggRole =:= subscriber) andalso (floor_sensor_count(aggregator) =:= 0),

    Ok = NodesOk andalso FloorOk andalso AggOk,
    io:format("  ~p: nodes=~p (~p floor node(s) x ~p devices + subscriber aggregator) -> ~p~n",
              [Scenario, lists:sort(Nodes), length(FloorNodes), DevPerFloor, Ok]),
    Ok.

floor_sensor_count(NodeName) ->
    case ps_bench_config_manager:fetch_devices_for_node(NodeName) of
        {ok, Devices} ->
            lists:sum([Count || {floor_sensor, Count} <- Devices]);
        _ ->
            0
    end.

%%--------------------------------------------------------------------
load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, floor1),
    application:set_env(?TEST_APP, device_definitions_directory, ?ROOT ++ "/devices/floors"),
    application:set_env(?TEST_APP, deployment_definitions_directory, ?ROOT ++ "/deployments/floors"),
    application:set_env(?TEST_APP, scenario_definitions_directory, ?ROOT ++ "/testcases/floors"),
    application:set_env(?TEST_APP, selected_scenario, floor_40x1),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark topology itest"}, {vsn, "1"},
                {modules, [?MODULE]}, {registered, []},
                {applications, [kernel, stdlib]},
                {mod, {?MODULE, []}}]},
    ok = application:load(AppSpec),
    ok = application:start(?TEST_APP).

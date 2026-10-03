%% Floors x devices topology load proof.
%%
%% Loads the committed floor deployments through the REAL config loader
%% (ps_bench_config_manager:load_config_from_env_vars/0 -> load_config/3), via the
%% steady-state DAP cell of each scaling, and asserts the paper topology (SS7.3):
%%
%%   dap_static_base    -> floor_40x1: 1 floor node (40 devices)
%%   dap_static_2xdev   -> floor_80x1: 1 floor node (80 devices)
%%   dap_static_2srv    -> floor_40x2: 2 floor nodes (40 devices each)
%%   dap_static_2srv_2x -> floor_80x2: 2 floor nodes (80 devices each)
%%
%% plus, in every cell, the six subscriber-only nodes, one per purpose. Floor nodes
%% only publish ({role, publisher_only}), so DAP and the topic-encoded baselines
%% measure the same six subscribers. No broker needed: config parse + structure.
%%
%% Run:
%%   erlc -I include -o integration_tests integration_tests/ps_bench_topology_itest.erl
%%   erl -noshell -pa _build/default/lib/*/ebin -pa integration_tests \
%%       -eval 'ps_bench_topology_itest:main()'
-module(ps_bench_topology_itest).

-export([main/0, start/2, stop/1, check_cell/1, device_count/1]).

-define(TEST_APP, ps_bench_topology_itest_app).
-define(SUBSCRIBERS, [sub_quality_assurance, sub_maintenance_predictive, sub_maintenance_routine,
                      sub_operations_forecast, sub_regulatory_reporting, sub_vendor_maintenance]).

start(_Type, _Args) ->
    ok = ps_bench_config_manager:load_config_from_env_vars(),
    {ok, spawn(fun() -> receive stop -> ok end end)}.

stop(_) -> ok.

main() ->
    try
        load_config_via_app(),

        Cells = [
            {dap_static_base,    [floor1],          40},
            {dap_static_2xdev,   [floor1],          80},
            {dap_static_2srv,    [floor1, floor2],  40},
            {dap_static_2srv_2x, [floor1, floor2],  80}
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

check_cell({Scenario, FloorNodes, DevPerFloor}) ->
    application:set_env(?TEST_APP, selected_scenario, Scenario),

    {ok, Nodes} = ps_bench_config_manager:fetch_node_name_list(),
    NodesOk = lists:sort(Nodes) =:= lists:sort(FloorNodes ++ ?SUBSCRIBERS),

    %% Each floor node only publishes and carries DevPerFloor devices.
    FloorOk = lists:all(
        fun(FN) ->
            {ok, publisher_only} =:= ps_bench_config_manager:fetch_role_for_node(FN)
            andalso device_count(FN) =:= DevPerFloor
        end, FloorNodes),

    %% Each per-purpose node is subscriber-only with no publisher devices.
    SubOk = lists:all(
        fun(SN) ->
            {ok, subscriber} =:= ps_bench_config_manager:fetch_role_for_node(SN)
            andalso device_count(SN) =:= 0
        end, ?SUBSCRIBERS),

    Ok = NodesOk andalso FloorOk andalso SubOk,
    io:format("  ~p: ~p floor node(s) x ~p devices, publisher-only; ~p subscriber nodes -> ~p~n",
              [Scenario, length(FloorNodes), DevPerFloor, length(?SUBSCRIBERS), Ok]),
    Ok.

device_count(NodeName) ->
    case ps_bench_config_manager:fetch_devices_for_node(NodeName) of
        {ok, Devices} ->
            lists:sum([Count || {_Type, Count} <- Devices]);
        _ ->
            0
    end.

%% The committed test suites, relative to this module's directory.
root() ->
    filename:join([filename:dirname(code:which(?MODULE)), "..", "configs", "builtin-test-suites"]).

%%--------------------------------------------------------------------
load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, floor1),
    application:set_env(?TEST_APP, device_definitions_directory, filename:join(root(), "devices")),
    application:set_env(?TEST_APP, deployment_definitions_directory, filename:join(root(), "deployments")),
    application:set_env(?TEST_APP, scenario_definitions_directory, filename:join([root(), "testcases", "experiments"])),
    application:set_env(?TEST_APP, selected_scenario, dap_static_base),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark topology itest"}, {vsn, "1"},
                {modules, [?MODULE]}, {registered, []},
                {applications, [kernel, stdlib]},
                {mod, {?MODULE, []}}]},
    ok = application:load(AppSpec),
    ok = application:start(?TEST_APP).

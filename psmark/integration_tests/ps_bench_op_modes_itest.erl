%% Operation ISSUER-MODE and OP-GROUP selection integration proof.
%%
%% Drives the REAL scenario_manager op-issue path (issue_ops_from_publishers/0)
%% against a live DAP broker, with two publishers (distinct topic/purpose) and
%% one subscriber running the full adapter+interface stack. We observe the
%% OP_NOTIF the broker relays back to each requester (which carries the
%% requester's own client-id in the topic and the DAP-OpType in a user-property)
%% to prove, end to end:
%%
%%   * all_publishers mode  -> BOTH publishers issue (2 distinct requesters);
%%   * single_random mode   -> exactly ONE publisher issues (1 requester), still
%%                             scoped to its own data;
%%   * op_group=no_queue    -> only AUDIT/HISTORY/UPDATE op types are issued
%%                             (never DELETE/RESTRICT);
%%   * op_group=queue       -> only DELETE/RESTRICT op types are issued
%%                             (never AUDIT/HISTORY/UPDATE).
%%
%% A requester only gets an OP_NOTIF when the subscriber that received its data
%% responded, so a non-empty result also confirms own-data scoping still routes
%% the operation to the right subscriber in both issuer modes.
%%
%% Run (DAP broker on DAP_PORT):
%%   erlc -I include -o integration_tests integration_tests/ps_bench_op_modes_itest.erl
%%   erl -noshell -pa _build/default/lib/*/ebin -pa integration_tests \
%%       -eval 'ps_bench_op_modes_itest:main()'
-module(ps_bench_op_modes_itest).

-export([main/0, start/2, stop/1]).

-define(TEST_APP, ps_bench_op_modes_itest_app).
-define(DIR, "/tmp/ps_bench_op_modes_itest").
-define(DAP_PORT, 18840).
-define(SM, ps_bench_scenario_manager).

-define(NO_QUEUE, [<<"AUDIT">>, <<"HISTORY">>, <<"UPDATE">>]).
-define(QUEUE,    [<<"DELETE">>, <<"RESTRICT">>]).

start(_Type, _Args) ->
    ok = ps_bench_config_manager:load_config_from_env_vars(),
    {ok, spawn(fun() -> receive stop -> ok end end)}.

stop(_) -> ok.

main() ->
    try
        setup_config_files(),
        load_config_via_app(),
        ok = ps_bench_store:initialize_node_storage(),

        %% (1) all_publishers + no_queue group
        R1 = run("op_all_nq_scn", all_nq),
        %% (2) single_random + no_queue group
        R2 = run("op_single_nq_scn", single_nq),
        %% (3) all_publishers + queue group
        R3 = run("op_all_q_scn", all_q),

        #{requesters := Req1, optypes := Ops1} = R1,
        #{requesters := Req2, optypes := Ops2} = R2,
        #{requesters := Req3, optypes := Ops3} = R3,

        io:format("~n==== OP ISSUER-MODE / OP-GROUP RESULTS ====~n"),
        io:format("  (1) all_publishers + no_queue:  requesters=~p  optypes=~p~n",
                  [lists:sort(Req1), lists:sort(Ops1)]),
        io:format("  (2) single_random + no_queue:   requesters=~p  optypes=~p~n",
                  [lists:sort(Req2), lists:sort(Ops2)]),
        io:format("  (3) all_publishers + queue:     requesters=~p  optypes=~p~n",
                  [lists:sort(Req3), lists:sort(Ops3)]),

        AllPubOk = length(Req1) =:= 2,
        SingleOk = length(Req2) =:= 1,
        NoQueueGroupOk = (Ops1 =/= []) andalso subset(Ops1, ?NO_QUEUE)
                         andalso disjoint(Ops1, ?QUEUE),
        QueueGroupOk = (Ops3 =/= []) andalso subset(Ops3, ?QUEUE)
                       andalso disjoint(Ops3, ?NO_QUEUE),

        io:format("~n  all_publishers issued from BOTH publishers (2 requesters): ~p~n", [AllPubOk]),
        io:format("  single_random issued from exactly ONE publisher: ~p~n", [SingleOk]),
        io:format("  no_queue group issued only AUDIT/HISTORY/UPDATE: ~p~n", [NoQueueGroupOk]),
        io:format("  queue group issued only DELETE/RESTRICT: ~p~n", [QueueGroupOk]),

        case AllPubOk andalso SingleOk andalso NoQueueGroupOk andalso QueueGroupOk of
            true  -> io:format("~nOP MODES PASSED~n"), halt(0);
            false -> io:format("~nOP MODES FAILED~n"), halt(1)
        end
    catch
        Class:Reason:Stack ->
            io:format("~nOP MODES CRASHED: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(2)
    end.

%%--------------------------------------------------------------------
%% One run: stand up sub + 2 pubs, publish data, drive issue_ops_from_publishers,
%% and collect the OP_NOTIF requester-ids and op types.
%%--------------------------------------------------------------------
run(Scenario, Tag) ->
    application:set_env(?TEST_APP, selected_scenario, list_to_atom(Scenario)),
    application:set_env(?TEST_APP, node_name, opm_node),
    drain(),
    persistent_term:erase({?SM, op_issuer}),

    ScnAtom = list_to_atom(Scenario),
    Suffix = atom_to_list(Tag),
    P1 = list_to_atom("opm_pub1_" ++ Suffix),
    P2 = list_to_atom("opm_pub2_" ++ Suffix),
    SubName = "opm_sub_" ++ Suffix,
    T1 = <<"ps_bench/device/qos_0/opmdev1">>,
    T2 = <<"ps_bench/device/qos_0/opmdev2">>,

    %% Subscriber: full adapter + interface stack so OP_REQ routing + responses
    %% run through the production path.
    {ok, SubAdapter} = ps_bench_erlang_mqtt_adapter:start_link(ScnAtom, ps_bench_dap_mqtt_interface, SubName, subscriber),
    ok = gen_server:call(SubAdapter, connect),
    ok = gen_server:call(SubAdapter, subscribe),
    timer:sleep(300),

    %% Two publishers (direct interface so this harness owns their OP_NOTIF inbox).
    {ok, Pid1} = ps_bench_dap_mqtt_interface:start_link(ScnAtom, P1, self()),
    {ok, Pid2} = ps_bench_dap_mqtt_interface:start_link(ScnAtom, P2, self()),
    {ok, _} = gen_server:call(P1, connect),
    {ok, _} = gen_server:call(P2, connect),
    timer:sleep(200),

    %% Each publishes data on its own topic so the subscriber becomes relevant.
    {ok, published} = gen_server:call(P1, {publish, #{}, T1, 1, <<"d1">>, [{qos, 0}]}),
    {ok, published} = gen_server:call(P2, {publish, #{}, T2, 1, <<"d2">>, [{qos, 0}]}),
    timer:sleep(500),

    %% Wire the publishers in as the scenario manager's device clients and drive
    %% the REAL op-issue path (reads issuer mode + op group from the config).
    persistent_term:put({?SM, node_role}, publisher),
    persistent_term:put({?SM, device_clients}, [{atom_to_list(P1), Pid1}, {atom_to_list(P2), Pid2}]),
    ok = ps_bench_scenario_manager:issue_ops_from_publishers(),
    timer:sleep(1500),

    {Requesters, OpTypes} = collect_notifs([], []),

    catch gen_server:call(P1, disconnect),
    catch gen_server:call(P2, disconnect),
    catch gen_server:call(SubAdapter, disconnect),
    persistent_term:erase({?SM, device_clients}),
    persistent_term:erase({?SM, op_issuer}),
    timer:sleep(200),

    #{requesters => lists:usort(Requesters), optypes => lists:usort(OpTypes)}.

%% Collect OP_NOTIF messages: the topic tail is the requester client-id, and the
%% DAP-OpType user-property is the op the requester issued.
collect_notifs(Reqs, Ops) ->
    receive
        {publish_recv, {_T, Topic, _P, Props}, _CN} ->
            case Topic of
                <<"OP_NOTIF/", Requester/binary>> ->
                    OpType = prop(<<"DAP-OpType">>, Props),
                    Ops1 = case OpType of undefined -> Ops; _ -> [OpType | Ops] end,
                    collect_notifs([Requester | Reqs], Ops1);
                _ ->
                    collect_notifs(Reqs, Ops)
            end;
        _Other ->
            collect_notifs(Reqs, Ops)
    after 200 ->
        {Reqs, Ops}
    end.

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

subset(Xs, Universe) -> lists:all(fun(X) -> lists:member(X, Universe) end, Xs).
disjoint(Xs, Ys) -> not lists:any(fun(X) -> lists:member(X, Ys) end, Xs).

drain() ->
    receive _ -> drain() after 0 -> ok end.

%%--------------------------------------------------------------------
load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, opm_node),
    application:set_env(?TEST_APP, device_definitions_directory, ?DIR ++ "/devices"),
    application:set_env(?TEST_APP, deployment_definitions_directory, ?DIR ++ "/deployments"),
    application:set_env(?TEST_APP, scenario_definitions_directory, ?DIR ++ "/scenarios"),
    application:set_env(?TEST_APP, selected_scenario, op_all_nq_scn),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark op modes itest"}, {vsn, "1"},
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
    ok = file:write_file(DevDir ++ "/opmdev1.device", Dev("opmdev1")),
    ok = file:write_file(DevDir ++ "/opmdev2.device", Dev("opmdev2")),

    Deployment =
        "[{name, opm_dep},\n"
        " {nodes, [{opm_node, [{devices, [{opmdev1, 1}, {opmdev2, 1}]}]}]}].\n",
    ok = file:write_file(DepDir ++ "/opm.deployment", Deployment),

    Scn = fun(Name, IssuerMode, OpGroup) ->
        "[{name, " ++ Name ++ "},\n"
        " {duration, {30, seconds}},\n"
        " {protocol, mqttv5},\n"
        " {deployment_name, opm_dep},\n"
        " {hosts, [{opm_node, [{hostname, \"127.0.0.1\"}]}]},\n"
        " {protocol_config,\n"
        "  [{client_interface_module, ps_bench_dap_mqtt_interface},\n"
        "   {broker, \"127.0.0.1\"},\n"
        "   {port, " ++ integer_to_list(?DAP_PORT) ++ "},\n"
        "   {qos, [{default_qos, 0}]},\n"
        "   {op_issuer_mode, " ++ IssuerMode ++ "},\n"
        "   {op_group, " ++ OpGroup ++ "},\n"
        "   {device_purposes, [{opmdev1, \"quality/assurance\"},\n"
        "                      {opmdev2, \"maintenance/predictive\"}]}]},\n"
        " {metric_config,\n"
        "  [{output_dir, \"" ?DIR "/results\"},\n"
        "   {hw_stats_poll_period_ms, 1000},\n"
        "   {metric_plugins, [{ps_bench_throughput_calc_plugin, erlang}]}]}].\n"
    end,
    ok = file:write_file(ScnDir ++ "/op_all_nq.scenario",    Scn("op_all_nq_scn",    "all_publishers", "no_queue")),
    ok = file:write_file(ScnDir ++ "/op_single_nq.scenario", Scn("op_single_nq_scn", "single_random",  "no_queue")),
    ok = file:write_file(ScnDir ++ "/op_all_q.scenario",     Scn("op_all_q_scn",     "all_publishers", "queue")),
    ok.

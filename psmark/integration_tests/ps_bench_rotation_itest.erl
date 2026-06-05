%% Part 3 integration proof: dynamic purpose rotation in lockstep.
%%
%% Drives the REAL ps_bench_dap_mqtt_interface against a live DAP broker with
%% rotation enabled (purpose_rotation_window_ms > 0). A publisher publishes on a
%% single fixed topic; its MP rotates through the placeholder purpose set as the
%% clock advances. A subscriber subscribes to that topic.
%%
%% Two runs, identical except for one thing - whether the subscriber receives the
%% rotation ticks that re-subscribe its SP:
%%
%%   LOCKSTEP  (subscriber DOES rotate): publisher MP and subscriber SP advance
%%             together, so delivery CONTINUES across every window -> all rotation
%%             purposes are observed in the delivered data.
%%   DESYNC    (subscriber does NOT rotate): the publisher rotates away from the
%%             subscriber's stuck SP, so delivery DROPS to zero in every window
%%             except the one the SP is stuck on -> only ONE purpose is observed.
%%
%% The contrast proves (a) the rotation actually changes the active purpose
%% (else desync would not drop) and (b) lockstep is what preserves delivery.
%%
%% Run (broker must be up on BROKER_PORT, DAP build):
%%   erlc -I include -o integration_tests integration_tests/ps_bench_rotation_itest.erl
%%   erl -noshell -pa _build/default/lib/*/ebin -pa integration_tests \
%%       -eval 'ps_bench_rotation_itest:main()'
-module(ps_bench_rotation_itest).

-export([main/0, start/2, stop/1]).

-define(TEST_APP, ps_bench_rotation_itest_app).
-define(BROKER_HOST, "127.0.0.1").
-define(BROKER_PORT, 18840).
-define(DIR, "/tmp/ps_bench_rotation_itest").

-define(WINDOW_MS, 1500).
-define(N_WINDOWS, 4).
-define(PUB_EVERY_MS, 120).
%% Stop publishing this many ms before the final boundary so the last sample
%% never tips into a partial extra window.
-define(EDGE_GUARD_MS, 120).
-define(TOPIC, <<"ps_bench/device/qos_0/rot_dev">>).
%% Mirror of ?PLACEHOLDER_ROTATION_PURPOSES in the interface.
-define(ROT_SET, [<<"maintenance/predictive">>, <<"maintenance/routine">>, <<"quality/assurance">>]).

start(_Type, _Args) ->
    ok = ps_bench_config_manager:load_config_from_env_vars(),
    {ok, spawn(fun() -> receive stop -> ok end end)}.

stop(_) -> ok.

main() ->
    try
        setup_config_files(),
        load_config_via_app(),

        Lock = run_phase(lockstep, true),
        Desync = run_phase(desync, false),

        #{pub_windows := LPub, recv_windows := LRecv, by_purpose := LByP} = Lock,
        #{pub_windows := DPub, recv_windows := DRecv, by_purpose := DByP} = Desync,

        %% Windows that had publishes but ZERO delivery = where delivery dropped.
        LockDropped = LPub -- LRecv,
        DesyncDropped = DPub -- DRecv,

        io:format("~n==== PART 3 RESULTS ====~n"),
        io:format("  rotation set: ~p~n", [?ROT_SET]),
        io:format("  window=~pms  windows=~p~n", [?WINDOW_MS, ?N_WINDOWS]),
        io:format("  LOCKSTEP (sub rotates):~n"),
        io:format("    windows published in=~p  windows delivered in=~p  dropped=~p~n",
                  [length(LPub), length(LRecv), LockDropped]),
        io:format("    delivered-by-purpose=~p~n", [LByP]),
        io:format("  DESYNC (sub stuck on SP):~n"),
        io:format("    windows published in=~p  windows delivered in=~p  dropped=~p~n",
                  [length(DPub), length(DRecv), DesyncDropped]),
        io:format("    delivered-by-purpose=~p~n", [DByP]),

        %% LOCKSTEP: delivery in EVERY window that had a publish, spanning >= 3
        %% windows (>= 2-3 rotations).
        LockOk = (length(LPub) >= 3) andalso (LockDropped =:= []),
        %% DESYNC: at least one window had publishes but ZERO delivery -> the
        %% drop the test must catch.
        DesyncOk = (DesyncDropped =/= []),

        io:format("~n  LOCKSTEP delivery continued in all ~p windows (no drop): ~p~n",
                  [length(LPub), LockOk]),
        io:format("  DESYNC delivery dropped to zero in ~p window(s): ~p~n",
                  [length(DesyncDropped), DesyncOk]),

        case LockOk andalso DesyncOk of
            true  -> io:format("~nPART 3 PASSED~n"), halt(0);
            false -> io:format("~nPART 3 FAILED~n"), halt(1)
        end
    catch
        Class:Reason:Stack ->
            io:format("~nPART 3 CRASHED: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(2)
    end.

%%--------------------------------------------------------------------
%% One run. RotateSubscriber controls whether the subscriber gets rotation
%% ticks (true = lockstep, false = desync). Returns #{Purpose => DeliveredCount}
%% keyed by the rotation purpose active at each message's receive time.
%%--------------------------------------------------------------------
run_phase(Tag, RotateSubscriber) ->
    drain(),  %% clear mailbox from any prior phase
    SubName = list_to_atom("rot_sub_" ++ atom_to_list(Tag)),
    PubName = list_to_atom("rot_pub_" ++ atom_to_list(Tag)),

    {ok, _} = ps_bench_dap_mqtt_interface:start_link(rot_scn, SubName, self()),
    {ok, _} = ps_bench_dap_mqtt_interface:start_link(rot_scn, PubName, self()),
    {ok, _} = gen_server:call(SubName, connect),
    {ok, _} = gen_server:call(PubName, connect),
    timer:sleep(200),

    ok = gen_server:call(SubName, {subscribe, #{}, [{<<"ps_bench/device/qos_0/#">>, [{qos, 0}]}]}),
    timer:sleep(200),

    %% Align the run to a window boundary so we only ever sample whole windows
    %% (no thin partial edge window whose few publishes all land in a transition).
    NowMs = erlang:system_time(millisecond),
    timer:sleep((?WINDOW_MS - (NowMs rem ?WINDOW_MS)) + 20),

    Deadline = erlang:monotonic_time(millisecond) + (?N_WINDOWS * ?WINDOW_MS) - ?EDGE_GUARD_MS,
    Acc0 = #{pub_windows => [], recv_windows => [], by_purpose => #{}},
    Acc = loop(SubName, PubName, RotateSubscriber, Deadline, Acc0),

    catch gen_server:call(SubName, disconnect),
    catch gen_server:call(PubName, disconnect),
    timer:sleep(200),
    Acc.

loop(SubName, PubName, RotateSubscriber, Deadline, Acc) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= Deadline of
        true ->
            %% final drain of in-flight messages
            collect_into(Acc);
        false ->
            %% Drive rotation ticks like the scenario manager's timer would.
            %% Re-subscribe the subscriber FIRST so its SP is current for this
            %% window before the publisher publishes (minimizes transition lag).
            case RotateSubscriber of
                true  -> gen_server:cast(SubName, rotate_purpose);
                false -> ok
            end,
            gen_server:cast(PubName, rotate_purpose),
            timer:sleep(10),
            %% record the absolute rotation window we are publishing in
            PubWin = ps_bench_utils:rotation_index(erlang:system_time(millisecond), ?WINDOW_MS),
            Acc0 = add_window(pub_windows, PubWin, Acc),
            %% publisher self-rotates its MP via the publish path
            _ = gen_server:call(PubName, {publish, #{}, ?TOPIC, 1, <<"d">>, [{qos, 0}]}),
            Acc1 = collect_into(Acc0),
            timer:sleep(?PUB_EVERY_MS),
            loop(SubName, PubName, RotateSubscriber, Deadline, Acc1)
    end.

%% Pull any pending received-publish messages, recording the absolute rotation
%% window they arrived in (boundary-robust delivery coverage) plus a by-purpose
%% tally for display.
collect_into(Acc) ->
    receive
        {publish_recv, {_TimeNs, _Topic, _Payload, _Props}, _ClientName} ->
            RecvMs = erlang:system_time(millisecond),
            Win = ps_bench_utils:rotation_index(RecvMs, ?WINDOW_MS),
            Purpose = ps_bench_utils:rotation_purpose(RecvMs, ?WINDOW_MS, ?ROT_SET),
            Acc1 = add_window(recv_windows, Win, Acc),
            ByP = maps:update_with(Purpose, fun(C) -> C + 1 end, 1, maps:get(by_purpose, Acc1)),
            collect_into(Acc1#{by_purpose := ByP});
        _Other ->
            collect_into(Acc)
    after 0 ->
        Acc
    end.

add_window(Key, Win, Acc) ->
    Cur = maps:get(Key, Acc),
    case lists:member(Win, Cur) of
        true  -> Acc;
        false -> Acc#{Key := [Win | Cur]}
    end.

drain() ->
    receive _ -> drain() after 0 -> ok end.

%%--------------------------------------------------------------------
load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, rot_node),
    application:set_env(?TEST_APP, device_definitions_directory, ?DIR ++ "/devices"),
    application:set_env(?TEST_APP, deployment_definitions_directory, ?DIR ++ "/deployments"),
    application:set_env(?TEST_APP, scenario_definitions_directory, ?DIR ++ "/scenarios"),
    application:set_env(?TEST_APP, selected_scenario, rot_scn),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark rotation itest"}, {vsn, "1"},
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
        "[{name, rot_dep},\n"
        " {nodes, [{rot_node, [{devices, [{rot_dev, 1}]}]}]}].\n",
    ok = file:write_file(DepDir ++ "/rot.deployment", Deployment),

    Scenario =
        "[{name, rot_scn},\n"
        " {duration, {30, seconds}},\n"
        " {protocol, mqttv5},\n"
        " {deployment_name, rot_dep},\n"
        " {hosts, [{rot_node, [{hostname, \"127.0.0.1\"}]}]},\n"
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
    ok = file:write_file(ScnDir ++ "/rot.scenario", Scenario),
    ok.

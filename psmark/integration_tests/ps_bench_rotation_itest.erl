%% Dynamic-purpose ADD/REMOVE TOGGLE integration proof (Christian's spec).
%%
%% Drives the REAL ps_bench_dap_mqtt_interface against a live DAP broker with
%% rotation enabled (purpose_rotation_window_ms > 0). Each window the active
%% filter alternates between ADDING and REMOVING a single purpose
%% (rotation_toggle_purpose). The subscriber's SP is only the toggled purpose and
%% the publisher's MP base does not contain it, so data flows exactly in "added"
%% windows, where the MP carries the toggled purpose too and so permits the SP, and
%% stops in "removed" windows, where the SP is empty (consent withdrawn). That makes
%% the toggle observable at the delivery layer.
%%
%% The node has the subscriber role: PSMark toggles only a subscriber node's SP.
%%
%% Two runs:
%%   LOCKSTEP (subscriber DOES toggle its SP): publisher MP and subscriber SP add
%%            and remove the toggle purpose in unison off the same clock. The
%%            toggle is present on BOTH sides in added windows -> delivery recurs
%%            across multiple added windows; in removed windows neither side has
%%            it -> no delivery. Proves the toggle works AND stays in lockstep.
%%   DESYNC   (subscriber FROZEN on a removed-window SP, never toggles): the
%%            publisher keeps toggling but the subscriber's SP never carries the
%%            toggle purpose, so the one possible link is never present ->
%%            delivery drops to ZERO. This is the drop the test must catch.
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
-define(N_WINDOWS, 6).
-define(PUB_EVERY_MS, 120).
%% Don't publish within this many ms of a window boundary, so an added-window
%% message can't be delivered just after the flip and miscounted in a removed
%% window.
-define(EDGE_GUARD_MS, 300).
-define(TOPIC, <<"ps_bench/device/qos_0/tog_dev">>).
%% Mirrors the interface placeholders so the harness can show the toggle and
%% bucket delivery by the toggle state.
-define(TOGGLE, <<"maintenance/routine">>).
-define(MP_BASE, [<<"maintenance/predictive">>]).
-define(SP_BASE, []).

start(_Type, _Args) ->
    ok = ps_bench_config_manager:load_config_from_env_vars(),
    {ok, spawn(fun() -> receive stop -> ok end end)}.

stop(_) -> ok.

main() ->
    try
        setup_config_files(),
        load_config_via_app(),

        show_schedule(),

        Lock = run_lockstep(),
        Desync = run_desync(),

        #{added_windows := LAdded, removed_hits := LRemovedHits, total := LTotal} = Lock,
        #{total := DTotal} = Desync,

        io:format("~n==== DYNAMIC PURPOSE TOGGLE RESULTS ====~n"),
        io:format("  toggle purpose=~p~n", [?TOGGLE]),
        io:format("  MP base=~p   SP base=~p (the toggle is the only link)~n",
                  [?MP_BASE, ?SP_BASE]),
        io:format("  window=~pms  windows=~p~n", [?WINDOW_MS, ?N_WINDOWS]),
        io:format("  LOCKSTEP (sub toggles SP in sync):~n"),
        io:format("    total delivered=~p~n", [LTotal]),
        io:format("    distinct ADDED windows with delivery=~p ~p~n",
                  [length(LAdded), lists:sort(LAdded)]),
        io:format("    deliveries landing in REMOVED windows=~p (expect 0)~n", [LRemovedHits]),
        io:format("  DESYNC (sub frozen on removed-window SP):~n"),
        io:format("    total delivered=~p (expect 0)~n", [DTotal]),

        %% LOCKSTEP: the toggle drives delivery on across >= 2 added windows
        %% (i.e. multiple add/remove cycles) and delivery is correctly OFF in
        %% removed windows (toggle removed -> consent withdrawn).
        LockOk = (length(LAdded) >= 2) andalso (LRemovedHits =:= 0),
        %% DESYNC: the toggle purpose never appears on the subscriber's SP, so
        %% the only possible link is absent -> zero delivery, the drop we catch.
        DesyncOk = (DTotal =:= 0),

        io:format("~n  LOCKSTEP delivery recurred across ~p added windows, none in removed: ~p~n",
                  [length(LAdded), LockOk]),
        io:format("  DESYNC delivery dropped to zero: ~p~n", [DesyncOk]),

        case LockOk andalso DesyncOk of
            true  -> io:format("~nDYNAMIC PURPOSE TOGGLE PASSED~n"), halt(0);
            false -> io:format("~nDYNAMIC PURPOSE TOGGLE FAILED~n"), halt(1)
        end
    catch
        Class:Reason:Stack ->
            io:format("~nTOGGLE TEST CRASHED: ~p:~p~n~p~n", [Class, Reason, Stack]),
            halt(2)
    end.

%% White-box: print the MP/SP filters the interface will compute for a few
%% consecutive windows, demonstrating the single-purpose add/remove toggle and
%% that both sides flip together.
show_schedule() ->
    io:format("~n  schedule (toggle added on even windows):~n"),
    Base = erlang:system_time(millisecond),
    Base0 = (Base div ?WINDOW_MS) * ?WINDOW_MS,
    lists:foreach(
      fun(K) ->
          T = Base0 + K * ?WINDOW_MS + (?WINDOW_MS div 2),
          Added = ps_bench_utils:rotation_toggle_added(T, ?WINDOW_MS),
          Mp = ps_bench_utils:rotation_toggle_filter(T, ?WINDOW_MS, ?MP_BASE, ?TOGGLE),
          Sp = ps_bench_utils:rotation_toggle_filter(T, ?WINDOW_MS, ?SP_BASE, ?TOGGLE),
          io:format("    win+~p added=~p  MP=~p  SP=~p~n", [K, Added, Mp, Sp])
      end, lists:seq(0, 3)).

%%--------------------------------------------------------------------
%% LOCKSTEP: subscriber toggles its SP in sync with the publisher's MP.
%%--------------------------------------------------------------------
run_lockstep() ->
    drain(),
    {ok, _} = ps_bench_dap_mqtt_interface:start_link(tog_scn, tog_sub_l, self()),
    {ok, _} = ps_bench_dap_mqtt_interface:start_link(tog_scn, tog_pub_l, self()),
    {ok, _} = gen_server:call(tog_sub_l, connect),
    {ok, _} = gen_server:call(tog_pub_l, connect),
    timer:sleep(200),

    ok = gen_server:call(tog_sub_l, {subscribe, #{}, [{<<"ps_bench/device/qos_0/#">>, [{qos, 0}]}]}),
    timer:sleep(200),

    align_to_window_boundary(),
    Deadline = erlang:monotonic_time(millisecond) + (?N_WINDOWS * ?WINDOW_MS) - ?EDGE_GUARD_MS,
    Acc = lockstep_loop(tog_sub_l, tog_pub_l, Deadline, #{added => [], removed_hits => 0, total => 0}),

    catch gen_server:call(tog_sub_l, disconnect),
    catch gen_server:call(tog_pub_l, disconnect),
    timer:sleep(200),
    #{added_windows => maps:get(added, Acc),
      removed_hits  => maps:get(removed_hits, Acc),
      total         => maps:get(total, Acc)}.

lockstep_loop(SubName, PubName, Deadline, Acc) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= Deadline of
        true ->
            collect(Acc);
        false ->
            %% Re-toggle the subscriber SP FIRST so it is current for this window,
            %% then the publisher (its publish path also re-registers the MP).
            gen_server:cast(SubName, rotate_purpose),
            gen_server:cast(PubName, rotate_purpose),
            timer:sleep(10),
            Acc1 = collect(Acc),
            Acc2 = case in_edge_guard() of
                       true ->
                           Acc1;  %% near a boundary: skip publishing this tick
                       false ->
                           _ = gen_server:call(PubName, {publish, #{}, ?TOPIC, 1, <<"d">>, [{qos, 0}]}),
                           collect(Acc1)
                   end,
            timer:sleep(?PUB_EVERY_MS),
            lockstep_loop(SubName, PubName, Deadline, Acc2)
    end.

%%--------------------------------------------------------------------
%% DESYNC: subscriber subscribes during a REMOVED window and never toggles, so
%% its SP never carries the toggle purpose.
%%--------------------------------------------------------------------
run_desync() ->
    drain(),
    {ok, _} = ps_bench_dap_mqtt_interface:start_link(tog_scn, tog_sub_d, self()),
    {ok, _} = ps_bench_dap_mqtt_interface:start_link(tog_scn, tog_pub_d, self()),
    {ok, _} = gen_server:call(tog_sub_d, connect),
    {ok, _} = gen_server:call(tog_pub_d, connect),
    timer:sleep(200),

    %% Subscribe in a removed window so the frozen SP is base-only (no toggle).
    wait_until_removed(),
    ok = gen_server:call(tog_sub_d, {subscribe, #{}, [{<<"ps_bench/device/qos_0/#">>, [{qos, 0}]}]}),
    timer:sleep(50),

    align_to_window_boundary(),
    Deadline = erlang:monotonic_time(millisecond) + (?N_WINDOWS * ?WINDOW_MS) - ?EDGE_GUARD_MS,
    Acc = desync_loop(tog_pub_d, Deadline, #{added => [], removed_hits => 0, total => 0}),

    catch gen_server:call(tog_sub_d, disconnect),
    catch gen_server:call(tog_pub_d, disconnect),
    timer:sleep(200),
    #{total => maps:get(total, Acc)}.

desync_loop(PubName, Deadline, Acc) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= Deadline of
        true ->
            collect(Acc);
        false ->
            %% Only the publisher toggles; the subscriber SP stays frozen.
            gen_server:cast(PubName, rotate_purpose),
            timer:sleep(10),
            Acc1 = case in_edge_guard() of
                       true  -> collect(Acc);
                       false ->
                           _ = gen_server:call(PubName, {publish, #{}, ?TOPIC, 1, <<"d">>, [{qos, 0}]}),
                           collect(Acc)
                   end,
            timer:sleep(?PUB_EVERY_MS),
            desync_loop(PubName, Deadline, Acc1)
    end.

%%--------------------------------------------------------------------
%% Drain delivered publishes, bucketing each by the toggle state of the window it
%% arrived in: added windows are expected (toggle present on both sides), any
%% delivery in a removed window is a violation.
collect(Acc) ->
    receive
        {publish_recv, {_TimeNs, _Topic, _Payload, _Props}, _ClientName} ->
            RecvMs = erlang:system_time(millisecond),
            Win = ps_bench_utils:rotation_index(RecvMs, ?WINDOW_MS),
            Acc1 = Acc#{total := maps:get(total, Acc) + 1},
            Acc2 = case ps_bench_utils:rotation_toggle_added(RecvMs, ?WINDOW_MS) of
                       true ->
                           Cur = maps:get(added, Acc1),
                           case lists:member(Win, Cur) of
                               true  -> Acc1;
                               false -> Acc1#{added := [Win | Cur]}
                           end;
                       false ->
                           Acc1#{removed_hits := maps:get(removed_hits, Acc1) + 1}
                   end,
            collect(Acc2);
        _Other ->
            collect(Acc)
    after 0 ->
        Acc
    end.

%% True when we are within EDGE_GUARD_MS of the next window boundary.
in_edge_guard() ->
    NowMs = erlang:system_time(millisecond),
    Into = NowMs rem ?WINDOW_MS,
    (?WINDOW_MS - Into) =< ?EDGE_GUARD_MS orelse Into =< ?EDGE_GUARD_MS.

%% Sleep to just past the next window boundary.
align_to_window_boundary() ->
    NowMs = erlang:system_time(millisecond),
    timer:sleep((?WINDOW_MS - (NowMs rem ?WINDOW_MS)) + 20).

%% Block until we are comfortably inside a REMOVED (toggle-absent) window.
wait_until_removed() ->
    NowMs = erlang:system_time(millisecond),
    Into = NowMs rem ?WINDOW_MS,
    Removed = not ps_bench_utils:rotation_toggle_added(NowMs, ?WINDOW_MS),
    case Removed andalso Into > 200 andalso Into < (?WINDOW_MS - 400) of
        true  -> ok;
        false -> timer:sleep(100), wait_until_removed()
    end.

drain() ->
    receive _ -> drain() after 0 -> ok end.

%%--------------------------------------------------------------------
load_config_via_app() ->
    application:set_env(?TEST_APP, node_name, tog_node),
    application:set_env(?TEST_APP, device_definitions_directory, ?DIR ++ "/devices"),
    application:set_env(?TEST_APP, deployment_definitions_directory, ?DIR ++ "/deployments"),
    application:set_env(?TEST_APP, scenario_definitions_directory, ?DIR ++ "/scenarios"),
    application:set_env(?TEST_APP, selected_scenario, tog_scn),
    AppSpec = {application, ?TEST_APP,
               [{description, "psmark toggle itest"}, {vsn, "1"},
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
        "[{type, tog_dev},\n"
        " {publication_frequency_ms, 150},\n"
        " {payload_bytes_mean, 16.0},\n"
        " {payload_bytes_variance, 0.0},\n"
        " {disconnect_check_period_ms, 0},\n"
        " {disconnect_chance_pct, 0.0},\n"
        " {reconnect_check_period_ms, 0},\n"
        " {reconnect_chance_pct, 0.0}].\n",
    ok = file:write_file(DevDir ++ "/tog_dev.device", Device),

    Deployment =
        "[{name, tog_dep},\n"
        " {nodes, [{tog_node, [{devices, [{tog_dev, 1}]}, {role, subscriber}]}]}].\n",
    ok = file:write_file(DepDir ++ "/tog.deployment", Deployment),

    Scenario =
        "[{name, tog_scn},\n"
        " {duration, {30, seconds}},\n"
        " {protocol, mqttv5},\n"
        " {deployment_name, tog_dep},\n"
        " {hosts, [{tog_node, [{hostname, \"127.0.0.1\"}]}]},\n"
        " {protocol_config,\n"
        "  [{client_interface_module, ps_bench_dap_mqtt_interface},\n"
        "   {broker, \"" ?BROKER_HOST "\"},\n"
        "   {port, " ++ integer_to_list(?BROKER_PORT) ++ "},\n"
        "   {qos, [{default_qos, 0}]},\n"
        "   {purpose_rotation_window_ms, " ++ integer_to_list(?WINDOW_MS) ++ "},\n"
        "   {rotation_toggle_purpose, \"maintenance/routine\"},\n"
        "   {device_purposes, [{tog_dev, \"maintenance/predictive\"}]},\n"
        "   {node_subscription_purposes, [{tog_node, \"maintenance/routine\"}]}]},\n"
        " {metric_config,\n"
        "  [{output_dir, \"" ?DIR "/results\"},\n"
        "   {hw_stats_poll_period_ms, 1000},\n"
        "   {metric_plugins, [{ps_bench_throughput_calc_plugin, erlang}]}]}].\n",
    ok = file:write_file(ScnDir ++ "/tog.scenario", Scenario),
    ok.

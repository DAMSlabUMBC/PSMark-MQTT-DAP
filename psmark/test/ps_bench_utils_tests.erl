-module(ps_bench_utils_tests).
-include_lib("eunit/include/eunit.hrl").

%% Existing single-filter, set-notation Cartesian product is preserved.
single_filter_set_notation_test() ->
    ?assertEqual(["maintenance/predictive", "maintenance/routine"],
                 ps_bench_utils:decompose_purpose_filter("maintenance/{predictive,routine}")).

%% Several independent filters separated by '|' expand to the union of each
%% filter's decomposition - never the cross product across the '|' boundary.
multi_filter_union_test() ->
    ?assertEqual(["quality/assurance", "operations/forecast"],
                 ps_bench_utils:decompose_purpose_filter("quality/assurance|operations/forecast")).

%% '|' composes with set-notation: each side expands on its own, then union.
multi_filter_with_set_notation_test() ->
    ?assertEqual(["quality/assurance/1", "quality/assurance/2",
                  "maintenance/predictive", "maintenance/routine"],
                 ps_bench_utils:decompose_purpose_filter(
                     "quality/assurance/{1,2}|maintenance/{predictive,routine}")).

%% Union semantics: a purpose produced by more than one filter appears once.
multi_filter_dedup_test() ->
    ?assertEqual(["quality/assurance", "operations/forecast"],
                 ps_bench_utils:decompose_purpose_filter(
                     "quality/assurance|operations/forecast|quality/assurance")).

%% --- Dynamic purpose rotation schedule (Part 3) ---
%% The schedule is a pure function of the wall clock, so the active purpose only
%% changes at window boundaries and is stable within a window.
rotation_advances_each_window_test() ->
    Set = [a, b, c],
    W = 1000,
    ?assertEqual(a, ps_bench_utils:rotation_purpose(0,    W, Set)),
    ?assertEqual(a, ps_bench_utils:rotation_purpose(999,  W, Set)),
    ?assertEqual(b, ps_bench_utils:rotation_purpose(1000, W, Set)),
    ?assertEqual(c, ps_bench_utils:rotation_purpose(2500, W, Set)),
    ?assertEqual(a, ps_bench_utils:rotation_purpose(3000, W, Set)).  %% wraps

%% LOCKSTEP: two independent callers (publisher MP, subscriber SP) that read the
%% same clock window land on the same purpose - the whole basis of lockstep.
rotation_lockstep_same_window_test() ->
    Set = [p1, p2, p3],
    W = 2000,
    Now = 7321,  %% somewhere mid-window
    Publisher = ps_bench_utils:rotation_purpose(Now, W, Set),
    Subscriber = ps_bench_utils:rotation_purpose(Now, W, Set),
    ?assertEqual(Publisher, Subscriber).

%% If the two callers are more than a window apart they diverge - exactly the
%% desync the integration test must catch as a delivery drop.
rotation_offset_breaks_lockstep_test() ->
    Set = [p1, p2, p3],
    W = 2000,
    Pub = ps_bench_utils:rotation_purpose(0, W, Set),
    SubLate = ps_bench_utils:rotation_purpose(2000, W, Set),
    ?assertNotEqual(Pub, SubLate).

rotation_empty_set_is_off_test() ->
    ?assertEqual(undefined, ps_bench_utils:rotation_purpose(1234, 1000, [])).

%% --- Dynamic purpose add/remove toggle (Christian's spec) ---
%% Even windows ADD the toggle purpose, odd windows REMOVE it; stable within a
%% window, flips only at window boundaries.
toggle_alternates_each_window_test() ->
    W = 1000,
    ?assertEqual(true,  ps_bench_utils:rotation_toggle_added(0,    W)),
    ?assertEqual(true,  ps_bench_utils:rotation_toggle_added(999,  W)),
    ?assertEqual(false, ps_bench_utils:rotation_toggle_added(1000, W)),
    ?assertEqual(false, ps_bench_utils:rotation_toggle_added(1999, W)),
    ?assertEqual(true,  ps_bench_utils:rotation_toggle_added(2000, W)).

%% The built filter carries the base every window and ADDS the toggle purpose
%% only on added windows (removes it on removed windows).
toggle_filter_adds_and_removes_test() ->
    W = 1000,
    Base = [<<"a/b">>],
    Toggle = <<"c/d">>,
    %% added window 0 -> base + toggle
    ?assertEqual(<<"a/b|c/d">>, ps_bench_utils:rotation_toggle_filter(0, W, Base, Toggle)),
    %% removed window 1 -> base only
    ?assertEqual(<<"a/b">>,     ps_bench_utils:rotation_toggle_filter(1000, W, Base, Toggle)),
    %% added again window 2
    ?assertEqual(<<"a/b|c/d">>, ps_bench_utils:rotation_toggle_filter(2000, W, Base, Toggle)).

%% LOCKSTEP: a publisher and subscriber that read the same window agree, window
%% for window, on whether the toggle purpose is present - even with DISTINCT
%% bases, the toggle token is added/removed in unison.
toggle_lockstep_same_window_test() ->
    W = 5000,
    Toggle = <<"shared">>,
    MpBase = [<<"mp/base">>],
    SpBase = [<<"sp/base">>],
    Now = 12345,
    PubAdded = ps_bench_utils:rotation_toggle_added(Now, W),
    SubAdded = ps_bench_utils:rotation_toggle_added(Now, W),
    ?assertEqual(PubAdded, SubAdded),
    MpHasToggle = binary:match(ps_bench_utils:rotation_toggle_filter(Now, W, MpBase, Toggle), Toggle) =/= nomatch,
    SpHasToggle = binary:match(ps_bench_utils:rotation_toggle_filter(Now, W, SpBase, Toggle), Toggle) =/= nomatch,
    ?assertEqual(MpHasToggle, SpHasToggle).

%% Out of phase by one window: the two callers DISAGREE on the toggle - exactly
%% the desync the integration test catches as a delivery drop.
toggle_offset_breaks_lockstep_test() ->
    W = 1000,
    ?assertNotEqual(ps_bench_utils:rotation_toggle_added(0, W),
                    ps_bench_utils:rotation_toggle_added(1000, W)).

%% Every client moves to the next purposes in the list each window.
rotated_tokens_test() ->
    L = [<<"a">>, <<"b">>, <<"c">>],
    ?assertEqual([<<"a">>], ps_bench_utils:rotated_tokens(0, 10, [<<"a">>], L)),
    ?assertEqual([<<"b">>], ps_bench_utils:rotated_tokens(10, 10, [<<"a">>], L)),
    ?assertEqual([<<"a">>], ps_bench_utils:rotated_tokens(10, 10, [<<"c">>], L)),
    ?assertEqual([<<"b">>, <<"c">>], ps_bench_utils:rotated_tokens(10, 10, [<<"a">>, <<"b">>], L)),
    %% A purpose outside the list stays put.
    ?assertEqual([<<"z">>], ps_bench_utils:rotated_tokens(10, 10, [<<"z">>], L)).

%% Subscribers walk the list backwards, so publisher and subscriber pairs actually shift.
rotated_tokens_reversed_test() ->
    L = [<<"a">>, <<"b">>, <<"c">>],
    R = lists:reverse(L),
    ?assertEqual([<<"a">>], ps_bench_utils:rotated_tokens(0, 10, [<<"a">>], R)),
    ?assertEqual([<<"c">>], ps_bench_utils:rotated_tokens(10, 10, [<<"a">>], R)),
    ?assertEqual([<<"b">>], ps_bench_utils:rotated_tokens(20, 10, [<<"a">>], R)),
    ?assertEqual([<<"a">>], ps_bench_utils:rotated_tokens(30, 10, [<<"a">>], R)).

%% N distinct issuers per tick, with the op types split evenly.
pick_op_issues_test() ->
    Clients = lists:seq(1, 40),
    Types = [audit, history, update],
    Issues = ps_bench_utils:pick_op_issues(Clients, 6, Types),
    ?assertEqual(6, length(Issues)),
    ?assertEqual(6, length(lists:usort([C || {C, _} <- Issues]))),
    [?assertEqual(2, length([x || {_, T0} <- Issues, T0 =:= T])) || T <- Types],
    %% Never more issuers than clients.
    ?assertEqual(2, length(ps_bench_utils:pick_op_issues([1, 2], 6, Types))),
    ?assertEqual([], ps_bench_utils:pick_op_issues([], 6, Types)).

scaled_publish_interval_test() ->
    ?assertEqual(100, ps_bench_utils:scaled_publish_interval(100, 1.0)),
    ?assertEqual(67, ps_bench_utils:scaled_publish_interval(100, 1.5)),
    ?assertEqual(7, ps_bench_utils:scaled_publish_interval(10, 1.5)),
    ?assertEqual(1, ps_bench_utils:scaled_publish_interval(1, 1.5)).

%% The same half of the subscribers is chosen on every node, whatever the order.
outage_selected_test() ->
    Subs = [d, b, a, c],
    ?assert(ps_bench_utils:outage_selected(a, Subs, 0.5)),
    ?assert(ps_bench_utils:outage_selected(b, Subs, 0.5)),
    ?assertNot(ps_bench_utils:outage_selected(c, Subs, 0.5)),
    ?assertNot(ps_bench_utils:outage_selected(d, Subs, 0.5)),
    ?assertNot(ps_bench_utils:outage_selected(floor1, Subs, 0.5)).

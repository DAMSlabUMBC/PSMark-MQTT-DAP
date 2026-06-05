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

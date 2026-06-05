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

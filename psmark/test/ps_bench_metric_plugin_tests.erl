-module(ps_bench_metric_plugin_tests).

-include_lib("eunit/include/eunit.hrl").

recv(Seq, TRecvNs) ->
    {me, <<"client">>, pub, <<"t">>, Seq, TRecvNs - 1000000, TRecvNs, 10}.

with_events(Events, Fun) ->
    ps_bench_store:initialize_node_storage(),
    ets:insert(psb_recv_events, Events),
    Fun(me, overall).

% A sub matched only by a slow device can get one message, or a few inside one second.
throughput_one_message_test() ->
    Result = with_events([recv(1, 5000000000)], fun ps_bench_throughput_calc_plugin:calculate_pairwise_throughput_for_one_node/2),
    ?assertEqual(1, element(4, Result)).

throughput_same_second_test() ->
    Events = [recv(N, 5000000000 + N * 1000000) || N <- lists:seq(1, 4)],
    Result = with_events(Events, fun ps_bench_throughput_calc_plugin:calculate_pairwise_throughput_for_one_node/2),
    ?assertEqual(4, element(4, Result)),
    ?assertEqual(4, element(8, Result)).

throughput_many_seconds_test() ->
    Events = [recv(N, N * 1000000000) || N <- lists:seq(1, 10)],
    Result = with_events(Events, fun ps_bench_throughput_calc_plugin:calculate_pairwise_throughput_for_one_node/2),
    ?assertEqual(10, element(4, Result)).

latency_one_message_test() ->
    Result = with_events([recv(1, 5000000000)], fun ps_bench_latency_calc_plugin:calculate_pairwise_latency_for_one_node/2),
    ?assert(is_tuple(Result)).

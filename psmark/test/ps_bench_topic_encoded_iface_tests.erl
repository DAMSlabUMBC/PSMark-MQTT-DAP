-module(ps_bench_topic_encoded_iface_tests).
-include_lib("eunit/include/eunit.hrl").

%% purpose_scoped_filters/2: rewrites the adapter's wildcard subscriptions into
%% a consumer's purpose-encoded topic filters (paper SS2.2 / SS7.3 topic-encoded
%% baseline, one subscriber per purpose).

wildcard_topics() ->
    [{<<"ps_bench/device/qos_0/#">>, [{qos, 0}]},
     {<<"ps_bench/device/qos_1/#">>, [{qos, 1}]},
     {<<"ps_bench/device/qos_2/#">>, [{qos, 2}]}].

unscoped_node_passthrough_test() ->
    Topics = wildcard_topics(),
    ?assertEqual(Topics,
                 ps_bench_topic_encoded_mqtt_interface:purpose_scoped_filters(undefined, Topics)).

single_purpose_scoping_test() ->
    Got = ps_bench_topic_encoded_mqtt_interface:purpose_scoped_filters(
            <<"quality/assurance">>, wildcard_topics()),
    ?assertEqual([{<<"ps_bench/device/qos_0/+/[quality/assurance]">>, [{qos, 0}]},
                  {<<"ps_bench/device/qos_1/+/[quality/assurance]">>, [{qos, 1}]},
                  {<<"ps_bench/device/qos_2/+/[quality/assurance]">>, [{qos, 2}]}],
                 Got).

brace_variants_one_filter_each_test() ->
    Got = ps_bench_topic_encoded_mqtt_interface:purpose_scoped_filters(
            <<"quality/assurance/{1,2}">>,
            [{<<"ps_bench/device/qos_0/#">>, [{qos, 0}]}]),
    ?assertEqual([{<<"ps_bench/device/qos_0/+/[quality/assurance/1]">>, [{qos, 0}]},
                  {<<"ps_bench/device/qos_0/+/[quality/assurance/2]">>, [{qos, 0}]}],
                 Got).

stress_density_filter_count_test() ->
    Ten = "{1,2,3,4,5,6,7,8,9,10}",
    SP = list_to_binary("vendor/maintenance/" ++ Ten ++ "/" ++ Ten),
    Got = ps_bench_topic_encoded_mqtt_interface:purpose_scoped_filters(
            SP, wildcard_topics()),
    %% 100 variants x 3 QoS wildcards = 300 filters
    ?assertEqual(300, length(Got)),
    ?assert(lists:member({<<"ps_bench/device/qos_0/+/[vendor/maintenance/10/10]">>, [{qos, 0}]}, Got)).

non_wildcard_topic_passthrough_test() ->
    Topics = [{<<"ps_bench/device/qos_0/some_sensor">>, [{qos, 0}]}],
    ?assertEqual(Topics,
                 ps_bench_topic_encoded_mqtt_interface:purpose_scoped_filters(
                   <<"quality/assurance">>, Topics)).

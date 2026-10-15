-module(ps_bench_store_dap_tests).
-include_lib("eunit/include/eunit.hrl").

%% DAP events are written oldest first, one quoted CSV row each; a payload with
%% commas and quotes stays one field.
dap_events_csv_test() ->
    ok = ps_bench_store:initialize_node_storage(),
    ok = ps_bench_store:record_dap_event(20, <<"pub_1">>, op_notification, <<"OP_NOTIF/pub_1">>,
                                         #{op_type => <<"AUDIT">>, status => <<"Success">>, sender => <<"Broker">>,
                                           detail => <<"sub_a,sub \"b\"">>}),
    ok = ps_bench_store:record_dap_event(10, <<"pub_1">>, mp_register, <<"t/a">>, #{detail => <<"quality/assurance">>}),
    File = filename:join("/tmp", "ps_bench_store_dap_tests.csv"),
    ok = ps_bench_store:write_dap_events_to_disk(File),
    {ok, Bin} = file:read_file(File),
    ?assertEqual([<<"TimeNs,Client,Event,Topic,OpType,OpId,Status,Sender,Detail">>,
                  <<"10,\"pub_1\",\"mp_register\",\"t/a\",\"\",\"\",\"\",\"\",\"quality/assurance\"">>,
                  <<"20,\"pub_1\",\"op_notification\",\"OP_NOTIF/pub_1\",\"AUDIT\",\"\",\"Success\",\"Broker\","
                    "\"sub_a,sub \"\"b\"\"\"">>],
                 binary:split(Bin, <<"\n">>, [global, trim])).

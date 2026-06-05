-module(ps_bench_scenario_manager).

-include("ps_bench_config.hrl").

-define(CLIENT_SUPERVISOR, ps_bench_client_sup).
-define(NODE_MANAGER, ps_bench_node_manager).

%% public
-export([initialize_scenario/0, run_scenario/0, stop_scenario/0, clean_up_scenario/0]).

% Currently we only support one scenario a run
initialize_scenario() ->
    
    {ok, ScenarioName} = ps_bench_config_manager:fetch_selected_scenario(),
    ps_bench_utils:log_state_change("Initializing Scenario: ~p", [ScenarioName]),
    
    initialize_clients(),
    print_clients(),
    connect_clients(),
    subscribe_clients(),
    timer:sleep(1000).

run_scenario() ->

    {ok, ScenarioName} = ps_bench_config_manager:fetch_selected_scenario(),
    ps_bench_utils:log_state_change("Starting Scenario: ~p", [ScenarioName]),

    {ok, DurationMs} = ps_bench_config_manager:fetch_scenario_duration(),

    % Display time and duration for user
    {{Y,M,D},{H,MM,SS}} = erlang:localtime(),
    TimeStr = lists:flatten(io_lib:format("~B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B", [Y, M, D,H,MM,SS])),
    ps_bench_utils:log_message("Starting at ~s and running for ~pms", [TimeStr, DurationMs]),

    start_client_loops(),
    _Ref = timer:apply_after(DurationMs, ?MODULE, stop_scenario, []).

stop_scenario() ->
    {ok, ScenarioName} = ps_bench_config_manager:fetch_selected_scenario(),
    ps_bench_utils:log_state_change("Stopping Scenario: ~p", [ScenarioName]),

    % Stop clients and notify the benchmarking scenario is complete
    stop_publishers(),
    timer:sleep(2000),
    % Note that we don't stop the subscribers here in case other nodes continue to publish for a brief time.
    % We wait until all clients are confirmed to have stopped publishing before removing subscribers
    gen_server:cast(?NODE_MANAGER, global_continue).

clean_up_scenario() ->
    ps_bench_utils:log_state_change("Removing Scenario Clients"),
    stop_subscriber(),
    destroy_clients().

initialize_clients() ->

    {ok, NodeName} = ps_bench_config_manager:fetch_node_name(),

    % Resolve and cache this node's role for the rest of the run. The role guards
    % every persistent_term client-class access below so that a subscriber-only
    % node (no device_clients) never crashes with a badkey, and a publisher node
    % keeps the legacy publisher+subscriber behavior unchanged.
    {ok, Role} = ps_bench_config_manager:fetch_role_for_this_node(),
    persistent_term:put({?MODULE, node_role}, Role),

    % Publisher nodes create one publication client per device
    case has_publishers() of
        true ->
            {ok, NodeDevices} = ps_bench_config_manager:fetch_devices_for_this_node(),
            ClientList = initialize_clients_for_devices(NodeName, NodeDevices, []),
            persistent_term:put({?MODULE, device_clients}, ClientList);
        false ->
            ok
    end,

    % Every role uses one subscription client which represents all subscriptions for this node
    case has_subscriber() of
        true ->
            SubscriptionClient = initialize_subscription_client(NodeName),
            persistent_term:put({?MODULE, subscription_client}, SubscriptionClient);
        false ->
            ok
    end.

%% Role helpers. node_role/0 is set once in initialize_clients/0.
node_role() ->
    persistent_term:get({?MODULE, node_role}, ?DEPLOYMENT_ROLE_PUBLISHER).

has_publishers() ->
    node_role() =:= ?DEPLOYMENT_ROLE_PUBLISHER.

has_subscriber() ->
    %% Both publisher and subscriber roles run a subscription client today.
    %% Threaded as a guard so a future publisher-only role drops in cleanly.
    node_role() =:= ?DEPLOYMENT_ROLE_PUBLISHER orelse node_role() =:= ?DEPLOYMENT_ROLE_SUBSCRIBER.

initialize_clients_for_devices(_NodeName, [], ClientList) ->
    ClientList;

initialize_clients_for_devices(NodeName, [{DeviceType, DeviceCount} | NodeDevices] , ClientList) ->
    
    % Initialize the device clients
    ClientListForDevice = initialize_client_for_device_type(NodeName, DeviceType, DeviceCount, []),

    % Reverse the list just to put it in index order
    NewClientList = ClientList ++ lists:reverse(ClientListForDevice),

    % Recursively process the rest of the devices
    initialize_clients_for_devices(NodeName, NodeDevices, NewClientList).

initialize_client_for_device_type(NodeName, DeviceType, DevicesToCreateCount, DeviceClientList) ->
    % Construct name and start child
    StringNodeName = atom_to_list(NodeName),
    ThisDeviceIndex = length(DeviceClientList) + 1,
    ClientName = StringNodeName ++ "_" ++ atom_to_list(DeviceType) ++ "_" ++ integer_to_list(ThisDeviceIndex),
    {ok, ClientPid} = supervisor:start_child(?CLIENT_SUPERVISOR, [ClientName, DeviceType]),
    NewDeviceClientList = [{ClientName, ClientPid} | DeviceClientList],

    if ThisDeviceIndex < DevicesToCreateCount -> 
            initialize_client_for_device_type(NodeName, DeviceType, DevicesToCreateCount, NewDeviceClientList);
       ThisDeviceIndex =:= DevicesToCreateCount -> 
            NewDeviceClientList
    end.

initialize_subscription_client(NodeName) ->
    ClientName = atom_to_list(NodeName) ++ "_subscription_client",
    {ok, SubscriptionClient} = supervisor:start_child(?CLIENT_SUPERVISOR, [ClientName, subscriber]),
    {ClientName, SubscriptionClient}.

connect_clients() ->
    % Connect subscriber first
    case has_subscriber() of
        true ->
            {_SubscriberName, SubscriberClient} = persistent_term:get({?MODULE, subscription_client}),
            gen_server:call(SubscriberClient, connect);
        false ->
            ok
    end,

    % Then connect each publication client
    case has_publishers() of
        true ->
            ClientList = persistent_term:get({?MODULE, device_clients}),
            ConnectFunction = fun({_ClientName, ClientPid}) -> gen_server:call(ClientPid, connect) end,
            lists:foreach(ConnectFunction, ClientList);
        false ->
            ok
    end.

subscribe_clients() ->
    %  Only subscriber client calls this
    case has_subscriber() of
        true ->
            {_SubscriberName, SubscriberClient} = persistent_term:get({?MODULE, subscription_client}),
            gen_server:call(SubscriberClient, subscribe);
        false ->
            ok
    end.

start_client_loops() ->
    % Only publishing clients start loops here
    case has_publishers() of
        true ->
            ClientList = persistent_term:get({?MODULE, device_clients}),
            StartFunction = fun({_ClientName, ClientPid}) -> gen_server:cast(ClientPid, start_client_loops) end,
            lists:foreach(StartFunction, ClientList);
        false ->
            ok
    end.

stop_publishers() ->
    % Stop client publication loops
    case has_publishers() of
        true ->
            ClientList = persistent_term:get({?MODULE, device_clients}),
            StopFunction = fun({_ClientName, ClientPid}) -> gen_server:cast(ClientPid, stop) end,
            lists:foreach(StopFunction, ClientList);
        false ->
            ok
    end.

stop_subscriber() ->
    % Stop subscriber
    case has_subscriber() of
        true ->
            {_SubscriberName, SubscriberClient} = persistent_term:get({?MODULE, subscription_client}),
            gen_server:cast(SubscriberClient, stop);
        false ->
            ok
    end.

destroy_clients() ->
    % Tear down the subscription client (if this role has one)
    case has_subscriber() of
        true ->
            {_SubscriberName, SubscriberClient} = persistent_term:get({?MODULE, subscription_client}),
            supervisor:terminate_child(?CLIENT_SUPERVISOR, SubscriberClient),
            persistent_term:erase({?MODULE, subscription_client});
        false ->
            ok
    end,

    % Tear down the publication clients (if this role has them)
    case has_publishers() of
        true ->
            ClientList = persistent_term:get({?MODULE, device_clients}),
            DestroyFunction = fun({_ClientName, ClientPid}) -> supervisor:terminate_child(?CLIENT_SUPERVISOR, ClientPid) end,
            lists:foreach(DestroyFunction, ClientList),
            persistent_term:erase({?MODULE, device_clients});
        false ->
            ok
    end.

print_clients() ->
    case has_subscriber() of
        true ->
            {SubscriberName, SubscriberClient} = persistent_term:get({?MODULE, subscription_client}),
            ps_bench_utils:log_message("~p - ~p", [SubscriberName, SubscriberClient]);
        false ->
            ok
    end,

    case has_publishers() of
        true ->
            ClientList = persistent_term:get({?MODULE, device_clients}),
            ps_bench_utils:log_state_change("All Clients"),
            PrintFunction = fun({ClientName, ClientPid}) -> ps_bench_utils:log_message("~p - ~p", [ClientName, ClientPid]) end,
            lists:foreach(PrintFunction, ClientList);
        false ->
            ok
    end.
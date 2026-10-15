% Environment vars
-define(ENV_NODE_NAME, node_name).
-define(ENV_DEVICE_DEF_DIR, device_definitions_directory).
-define(ENV_DEPLOYMENT_DEF_DIR, deployment_definitions_directory).
-define(ENV_SCENARIO_DEF_DIR, scenario_definitions_directory).
-define(ENV_SELECTED_SCENARIO, selected_scenario).
-define(ENV_REQ_KEY_LIST, [?ENV_NODE_NAME, ?ENV_DEVICE_DEF_DIR, ?ENV_DEPLOYMENT_DEF_DIR, ?ENV_SCENARIO_DEF_DIR, ?ENV_SELECTED_SCENARIO]).

% Cookie for connections
-define(PS_BENCH_COOKIE, psmark_cookie).

% Wildcard string for device file searching
-define(DEVICE_FILE_EXT, "**/*.device").
-define(DEPLOYMENT_FILE_EXT, "**/*.deployment").
-define(SCENARIO_FILE_EXT, "**/*.scenario").

% Device file fields
-define(DEVICE_TYPE_PROP, type).
-define(DEVICE_PUB_FREQ_PROP, publication_frequency_ms).
-define(DEVICE_SIZE_MEAN_PROP, payload_bytes_mean).
-define(DEVICE_SIZE_VARIANCE_PROP, payload_bytes_variance).
-define(DEVICE_DISCON_CHECK_MS_PROP, disconnect_check_period_ms).
-define(DEVICE_DISCON_PCT_PROP, disconnect_chance_pct).
-define(DEVICE_RECON_CHECK_MS_PROP, reconnect_check_period_ms).
-define(DEVICE_RECON_PCT_PROP, reconnect_chance_pct).
-define(DEVICE_KEY_LIST, [?DEVICE_TYPE_PROP, ?DEVICE_PUB_FREQ_PROP, ?DEVICE_SIZE_MEAN_PROP, ?DEVICE_SIZE_VARIANCE_PROP,
                            ?DEVICE_DISCON_CHECK_MS_PROP, ?DEVICE_DISCON_PCT_PROP, ?DEVICE_RECON_CHECK_MS_PROP, ?DEVICE_RECON_PCT_PROP]).

% Deployment file fields
-define(DEPLOYMENT_NAME_PROP, name).
-define(DEPLOYMENT_NODES_PROP, nodes).
-define(DEPLOYMENT_DEVICES_PROP, devices).
-define(DEPLOYMENT_REQ_KEYS, [?DEPLOYMENT_NAME_PROP, ?DEPLOYMENT_NODES_PROP]).
-define(DEPLOYMENT_REQ_NODE_KEYS, [?DEPLOYMENT_DEVICES_PROP]).
% Optional per-node deployment keys (recognized, not required, no warning)
-define(DEPLOYMENT_ROLE_PROP, role).
-define(DEPLOYMENT_OPT_NODE_KEYS, [?DEPLOYMENT_ROLE_PROP]).
% Node roles: publisher (default) runs device publishers AND a subscription client
% (unchanged legacy behavior); subscriber runs ONLY a subscription client;
% publisher_only runs ONLY device publishers.
-define(DEPLOYMENT_ROLE_PUBLISHER, publisher).
-define(DEPLOYMENT_ROLE_SUBSCRIBER, subscriber).
-define(DEPLOYMENT_ROLE_PUBLISHER_ONLY, publisher_only).

% Scenario file fields
-define(SCENARIO_NAME_PROP, name).
-define(SCENARIO_DURATION_PROP, duration).
-define(SCENARIO_PROTOCOL_PROP, protocol).
-define(SCENARIO_DEPLOYMENT_NAME_PROP, deployment_name).
-define(SCENARIO_HOSTS_PROP, hosts).
-define(SCENARIO_HOST_HOSTNAME_PROP, hostname).
-define(SCENARIO_HOST_RNG_SEED_PROP, rng_seed).
-define(SCENARIO_PROTOCOL_CONFIG_PROP, protocol_config).
-define(SCENARIO_METRIC_CONFIG_PROP, metric_config).
-define(SCENARIO_REQ_KEY_LIST, [?SCENARIO_NAME_PROP, ?SCENARIO_DURATION_PROP, ?SCENARIO_PROTOCOL_PROP, ?SCENARIO_DEPLOYMENT_NAME_PROP, 
                                    ?SCENARIO_HOSTS_PROP, ?SCENARIO_PROTOCOL_CONFIG_PROP, ?SCENARIO_METRIC_CONFIG_PROP]).

% Supported protocol types
-define(MQTT_V5_PROTOCOL, mqttv5).
-define(MQTT_V311_PROTOCOL, mqttv311).
-define(DDS_PROTOCOL, dds).
-define(SUPPORTED_PROTOCOLS, [?MQTT_V5_PROTOCOL, ?MQTT_V311_PROTOCOL, ?DDS_PROTOCOL]).

% MQTT protocol_config fields
-define(MQTT_CLIENT_INTERFACE_MODULE_PROP, client_interface_module).
-define(MQTT_BROKER_IP_PROP, broker).
-define(MQTT_BROKER_PORT_PROP, port).
-define(MQTT_QOS_PROP, qos).
-define(MQTT_DEVICE_PURPOSES_PROP, device_purposes).
-define(MQTT_PURPOSE_VARIANT_COUNT_PROP, purpose_variant_count).
-define(MQTT_PURPOSE_FANOUT_MODEL_PROP, purpose_fanout_model).
% Dynamic purpose rotation: window each rotation purpose stays active (ms).
% 0 (default) disables rotation, leaving all existing scenarios unaffected.
-define(MQTT_PURPOSE_ROTATION_WINDOW_PROP, purpose_rotation_window_ms).
% Dynamic toggle purpose: the single purpose alternately ADDED to and REMOVED
% from each client's real filter every rotation window (paper SS7.3 dynamic-
% purposes pattern). Publishers toggle it on their registered MP, subscriber
% nodes on their DAP-SP, in lockstep. Unset (default) = no toggle.
-define(MQTT_ROTATION_TOGGLE_PROP, rotation_toggle_purpose).
% Per-node subscriber purpose (DAP-SP): proplist NodeName -> SP string. A value
% may be a single purpose or a '|'-joined multi-filter (paper SS2.2 consumer
% grouping). Nodes not listed fall back to "*" (match-any), preserving the
% legacy everything-subscriber behavior of all existing scenarios.
-define(MQTT_NODE_SP_PROP, node_subscription_purposes).
% Session expiry (s) for DAP clients, so operation traffic survives a reconnect
-define(DAP_SESSION_EXPIRY_SECS, 3600).
% NOTE: We do not require a client interface module as we fall back to a default one
% NOTE: We do not require a default MQTT QoS as we fall back to 0
-define(MQTT_REQ_KEY_LIST, [?MQTT_BROKER_IP_PROP, ?MQTT_BROKER_PORT_PROP]).

% MQTT constants
-define(MQTT_DEFAULT_CLIENT_INTERFACE_MODULE, ps_bench_default_mqtt_interface).
-define(MQTT_DEFAULT_QOS_PROP, default_qos).
-define(MQTT_TOPIC_PREFIX, <<"ps_bench/device/">>).

% DDS protocol_config fields
-define(DDS_NIF_MODULE_PROP, nif_module).
-define(DDS_NIF_FULL_PATH_PROP, nif_full_path).
-define(DDS_CONFIG_FILE_PATH_PROP, config_file).
-define(DDS_DOMAIN_ID_PROP, domain_id).
-define(DDS_QOS_PROFILE_PROP, qos_profile).
% NOTE: We do not require a qos_xml_file as DDS defines sensible defaults
% NOTE: We do not require a nif module as we fall back to the default one
-define(DDS_REQ_KEY_LIST, [?DDS_DOMAIN_ID_PROP]).

% DDS constants
-define(DDS_DEFAULT_NIF_MODULE, ps_bench_default_dds_interface).
-define(DDS_DEFAULT_NIF_FULL_PATH, "priv/dds_cplusplus/lib/libps_bench_default_dds_interface").
-define(DDS_DEFAULT_CONFIG_FILE_PATH, "configs/dds_configs/ps_bench_default_dds_interface.ini").
-define(DDS_DEFAULT_PROFILE, ""). % Intentionally empty
-define(DDS_TOPIC, "PS_BENCH_TOPIC").

% Interface types for reference
-define(PYTHON_INTERFACE, python).
-define(ERLANG_INTERFACE, erlang).
-define(NIF_INTERFACE, nif).

% metrics and runtime props 
-define(METRIC_STORAGE_CONSTANT, metrics).
-define(METRIC_RESULTS_DIR_PROP, output_dir).
-define(METRIC_HW_STATS_POLL_PERIOD, hw_stats_poll_period_ms).
-define(METRIC_PYTHON_ENGINE_PATH, python_metric_engine_path).
-define(METRIC_PLUGINS_PROP, metric_plugins).
% NOTE: We do not require a window, rollup period, or python path to be defined
-define(METRIC_REQ_KEY_LIST, [?METRIC_PLUGINS_PROP]).

% Set some defaults for metric calculation
-define(DEFAULT_OUT_DIR, "results").
-define(DEFAULT_PYTHON_ENGINE_PATH, "priv/py_engine").
-define(DEFAULT_PYTHON_EXECUTABLE, "python3").

% Message Atoms
-define(CONNECTED_MSG, connected).
-define(DISCONNECTED_MSG, disconnected).
-define(PUBLISH_RECV_MSG, publish_recv).
-define(SUBSCRIBED_MSG, subscribed).
-define(UNSUBSCRIBED_MSG, unsubscribed).

% DAP operation wire constants. These MUST match the broker exactly
% (mqtt-dap-mosquitto include/mosquitto/defs.h); they are NOT the Python
% GlobalDefs values, which drift (e.g. ORS is "OP_REQ" here, "ORS" there).
-define(DAP_TOPIC_OSYS, <<"$OP_SYS">>).      % op submit / status bus
-define(DAP_TOPIC_OP_REQ, <<"OP_REQ">>).     % forwarded request inbox base: OP_REQ/<client-id>
-define(DAP_TOPIC_OP_NOTIF, <<"OP_NOTIF">>). % requester notification base: OP_NOTIF/<client-id>
-define(DAP_OP_PURPOSE, <<"DAP_OP">>).       % operation-processing consent/purpose value
-define(DAP_KEY_CONSENT, <<"DAP-Allow">>).
-define(DAP_KEY_CLIENTID, <<"DAP-ClientID">>).
-define(DAP_KEY_SP, <<"DAP-SP">>).
-define(DAP_KEY_OPTYPE, <<"DAP-OpType">>).
-define(DAP_KEY_OPID, <<"DAP-OpId">>).
-define(DAP_KEY_OPTFS, <<"DAP-OpTFs">>).
-define(DAP_KEY_OPPFS, <<"DAP-OpPFs">>).
-define(DAP_KEY_STATUS, <<"DAP-Status">>).
-define(DAP_KEY_REASON, <<"DAP-Reason">>).
-define(DAP_ALLOW_ALL_FILTER, <<"*">>).      % MOSQ_DAP_ALLOW_ALL_FILTER

% ===== Operation issuance (paper SS7.3 operation cases) =====
% op_issue_interval_ms: 0 (default) disables publisher op issuance entirely.
% The paper's operation cells use 1000 (cases i/ii: every 1 s) and the
% full-dynamism cells reuse the rotation interval.
-define(MQTT_OP_ISSUE_INTERVAL_PROP, op_issue_interval_ms).
% Slow subscribers, to build broker-side queues: QoS 1 acks delayed by this many
% ms, and at most this many messages in flight to each subscriber (0 = unset)
-define(MQTT_SUB_ACK_DELAY_PROP, subscriber_ack_delay_ms).
-define(MQTT_SUB_RECEIVE_MAX_PROP, subscriber_receive_maximum).

% Operation ISSUER MODE selects WHO issues operations each interval. Both modes
% scope the operation to the ISSUER's OWN topic/purpose (DAP-OpTFs/OpPFs); they
% differ only in how many publishers issue:
%   all_publishers (default) - every connected publisher issues (matrix iii/iv);
%   single_random            - one randomly chosen publisher, pinned for the run,
%                              issues (matrix i/ii). It still scopes to its own data.
-define(MQTT_OP_ISSUER_MODE_PROP, op_issuer_mode).
-define(OP_ISSUER_MODE_ALL, all_publishers).
-define(OP_ISSUER_MODE_SINGLE, single_random).

% Operation GROUP selects WHICH operations are issued. The paper's matrix tests
% the queue-free group (cases i/iii: AUDIT/HISTORY/UPDATE) separately from the
% queue-interacting group (cases ii/iv: DELETE/RESTRICT).
-define(MQTT_OP_GROUP_PROP, op_group).
-define(OP_GROUP_NO_QUEUE, no_queue).            % AUDIT/HISTORY/UPDATE, no queue interaction
-define(OP_GROUP_QUEUE, queue).                  % DELETE/RESTRICT, queue interaction
-define(OP_GROUP_NO_QUEUE_TYPES, [<<"AUDIT">>, <<"HISTORY">>, <<"UPDATE">>]).
-define(OP_GROUP_QUEUE_TYPES, [<<"DELETE">>, <<"RESTRICT">>]).

% Records for metric storage
-define(PUB_AGG_RECORD_NAME, pub_aggregate).
-record(?PUB_AGG_RECORD_NAME, {node_name, binary_aggregate}).
#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Paper Appendix B (Table 5) full-matrix runner - BREADTH-FIRST, NATIVE.
#
# Pass-based: one rep of EVERY cell in pass 1, then a full second pass, then a
# third (NOT all reps of one cell before the next). If interrupted we keep 1-2
# reps of every cell instead of 5 of some and none of others. Each cell's
# results land in RESULTS_ROOT/pass<N>/<cell>/ as the cell completes, and a
# DONE marker makes re-runs resumable.
#
# Execution model (native): per cell, start the cell's broker as a container
# on a dedicated host port (18883 DAP / 18884 stock - the host's pre-existing
# 1883/1884 brokers stay untouched), launch one native PSMark beam per
# scenario host (-name <node>@127.0.0.1, cookie psmark_cookie), wait for the
# benchmark lifecycle to finish, tear the broker down, mark DONE.
#
# SAFETY: refuses to start unless RUN_MATRIX=1. CHECK_ONLY=1 runs the pre-run
# checks and exits. ONLY_CELL=<name> restricts a pass to a single cell.
# ============================================================================

# ---- knobs -----------------------------------------------------------------
PASSES=${PASSES:-3}
ONLY_CELL=${ONLY_CELL:-}

# Purpose-density STRESS cell selector - THE single fallback constant.
# 100x sits exactly AT the DAP broker's 100-filter SP limit. Fall back by
# setting BOTH constants to the 20x then 10x pair (DAP cell and its
# topic-encoded baseline move together).
STRESS_CELL=${STRESS_CELL:-dap_stress_purpose}               # dap_stress_purpose_20x | dap_stress_purpose_10x
TOPIC_STRESS_CELL=${TOPIC_STRESS_CELL:-topic_stress_purpose} # topic_stress_20x_purpose | topic_stress_10x_purpose

# ---- paths / images ----------------------------------------------------------
RUN_DIR="$(pwd)"
SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PS_BENCH_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)
CONTAINER_CONFIG_DIR=$(cd -- "${PS_BENCH_DIR}/../container_configs" && pwd)
CONFIG_ROOT="${PS_BENCH_DIR}/configs/builtin-test-suites"
SCEN_ROOT="${CONFIG_ROOT}/testcases/experiments"
RESULTS_ROOT="${RUN_DIR}/results/paper_matrix"
PROGRESS_LOG="${RESULTS_ROOT}/matrix_progress.log"
CONTAINER_METRICS_PATH="/tmp/dap_broker_metrics.csv"
CONTAINER_STATS_PATH="/tmp/dap_broker_stats.csv"

DAP_BROKER_IMAGE="mqtt-dap-mosquitto:develop"      # built from mqtt-dap-mosquitto develop (>= 0ba79c81)
STOCK_BROKER_IMAGE="eclipse-mosquitto:2"
DAP_HOST_PORT=18883                                 # matches {port, 18883} in DAP scenarios
STOCK_HOST_PORT=18884                               # matches {port, 18884} in baseline scenarios
BROKER_NAME="psmark-matrix-broker"                  # ONLY container this script ever stops
COOKIE="psmark_cookie"                              # must equal ?PS_BENCH_COOKIE
GRACE_SECS=300                                      # connect+init+metrics overhead beyond scenario duration

DOCKER_STATS_HEADER="TIME,NAME,CPU %,MEM USAGE / LIMIT,MEM %,NET I/O,BLOCK I/O"
DOCKER_DB_HEADER="TIME,DAP DB BYTES"
DOCKER_STATS_CMD="docker container stats --no-stream --format {{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}} ${BROKER_NAME}"
DATABASE_CMD="sqlite3"
DATABASE_PATH="/tmp/mosquitto.sqlite3"
DATABASE_SIZE_QUERY="select SUM(\"pgsize\") FROM \"dbstat\" WHERE name LIKE 'dap_%';"
DOCKER_DATABASE_CMD="docker exec -it $BROKER_NAME $DATABASE_CMD $DATABASE_PATH \"$DATABASE_SIZE_QUERY\""

# ---- the matrix (Appendix B, Table 5; 55 cells) ------------------------------
# group:scenario_name - dap = DAP broker on 18883, stock = stock broker on 18884.
# The DAP 1x purpose datapoint REUSES dap_static_base (steady-state 40x1); it is
# deliberately not a separate cell.
CELLS=(
  # 1. Steady-State Scalability (8)
  "dap:dap_static_base"        "stock:baseline_base"
  "dap:dap_static_2xdev"       "stock:baseline_2xdev"
  "dap:dap_static_2srv"        "stock:baseline_2srv"
  "dap:dap_static_2srv_2x"     "stock:baseline_2srv_2xdev"
  # 2. Purpose Scalability (7; + 1x reuse of dap_static_base)
  "stock:baseline_topic_purposes"
  "dap:dap_double_purpose"     "stock:topic_double_purpose"
  "dap:dap_triple_purpose"     "stock:topic_triple_purpose"
  "dap:${STRESS_CELL}"         "stock:${TOPIC_STRESS_CELL}"
  # 3. Operation Impact (16: 4 cases x 4 scalings)
  "dap:dap_ops_i_audit_single"   "dap:dap_ops_i_audit_single_2xdev"   "dap:dap_ops_i_audit_single_2srv"   "dap:dap_ops_i_audit_single_2srv_2x"
  "dap:dap_ops_ii_delete_single" "dap:dap_ops_ii_delete_single_2xdev" "dap:dap_ops_ii_delete_single_2srv" "dap:dap_ops_ii_delete_single_2srv_2x"
  "dap:dap_ops_iii_audit_all"    "dap:dap_ops_iii_audit_all_2xdev"    "dap:dap_ops_iii_audit_all_2srv"    "dap:dap_ops_iii_audit_all_2srv_2x"
  "dap:dap_ops_iv_delete_all"    "dap:dap_ops_iv_delete_all_2xdev"    "dap:dap_ops_iv_delete_all_2srv"    "dap:dap_ops_iv_delete_all_2srv_2x"
  # 4. Dynamic Purpose Changes (12: 3 intervals x 4 scalings)
  "dap:dap_dynamic_1s"  "dap:dap_dynamic_1s_2xdev"  "dap:dap_dynamic_1s_2srv"  "dap:dap_dynamic_1s_2srv_2x"
  "dap:dap_dynamic_5s"  "dap:dap_dynamic_5s_2xdev"  "dap:dap_dynamic_5s_2srv"  "dap:dap_dynamic_5s_2srv_2x"
  "dap:dap_dynamic_10s" "dap:dap_dynamic_10s_2xdev" "dap:dap_dynamic_10s_2srv" "dap:dap_dynamic_10s_2srv_2x"
  # 5. Full Dynamism (12: 3 intervals x 4 scalings)
  "dap:dap_full_dynamism_1s"  "dap:dap_full_dynamism_1s_2xdev"  "dap:dap_full_dynamism_1s_2srv"  "dap:dap_full_dynamism_1s_2srv_2x"
  "dap:dap_full_dynamism_5s"  "dap:dap_full_dynamism_5s_2xdev"  "dap:dap_full_dynamism_5s_2srv"  "dap:dap_full_dynamism_5s_2srv_2x"
  "dap:dap_full_dynamism_10s" "dap:dap_full_dynamism_10s_2xdev" "dap:dap_full_dynamism_10s_2srv" "dap:dap_full_dynamism_10s_2srv_2x"
)

# ---- helpers ----------------------------------------------------------------
log() {
  local msg="[$(date '+%F %T')] $*"
  echo "${msg}" | tee -a "${PROGRESS_LOG}"
}

scenario_file_for() {
  grep -rl "{name, $1}" "${SCEN_ROOT}" --include='*.scenario' | head -1
}

node_names_for() {  # print one node name per line from the scenario's hosts block
  erl -noshell -eval "{ok,[T]}=file:consult(\"$1\"),
    lists:foreach(fun({N,_}) -> io:format(\"~s~n\",[N]) end, proplists:get_value(hosts,T)), halt(0)."
}

duration_secs_for() {  # scenario {duration,{N,Units}} -> seconds
  erl -noshell -eval "{ok,[T]}=file:consult(\"$1\"),
    {N,U}=proplists:get_value(duration,T),
    S=case U of seconds -> N; minutes -> N*60; hours -> N*3600; milliseconds -> N div 1000 end,
    io:format(\"~b~n\",[S]), halt(0)."
}

broker_port_for() { case "$1" in dap) echo "${DAP_HOST_PORT}";; *) echo "${STOCK_HOST_PORT}";; esac }

start_broker() {  # start_broker <group>
  local group="$1"
  case "${group}" in
    dap)
      docker run -d --rm --name "${BROKER_NAME}" -p "127.0.0.1:${DAP_HOST_PORT}:1883" \
        -v "${CONTAINER_CONFIG_DIR}/mosquitto-dap.conf:/etc/mosquitto/mosquitto.conf:ro" \
        "${DAP_BROKER_IMAGE}" >/dev/null ;;
    stock)
      docker run -d --rm --name "${BROKER_NAME}" -p "127.0.0.1:${STOCK_HOST_PORT}:1883" \
        -v "${CONTAINER_CONFIG_DIR}/mosquitto-stock.conf:/mosquitto/config/mosquitto.conf:ro" \
        "${STOCK_BROKER_IMAGE}" >/dev/null ;;
  esac
  # wait for the listener (30 s)
  local port; port=$(broker_port_for "${group}")
  for _ in $(seq 1 30); do
    if (echo > "/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

stop_broker() {  # stop_broker <cell_dir> ; capture logs first, record teardown status
  local cell_dir="$1"
  docker logs "${BROKER_NAME}" > "${cell_dir}/broker.log" 2>&1 || true
  # The DAP broker is known to segfault on shutdown (exit 139). That is a
  # teardown-time crash AFTER data collection; record it, never fail the cell
  # transition on it - just make sure the container and port are really gone.
  local rc=0
  docker stop -t 10 "${BROKER_NAME}" >/dev/null 2>&1 || rc=$?
  docker kill "${BROKER_NAME}" >/dev/null 2>&1 || true
  for _ in $(seq 1 15); do
    docker inspect "${BROKER_NAME}" >/dev/null 2>&1 || break
    sleep 1
  done
  if docker inspect "${BROKER_NAME}" >/dev/null 2>&1; then
    log "ERROR: broker container ${BROKER_NAME} would not die"; return 1
  fi
  echo "stop_rc=${rc}" >> "${cell_dir}/broker_teardown.txt"
  return 0
}

# ---- native cell-execution core ----------------------------------------------
# One PSMark beam per scenario host: -name <node>@127.0.0.1 matches the
# scenarios' 127.0.0.1 hostnames, so fetch_node_list's <node>@<host> atoms equal
# the real node names and the lifecycle barriers connect. Each node gets its own
# workdir (own sys.config, out/, results/, Mnesia.<node> dir - no collisions).
run_cell_native() {  # run_cell_native <group> <cell> <cell_dir> -> 0 ok / 1 fail
  local group="$1" cell="$2" cell_dir="$3" pass="$4"
  local scenario_file scenario_dir nodes dur deadline
  scenario_file=$(scenario_file_for "${cell}")
  mapfile -t nodes < <(node_names_for "${scenario_file}")
  dur=$(duration_secs_for "${scenario_file}")

  # Refuse if any of our node names is already alive in epmd (stale crashed
  # run) - we will NOT kill processes we did not start this cell.
  local stale=""
  for n in "${nodes[@]}"; do
    if epmd -names 2>/dev/null | grep -q "^name ${n} at"; then stale="${stale} ${n}"; fi
  done
  if [ -n "${stale}" ]; then
    log "cell ${cell}: FAIL - stale epmd registrations:${stale} (clean them up first)"
    return 1
  fi

  local ebins=( "${PS_BENCH_DIR}"/_build/default/lib/*/ebin )
  local pids=() pid n workdir
  for n in "${nodes[@]}"; do
    workdir="${cell_dir}/nodes/${n}"
    mkdir -p "${workdir}/out"
    cat > "${workdir}/sys.config" <<EOF
[
  {kernel, [{logger, [{handler, default, logger_std_h,
                       #{config => #{file => "out/psbench.csv"}}}]}]},
  {ps_bench, [
    {node_name, ${n}},
    {device_definitions_directory,     "${CONFIG_ROOT}/devices"},
    {deployment_definitions_directory, "${CONFIG_ROOT}/deployments"},
    {scenario_definitions_directory,   "${SCEN_ROOT}"},
    {selected_scenario, ${cell}}
  ]}
].
EOF
    ( cd "${workdir}" && exec erl -pa "${ebins[@]}" \
        -name "${n}@127.0.0.1" -setcookie "${COOKIE}" \
        -kernel inet_dist_listen_min 15000 -kernel inet_dist_listen_max 15200 \
        -config sys -noshell -eval '
          case application:ensure_all_started(ps_bench) of
              {ok, _} ->
                  Wait = fun W() ->
                             timer:sleep(2000),
                             case lists:keymember(ps_bench, 1, application:which_applications()) of
                                 true  -> W();
                                 false -> halt(0)
                             end
                         end,
                  Wait();
              Err ->
                  io:format("START_FAILED ~p~n", [Err]),
                  halt(1)
          end.' ) > "${cell_dir}/nodes/${n}/node.log" 2>&1 &
    pids+=($!)
  done
  log "cell ${cell}: spawned ${#nodes[@]} nodes (${nodes[*]}), duration ${dur}s"

  # Wait for every beam, bounded by duration + grace; on timeout kill OUR pids.
  deadline=$(( $(date +%s) + dur + GRACE_SECS ))
  local fail=0 alive

  echo $DOCKER_STATS_HEADER > "${cell_dir}/broker_container_stats_${cell}_${pass}.csv"
  echo $DOCKER_DB_HEADER > "${cell_dir}/broker_db_stats_${cell}_${pass}.csv"
  while :; do

    CURR_DATE=`date +%s`

    # Take docker stats
    echo "$CURR_DATE,`$DOCKER_STATS_CMD`" > temp_record.csv && cat temp_record.csv >> "${cell_dir}/broker_container_stats_${cell}_${pass}.csv"

    # Take DB stats
    echo "$CURR_DATE,`eval $DOCKER_DATABASE_CMD`" > temp_record.csv && cat temp_record.csv >> "${cell_dir}/broker_db_stats_${cell}_${pass}.csv"

    alive=0
    for pid in "${pids[@]}"; do kill -0 "${pid}" 2>/dev/null && alive=$((alive+1)); done
    [ "${alive}" -eq 0 ] && break
    if [ "$(date +%s)" -ge "${deadline}" ]; then
      log "cell ${cell}: TIMEOUT after $((dur + GRACE_SECS))s with ${alive} nodes still up - killing them"
      for pid in "${pids[@]}"; do kill "${pid}" 2>/dev/null || true; done
      sleep 5
      for pid in "${pids[@]}"; do kill -9 "${pid}" 2>/dev/null || true; done
      fail=1; break
    fi
    sleep 1
  done

  docker cp ${BROKER_NAME}:${CONTAINER_METRICS_PATH} "${cell_dir}/broker_internal_metrics_${cell}_${pass}.csv"
  docker cp ${BROKER_NAME}:${CONTAINER_STATS_PATH} "${cell_dir}/broker_internal_stats_${cell}_${pass}.csv"

  for pid in "${pids[@]}"; do
    if ! wait "${pid}"; then fail=1; fi
  done
  return "${fail}"
}

# ---- guard rails ------------------------------------------------------------
if [ "${RUN_MATRIX:-0}" != "1" ] && [ "${CHECK_ONLY:-0}" != "1" ]; then
  cat >&2 <<'EOF'
REFUSING TO RUN: set RUN_MATRIX=1 to launch (optionally ONLY_CELL=<name>),
or CHECK_ONLY=1 to validate without starting anything.
EOF
  exit 2
fi

mkdir -p "${RESULTS_ROOT}"
touch "${PROGRESS_LOG}"

# Pre-run checks - fail closed BEFORE pass 1.
command -v erl >/dev/null || { echo "PRE-RUN FAIL: erl not on PATH" >&2; exit 1; }
ls "${PS_BENCH_DIR}"/_build/default/lib/ps_bench/ebin/ps_bench_app.beam >/dev/null 2>&1 \
  || { echo "PRE-RUN FAIL: ps_bench not compiled (run BUILD_WITHOUT_QUIC=1 rebar3 compile)" >&2; exit 1; }
for img in "${DAP_BROKER_IMAGE}" "${STOCK_BROKER_IMAGE}"; do
  docker image inspect "${img}" >/dev/null 2>&1 \
    || { echo "PRE-RUN FAIL: broker image '${img}' not found locally" >&2; exit 1; }
done
for conf in mosquitto-dap.conf mosquitto-stock.conf; do
  [ -f "${CONTAINER_CONFIG_DIR}/${conf}" ] \
    || { echo "PRE-RUN FAIL: missing ${CONTAINER_CONFIG_DIR}/${conf}" >&2; exit 1; }
done
for p in "${DAP_HOST_PORT}" "${STOCK_HOST_PORT}"; do
  if (echo > "/dev/tcp/127.0.0.1/${p}") 2>/dev/null; then
    echo "PRE-RUN FAIL: host port ${p} already in use" >&2; exit 1
  fi
done
if docker inspect "${BROKER_NAME}" >/dev/null 2>&1; then
  echo "PRE-RUN FAIL: container ${BROKER_NAME} already exists (previous run not cleaned up?)" >&2; exit 1
fi
for entry in "${CELLS[@]}"; do
  cell="${entry#*:}"
  f=$(scenario_file_for "${cell}") || true
  [ -n "${f}" ] || { echo "PRE-RUN FAIL: no scenario file declares {name, ${cell}}" >&2; exit 1; }
done

if [ "${CHECK_ONLY:-0}" = "1" ]; then
  echo "PRE-RUN CHECKS PASSED (check-only): ${#CELLS[@]} cells resolved, broker images + confs present, ports free, erl + beams ready. Nothing was started."
  exit 0
fi

# ---- breadth-first matrix -----------------------------------------------------
cleanup() { docker stop -t 5 "${BROKER_NAME}" >/dev/null 2>&1 || true; }
trap cleanup EXIT

for pass in $(seq 1 "${PASSES}"); do
  log "=== PASS ${pass}/${PASSES} START ==="
  for entry in "${CELLS[@]}"; do
    group="${entry%%:*}"; cell="${entry#*:}"
    if [ -n "${ONLY_CELL}" ] && [ "${cell}" != "${ONLY_CELL}" ]; then continue; fi
    cell_dir="${RESULTS_ROOT}/pass${pass}/${cell}"
    if [ -f "${cell_dir}/DONE" ]; then
      log "pass ${pass} cell ${cell}: already DONE, skipping (resume)"
      continue
    fi
    mkdir -p "${cell_dir}"

    log "pass ${pass} cell ${cell} (${group}): broker up"
    if ! start_broker "${group}"; then
      log "pass ${pass} cell ${cell}: FAIL - broker did not come up; continuing with next cell"
      stop_broker "${cell_dir}" || true
      continue
    fi

    log "pass ${pass} cell ${cell}: RUN START"
    cell_rc=0
    run_cell_native "${group}" "${cell}" "${cell_dir}" "${pass}" || cell_rc=$?

    stop_broker "${cell_dir}" || { log "pass ${pass} cell ${cell}: broker teardown FAILED - aborting run"; exit 1; }

    if [ "${cell_rc}" -ne 0 ]; then
      log "pass ${pass} cell ${cell}: FAILED (see ${cell_dir}/nodes/*/node.log) - continuing with next cell"
      continue   # breadth-first: a bad cell must not starve the rest of the pass
    fi
    date '+%F %T' > "${cell_dir}/DONE"
    log "pass ${pass} cell ${cell}: DONE"
  done
  log "=== PASS ${pass}/${PASSES} COMPLETE ==="
done

log "Matrix run finished."

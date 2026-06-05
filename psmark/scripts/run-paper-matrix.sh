#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Paper SS7.3 full-matrix runner - BREADTH-FIRST / round-robin ordering.
#
# Pass-based: one rep of EVERY cell in pass 1, then a full second pass, then a
# third (NOT all reps of one cell before the next). If the run is interrupted
# we keep 1-2 reps of every cell instead of 5 of some and none of others.
# Each cell's results are moved into RESULTS_ROOT/pass<N>/<cell>/ as soon as
# the cell completes, and a DONE marker makes re-runs resumable: already-done
# (pass, cell) pairs are skipped, so a mid-run stop leaves a usable partial
# matrix that a re-invocation finishes.
#
# SAFETY: this script refuses to start unless RUN_MATRIX=1 is set explicitly.
# ============================================================================

# ---- knobs -----------------------------------------------------------------
PASSES=${PASSES:-3}

# Purpose-density STRESS cell selector - THE single fallback constant.
# Target is 100x per Christian. If PSMark/the broker cannot handle that density
# (crash, OOM, mass disconnect - note the broker's 100-filter SP limit is
# EXACTLY met at 100x), fall back by setting these to the 20x and then the 10x
# pair. Both sides (DAP cell and its topic-encoded baseline) must move together.
STRESS_CELL=${STRESS_CELL:-dap_stress_purpose}             # dap_stress_purpose_20x | dap_stress_purpose_10x
TOPIC_STRESS_CELL=${TOPIC_STRESS_CELL:-topic_stress_purpose} # topic_stress_20x_purpose | topic_stress_10x_purpose

# ---- paths -----------------------------------------------------------------
RUN_DIR="$(pwd)"
SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PS_BENCH_DIR=$(cd -- "${SCRIPT_DIR}/.." && pwd)
COMPOSE_DIR=$(cd -- "${PS_BENCH_DIR}/../container_configs/docker_files/compose_yamls" && pwd)
CONTAINER_CONFIG_DIR=$(cd -- "${PS_BENCH_DIR}/../container_configs" && pwd)
SCEN_ROOT="${PS_BENCH_DIR}/configs/builtin-test-suites/testcases/experiments"
RESULTS_ROOT="${RUN_DIR}/results/paper_matrix"
PROGRESS_LOG="${RESULTS_ROOT}/matrix_progress.log"

# Broker stacks per cell group. The DAP compose file does NOT exist yet (the
# DAP broker is built from mqtt-dap-mosquitto develop); creating it is a
# pre-run prerequisite this script checks for instead of guessing around.
STOCK_COMPOSE="${COMPOSE_DIR}/docker-compose.single.mosquitto.yml"
DAP_COMPOSE="${COMPOSE_DIR}/docker-compose.single.mosquitto-dap.yml"

# ---- the matrix (paper order; "group" selects the broker stack) ------------
# group:scenario_name  - groups: dap (DAP broker, scenarios use port 1883),
#                                 stock (baseline broker, scenarios use port 1884)
CELLS=(
  # Steady-state scalings: DAP vs topic-only stock MQTT baseline
  "dap:dap_static_base"        "stock:baseline_base"
  "dap:dap_static_2xdev"       "stock:baseline_2xdev"
  "dap:dap_static_2srv"        "stock:baseline_2srv"
  "dap:dap_static_2srv_2x"     "stock:baseline_2srv_2xdev"
  # Purpose scaling: DAP vs topic-encoded MQTT baseline
  #   1x DAP cell IS dap_static_base (not duplicated);
  #   1x topic-encoded baseline is baseline_topic_purposes (topic_base_purpose.scenario)
  "stock:baseline_topic_purposes"
  "dap:dap_double_purpose"     "stock:topic_double_purpose"
  "dap:dap_triple_purpose"     "stock:topic_triple_purpose"
  "dap:${STRESS_CELL}"         "stock:${TOPIC_STRESS_CELL}"
  # Operations cases i-iv (DAP-only - baselines have no operation concept)
  "dap:dap_ops_i_audit_single"
  "dap:dap_ops_ii_delete_single"
  "dap:dap_ops_iii_audit_all"
  "dap:dap_ops_iv_delete_all"
  # Dynamic purposes 1/5/10 s (DAP-only)
  "dap:dap_dynamic_1s" "dap:dap_dynamic_5s" "dap:dap_dynamic_10s"
  # Full dynamism 1/5/10 s (DAP-only)
  "dap:dap_full_dynamism_1s" "dap:dap_full_dynamism_5s" "dap:dap_full_dynamism_10s"
)

# ---- helpers ----------------------------------------------------------------
log() {
  local msg="[$(date '+%F %T')] $*"
  echo "${msg}" | tee -a "${PROGRESS_LOG}"
}

scenario_file_for() {
  # Locate the scenario file declaring {name, <cell>} under experiments/.
  grep -rl "{name, $1}" "${SCEN_ROOT}" --include='*.scenario' | head -1
}

compose_for_group() {
  case "$1" in
    dap)   echo "${DAP_COMPOSE}" ;;
    stock) echo "${STOCK_COMPOSE}" ;;
    *)     echo "" ;;
  esac
}

# ---- guard rails ------------------------------------------------------------
if [ "${RUN_MATRIX:-0}" != "1" ]; then
  cat >&2 <<'EOF'
REFUSING TO RUN: set RUN_MATRIX=1 to launch the full paper matrix.
This script is the breadth-first matrix runner; it was authored ahead of the
go-decision. Review the cell list, the broker stacks (the DAP compose file
must exist), the port plan (scenarios use 1883/1884 - check for collisions
with any broker already on those ports), and the STRESS_CELL density first.
EOF
  exit 2
fi

mkdir -p "${RESULTS_ROOT}"
touch "${PROGRESS_LOG}"

# Pre-run checks: every cell must resolve to a scenario file, and every group's
# compose stack must exist. Fail closed BEFORE pass 1 rather than mid-matrix.
declare -A SEEN_GROUPS=()
for entry in "${CELLS[@]}"; do
  group="${entry%%:*}"; cell="${entry#*:}"
  f=$(scenario_file_for "${cell}") || true
  if [ -z "${f}" ]; then
    echo "PRE-RUN FAIL: no scenario file declares {name, ${cell}}" >&2
    exit 1
  fi
  SEEN_GROUPS["${group}"]=1
done
for group in "${!SEEN_GROUPS[@]}"; do
  cf=$(compose_for_group "${group}")
  if [ -z "${cf}" ] || [ ! -f "${cf}" ]; then
    echo "PRE-RUN FAIL: broker compose for group '${group}' missing: ${cf:-<unset>}" >&2
    echo "(the DAP broker stack must be created from mqtt-dap-mosquitto develop)" >&2
    exit 1
  fi
done

# ---- breadth-first matrix ----------------------------------------------------
current_compose=""
cleanup() {
  if [ -n "${current_compose}" ]; then
    (cd "${PS_BENCH_DIR}" && docker compose -f "${current_compose}" down --remove-orphans >/dev/null 2>&1) || true
  fi
}
trap cleanup EXIT

for pass in $(seq 1 "${PASSES}"); do
  log "=== PASS ${pass}/${PASSES} START ==="
  for entry in "${CELLS[@]}"; do
    group="${entry%%:*}"; cell="${entry#*:}"
    cell_dir="${RESULTS_ROOT}/pass${pass}/${cell}"
    if [ -f "${cell_dir}/DONE" ]; then
      log "pass ${pass} cell ${cell}: already DONE, skipping (resume)"
      continue
    fi
    mkdir -p "${cell_dir}"

    scenario_file=$(scenario_file_for "${cell}")
    scenario_dir=$(dirname "${scenario_file}")
    rel_dir="${scenario_dir#${PS_BENCH_DIR}}"
    container_dir="/app${rel_dir}"
    compose_file=$(compose_for_group "${group}")

    cp "${compose_file}" "${RUN_DIR}/"
    run_compose_file="${RUN_DIR}/$(basename "${compose_file}")"
    if [ "${group}" = "stock" ]; then
      cp "${CONTAINER_CONFIG_DIR}/mosquitto.conf" "${RUN_DIR}/"
    fi

    run_tag="${cell}_pass${pass}"
    log "pass ${pass} cell ${cell} (${group}): RUN START"
    current_compose="${run_compose_file}"
    if ! (SCEN_DIR="${container_dir}" \
          SCENARIO="${cell}" \
          RUN_TAG="${run_tag}" \
          docker compose -f "${run_compose_file}" up --force-recreate --abort-on-container-exit \
            > "${cell_dir}/run.log" 2>&1)
    then
      log "pass ${pass} cell ${cell}: FAILED (see ${cell_dir}/run.log) - continuing with next cell"
      docker compose -f "${run_compose_file}" down --remove-orphans >/dev/null 2>&1 || true
      current_compose=""
      continue   # breadth-first: a bad cell must not starve the rest of the pass
    fi
    docker compose -f "${run_compose_file}" down --remove-orphans >/dev/null 2>&1 || true
    current_compose=""

    # Persist this cell's results IMMEDIATELY so an interrupt after this point
    # still leaves a usable partial matrix.
    if [ -d "${RUN_DIR}/results" ]; then
      find "${RUN_DIR}/results" -mindepth 1 -maxdepth 1 -name "*${run_tag}*" -exec mv {} "${cell_dir}/" \; 2>/dev/null || true
    fi
    date '+%F %T' > "${cell_dir}/DONE"
    log "pass ${pass} cell ${cell}: DONE"
  done
  log "=== PASS ${pass}/${PASSES} COMPLETE ==="
done

log "Matrix complete: ${PASSES} passes over ${#CELLS[@]} cells."

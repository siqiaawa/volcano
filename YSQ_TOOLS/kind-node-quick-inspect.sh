#!/usr/bin/env bash

# Inspect one retained Kind cluster without printing its raw logs.

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=kind-diagnostic-lib.sh
source "${script_dir}/kind-diagnostic-lib.sh"

usage() {
  printf 'Usage: %s KIND_CLUSTER_NAME\n' "$(basename "$0")"
  printf 'Example: %s volcano-kind-debug-20260722-114209\n' "$(basename "$0")"
}

if (($# != 1)); then
  usage >&2
  exit 64
fi

cluster_name="$1"
if [[ ! "${cluster_name}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  printf 'RESULT_CODE=INVALID_CLUSTER_NAME\n'
  exit 64
fi

node_container="${cluster_name}-control-plane"
if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  printf '%s\n' '=== SAFE_NODE_EXIT_RESULT ==='
  printf 'RESULT_CODE=DOCKER_UNREACHABLE\n'
  printf 'NEXT_ACTION=CHECK_DOCKER_DAEMON\n'
  printf '%s\n' '=== END_SAFE_NODE_EXIT_RESULT ==='
  exit 1
fi

if ! docker inspect "${node_container}" >/dev/null 2>&1; then
  printf '%s\n' '=== SAFE_NODE_EXIT_RESULT ==='
  printf 'RESULT_CODE=NODE_NOT_FOUND\n'
  printf 'NEXT_ACTION=CHECK_CLUSTER_NAME\n'
  printf '%s\n' '=== END_SAFE_NODE_EXIT_RESULT ==='
  exit 1
fi

temp_log="$(mktemp)"
trap 'rm -f -- "${temp_log}"' EXIT
docker logs "${node_container}" >"${temp_log}" 2>&1 || true

container_state="$(docker inspect "${node_container}" --format '{{.State.Status}}' 2>/dev/null || true)"
container_exit="$(docker inspect "${node_container}" --format '{{.State.ExitCode}}' 2>/dev/null || true)"
oom_killed="$(docker inspect "${node_container}" --format '{{.State.OOMKilled}}' 2>/dev/null || true)"
node_log_driver="$(docker inspect "${node_container}" --format '{{.HostConfig.LogConfig.Type}}' 2>/dev/null || true)"
host_cgroup_version="$(docker info --format '{{.CgroupVersion}}' 2>/dev/null || true)"
docker_log_lines="$(wc -l <"${temp_log}" | tr -d '[:space:]')"

classify_kind_node_log "${temp_log}"
if [[ "${oom_killed}" == "true" ]]; then
  KIND_LOG_SIGNATURE="OOM_KILLED"
  KIND_LOG_RESULT_CODE="NODE_OOM"
  KIND_LOG_NEXT_ACTION="CHECK_HOST_MEMORY"
elif [[ "${container_state}" == "running" && "${KIND_LOG_SIGNATURE}" == "UNCLASSIFIED_LOG" ]]; then
  KIND_LOG_RESULT_CODE="NODE_STILL_RUNNING"
  KIND_LOG_NEXT_ACTION="RUN_MAIN_READINESS_DIAGNOSTIC"
fi

printf '%s\n' '=== SAFE_NODE_EXIT_RESULT ==='
printf 'RESULT_CODE=%s\n' "${KIND_LOG_RESULT_CODE}"
printf 'ERROR_SIGNATURE=%s\n' "${KIND_LOG_SIGNATURE}"
printf 'CONTAINER_STATE=%s\n' "${container_state:-unknown}"
printf 'CONTAINER_EXIT=%s\n' "${container_exit:-unknown}"
printf 'OOM_KILLED=%s\n' "${oom_killed:-unknown}"
printf 'NODE_LOG_DRIVER=%s\n' "${node_log_driver:-unknown}"
printf 'DOCKER_LOG_LINES=%s\n' "${docker_log_lines:-unknown}"
printf 'HOST_CGROUP_VERSION=%s\n' "${host_cgroup_version:-unknown}"
printf 'NEXT_ACTION=%s\n' "${KIND_LOG_NEXT_ACTION}"
printf 'CLEANUP_CLUSTER=%s\n' "${cluster_name}"
printf '%s\n' '=== END_SAFE_NODE_EXIT_RESULT ==='

if [[ "${KIND_LOG_RESULT_CODE}" == "NODE_STILL_RUNNING" ]]; then
  exit 0
fi
exit 1

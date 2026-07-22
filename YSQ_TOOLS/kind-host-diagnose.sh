#!/usr/bin/env bash

# Diagnose whether a Linux host can run the Kind node image used by Volcano's
# HyperNode E2E suite. The script does not change Docker daemon configuration or
# restart services. A failed smoke-test cluster is retained for investigation.

set -uo pipefail

readonly SCRIPT_VERSION="1.3.0"
readonly DEFAULT_NODE_IMAGE="kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=kind-diagnostic-lib.sh
source "${script_dir}/kind-diagnostic-lib.sh"

run_kind=1
keep_cluster=0
create_archive=1
concise_output=0
cluster_name=""
node_image="${DEFAULT_NODE_IMAGE}"
output_base="${HOME}/volcano-kind-diagnostics"

usage() {
  cat <<'EOF'
Usage: kind-host-diagnose.sh [options]

Options:
  --skip-kind              Collect host diagnostics without creating a cluster.
  --keep-cluster           Keep the debug cluster even when creation succeeds.
  --no-archive             Keep the report directory without creating a tarball.
  --concise                Hide progress and print only a short safe result block.
  --confidential           Equivalent to --no-archive --concise.
  --cluster-name NAME      Use NAME instead of an automatically generated name.
  --node-image IMAGE       Override the Kind node image used by the smoke test.
  --output-dir DIR         Store reports under DIR.
  -h, --help               Show this help.

The default smoke test creates one Kind control-plane node. If creation fails,
the node is retained so Docker and systemd logs can be collected. The script
prints the report archive path and any required cleanup command at the end.
EOF
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 64
}

while (($# > 0)); do
  case "$1" in
    --skip-kind)
      run_kind=0
      shift
      ;;
    --keep-cluster)
      keep_cluster=1
      shift
      ;;
    --no-archive)
      create_archive=0
      shift
      ;;
    --concise)
      concise_output=1
      shift
      ;;
    --confidential)
      create_archive=0
      concise_output=1
      shift
      ;;
    --cluster-name)
      (($# >= 2)) || die "--cluster-name requires a value"
      cluster_name="$2"
      shift 2
      ;;
    --node-image)
      (($# >= 2)) || die "--node-image requires a value"
      node_image="$2"
      shift 2
      ;;
    --output-dir)
      (($# >= 2)) || die "--output-dir requires a value"
      output_base="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

timestamp="$(date '+%Y%m%d-%H%M%S')"
if [[ -z "${cluster_name}" ]]; then
  cluster_name="volcano-kind-debug-${timestamp,,}"
fi
if [[ ! "${cluster_name}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  die "cluster name must contain only lowercase letters, digits, and hyphens"
fi

report_dir="${output_base%/}/kind-host-diagnostics-${timestamp}"
archive_path="${report_dir}.tar.gz"
mkdir -p "${report_dir}" || die "cannot create report directory: ${report_dir}"

summary_file="${report_dir}/summary.txt"
diagnosis_file="${report_dir}/diagnosis.txt"
safe_result_file="${report_dir}/safe-result.txt"
smoke_status="SKIPPED"
smoke_exit_code=""
cluster_retained=0
docker_ready=0
node_container=""

say() {
  if ((concise_output == 1)); then
    printf '%s\n' "$*" >>"${summary_file}"
  else
    printf '%s\n' "$*" | tee -a "${summary_file}"
  fi
}

section() {
  say ""
  say "== $* =="
}

capture() {
  local filename="$1"
  local description="$2"
  shift 2

  {
    printf 'Description: %s\n' "${description}"
    printf 'Command:'
    printf ' %q' "$@"
    printf '\n\n'
    "$@"
    local rc=$?
    printf '\nExit code: %d\n' "${rc}"
    return "${rc}"
  } >"${report_dir}/${filename}" 2>&1
}

capture_shell() {
  local filename="$1"
  local description="$2"
  local command="$3"

  {
    printf 'Description: %s\n' "${description}"
    printf 'Command: %s\n\n' "${command}"
    bash -o pipefail -c "${command}"
    local rc=$?
    printf '\nExit code: %d\n' "${rc}"
    return "${rc}"
  } >"${report_dir}/${filename}" 2>&1
}

if ((concise_output == 1)); then
  printf 'KIND_DIAG=RUNNING\n'
  printf 'WAIT_HINT=the single-node check can take up to two minutes\n'
fi

say "Volcano Kind host diagnostics"
say "Script version: ${SCRIPT_VERSION}"
say "Started: $(date --iso-8601=seconds 2>/dev/null || date)"
say "Report directory: ${report_dir}"
say "Cluster name: ${cluster_name}"
say "Node image: ${node_image}"

section "Host and resource information"
capture_shell "host.txt" "Operating system, kernel, virtualization, and identity" '
date --iso-8601=seconds 2>/dev/null || date
printf "\n-- identity --\n"
whoami
id
hostname
printf "\n-- os-release --\n"
cat /etc/os-release 2>/dev/null || true
printf "\n-- kernel --\n"
uname -a
printf "\n-- virtualization --\n"
systemd-detect-virt 2>&1 || true
printf "\n-- process 1 --\n"
ps -p 1 -o pid,comm,args 2>&1 || true
printf "\n-- CPU --\n"
nproc 2>&1 || true
printf "\n-- memory --\n"
free -h 2>&1 || true
printf "\n-- filesystem --\n"
df -hT / 2>&1 || true
df -i / 2>&1 || true
printf "\n-- limits --\n"
ulimit -a 2>&1 || true
' || true
say "Captured host.txt"

capture_shell "cgroup.txt" "Host cgroup layout and relevant kernel settings" '
printf "%s\n" "-- cgroup filesystem type --"
stat -fc %T /sys/fs/cgroup 2>&1 || true
printf "\n%s\n" "-- process 1 cgroup --"
cat /proc/1/cgroup 2>&1 || true
printf "\n%s\n" "-- cgroup mounts --"
findmnt -R /sys/fs/cgroup 2>&1 || mount | grep -E "cgroup|cgroup2" || true
printf "\n%s\n" "-- namespaces --"
ls -l /proc/1/ns 2>&1 || true
printf "\n%s\n" "-- networking sysctls --"
sysctl net.ipv4.ip_forward 2>&1 || true
sysctl net.bridge.bridge-nf-call-iptables 2>&1 || true
printf "\n%s\n" "-- kernel modules --"
lsmod 2>&1 | grep -E "^(overlay|br_netfilter)" || true
' || true
say "Captured cgroup.txt"

capture_shell "storage.txt" "Memory, Docker storage, disk space, and inode usage" '
free -h 2>&1 || true
printf "\n-- filesystems --\n"
df -hT 2>&1 || true
printf "\n-- inodes --\n"
df -i 2>&1 || true
printf "\n-- Docker root --\n"
docker_root=$(docker info --format "{{.DockerRootDir}}" 2>/dev/null || true)
printf "%s\n" "${docker_root}"
if [[ -n "${docker_root}" ]]; then
  df -hT "${docker_root}" 2>&1 || true
  df -i "${docker_root}" 2>&1 || true
fi
' || true
say "Captured storage.txt"

section "Tool versions"
{
  for tool in git go make gcc docker kind kubectl helm tar; do
    printf '\n-- %s --\n' "${tool}"
    if ! command -v "${tool}" >/dev/null 2>&1; then
      printf 'NOT FOUND\n'
      continue
    fi
    command -v "${tool}"
    case "${tool}" in
      git) git --version 2>&1 ;;
      go) go version 2>&1; go env GOTOOLCHAIN 2>&1 ;;
      make) make --version 2>&1 | head -n 2 ;;
      gcc) gcc --version 2>&1 | head -n 2 ;;
      docker) docker version 2>&1 ;;
      kind) kind version 2>&1 ;;
      kubectl) kubectl version --client 2>&1 ;;
      helm) helm version --short 2>&1 ;;
      tar) tar --version 2>&1 | head -n 2 ;;
    esac
  done
} >"${report_dir}/tool-versions.txt" 2>&1
say "Captured tool-versions.txt"

section "Docker diagnostics"
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker_ready=1
  capture_shell "docker-info.txt" "Selected Docker daemon properties" '
docker info --format "Server={{.ServerVersion}}
Storage={{.Driver}}
Logging={{.LoggingDriver}}
CgroupDriver={{.CgroupDriver}}
CgroupVersion={{.CgroupVersion}}
OS={{.OperatingSystem}}
OSType={{.OSType}}
Architecture={{.Architecture}}
Kernel={{.KernelVersion}}
DockerRoot={{.DockerRootDir}}
CPUs={{.NCPU}}
Memory={{.MemTotal}}" 2>&1
printf "\n-- security and runtime excerpt --\n"
docker info 2>&1 | grep -E "^[[:space:]]*(Server Version|Storage Driver|Logging Driver|Cgroup Driver|Cgroup Version|Kernel Version|Operating System|OSType|Architecture|Docker Root Dir|Security Options|rootless|containerd version|runc version)" || true
' || true
  capture "docker-system-df.txt" "Docker disk usage" docker system df || true
  capture "docker-containers.txt" "Existing Docker containers" docker ps -a --no-trunc || true
  say "Docker daemon is reachable"
else
  capture "docker-error.txt" "Docker daemon connectivity failure" docker info || true
  say "Docker daemon is NOT reachable; Kind smoke test cannot run"
fi

section "Kind single-node smoke test"
if ((run_kind == 0)); then
  say "Skipped by --skip-kind"
elif ((docker_ready == 0)); then
  smoke_status="FAIL"
  say "Failed prerequisite: Docker daemon is not reachable"
elif ! command -v kind >/dev/null 2>&1; then
  smoke_status="FAIL"
  say "Failed prerequisite: kind command is not installed"
elif ! command -v kubectl >/dev/null 2>&1; then
  smoke_status="FAIL"
  say "Failed prerequisite: kubectl command is not installed"
elif kind get clusters 2>/dev/null | grep -Fxq "${cluster_name}"; then
  smoke_status="FAIL"
  say "Refusing to reuse existing Kind cluster: ${cluster_name}"
else
  say "Creating one control-plane node with --retain and verbose logging"
  kind create cluster \
    --name "${cluster_name}" \
    --image "${node_image}" \
    --retain \
    --wait 120s \
    -v 9 \
    >"${report_dir}/kind-create.log" 2>&1
  smoke_exit_code=$?

  node_container="${cluster_name}-control-plane"
  if docker inspect "${node_container}" >/dev/null 2>&1; then
    capture_shell "kind-node-state.txt" "Kind node container state and logging mode" \
      "docker inspect '${node_container}' --format 'Status={{.State.Status}} ExitCode={{.State.ExitCode}} OOMKilled={{.State.OOMKilled}} Error={{.State.Error}} Started={{.State.StartedAt}} Finished={{.State.FinishedAt}} Privileged={{.HostConfig.Privileged}} Cgroupns={{.HostConfig.CgroupnsMode}} LogDriver={{.HostConfig.LogConfig.Type}}'" || true
    capture "kind-node-docker.log" "Kind node Docker stdout/stderr" docker logs --timestamps "${node_container}" || true

    node_running="$(docker inspect "${node_container}" --format '{{.State.Running}}' 2>/dev/null || true)"
    if [[ "${node_running}" == "true" ]]; then
      capture "kind-node-systemd-status.txt" "systemd state inside the Kind node" \
        docker exec "${node_container}" systemctl --no-pager --failed || true
      capture "kind-node-journal.log" "Recent systemd journal inside the Kind node" \
        docker exec "${node_container}" journalctl -b --no-pager -n 500 || true
      capture_shell "kind-node-cgroup.txt" "PID 1 and cgroup view inside the Kind node" \
        "docker exec '${node_container}' bash -c 'ps -p 1 -o pid,comm,args; echo; stat -fc %T /sys/fs/cgroup; echo; cat /proc/1/cgroup; echo; findmnt -R /sys/fs/cgroup || true'" || true
    fi
  else
    printf 'Container %s was not found after kind create.\n' "${node_container}" \
      >"${report_dir}/kind-node-state.txt"
  fi

  mkdir -p "${report_dir}/kind-export"
  kind export logs "${report_dir}/kind-export" --name "${cluster_name}" \
    >"${report_dir}/kind-export-command.log" 2>&1 || true

  if ((smoke_exit_code == 0)); then
    smoke_status="PASS"
    say "Kind smoke test passed"
    capture "kind-kubectl-nodes.txt" "Nodes in the debug cluster" \
      kubectl --context "kind-${cluster_name}" get nodes -o wide || true
    capture "kind-kubectl-pods.txt" "System Pods in the debug cluster" \
      kubectl --context "kind-${cluster_name}" get pods -A -o wide || true
    capture "kind-cluster-info.txt" "Kubernetes cluster information" \
      kubectl --context "kind-${cluster_name}" cluster-info || true

    if ((keep_cluster == 0)); then
      kind delete cluster --name "${cluster_name}" \
        >"${report_dir}/kind-delete.log" 2>&1 || true
      say "Successful debug cluster was deleted"
    else
      cluster_retained=1
      say "Successful debug cluster retained by --keep-cluster"
    fi
  else
    smoke_status="FAIL"
    cluster_retained=1
    say "Kind smoke test failed with exit code ${smoke_exit_code}"
    say "Failed debug cluster retained for inspection"
  fi
fi

section "Automated diagnosis"
logging_driver=""
virtualization=""
host_cgroup_version="unknown"
container_cgroup_fs="unknown"
container_state="unknown"
container_exit_code="unknown"
node_oom="unknown"
node_log_driver="unknown"
pid1="unknown"
systemd_state="unknown"
multi_user_state="unknown"
failed_units="unknown"
docker_log_lines="unknown"
docker_ready_lines="unknown"
journal_ready_lines="unknown"
permission_signal=0
kind_wait_log_error=0
kind_log_signature="NOT_AVAILABLE"
kind_log_result_code=""
kind_log_next_action=""
if ((docker_ready == 1)); then
  logging_driver="$(docker info --format '{{.LoggingDriver}}' 2>/dev/null || true)"
  host_cgroup_version="$(docker info --format '{{.CgroupVersion}}' 2>/dev/null || true)"
  [[ -n "${host_cgroup_version}" ]] || host_cgroup_version="unknown"
fi
virtualization="$(systemd-detect-virt 2>/dev/null || true)"
if [[ -n "${node_container}" ]] && docker inspect "${node_container}" >/dev/null 2>&1; then
  container_state="$(docker inspect "${node_container}" --format '{{.State.Status}}' 2>/dev/null || true)"
  container_exit_code="$(docker inspect "${node_container}" --format '{{.State.ExitCode}}' 2>/dev/null || true)"
  node_oom="$(docker inspect "${node_container}" --format '{{.State.OOMKilled}}' 2>/dev/null || true)"
  node_log_driver="$(docker inspect "${node_container}" --format '{{.HostConfig.LogConfig.Type}}' 2>/dev/null || true)"
  [[ -n "${container_state}" ]] || container_state="unknown"
  [[ -n "${container_exit_code}" ]] || container_exit_code="unknown"
  [[ -n "${node_oom}" ]] || node_oom="unknown"
  [[ -n "${node_log_driver}" ]] || node_log_driver="unknown"

  docker_log_lines="$(docker logs "${node_container}" 2>&1 | wc -l | tr -d '[:space:]')"
  docker_ready_lines="$(docker logs "${node_container}" 2>&1 | \
    grep -Ec 'Reached target .*Multi-User System|detected cgroup v1' || true)"
  [[ -n "${docker_log_lines}" ]] || docker_log_lines="unknown"
  [[ -n "${docker_ready_lines}" ]] || docker_ready_lines="unknown"

  node_running="$(docker inspect "${node_container}" --format '{{.State.Running}}' 2>/dev/null || true)"
  if [[ "${node_running}" == "true" ]]; then
    pid1="$(docker exec "${node_container}" ps -p 1 -o comm= 2>/dev/null | head -n 1 | tr -d '[:space:]' || true)"
    systemd_state="$(docker exec "${node_container}" systemctl is-system-running 2>/dev/null | head -n 1 | tr -d '\r' || true)"
    multi_user_state="$(docker exec "${node_container}" systemctl is-active multi-user.target 2>/dev/null | head -n 1 | tr -d '\r' || true)"
    failed_units="$(docker exec "${node_container}" systemctl --failed --no-legend --plain 2>/dev/null | \
      sed '/^[[:space:]]*$/d' | wc -l | tr -d '[:space:]')"
    journal_ready_lines="$(docker exec "${node_container}" bash -c \
      'journalctl -b -o cat --no-pager 2>/dev/null | grep -Ec "Reached target .*Multi-User System|detected cgroup v1"' \
      2>/dev/null || true)"
    container_cgroup_fs="$(docker exec "${node_container}" stat -fc %T /sys/fs/cgroup 2>/dev/null | \
      head -n 1 | tr -d '[:space:]' || true)"
    [[ -n "${pid1}" ]] || pid1="unknown"
    [[ -n "${systemd_state}" ]] || systemd_state="unknown"
    [[ -n "${multi_user_state}" ]] || multi_user_state="unknown"
    [[ -n "${failed_units}" ]] || failed_units="unknown"
    [[ -n "${journal_ready_lines}" ]] || journal_ready_lines="unknown"
    [[ -n "${container_cgroup_fs}" ]] || container_cgroup_fs="unknown"
  fi
fi
if grep -qiE 'operation not permitted|permission denied|failed to mount|read-only file system|cgroup.*(fail|error)' \
    "${report_dir}/kind-node-docker.log" "${report_dir}/kind-node-journal.log" 2>/dev/null; then
  permission_signal=1
fi
if grep -q 'could not find a log line that matches' "${report_dir}/kind-create.log" 2>/dev/null; then
  kind_wait_log_error=1
fi
if [[ "${docker_log_lines}" == "0" ]]; then
  KIND_LOG_SIGNATURE="NO_LOG_OUTPUT"
  KIND_LOG_RESULT_CODE="DOCKER_LOG_EMPTY"
  KIND_LOG_NEXT_ACTION="CHECK_DOCKER_STDOUT_PATH"
elif [[ -f "${report_dir}/kind-node-docker.log" ]]; then
  classify_kind_node_log "${report_dir}/kind-node-docker.log"
fi
if [[ -n "${KIND_LOG_SIGNATURE:-}" ]]; then
  kind_log_signature="${KIND_LOG_SIGNATURE}"
  kind_log_result_code="${KIND_LOG_RESULT_CODE}"
  kind_log_next_action="${KIND_LOG_NEXT_ACTION}"
fi

result_code="UNKNOWN_LOCAL_REVIEW"
next_action="REVIEW_LOCAL_DIAGNOSTICS"
if [[ "${smoke_status}" == "SKIPPED" ]]; then
  result_code="HOST_INFO_ONLY"
  next_action="RUN_WITHOUT_SKIP_KIND"
elif [[ "${smoke_status}" == "PASS" ]]; then
  result_code="KIND_OK"
  next_action="RUN_FULL_HYPERNODE_E2E"
elif ((docker_ready == 0)); then
  result_code="DOCKER_UNREACHABLE"
  next_action="CHECK_DOCKER_DAEMON"
elif ! command -v kind >/dev/null 2>&1 || ! command -v kubectl >/dev/null 2>&1; then
  result_code="PREREQUISITE_MISSING"
  next_action="CHECK_KIND_AND_KUBECTL"
elif [[ "${logging_driver}" == "none" ]]; then
  result_code="DOCKER_LOGGING_DISABLED"
  next_action="REVIEW_DOCKER_LOGGING_CONFIG"
elif [[ "${node_oom}" == "true" ]]; then
  result_code="NODE_OOM"
  next_action="CHECK_HOST_MEMORY"
elif [[ -n "${kind_log_result_code}" && "${kind_log_signature}" != "UNCLASSIFIED_LOG" ]]; then
  result_code="${kind_log_result_code}"
  next_action="${kind_log_next_action}"
elif [[ "${container_state}" =~ ^(exited|dead)$ ]]; then
  result_code="NODE_CONTAINER_EXITED"
  next_action="CHECK_LOCAL_NODE_DOCKER_LOG"
elif ((permission_signal == 1)); then
  result_code="CGROUP_PERMISSION"
  next_action="CHECK_HOST_CGROUP_AND_NESTING"
elif [[ "${virtualization}" =~ ^(docker|lxc|lxc-libvirt|openvz|podman|container-other)$ ]]; then
  result_code="NESTED_CONTAINER"
  next_action="USE_VM_OR_ENABLE_NESTING"
elif [[ "${container_state}" == "running" && "${pid1}" != "systemd" ]]; then
  result_code="SYSTEMD_NOT_PID1"
  next_action="CHECK_NODE_IMAGE_ENTRYPOINT"
elif [[ "${systemd_state}" =~ ^(starting|initializing)$ ]]; then
  result_code="SYSTEMD_NOT_READY"
  next_action="CHECK_LOCAL_FAILED_UNITS_AND_JOURNAL"
elif [[ "${systemd_state}" =~ ^(maintenance|emergency|offline|failed)$ ]]; then
  result_code="SYSTEMD_FAILED"
  next_action="CHECK_LOCAL_SYSTEMD_JOURNAL"
elif [[ "${multi_user_state}" =~ ^(inactive|failed|deactivating)$ ]]; then
  result_code="MULTI_USER_INACTIVE"
  next_action="CHECK_LOCAL_FAILED_UNITS_AND_JOURNAL"
elif [[ "${multi_user_state}" == "active" && "${docker_log_lines}" =~ ^[0-9]+$ ]] && \
    ((docker_log_lines == 0)); then
  result_code="DOCKER_LOG_EMPTY"
  next_action="CHECK_DOCKER_STDOUT_PATH"
elif [[ "${multi_user_state}" == "active" && "${journal_ready_lines}" =~ ^[0-9]+$ && \
    "${docker_ready_lines}" =~ ^[0-9]+$ ]] && ((journal_ready_lines > 0 && docker_ready_lines == 0)); then
  result_code="READY_LOG_NOT_EXPORTED"
  next_action="CHECK_DOCKER_STDOUT_PATH"
elif [[ "${multi_user_state}" == "active" && "${journal_ready_lines}" =~ ^[0-9]+$ && \
    "${docker_ready_lines}" =~ ^[0-9]+$ ]] && ((journal_ready_lines == 0 && docker_ready_lines == 0)); then
  result_code="READY_MARKER_MISSING"
  next_action="KIND_SYSTEMD_READINESS_INCOMPATIBLE"
elif [[ "${docker_ready_lines}" =~ ^[0-9]+$ ]] && ((docker_ready_lines > 0 && kind_wait_log_error == 1)); then
  result_code="KIND_LOG_WAIT_MISMATCH"
  next_action="CHECK_KIND_DOCKER_LOG_STREAM"
elif [[ "${systemd_state}" == "degraded" && "${failed_units}" =~ ^[0-9]+$ ]] && \
    ((failed_units > 0)); then
  result_code="SYSTEMD_DEGRADED"
  next_action="CHECK_LOCAL_FAILED_UNITS"
elif [[ "${container_state}" == "running" && "${systemd_state}" == "unknown" ]]; then
  result_code="SYSTEMD_QUERY_FAILED"
  next_action="CHECK_LOCAL_DOCKER_EXEC"
fi

{
  printf 'RESULT_CODE=%s\n' "${result_code}"
  printf 'Smoke test: %s\n' "${smoke_status}"
  [[ -n "${smoke_exit_code}" ]] && printf 'Kind exit code: %s\n' "${smoke_exit_code}"
  printf 'Docker logging driver: %s\n' "${logging_driver:-unknown}"
  printf 'Host virtualization: %s\n' "${virtualization:-unknown}"
  printf 'Node OOM state: %s\n' "${node_oom:-unknown}"
  printf 'Container state: %s\n' "${container_state}"
  printf 'Container exit code: %s\n' "${container_exit_code}"
  printf 'Node log driver: %s\n' "${node_log_driver}"
  printf 'Error signature: %s\n' "${kind_log_signature}"
  printf 'PID 1: %s\n' "${pid1}"
  printf 'systemd state: %s\n' "${systemd_state}"
  printf 'multi-user target: %s\n' "${multi_user_state}"
  printf 'Failed units: %s\n' "${failed_units}"
  printf 'Docker log lines: %s\n' "${docker_log_lines}"
  printf 'Docker readiness lines: %s\n' "${docker_ready_lines}"
  printf 'Journal readiness lines: %s\n' "${journal_ready_lines}"
  printf 'Host cgroup version: %s\n' "${host_cgroup_version}"
  printf 'Container cgroup filesystem: %s\n' "${container_cgroup_fs}"
  printf 'Next action: %s\n\n' "${next_action}"

  if [[ "${result_code}" == "HOST_INFO_ONLY" ]]; then
    printf 'Conclusion: host information was collected without running Kind.\n'
  elif [[ "${result_code}" == "KIND_OK" ]]; then
    printf 'Conclusion: this host can start the required Kind v1.36.1 node image.\n'
    printf 'If the five-node Volcano configuration still fails, inspect host memory, disk,\n'
    printf 'inode usage, and Docker limits in this report.\n'
  elif [[ "${result_code}" == "DOCKER_UNREACHABLE" ]]; then
    printf 'Likely cause: Docker is missing, stopped, or inaccessible to the current user.\n'
  elif [[ "${result_code}" == "PREREQUISITE_MISSING" ]]; then
    printf 'Likely cause: kind or kubectl is not installed or is not available in PATH.\n'
  elif [[ "${result_code}" == "DOCKER_LOGGING_DISABLED" ]]; then
    printf 'Likely cause: Docker logging driver is "none". Kind cannot observe the\n'
    printf 'systemd readiness line and reports "could not find a log line". Do not\n'
    printf 'overwrite daemon.json or restart Docker before reviewing existing settings.\n'
  elif [[ "${result_code}" == "NODE_OOM" ]]; then
    printf 'Likely cause: the Kind node was killed by the host out-of-memory mechanism.\n'
  elif [[ "${result_code}" == "NODE_CONTAINER_EXITED" ]]; then
    printf 'Likely cause: the Kind node container exited before systemd became ready.\n'
  elif [[ "${result_code}" == "CGROUP_INITIALIZATION_FAILED" ]]; then
    printf 'Likely cause: systemd could not initialize its cgroup hierarchy.\n'
  elif [[ "${result_code}" == "SYSTEMD_FATAL" ]]; then
    printf 'Likely cause: systemd encountered a fatal startup error.\n'
  elif [[ "${result_code}" == "NODE_MOUNT_PERMISSION" ]]; then
    printf 'Likely cause: the host denied a mount required by the Kind node.\n'
  elif [[ "${result_code}" == "NODE_NO_SPACE" ]]; then
    printf 'Likely cause: Docker storage or inode capacity is exhausted.\n'
  elif [[ "${result_code}" == "NODE_MEMORY_FAILURE" ]]; then
    printf 'Likely cause: the node could not allocate memory during startup.\n'
  elif [[ "${result_code}" == "NODE_ARCH_MISMATCH" ]]; then
    printf 'Likely cause: the Kind node image architecture does not match the host.\n'
  elif [[ "${result_code}" == "CGROUP_PERMISSION" ]]; then
    printf 'Likely cause: the host cannot provide the privileged mount/cgroup behavior\n'
    printf 'required by a Kind node. Check whether this server is itself a restricted\n'
    printf 'LXC, OpenVZ, Docker, or other nested-container environment.\n'
  elif [[ "${result_code}" == "NESTED_CONTAINER" ]]; then
    printf 'Likely cause: Kind is running inside a containerized server environment\n'
    printf 'without sufficient nested-container and cgroup delegation support.\n'
  elif [[ "${result_code}" == "SYSTEMD_NOT_PID1" ]]; then
    printf 'Likely cause: PID 1 in the Kind node is not systemd.\n'
  elif [[ "${result_code}" =~ ^(SYSTEMD_NOT_READY|SYSTEMD_FAILED|MULTI_USER_INACTIVE|SYSTEMD_DEGRADED)$ ]]; then
    printf 'Likely cause: systemd did not reach a healthy multi-user target.\n'
  elif [[ "${result_code}" == "DOCKER_LOG_EMPTY" ]]; then
    printf 'Likely cause: the running node produced no Docker stdout logs for Kind to match.\n'
  elif [[ "${result_code}" == "READY_LOG_NOT_EXPORTED" ]]; then
    printf 'Likely cause: systemd recorded readiness in journal, but Docker stdout did not.\n'
  elif [[ "${result_code}" == "READY_MARKER_MISSING" ]]; then
    printf 'Likely cause: multi-user is active, but neither journal nor Docker logs contain\n'
    printf 'the readiness marker expected by Kind.\n'
  elif [[ "${result_code}" == "KIND_LOG_WAIT_MISMATCH" ]]; then
    printf 'Likely cause: the readiness marker exists, but Kind did not consume it.\n'
  elif [[ "${result_code}" == "SYSTEMD_QUERY_FAILED" ]]; then
    printf 'Likely cause: the node runs, but Docker exec could not query systemd.\n'
  else
    printf 'Conclusion: no single cause was identified automatically. Review, in order:\n'
    printf '1. kind-node-state.txt\n'
    printf '2. kind-node-docker.log\n'
    printf '3. kind-node-journal.log\n'
    printf '4. docker-info.txt, cgroup.txt, host.txt, and storage.txt\n'
  fi

  if ((cluster_retained == 1)); then
    printf '\nCleanup command after investigation:\n'
    printf 'kind delete cluster --name %q\n' "${cluster_name}"
  fi
} >"${diagnosis_file}"

{
  printf '%s\n' '=== SAFE_KIND_RESULT ==='
  printf 'RESULT_CODE=%s\n' "${result_code}"
  printf 'KIND_EXIT=%s\n' "${smoke_exit_code:-not-run}"
  printf 'CONTAINER_STATE=%s\n' "${container_state}"
  printf 'CONTAINER_EXIT=%s\n' "${container_exit_code}"
  printf 'OOM_KILLED=%s\n' "${node_oom}"
  printf 'NODE_LOG_DRIVER=%s\n' "${node_log_driver}"
  printf 'ERROR_SIGNATURE=%s\n' "${kind_log_signature}"
  printf 'PID1=%s\n' "${pid1}"
  printf 'SYSTEMD_STATE=%s\n' "${systemd_state}"
  printf 'MULTI_USER=%s\n' "${multi_user_state}"
  printf 'FAILED_UNITS=%s\n' "${failed_units}"
  printf 'DOCKER_LOG_LINES=%s\n' "${docker_log_lines}"
  printf 'DOCKER_READY_LINES=%s\n' "${docker_ready_lines}"
  printf 'JOURNAL_READY_LINES=%s\n' "${journal_ready_lines}"
  printf 'HOST_CGROUP_VERSION=%s\n' "${host_cgroup_version}"
  printf 'CONTAINER_CGROUP_FS=%s\n' "${container_cgroup_fs}"
  printf 'NEXT_ACTION=%s\n' "${next_action}"
  if ((cluster_retained == 1)); then
    printf 'CLEANUP_REQUIRED=yes\n'
    printf 'CLEANUP_CLUSTER=%s\n' "${cluster_name}"
  else
    printf 'CLEANUP_REQUIRED=no\n'
  fi
  printf '%s\n' '=== END_SAFE_KIND_RESULT ==='
} >"${safe_result_file}"

if ((concise_output == 1)); then
  cat "${diagnosis_file}" >>"${summary_file}"
  cat "${safe_result_file}"
else
  cat "${diagnosis_file}" | tee -a "${summary_file}"
  printf '\n'
  cat "${safe_result_file}"
fi

section "Report archive"
if ((create_archive == 0)); then
  say "Archive disabled by --no-archive; report remains only in: ${report_dir}"
elif command -v tar >/dev/null 2>&1; then
  tar -czf "${archive_path}" -C "$(dirname "${report_dir}")" "$(basename "${report_dir}")"
  archive_rc=$?
  if ((archive_rc == 0)); then
    say "Archive: ${archive_path}"
  else
    say "Archive creation failed; use report directory: ${report_dir}"
  fi
else
  say "tar is not installed; use report directory: ${report_dir}"
fi

say "Finished: $(date --iso-8601=seconds 2>/dev/null || date)"
say "Confidential environments: keep the report on the authorized server and do not transfer it."

if [[ "${smoke_status}" == "FAIL" ]]; then
  exit 1
fi
exit 0

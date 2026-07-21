#!/usr/bin/env bash

# Diagnose whether a Linux host can run the Kind node image used by Volcano's
# HyperNode E2E suite. The script does not change Docker daemon configuration or
# restart services. A failed smoke-test cluster is retained for investigation.

set -uo pipefail

readonly SCRIPT_VERSION="1.1.0"
readonly DEFAULT_NODE_IMAGE="kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5"

run_kind=1
keep_cluster=0
create_archive=1
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
smoke_status="SKIPPED"
smoke_exit_code=""
cluster_retained=0
docker_ready=0

say() {
  printf '%s\n' "$*" | tee -a "${summary_file}"
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
node_oom=""
permission_signal=0
if ((docker_ready == 1)); then
  logging_driver="$(docker info --format '{{.LoggingDriver}}' 2>/dev/null || true)"
fi
virtualization="$(systemd-detect-virt 2>/dev/null || true)"
if [[ -f "${report_dir}/kind-node-state.txt" ]]; then
  node_oom="$(grep -Eo 'OOMKilled=(true|false)' "${report_dir}/kind-node-state.txt" | head -n 1 || true)"
fi
if grep -qiE 'operation not permitted|permission denied|failed to mount|read-only file system|cgroup.*(fail|error)' \
    "${report_dir}/kind-node-docker.log" "${report_dir}/kind-node-journal.log" 2>/dev/null; then
  permission_signal=1
fi

result_code="UNKNOWN_LOCAL_REVIEW"
if [[ "${smoke_status}" == "SKIPPED" ]]; then
  result_code="HOST_INFO_ONLY"
elif [[ "${smoke_status}" == "PASS" ]]; then
  result_code="KIND_OK"
elif ((docker_ready == 0)); then
  result_code="DOCKER_UNREACHABLE"
elif ! command -v kind >/dev/null 2>&1 || ! command -v kubectl >/dev/null 2>&1; then
  result_code="PREREQUISITE_MISSING"
elif [[ "${logging_driver}" == "none" ]]; then
  result_code="DOCKER_LOGGING_DISABLED"
elif [[ "${node_oom}" == "OOMKilled=true" ]]; then
  result_code="NODE_OOM"
elif ((permission_signal == 1)); then
  result_code="CGROUP_PERMISSION"
elif [[ "${virtualization}" =~ ^(docker|lxc|lxc-libvirt|openvz|podman|container-other)$ ]]; then
  result_code="NESTED_CONTAINER"
fi

{
  printf 'RESULT_CODE=%s\n' "${result_code}"
  printf 'Smoke test: %s\n' "${smoke_status}"
  [[ -n "${smoke_exit_code}" ]] && printf 'Kind exit code: %s\n' "${smoke_exit_code}"
  printf 'Docker logging driver: %s\n' "${logging_driver:-unknown}"
  printf 'Host virtualization: %s\n' "${virtualization:-unknown}"
  printf 'Node OOM state: %s\n\n' "${node_oom:-unknown}"

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
  elif [[ "${result_code}" == "CGROUP_PERMISSION" ]]; then
    printf 'Likely cause: the host cannot provide the privileged mount/cgroup behavior\n'
    printf 'required by a Kind node. Check whether this server is itself a restricted\n'
    printf 'LXC, OpenVZ, Docker, or other nested-container environment.\n'
  elif [[ "${result_code}" == "NESTED_CONTAINER" ]]; then
    printf 'Likely cause: Kind is running inside a containerized server environment\n'
    printf 'without sufficient nested-container and cgroup delegation support.\n'
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
cat "${diagnosis_file}" | tee -a "${summary_file}"

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

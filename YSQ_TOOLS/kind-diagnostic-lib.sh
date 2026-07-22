#!/usr/bin/env bash

# Shared, non-sensitive classification for Kind node startup logs.

classify_kind_node_log() {
  local log_file="$1"

  KIND_LOG_SIGNATURE="UNCLASSIFIED_LOG"
  KIND_LOG_RESULT_CODE="NODE_CONTAINER_EXITED"
  KIND_LOG_NEXT_ACTION="CHECK_LOCAL_NODE_DOCKER_LOG"

  if [[ ! -s "${log_file}" ]]; then
    KIND_LOG_SIGNATURE="NO_LOG_OUTPUT"
    KIND_LOG_RESULT_CODE="DOCKER_LOG_EMPTY"
    KIND_LOG_NEXT_ACTION="CHECK_DOCKER_STDOUT_PATH"
  elif grep -qi 'failed to create /init.scope control group' "${log_file}"; then
    KIND_LOG_SIGNATURE="CGROUP_INIT_SCOPE_FAILED"
    KIND_LOG_RESULT_CODE="CGROUP_INITIALIZATION_FAILED"
    KIND_LOG_NEXT_ACTION="CHECK_DOCKER_CGROUP_DELEGATION"
  elif grep -qiE '(cgroup[^[:cntrl:]]*read-only file system|read-only file system[^[:cntrl:]]*cgroup|/sys/fs/cgroup[^[:cntrl:]]*read-only)' "${log_file}"; then
    KIND_LOG_SIGNATURE="CGROUP_READ_ONLY"
    KIND_LOG_RESULT_CODE="CGROUP_INITIALIZATION_FAILED"
    KIND_LOG_NEXT_ACTION="CHECK_DOCKER_CGROUP_DELEGATION"
  elif grep -qi 'failed to allocate manager object' "${log_file}"; then
    KIND_LOG_SIGNATURE="SYSTEMD_MANAGER_FAILED"
    KIND_LOG_RESULT_CODE="SYSTEMD_FATAL"
    KIND_LOG_NEXT_ACTION="CHECK_LOCAL_SYSTEMD_AND_CGROUP"
  elif grep -qiE 'failed to (initialize|create)[^[:cntrl:]]*control group' "${log_file}"; then
    KIND_LOG_SIGNATURE="CGROUP_INITIALIZATION_FAILED"
    KIND_LOG_RESULT_CODE="CGROUP_INITIALIZATION_FAILED"
    KIND_LOG_NEXT_ACTION="CHECK_DOCKER_CGROUP_DELEGATION"
  elif grep -qiE 'failed to mount[^[:cntrl:]]*(operation not permitted|permission denied)' "${log_file}"; then
    KIND_LOG_SIGNATURE="NODE_MOUNT_PERMISSION"
    KIND_LOG_RESULT_CODE="NODE_MOUNT_PERMISSION"
    KIND_LOG_NEXT_ACTION="CHECK_PRIVILEGED_MOUNT_SUPPORT"
  elif grep -qi 'no space left on device' "${log_file}"; then
    KIND_LOG_SIGNATURE="NODE_NO_SPACE"
    KIND_LOG_RESULT_CODE="NODE_NO_SPACE"
    KIND_LOG_NEXT_ACTION="CHECK_DOCKER_DISK_AND_INODES"
  elif grep -qiE 'out of memory|cannot allocate memory' "${log_file}"; then
    KIND_LOG_SIGNATURE="NODE_MEMORY_FAILURE"
    KIND_LOG_RESULT_CODE="NODE_MEMORY_FAILURE"
    KIND_LOG_NEXT_ACTION="CHECK_HOST_MEMORY"
  elif grep -qi 'exec format error' "${log_file}"; then
    KIND_LOG_SIGNATURE="NODE_ARCH_MISMATCH"
    KIND_LOG_RESULT_CODE="NODE_ARCH_MISMATCH"
    KIND_LOG_NEXT_ACTION="CHECK_NODE_IMAGE_ARCHITECTURE"
  elif grep -qiE '\[!!!!!!\]|freezing execution|systemd[^[:cntrl:]]*fatal' "${log_file}"; then
    KIND_LOG_SIGNATURE="SYSTEMD_FATAL"
    KIND_LOG_RESULT_CODE="SYSTEMD_FATAL"
    KIND_LOG_NEXT_ACTION="CHECK_LOCAL_SYSTEMD_JOURNAL"
  elif grep -qi 'operation not permitted' "${log_file}"; then
    KIND_LOG_SIGNATURE="OPERATION_NOT_PERMITTED"
    KIND_LOG_RESULT_CODE="CGROUP_PERMISSION"
    KIND_LOG_NEXT_ACTION="CHECK_HOST_CGROUP_AND_NESTING"
  elif grep -qi 'permission denied' "${log_file}"; then
    KIND_LOG_SIGNATURE="PERMISSION_DENIED"
    KIND_LOG_RESULT_CODE="CGROUP_PERMISSION"
    KIND_LOG_NEXT_ACTION="CHECK_HOST_CGROUP_AND_NESTING"
  fi
}

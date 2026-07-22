#!/usr/bin/env bash

# Confidential, concise entry point for the Kind host diagnostic tool.

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${script_dir}/kind-host-diagnose.sh" --confidential "$@"

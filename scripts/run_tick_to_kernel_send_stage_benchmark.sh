#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
BENCHMARK_TARGET=tick_to_kernel_send_stage_benchmark \
  exec bash "$SCRIPT_DIR/run_tick_to_kernel_send_benchmark.sh" "$@"

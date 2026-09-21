#!/bin/bash

set -euo pipefail

CPU_ID=${1:-2}
SAMPLES=${2:-1000000}
OUTPUT_DIR=${3:-runs/matching}
METADATA_OUTPUT="${OUTPUT_DIR}/metadata.txt"

mkdir -p "$OUTPUT_DIR"

cmake -DCMAKE_BUILD_TYPE=Release -G Ninja -S . -B ./cmake-build-release
cmake --build ./cmake-build-release --target matching_latency_benchmark -j 4

{
  echo "run_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "requested_cpu=$CPU_ID"
  echo "requested_samples=$SAMPLES"
  uname -a
  echo "g++_version=$(g++ -dumpfullversion -dumpversion)"
  cmake --version
  if command -v lscpu >/dev/null 2>&1; then
    lscpu
  fi
  if command -v swapon >/dev/null 2>&1; then
    swapon --show
  fi
  echo "compile_command:"
  ninja -C cmake-build-release -t commands matching_latency_benchmark
} | tee "$METADATA_OUTPUT"

RUN_ARGS=(--scenario all --warmup 100000 --samples "$SAMPLES" --output-dir "$OUTPUT_DIR")
if command -v taskset >/dev/null 2>&1; then
  taskset -c "$CPU_ID" ./cmake-build-release/matching_latency_benchmark \
    "${RUN_ARGS[@]}" | tee -a "$METADATA_OUTPUT"
else
  echo "warning: taskset is unavailable; benchmark thread is not pinned" >&2
  ./cmake-build-release/matching_latency_benchmark \
    "${RUN_ARGS[@]}" | tee -a "$METADATA_OUTPUT"
fi

for SCENARIO in add cancel match_one sweep4; do
  python3 scripts/summarize_latency.py \
    "$OUTPUT_DIR/matching_${SCENARIO}.csv" \
    --column corrected_ns | tee "$OUTPUT_DIR/matching_${SCENARIO}.summary.txt"
done

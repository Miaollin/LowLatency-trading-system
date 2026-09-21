#!/bin/bash

set -euo pipefail

CPU_ID=${1:-2}
SAMPLES=${2:-1000000}
OUTPUT=${3:-tsc_latency.csv}
METADATA_OUTPUT="${OUTPUT}.metadata.txt"
SUMMARY_OUTPUT="${OUTPUT}.summary.txt"

mkdir -p "$(dirname -- "$OUTPUT")"

cmake -DCMAKE_BUILD_TYPE=Release -G Ninja -S . -B ./cmake-build-release
cmake --build ./cmake-build-release --target tsc_measurement_benchmark -j 4

if command -v taskset >/dev/null 2>&1; then
  taskset -c "$CPU_ID" ./cmake-build-release/tsc_measurement_benchmark \
    --warmup 100000 --samples "$SAMPLES" --output "$OUTPUT" | tee "$METADATA_OUTPUT"
else
  echo "warning: taskset is unavailable; benchmark thread is not pinned" >&2
  ./cmake-build-release/tsc_measurement_benchmark \
    --warmup 100000 --samples "$SAMPLES" --output "$OUTPUT" | tee "$METADATA_OUTPUT"
fi

python3 scripts/summarize_latency.py "$OUTPUT" --column corrected_ns | tee "$SUMMARY_OUTPUT"

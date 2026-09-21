#!/bin/bash

set -euo pipefail

MAIN_CPU=${1:-2}
GATEWAY_CPU=${2:-4}
ORDER_SERVER_CPU=${3:-6}
MATCHING_CPU=${4:-8}
SAMPLES=${5:-100000}
OUTPUT_DIR=${6:-runs/order-rtt}
PORT=${7:-19001}
WARMUP=${WARMUP:-10000}
METADATA_OUTPUT="${OUTPUT_DIR}/metadata.txt"

if [[ "$MAIN_CPU" == "$GATEWAY_CPU" || "$MAIN_CPU" == "$ORDER_SERVER_CPU" ||
      "$MAIN_CPU" == "$MATCHING_CPU" || "$GATEWAY_CPU" == "$ORDER_SERVER_CPU" ||
      "$GATEWAY_CPU" == "$MATCHING_CPU" || "$ORDER_SERVER_CPU" == "$MATCHING_CPU" ]]; then
  echo "main, gateway, order-server, and matching CPUs must be distinct" >&2
  exit 2
fi

if command -v lscpu >/dev/null 2>&1; then
  cpu_core() {
    lscpu -p=CPU,CORE | awk -F, -v cpu="$1" '$1 == cpu { print $2; exit }'
  }
  MAIN_CORE=$(cpu_core "$MAIN_CPU")
  GATEWAY_CORE=$(cpu_core "$GATEWAY_CPU")
  ORDER_SERVER_CORE=$(cpu_core "$ORDER_SERVER_CPU")
  MATCHING_CORE=$(cpu_core "$MATCHING_CPU")
  if [[ -z "$MAIN_CORE" || -z "$GATEWAY_CORE" || -z "$ORDER_SERVER_CORE" ||
        -z "$MATCHING_CORE" ]]; then
    echo "one or more requested CPUs do not exist" >&2
    exit 2
  fi
  if [[ "$MAIN_CORE" == "$GATEWAY_CORE" || "$MAIN_CORE" == "$ORDER_SERVER_CORE" ||
        "$MAIN_CORE" == "$MATCHING_CORE" || "$GATEWAY_CORE" == "$ORDER_SERVER_CORE" ||
        "$GATEWAY_CORE" == "$MATCHING_CORE" || "$ORDER_SERVER_CORE" == "$MATCHING_CORE" ]]; then
    echo "requested CPUs share physical cores; choose one logical CPU per CORE from: lscpu -e=CPU,CORE" >&2
    exit 2
  fi
fi

mkdir -p "$OUTPUT_DIR"

cmake -DCMAKE_BUILD_TYPE=Release -G Ninja -S . -B ./cmake-build-release
cmake --build ./cmake-build-release --target order_rtt_benchmark -j 4

{
  echo "run_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "benchmark=order_rtt_loopback_single_outstanding"
  echo "main_cpu=$MAIN_CPU"
  echo "gateway_cpu=$GATEWAY_CPU"
  echo "order_server_cpu=$ORDER_SERVER_CPU"
  echo "matching_cpu=$MATCHING_CPU"
  echo "requested_samples=$SAMPLES"
  echo "warmup=$WARMUP"
  echo "port=$PORT"
  uname -a
  echo "g++_version=$(g++ -dumpfullversion -dumpversion)"
  cmake --version
  if command -v lscpu >/dev/null 2>&1; then
    lscpu
    lscpu -e=CPU,CORE,SOCKET,NODE,ONLINE,MAXMHZ,MINMHZ
  fi
  if command -v swapon >/dev/null 2>&1; then
    swapon --show
  fi
  if [[ -r /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]]; then
    echo "cpu0_governor=$(< /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
  fi
  echo "compile_command:"
  ninja -C cmake-build-release -t commands order_rtt_benchmark
} | tee "$METADATA_OUTPUT"

RUN_ARGS=(
  --scenario all
  --warmup "$WARMUP"
  --samples "$SAMPLES"
  --output-dir "$OUTPUT_DIR"
  --port "$PORT"
  --gateway-cpu "$GATEWAY_CPU"
  --order-server-cpu "$ORDER_SERVER_CPU"
  --matching-cpu "$MATCHING_CPU"
)

if command -v taskset >/dev/null 2>&1; then
  taskset -c "$MAIN_CPU" ./cmake-build-release/order_rtt_benchmark \
    "${RUN_ARGS[@]}" | tee -a "$METADATA_OUTPUT"
else
  echo "warning: taskset is unavailable; main benchmark thread is not pinned" >&2
  ./cmake-build-release/order_rtt_benchmark \
    "${RUN_ARGS[@]}" | tee -a "$METADATA_OUTPUT"
fi

for SCENARIO in new cancel; do
  python3 scripts/summarize_latency.py \
    "$OUTPUT_DIR/order_rtt_${SCENARIO}.csv" \
    --column corrected_ns | tee "$OUTPUT_DIR/order_rtt_${SCENARIO}.summary.txt"
done

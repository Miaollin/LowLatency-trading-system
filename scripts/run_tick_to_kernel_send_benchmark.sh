#!/bin/bash

set -euo pipefail

MAIN_CPU=${1:-0}
MDC_CPU=${2:-2}
TRADE_ENGINE_CPU=${3:-4}
GATEWAY_CPU=${4:-6}
SINK_CPU=${5:-8}
SAMPLES=${6:-100000}
OUTPUT_DIR=${7:-runs/tick-to-kernel-send}
ORDER_PORT=${8:-19101}
INCREMENTAL_PORT=${9:-22101}
SNAPSHOT_PORT=${10:-22100}
WARMUP=${WARMUP:-10000}
BENCHMARK_TARGET=${BENCHMARK_TARGET:-tick_to_kernel_send_benchmark}
METADATA_OUTPUT="${OUTPUT_DIR}/metadata.txt"

CPUS=("$MAIN_CPU" "$MDC_CPU" "$TRADE_ENGINE_CPU" "$GATEWAY_CPU" "$SINK_CPU")
for ((i = 0; i < ${#CPUS[@]}; ++i)); do
  for ((j = i + 1; j < ${#CPUS[@]}; ++j)); do
    if [[ "${CPUS[$i]}" == "${CPUS[$j]}" ]]; then
      echo "main, MDC, TradeEngine, OrderGateway, and sink CPUs must be distinct" >&2
      exit 2
    fi
  done
done

if command -v lscpu >/dev/null 2>&1; then
  cpu_core() {
    lscpu -p=CPU,CORE | awk -F, -v cpu="$1" '$1 == cpu { print $2; exit }'
  }
  CORES=()
  for cpu in "${CPUS[@]}"; do
    core=$(cpu_core "$cpu")
    if [[ -z "$core" ]]; then
      echo "requested CPU $cpu does not exist" >&2
      exit 2
    fi
    CORES+=("$core")
  done
  for ((i = 0; i < ${#CORES[@]}; ++i)); do
    for ((j = i + 1; j < ${#CORES[@]}; ++j)); do
      if [[ "${CORES[$i]}" == "${CORES[$j]}" ]]; then
        echo "requested CPUs share physical cores; choose five CPUs with distinct CORE values from: lscpu -e=CPU,CORE" >&2
        exit 2
      fi
    done
  done
fi

mkdir -p "$OUTPUT_DIR"

cmake -DCMAKE_BUILD_TYPE=Release -G Ninja -S . -B ./cmake-build-release
cmake --build ./cmake-build-release --target "$BENCHMARK_TARGET" -j 4

{
  echo "run_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "benchmark=application_tick_to_kernel_send_single_outstanding"
  echo "benchmark_target=$BENCHMARK_TARGET"
  echo "main_cpu=$MAIN_CPU"
  echo "mdc_cpu=$MDC_CPU"
  echo "trade_engine_cpu=$TRADE_ENGINE_CPU"
  echo "gateway_cpu=$GATEWAY_CPU"
  echo "sink_cpu=$SINK_CPU"
  echo "requested_samples=$SAMPLES"
  echo "warmup=$WARMUP"
  echo "order_port=$ORDER_PORT"
  echo "incremental_port=$INCREMENTAL_PORT"
  echo "snapshot_port=$SNAPSHOT_PORT"
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
  ninja -C cmake-build-release -t commands "$BENCHMARK_TARGET"
} | tee "$METADATA_OUTPUT"

RUN_ARGS=(
  --warmup "$WARMUP"
  --samples "$SAMPLES"
  --output-dir "$OUTPUT_DIR"
  --order-port "$ORDER_PORT"
  --incremental-port "$INCREMENTAL_PORT"
  --snapshot-port "$SNAPSHOT_PORT"
  --mdc-cpu "$MDC_CPU"
  --trade-engine-cpu "$TRADE_ENGINE_CPU"
  --gateway-cpu "$GATEWAY_CPU"
  --sink-cpu "$SINK_CPU"
)

if command -v taskset >/dev/null 2>&1; then
  taskset -c "$MAIN_CPU" "./cmake-build-release/$BENCHMARK_TARGET" \
    "${RUN_ARGS[@]}" | tee -a "$METADATA_OUTPUT"
else
  echo "warning: taskset is unavailable; main benchmark thread is not pinned" >&2
  "./cmake-build-release/$BENCHMARK_TARGET" \
    "${RUN_ARGS[@]}" | tee -a "$METADATA_OUTPUT"
fi

python3 scripts/summarize_latency.py \
  "$OUTPUT_DIR/tick_to_kernel_send.csv" \
  --column corrected_ns | tee "$OUTPUT_DIR/tick_to_kernel_send.summary.txt"

if [[ -f "$OUTPUT_DIR/tick_to_kernel_send_stages.csv" ]]; then
  for metric in mdc_processing_ns md_to_trade_queue_ns strategy_ns \
      trade_to_gateway_queue_ns gateway_wait_ns tcp_send_ns total_ns; do
    python3 scripts/summarize_latency.py \
      "$OUTPUT_DIR/tick_to_kernel_send_stages.csv" \
      --column "$metric" | tee "$OUTPUT_DIR/${metric}.summary.txt"
  done
fi

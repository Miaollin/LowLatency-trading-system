# TSC measurement benchmark

This benchmark is the reference path for new latency measurements. The legacy
`START_MEASURE`/`END_MEASURE` sites still log TSC ticks for compatibility, but
their per-sample logging perturbs subsequent measurements. The legacy notebook
also assumes a fixed 2.60 GHz conversion and must not be used for results from a
different machine.

## What the reference path does

- reads timestamps with `LFENCE; RDTSCP; LFENCE` on x86;
- rejects a sample if start/end `TSC_AUX` values differ;
- calibrates TSC frequency against `CLOCK_MONOTONIC_RAW` at startup;
- measures the median empty-bracket timestamp overhead;
- records raw ticks in a fixed-capacity, single-writer buffer;
- writes CSV and calculates statistics only after measurement ends.

The non-x86 implementation is a development fallback based on `steady_clock`.
Output containing `clock=steady_clock_fallback` is not an x86 TSC benchmark and
must not be used as a resume result.

## Run on x86-64 Linux

Choose an otherwise idle physical CPU. The first argument is its logical CPU
number, the second is sample count, and the third is the CSV path:

```bash
bash scripts/run_tsc_benchmark.sh 2 1000000 runs/tsc_latency.csv
```

The script builds a Release (`-O3 -DNDEBUG`) target, pins it with `taskset`, and
produces:

```text
runs/tsc_latency.csv
runs/tsc_latency.csv.metadata.txt
runs/tsc_latency.csv.summary.txt
```

The CSV retains both raw and measurement-overhead-corrected values. The summary
contains count, mean, median/P50, P90, P95, P99, P99.9, min, max, and population
standard deviation. It does not trim outliers.

Before accepting a run, confirm that `clock=x86_tsc`, `migration_samples=0`,
`invalid_samples=0`, and `dropped_samples=0`. Also record the instance type,
CPU model, kernel, compiler version, affinity, SMT state, and swap activity.

`tsc_measurement_benchmark` measures only a tiny deterministic validation
operation. A Matching Engine benchmark should replace that operation with its
explicit request-processing boundary while retaining the same recorder and
offline-output structure.

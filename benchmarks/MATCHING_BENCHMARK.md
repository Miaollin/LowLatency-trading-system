# Matching Engine latency benchmark

`matching_latency_benchmark` measures the real array-backed `MEOrderBook`
through the public `MatchingEngine::processClientRequest()` entry point.

## Measurement boundary

```text
serialized TSC start
  -> MatchingEngine::processClientRequest
  -> MEOrderBook::add or cancel
  -> match/remove/add operations as required
  -> client-response LFQueue writes
  -> market-update LFQueue writes
serialized TSC end
```

Request construction, passive-book setup, output-queue draining, and book
cleanup are outside this boundary. This is an in-process request-processing
latency. It is not network latency, order RTT, or throughput.

## Workloads

- `add`: add one non-crossing BUY order to an empty book; cancel it after timing.
- `cancel`: add one live BUY order before timing; time its successful cancel.
- `match_one`: install one passive SELL order; time a BUY that fully matches it.
- `sweep4`: install four SELL levels with one order and quantity 10 at each;
  time a quantity-40 BUY that fully consumes all four levels.

Order IDs rotate through the configured ID range. Each measured iteration ends
with an empty book. The harness verifies response and market-update counts after
every operation and stops on a mismatch.

## Benchmark-only build configuration

The target compiles the production matching source files with:

```text
LLT_BENCHMARK_MODE=1
ME_MAX_TICKERS=1
ME_MAX_NUM_CLIENTS=8
ME_MAX_ORDER_IDS=65536
ME_MAX_PRICE_LEVELS=256
```

`LLT_BENCHMARK_MODE` disables asynchronous logging and the existing nested
`START_MEASURE`/`END_MEASURE` sites. It does not replace the matching algorithm,
order book, memory pool, or LFQueue implementation. Other executables retain
the original capacities and logging behavior.

The reduced capacities prevent the default multi-gigabyte lookup arrays from
dominating benchmark startup and allow the relevant lookup pages to be
pre-faulted. Results must disclose this configuration; do not present them as
measurements with the project's default maximum capacities.

## Run

On x86-64 Linux, choose an otherwise idle physical CPU:

```bash
bash scripts/run_matching_benchmark.sh 2 1000000 runs/matching-run-01
```

Run at least five times with a fixed environment. Keep every raw CSV and do not
trim outliers. Accept a run only when it reports `clock=x86_tsc`, zero migration,
zero invalid samples, zero dropped samples, and Release flags `-O3 -DNDEBUG`.

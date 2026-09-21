# Order RTT benchmark

This target measures the repository's real order path over TCP loopback:

```text
OrderGateway: timestamp immediately before the first successful kernel send
  -> TCP loopback
  -> OrderServer::recvCallback
  -> FIFOSequencer::sequenceAndPublish
  -> MatchingEngine::processClientRequest
  -> MEOrderBook::add or cancel response path
  -> MatchingEngine response LFQueue
  -> OrderServer TCP send
  -> TCP loopback
  -> OrderGateway::recvCallback: sequence/client/order/type validated
  -> timestamp
```

It records `NEW -> ACCEPTED` and `CANCEL -> CANCELED` independently. There is
exactly one outstanding request. The unmeasured operation in each add/cancel
pair restores an empty order book before the next sample. This is an idle-path
component RTT benchmark, not a saturated-throughput test and not a physical
network RTT benchmark.

The two scenarios have an important source-level asymmetry. `MEOrderBook::add`
enqueues `ACCEPTED` before `checkForMatch()` and `addOrder()`, so NEW RTT ends at
acceptance and does not prove that the full book insertion has completed.
`MEOrderBook::cancel` removes the order and enqueues its market update before it
enqueues `CANCELED`, so CANCEL RTT includes the book removal path. Report these
as order-response RTTs; use the separate matching microbenchmark for full
OrderBook add latency.

The benchmark build disables asynchronous logging, uses reduced compile-time
capacities, and preallocates all sample storage. CSV output happens only after
the measurement loop. Samples with different start/end `TSC_AUX` values are
rejected as CPU migrations.

Run on Linux with four distinct logical CPUs (prefer distinct physical cores):

```bash
bash scripts/run_order_rtt_benchmark.sh 2 4 6 8 100000 runs/order-rtt
```

Arguments are main CPU, OrderGateway CPU, OrderServer CPU, MatchingEngine CPU,
sample count, output directory, and optional TCP port. Set warm-up separately:

```bash
WARMUP=100000 bash scripts/run_order_rtt_benchmark.sh \
  2 4 6 8 1000000 runs/order-rtt-formal 19001
```

Outputs:

- `order_rtt_new.csv` and `order_rtt_cancel.csv`: raw per-sample ticks/ns.
- matching `.summary.txt`: count, mean, median/P50, P90, P95, P99, P99.9,
  min, max, and population standard deviation.
- `metadata.txt`: build command, CPU assignment/topology, OS/compiler, TSC
  calibration, migration counts, and benchmark boundary.

For repeatable resume claims, run at least five fresh output directories and
report the median run plus the full run-to-run range. Do not call these numbers
exchange colocation latency: both sides run on one host through loopback.

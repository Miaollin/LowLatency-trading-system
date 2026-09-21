# Strategy Tick-to-Trade benchmark

This benchmark measures a decoded market event through the real trading
strategy path to the first client-order queue commit.

## Measurement boundary

```text
serialized TSC start
  -> TradeEngine::processMarketUpdate
  -> MarketOrderBook::onMarketUpdate
  -> PositionKeeper / FeatureEngine
  -> MarketMaker or LiquidityTaker
  -> RiskManager
  -> OrderManager::newOrder
  -> TradeEngine::sendClientRequest
  -> ClientRequestLFQueue::updateWriteIndex
serialized TSC end
```

The end timestamp is taken inside `sendClientRequest`, immediately after the
first request queue commit. If a maker event produces two quotes, only time to
the first (BUY) quote is recorded. The harness still validates both requests.

This benchmark excludes multicast receive, packet decoding, market-data input
queue waiting, thread handoff, OrderGateway processing, TCP, exchange handling,
and order response. It must be described as strategy decision Tick-to-Trade,
not network-to-wire latency or order RTT.

## Workloads

- `maker_book_to_first_order`: the book starts at `100@99 x 101@100` (bid and
  ask quantities are 100). A bid `MODIFY` toggles its quantity between 100 and
  101, recomputes BBO/fair price, and triggers two passive quotes. The measured
  endpoint is the first BUY quote at 99.
- `taker_trade_to_order`: the same BBO is installed. A BUY `TRADE` of quantity
  100 produces an aggressive-trade ratio of 1.0, exceeds the 0.5 threshold,
  and triggers one aggressive BUY at the ask price 101.

After each iteration the harness consumes and validates every generated
request, then supplies synthetic `ACCEPTED` and `CANCELED` responses outside the
timed interval so the real OrderManager state machine is ready for the next
event. No fill is injected, so risk position remains constant.

## Benchmark-only configuration

```text
LLT_BENCHMARK_MODE=1
ME_MAX_TICKERS=1
ME_MAX_NUM_CLIENTS=1
ME_MAX_ORDER_IDS=65536
ME_MAX_PRICE_LEVELS=256
clip=10
threshold=0.5
```

Logging and nested legacy measurements are disabled. The production
MarketOrderBook, FeatureEngine, strategy, RiskManager, OrderManager, and LFQueue
implementations remain on the measured path.

## Run

```bash
bash scripts/run_tick_to_trade_benchmark.sh 2 1000000 runs/ttt-run-01
```

Use at least five runs. Accept a run only with `clock=x86_tsc`, Release flags
`-O3 -DNDEBUG`, and zero migration, invalid, and dropped samples.

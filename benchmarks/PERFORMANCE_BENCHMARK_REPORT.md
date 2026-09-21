# LowLatency Trading System 性能测试总报告

> 用途：项目复习、面试准备、简历指标留档  
> 测试日期：2026-09-01；Application Tick-to-Kernel-Send 复测：2026-09-05  
> 数据来源：项目真实 benchmark 源码、阿里云 `runs/` 下的原始 CSV、summary 和 metadata  
> 所有表格默认使用 `corrected_ns`，即扣除本轮相应 timestamp-pair overhead 后的数值

> 性能工程主线：**先建立可信 baseline，再用数据定位热点；每次只修改一个主要变量，最后用相同
> workload、相同绑核和相同统计口径复测。** 本文前半部分的五轮数据是当前项目的 baseline，
> 不是把博客中的优化数字当成了本项目实测结果。博客只作为优化方向参考：
> [《12｜基于现代 C++ 完整实现一个低延迟交易系统》](https://nioer.blog.csdn.net/article/details/162448231)。

## 1. 最重要的结论

本项目目前完成了五层性能测试：

1. TSC 测量框架自检；
2. Matching Engine / OrderBook 同线程 microbenchmark；
3. 策略决策 Tick-to-Trade microbenchmark；
4. 包含 UDP multicast、真实组件线程和 TCP send syscall 的 Application Tick-to-Kernel-Send；
5. 包含真实 TCP loopback、OrderServer、FIFOSequencer 和 MatchingEngine 的 Order RTT benchmark。

正式性能数据汇总如下：

| 测试 | 场景 | 五轮样本 | P50 | P99 | P99.9 | 单位 |
|---|---|---:|---:|---:|---:|---|
| Matching Engine | add | 5 × 1,000,000 | 77.14 | 104.29 | 120.71 | ns |
| Matching Engine | cancel | 5 × 1,000,000 | 95.00 | 106.43 | 128.57 | ns |
| Matching Engine | match one level | 5 × 1,000,000 | 134.29 | 169.29 | 193.57 | ns |
| Matching Engine | sweep four levels | 5 × 1,000,000 | 478.57 | 547.86 | 798.57 | ns |
| Tick-to-Trade | maker book update → first order | 5 × 1,000,000 | 45.00 | 47.86 | 62.14 | ns |
| Tick-to-Trade | taker trade → first order | 5 × 1,000,000 | 41.43 | 44.29 | 47.86 | ns |
| Application Tick-to-Kernel-Send | UDP recv complete → complete TCP buffer drain | 5 × 1,000,000 | 3.068 | 3.810 | 7.463 | µs |
| Order RTT | NEW → ACCEPTED | 5 × 1,000,000 | 7.386 | 11.020 | 14.793 | µs |
| Order RTT | CANCEL → CANCELED | 5 × 1,000,000 | 7.391 | 11.013 | 14.889 | µs |

表中的值不是挑选“最好的一轮”，而是先对每轮独立统计，再取五轮统计值的中位数。

正式 benchmark 共记录：

```text
Matching Engine：4 scenarios × 5 runs × 1,000,000 = 20,000,000 samples
Tick-to-Trade：  2 scenarios × 5 runs × 1,000,000 = 10,000,000 samples
Application TKS：优化前后 × 5 runs × 1,000,000 = 10,000,000 samples
Order RTT：      2 scenarios × 5 runs × 1,000,000 = 10,000,000 samples
TSC 自检：                                            1,000,000 samples
总计：                                              51,000,000 samples
```

所有正式 Matching、Tick-to-Trade 和 RTT 轮次均为：

```text
migration_samples = 0
invalid_samples   = 0
dropped_samples   = 0
```

## 2. 测试环境

来自各轮 `metadata.txt` 的环境：

```text
Cloud:              Alibaba Cloud
Virtualization:     KVM full virtualization
CPU model:          Intel(R) Xeon(R) 6982P-C
Guest CPU:          16 logical CPUs
Topology:           8 physical cores, 2 threads per core
NUMA:               1 node
Compiler:           GCC 11.4.0
CMake:              3.22.1
Build:              Release, -O3 -DNDEBUG
TSC:                constant_tsc, nonstop_tsc, rdtscp, tsc_known_freq
Calibrated TSC:     approximately 2.8 GHz
```

Matching 和 Tick-to-Trade 是单线程同步 microbenchmark，使用：

```text
taskset -c 2
```

Order RTT 使用四个不同物理核：

| 线程/组件 | 逻辑 CPU | 物理 Core |
|---|---:|---:|
| Benchmark main / response consumer | 2 | 1 |
| OrderGateway | 4 | 2 |
| OrderServer / FIFOSequencer | 6 | 3 |
| MatchingEngine / OrderBook | 8 | 4 |

CPU 2/3、4/5、6/7 等分别是同一物理核的 SMT siblings。RTT 没有使用 `2,3,4,5`，
因为那会让 main 与 Gateway、OrderServer 与 MatchingEngine 分别争用同一物理核。

## 3. 通用测量框架

### 3.1 时间戳实现

文件：`common/tsc_clock.h`  
函数：`Common::readTSC()`

x86-64 使用：

```text
LFENCE
RDTSCP
LFENCE
```

作用：

- `RDTSCP` 读取 TSC ticks 和 `TSC_AUX`；
- 前后的 `LFENCE` 限制被测代码与时间戳发生不希望的指令重排；
- `TSC_AUX` 通常标识当前逻辑 CPU，起止值不同则拒绝样本；
- 非 x86 的 `steady_clock` 只用于开发兼容，不能作为本次 x86 benchmark 数据。

### 3.2 TSC frequency

文件：`common/tsc_clock.h`  
函数：`Common::calibrateTSCHz()`

程序启动时将 TSC 增量与 `CLOCK_MONOTONIC_RAW` 对比，多轮测量后取中位数。本次服务器
校准结果约为 2.8 GHz。

换算公式：

```text
nanoseconds = ticks × 1,000,000,000 / tsc_hz
```

本次 2.8 GHz 环境中，一个 tick 约为 0.357 ns。

### 3.3 Measurement overhead

文件：`common/tsc_clock.h`  
函数：`Common::measureTSCOverhead()`

启动时连续读取两个序列化时间戳，以大量空区间样本的中位数作为时间戳开销：

| 测试 | overhead |
|---|---:|
| TSC 框架自检 | 68 ticks |
| Matching 五轮 | 66 ticks |
| Tick-to-Trade 五轮 | 72 ticks |
| Order RTT 五轮 | 72 ticks |

每个 benchmark 使用自己启动时测得的 overhead，不使用写死的 CPU 频率或固定开销。

CSV 同时保留：

```text
raw_ticks
corrected_ticks
raw_ns
corrected_ns
```

其中：

```text
corrected_ticks = max(raw_ticks - overhead_ticks, 0)
```

### 3.4 Hot path recorder

文件：`common/latency_recorder.h`  
类：`Common::LatencyRecorder`

- 构造时一次性分配固定容量数组；
- 正式测试前触碰每个元素，尽量提前完成 page fault；
- `record()` 中不执行动态内存分配、锁、字符串格式化和文件 I/O；
- hot path 只校验 TSC_AUX、校验 tick 顺序并写入原始 tick；
- 测试结束后才写 CSV 和离线计算 percentile。

### 3.5 离线统计

文件：`scripts/summarize_latency.py`

计算：

```text
sample count
mean
median / P50
P90
P95
P99
P99.9
min
max
population standard deviation
```

程序不会删除 outlier。这样既保留完整真实分布，又避免排序和文件输出污染被测 hot path。

## 4. TSC 测量框架自检

### 4.1 目标

TSC benchmark 不是业务性能指标。它验证：

- 时间戳可以正常读取；
- TSC frequency 可以校准；
- measurement overhead 可以测量；
- migration/invalid/drop 统计正常；
- CSV 和离线统计链路正常。

源码：`benchmarks/tsc_measurement_benchmark.cpp`

被测操作 `measuredOperation()` 是一个很小的确定性整数状态更新，并通过 compiler barrier
阻止编译器将其删除。

### 4.2 边界

```text
serialized timestamp A
↓
state = state * constant + constant
↓
compiler barrier
↓
serialized timestamp B
```

### 4.3 运行命令

```bash
bash scripts/run_tsc_benchmark.sh 2 1000000 runs/tsc_latency.csv
```

### 4.4 实际结果

```text
clock=x86_tsc
tsc_hz=2.79999e+09
measurement_overhead_ticks=68
requested_samples=1000000
recorded_samples=1000000
migration_samples=0
invalid_samples=0
dropped_samples=0
```

| 指标 | corrected_ns |
|---|---:|
| Count | 1,000,000 |
| Mean | 0.358 ns |
| P50 | 0 ns |
| P90 | 0 ns |
| P95 | 0 ns |
| P99 | 7.143 ns |
| P99.9 | 9.286 ns |
| Min | 0 ns |
| Max | 17,714.3 ns |
| Standard deviation | 31.866 ns |

P50 为 0 不代表 CPU 在零时间完成操作。原因是微小操作成本小于或接近空时间戳对的
overhead，扣除后按 0 截断。这进一步说明 TSC 自检只能证明测量框架工作，不能当作业务性能
或简历中的延迟指标。

## 5. Matching Engine / OrderBook microbenchmark

### 5.1 测量边界

源码：`benchmarks/matching_latency_benchmark.cpp`  
入口：`Exchange::MatchingEngine::processClientRequest()`

```text
timestamp A
↓
MatchingEngine::processClientRequest
↓
MEOrderBook::add 或 cancel
↓
checkForMatch / match / addOrder / removeOrder
↓
ClientResponse LFQueue writes
↓
MarketUpdate LFQueue writes
↓
processClientRequest 返回
↓
timestamp B
```

测量包括：

- 真实 array-backed `MEOrderBook`；
- MemoryPool 分配/释放；
- 价格档位和订单链表操作；
- 撮合；
- response 和 market update LFQueue 写入。

测量不包括：

- OrderServer 和 FIFOSequencer；
- TCP 网络；
- 线程间 request queue wait；
- workload setup、cleanup 和输出队列 drain。

所以它是同步函数/组件处理延迟，不是 Order RTT。

### 5.2 Benchmark 专用容量

```text
LLT_BENCHMARK_MODE=1
ME_MAX_TICKERS=1
ME_MAX_NUM_CLIENTS=8
ME_MAX_ORDER_IDS=65536
ME_MAX_PRICE_LEVELS=256
```

这会关闭 Logger 和嵌套的旧 `START_MEASURE/END_MEASURE`，但不替换 MatchingEngine、
MEOrderBook、MemoryPool 或 LFQueue 算法。容量缩小仅作用于 benchmark target，用于避免默认
大数组占用数 GB 内存，并让相关页面能够预热。

面试和简历必须披露这是 reduced-capacity benchmark configuration。

### 5.3 四种 workload

#### add

```text
空订单簿
↓
测量：添加一个 BUY 10@100
↓
验证 1 个 ACCEPTED response + 1 个 ADD market update
↓
不测量：cancel 清理订单
```

#### cancel

```text
不测量：添加一个 BUY 10@100
↓
测量：成功 cancel
↓
验证 1 个 CANCELED response + 1 个 CANCEL market update
```

#### match_one

```text
不测量：放入一个被动 SELL 10@100
↓
测量：主动 BUY 10@100 完全成交一个价位
↓
验证 3 个 response + 2 个 market update
```

三个 response 为 aggressive ACCEPTED、aggressive FILLED 和 passive FILLED；两个 market
update 为 TRADE 和 passive order CANCEL。

#### sweep4

`SWEEP4` 不是四次独立下单，而是一张主动订单连续吃掉四个价格档位：

```text
预先放入：SELL 10@100、10@101、10@102、10@103
↓
测量：BUY 40@103
↓
连续撮合四个价格档位
↓
验证 9 个 response + 8 个 market update
```

### 5.4 运行配置

```text
正式轮次：matching-run-01 ～ matching-run-05
每轮 warm-up：每种场景 100,000
每轮 samples：每种场景 1,000,000
CPU：taskset -c 2
TSC frequency：2.8 GHz
measurement overhead：66 ticks
日志：关闭
```

运行单轮：

```bash
bash scripts/run_matching_benchmark.sh 2 1000000 runs/matching-run-01
```

### 5.5 五轮结果中位数

单位均为 ns：

| Scenario | Mean | P50 | P90 | P95 | P99 | P99.9 | Min | Max | Stddev |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| add | 78.86 | 77.14 | 80.71 | 90.00 | 104.29 | 120.71 | 71.43 | 12,683.6 | 55.81 |
| cancel | 96.15 | 95.00 | 97.14 | 99.29 | 106.43 | 128.57 | 85.71 | 12,570.0 | 59.47 |
| match_one | 137.47 | 134.29 | 140.00 | 152.14 | 169.29 | 193.57 | 129.29 | 12,491.4 | 65.61 |
| sweep4 | 489.53 | 478.57 | 519.29 | 527.14 | 547.86 | 798.57 | 457.86 | 14,175.0 | 125.52 |

这里的 Max 是“五轮各自 Max 的中位数”，不是全部 500 万样本的单个最大值。

### 5.6 跨轮范围

| Scenario | P50 range | P99 range | P99.9 range |
|---|---:|---:|---:|
| add | 76.43～77.14 ns | 104.29～105.00 ns | 119.29～121.43 ns |
| cancel | 95.00～95.00 ns | 105.00～108.57 ns | 127.86～130.00 ns |
| match_one | 134.29～135.00 ns | 145.00～177.14 ns | 189.29～206.43 ns |
| sweep4 | 477.86～492.86 ns | 532.86～580.72 ns | 787.86～800.72 ns |

add/cancel 的主体分布非常稳定；match_one 的 P99 跨轮波动更明显，但 P50 和 P99.9 仍保持在
较窄范围。sweep4 的 P50 大约是 match_one 的 3.6 倍，符合它执行四次撮合、生成更多 response
和 market update 的 workload 差异，但不能将这个比例简单解释为算法复杂度。

## 6. 策略 Tick-to-Trade microbenchmark

### 6.1 准确名称

这里测的是：

> 已解码 market update 进入 TradeEngine，到策略产生的第一笔 ClientRequest 完成 LFQueue
> commit 的同步策略反应延迟。

它不是从网卡收到 tick 到订单上网卡的 wire-to-wire latency。

### 6.2 测量边界

主要源码：

- `benchmarks/tick_to_trade_benchmark.cpp`
- `trading/strategy/trade_engine.cpp`
- `trading/strategy/market_order_book.cpp`
- `trading/strategy/feature_engine.h`
- `trading/strategy/market_maker.cpp`
- `trading/strategy/liquidity_taker.cpp`
- `trading/strategy/risk_manager.cpp`
- `trading/strategy/order_manager.cpp`

```text
timestamp A
↓
TradeEngine::processMarketUpdate（输入已经解码）
↓
MarketOrderBook::onMarketUpdate
↓
PositionKeeper / FeatureEngine
↓
MarketMaker 或 LiquidityTaker
↓
RiskManager
↓
OrderManager::newOrder
↓
TradeEngine::sendClientRequest
↓
ClientRequestLFQueue::updateWriteIndex
↓
timestamp B
```

终点位于 `TradeEngine::sendClientRequest()` 内，在第一笔请求完成 `updateWriteIndex()` 后。

不包括：

- multicast/socket receive；
- MarketDataConsumer 解码；
- market data input LFQueue wait；
- 线程切换；
- OrderGateway；
- TCP send；
- exchange 处理和 response。

### 6.3 Maker workload

场景：`maker_book_to_first_order`

初始化订单簿：

```text
bid = 100@99
ask = 100@101
```

每次将 bid quantity 在 100 和 101 之间切换，触发：

```text
MarketOrderBook 更新
→ BBO / fair price
→ MarketMaker
→ RiskManager / OrderManager
→ 生成两侧报价
```

只测到第一笔 BUY quote 完成 LFQueue commit；第二笔 SELL quote 不在计时终点内，但 harness
仍然验证两笔请求都正确生成。

### 6.4 Taker workload

场景：`taker_trade_to_order`

在相同 BBO 上注入一个 BUY TRADE event，aggressive-trade ratio 为 1.0，超过配置阈值 0.5，
LiquidityTaker 产生一笔价格 101、数量 10 的主动 BUY 请求。

每次迭代后，harness 在计时区间外注入 synthetic ACCEPTED/CANCELED，使真实 OrderManager
状态机能够进入下一轮；没有注入 fill，因此风险持仓保持不变。

### 6.5 运行配置

```text
正式轮次：ttt-run-01 ～ ttt-run-05
每轮 warm-up：每种场景 100,000
每轮 samples：每种场景 1,000,000
CPU：taskset -c 2
TSC frequency：2.8 GHz
measurement overhead：72 ticks
clip：10
threshold：0.5
日志：关闭
```

运行单轮：

```bash
bash scripts/run_tick_to_trade_benchmark.sh 2 1000000 runs/ttt-run-01
```

### 6.6 五轮结果中位数

单位均为 ns：

| Scenario | Mean | P50 | P90 | P95 | P99 | P99.9 | Min | Max | Stddev |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| maker book → first order | 45.36 | 45.00 | 46.43 | 46.43 | 47.86 | 62.14 | 42.14 | 7,547.86 | 33.14 |
| taker trade → first order | 41.95 | 41.43 | 43.57 | 43.57 | 44.29 | 47.86 | 38.57 | 11,837.10 | 37.34 |

### 6.7 跨轮范围

| Scenario | P50 range | P99 range | P99.9 range |
|---|---:|---:|---:|
| maker | 43.57～46.43 ns | 47.14～52.14 ns | 50.00～65.71 ns |
| taker | 40.00～42.14 ns | 42.86～44.29 ns | 47.14～47.86 ns |

40～60 ns 的结果合理解释是“warm-cache、同步调用、输入已经解码、到第一笔 LFQueue commit”，
不能描述为网络 Tick-to-Trade。Maker 会更新订单簿/BBO并产生双边报价，Taker 路径只产生一笔
订单，所以本 workload 下 Taker 略快。

## 7. Order RTT benchmark

### 7.1 测量边界

主要源码：

- `benchmarks/order_rtt_benchmark.cpp`
- `trading/order_gw/order_gateway.cpp`
- `common/tcp_socket.cpp`
- `common/tcp_server.cpp`
- `exchange/order_server/order_server.cpp`
- `exchange/order_server/fifo_sequencer.h`
- `exchange/matcher/matching_engine.cpp`
- `exchange/matcher/me_order_book.cpp`

```text
OrderGateway：进入首次成功的内核 send() 前读取 timestamp A
↓
TCP loopback
↓
OrderServer::recvCallback
↓
FIFOSequencer::sequenceAndPublish
↓
MatchingEngine::processClientRequest
↓
MEOrderBook response path
↓
MatchingEngine response LFQueue
↓
OrderServer TCP send
↓
TCP loopback
↓
OrderGateway::recvCallback
↓
验证 seq_num、client_id、client_order_id、response type
↓
读取 timestamp B
```

测量终点在 response 写入 TradeEngine incoming LFQueue 之前。

包括：

- 两次 TCP loopback；
- OrderServer receive/send；
- FIFOSequencer；
- OrderServer↔MatchingEngine LFQueue；
- MatchingEngine 和对应的 OrderBook response path；
- OrderGateway 接收、解码和关联校验。

不包括：

- TradeEngine 产生请求到 OrderGateway 消费请求的 queue wait；
- TradeEngine 消费 response；
- 物理 NIC、交换机或跨机器网络；
- 多 outstanding request 的排队延迟。

### 7.2 为什么不能用原来的 T12 当 RTT 起点

原 `T12_OrderGateway_TCP_write` 只到：

```text
TCPSocket::send()
→ memcpy 到 64 MB 用户态 outbound_data_
```

它没有执行内核 `send()`，更没有等待 exchange response，因此既不是网络发送延迟，也不是
Order RTT。

新的起点在 `Common::TCPSocket::sendAndRecv()` 内。程序在进入内核 `send()` 前读取 TSC，
并且只有该调用返回 `n > 0` 时才通过 observer 确认该时间戳是有效起点。EAGAIN 不会启动样本。

### 7.3 Response correlation

每次只有一个 outstanding request，但仍严格校验：

```text
application sequence number
client id
client order id
expected response type
```

NEW 必须匹配 ACCEPTED，CANCEL 必须匹配 CANCELED。

### 7.4 Workload

通用参数：

```text
client id = 0
ticker id = 0
side = BUY
price = 100
quantity = 10
transport = 127.0.0.1 TCP loopback
outstanding requests = 1
```

NEW 场景测 NEW→ACCEPTED，然后发送不测量的 CANCEL 清理订单。CANCEL 场景先发送不测量的
NEW 建单，再测 CANCEL→CANCELED。每一步都等待相应 market update，确保下一轮开始前订单簿
状态一致。

### 7.5 NEW 与 CANCEL 的源码语义差异

`MEOrderBook::add()` 先将 `ACCEPTED` 写入 response LFQueue，之后才执行：

```text
checkForMatch()
addOrder()
sendMarketUpdate(ADD)
```

OrderServer 与 MatchingEngine 并发运行，因此 NEW response 可能在完整 addOrder 尚未完成时已经
返回。NEW RTT 必须叫 `NEW → ACCEPTED response RTT`，不能叫完整 OrderBook insertion latency。

`MEOrderBook::cancel()` 则先 `removeOrder()`、写 CANCEL market update，之后才写 CANCELED
response。因此 CANCEL RTT 包含订单簿移除路径。

### 7.6 Benchmark 配置

```text
LLT_BENCHMARK_MODE=1
LLT_ORDER_RTT_BENCHMARK=1
LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK=1
LLT_TCP_BUFFER_SIZE=65536
ME_MAX_TICKERS=1
ME_MAX_NUM_CLIENTS=1
ME_MAX_ORDER_IDS=65536
ME_MAX_PRICE_LEVELS=256
client/market LFQueue capacity=4096
```

Logger 线程和文件输出在测试期间关闭；原始样本预分配；CSV 在测试结束后写出。

### 7.7 正式轮次选择

`order-rtt-run-01` 只有 100,000 samples、10,000 warm-up，因此只作为短验证轮。

正式五轮为：

```text
order-rtt-run-02 ～ order-rtt-run-06
每轮每种场景：1,000,000 samples
每轮每种场景：100,000 warm-up
```

运行单轮：

```bash
WARMUP=100000 bash scripts/run_order_rtt_benchmark.sh \
  2 4 6 8 1000000 runs/order-rtt-run-06
```

### 7.8 五轮结果中位数

单位均为 µs：

| Scenario | Mean | P50 | P90 | P95 | P99 | P99.9 | Min | Max | Stddev |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| NEW → ACCEPTED | 7.527 | 7.386 | 8.209 | 8.692 | 11.020 | 14.793 | 6.364 | 41.353 | 0.754 |
| CANCEL → CANCELED | 7.584 | 7.391 | 8.297 | 8.859 | 11.013 | 14.889 | 6.283 | 49.329 | 0.767 |

### 7.9 跨轮范围

| Scenario | P50 range | P99 range | P99.9 range | absolute max range |
|---|---:|---:|---:|---:|
| NEW | 7.308～7.408 µs | 10.554～11.171 µs | 14.407～14.880 µs | 33.391～113.672 µs |
| CANCEL | 7.329～7.428 µs | 10.688～11.223 µs | 14.601～14.927 µs | 42.054～148.792 µs |

P50/P99/P99.9 跨轮较稳定，absolute max 波动大。KVM 上的偶发 vCPU 抢占、宿主机中断和
steal time 很容易影响单个最大值。因此 max 必须保留，但简历应重点报告 P50/P99/P99.9。

## 8. 为什么三类业务延迟差别这么大

| 指标 | 典型 P50 | 核心原因 |
|---|---:|---|
| Strategy Tick-to-Trade | 41～45 ns | 同线程、输入已解码、warm-cache，只到第一笔 LFQueue commit |
| Matching request processing | 77～479 ns | 同线程执行真实订单簿、撮合和输出 LFQueue writes |
| Application Tick-to-Kernel-Send | 约 3.07 µs | MDC/TradeEngine/OrderGateway 三线程、两个 LFQueue handoff、TCP send syscall |
| Order RTT | 约 7.39 µs | 三个组件线程、两次 TCP loopback、FIFO 和多个 LFQueue handoff |

这些数值不能直接相加：

- 三个 benchmark 的起止边界不同；
- RTT 的 NEW response 不等待完整 addOrder；
- TTT 不包含网络、MarketDataConsumer 或 OrderGateway；
- microbenchmark 没有线程间排队。

面试时不要说“TTT 是 45 ns，所以完整交易链路也是几十 ns”。正确说法是“同步策略决策
路径 P50 约 45 ns；真实组件 TCP loopback Order RTT P50 约 7.39 µs”。

## 9. 为 benchmark 修复或增加的关键实现

### 9.1 TSC

- 将简单 `RDTSC` 改为 `LFENCE; RDTSCP; LFENCE`；
- 使用 `TSC_AUX` 检测 CPU migration；
- 启动时动态校准 TSC frequency；
- 测量 empty timestamp pair overhead；
- 原始 tick 预分配记录并离线统计。

### 9.2 Benchmark isolation

- `LLT_BENCHMARK_MODE` 关闭 asynchronous Logger；
- 关闭 benchmark 内部嵌套的旧 `START_MEASURE/END_MEASURE`；
- setup、cleanup、queue drain 和 CSV 输出放在计时边界外；
- benchmark target 使用独立的小容量，避免默认多 GB 数组导致 OOM。

### 9.3 Matching / TTT correctness

- 每轮验证 response 和 market update/request 数量；
- 旋转 order id，避免错误复用仍然 live 的订单；
- 每个 iteration 恢复确定状态；
- TTT 使用真实 MarketOrderBook、strategy、RiskManager 和 OrderManager。

### 9.4 TCP / RTT correctness

- 非阻塞 send 的 partial write 不再丢弃未发送数据；
- EAGAIN/EWOULDBLOCK 保留缓冲区，EINTR 重试；
- outbound buffer 支持 compact 和容量检查；
- TCP 分帧后重叠左移由 `memcpy` 改为 `memmove`；
- OrderGateway、OrderServer 和 MatchingEngine 使用 atomic run flag；
- stop 时 join 线程，避免对象析构后线程继续访问；
- 线程启动工具不再引用捕获临时 lambda；
- RTT 组件支持显式绑核。

## 10. Benchmark 的准确性与局限

### 10.1 已经处理

- Release `-O3 -DNDEBUG`；
- 序列化 TSC；
- 动态 TSC frequency calibration；
- empty-bracket overhead correction；
- TSC_AUX migration rejection；
- CPU affinity；
- warm-up；
- hot path 无日志、无 CSV I/O、无 percentile sorting；
- sample buffer 预分配；
- 五轮重复运行；
- outlier 不删除；
- metadata 留档。

### 10.2 仍然存在

- KVM vCPU 不等于独占裸金属物理核；
- 可能存在 hypervisor steal time；
- 没有 isolcpus、IRQ affinity 和实时调度保证；
- 未证明服务器 swap 在全部测试时完全关闭；
- overhead correction 是 empty TSC pair 的近似，不能消除全部 instrumentation 开销；
- reduced-capacity benchmark 的 cache footprint 小于项目默认配置；
- Matching/TTT 是 warm-cache 同步 microbenchmark；
- RTT 使用 loopback，不包含物理 NIC 和跨机器网络；
- RTT 只有一个 outstanding request，不是饱和负载；
- 当前 LFQueue 没有完整 backpressure/满队列保护，不能直接用于无限制压力测试。

因此这些数据适合：

> 展示自己能定义边界、构造 workload、实现低开销 instrumentation、验证协议正确性并分析
> latency distribution。

不适合：

> 宣称项目已经达到生产 HFT 交易所的绝对延迟、容量和可靠性水平。

## 11. 尚未独立测量的指标

| 指标 | 当前状态 |
|---|---|
| MatchingEngine request-processing latency | 已测 |
| OrderBook add/cancel/match/sweep | 已测 |
| Strategy decision Tick-to-Trade | 已测 |
| OrderGateway→OrderServer→MatchingEngine→OrderGateway response RTT | 已测 |
| MarketDataConsumer 网络接收/解码 latency | 未独立测量 |
| OrderGateway→OrderServer 单向网络 latency | 未独立测量 |
| FIFOSequencer 单独 latency | 未独立测量 |
| MarketDataConsumer UDP recv 返回到完整订单进入内核 TCP buffer | 已测 |
| 使用 NIC 硬件时间戳的 wire-to-wire Tick-to-Trade | 未测 |
| 从 market tick 到 exchange response 返回的策略端到端闭环 | 未测 |
| 多 client scaling | 未测 |
| 饱和 offered load / throughput / queueing latency curve | 未测 |
| 跨机器物理网络 RTT | 未测 |

## 12. 结果文件与复现命令

服务器项目目录：

```text
/root/trading-system
```

### TSC

```bash
bash scripts/run_tsc_benchmark.sh 2 1000000 runs/tsc_latency.csv
cat runs/tsc_latency.csv.summary.txt
```

### Matching Engine

```bash
bash scripts/run_matching_benchmark.sh 2 1000000 runs/matching-run-01

cat runs/matching-run-01/matching_add.summary.txt
cat runs/matching-run-01/matching_cancel.summary.txt
cat runs/matching-run-01/matching_match_one.summary.txt
cat runs/matching-run-01/matching_sweep4.summary.txt
```

### Tick-to-Trade

```bash
bash scripts/run_tick_to_trade_benchmark.sh 2 1000000 runs/ttt-run-01

cat runs/ttt-run-01/ttt_maker_book_to_first_order.summary.txt
cat runs/ttt-run-01/ttt_taker_trade_to_order.summary.txt
```

### Order RTT

```bash
WARMUP=100000 bash scripts/run_order_rtt_benchmark.sh \
  2 4 6 8 1000000 runs/order-rtt-run-06

cat runs/order-rtt-run-06/order_rtt_new.summary.txt
cat runs/order-rtt-run-06/order_rtt_cancel.summary.txt
```

### 验证编译配置

```bash
ninja -C cmake-build-release -t commands matching_latency_benchmark \
  | grep -E -- '-O3|-DNDEBUG'
```

### 验证样本有效性

```bash
grep -E 'migration|invalid|dropped|scenario=' runs/matching-run-01/metadata.txt
grep -E 'migration|invalid|dropped|scenario=' runs/ttt-run-01/metadata.txt
grep -E 'migration|invalid|dropped|scenario=' runs/order-rtt-run-06/metadata.txt
```

## 13. 面试常见问题

### Q1：你为什么不直接使用 chrono？

Hot path 需要尽量低的读取成本，所以使用 TSC。为了提高可信度，我没有直接裸用 RDTSC，
而是使用 `LFENCE; RDTSCP; LFENCE`，利用 TSC_AUX 检测迁核，并在启动阶段相对
`CLOCK_MONOTONIC_RAW` 校准频率。chrono/clock 仍用于启动校准和 benchmark timeout，不在每个
业务样本的 hot path 上调用。

### Q2：为什么不能用固定 2.6 GHz 换算？

实际测试服务器 TSC 约为 2.8 GHz。写死 2.6 GHz 会系统性放大所有 ns/us 结果。当前每次
benchmark 启动时动态校准，并把 tsc_hz 写入 metadata。

### Q3：为什么要记录 raw ticks，而不是 hot path 直接计算 percentile？

hot path 排序、浮点换算、锁或文件 I/O 会反过来污染被测路径。当前只写入预分配数组，测试
结束后再转换并离线计算 percentile。

### Q4：Matching Engine 的边界是什么？

从调用 `MatchingEngine::processClientRequest()` 前到其返回后，包括真实 OrderBook、MemoryPool、
撮合以及 response/market-update LFQueue writes；不包括 TCP、OrderServer、FIFO 和线程排队。

### Q5：SWEEP4 是什么？

预先建立四个 SELL 价位 100～103，每档数量 10，再用一张 BUY 40@103 连续吃完四档。测的是
一次 aggressive request 扫四档的完整同步处理，不是四次独立下单。

### Q6：Tick-to-Trade 为什么只有约 40 ns？

因为输入已经是解码后的 `MEMarketUpdate`，并且 benchmark 同步调用、warm-cache，只测到第一笔
ClientRequest LFQueue commit。它不包含 MarketDataConsumer、socket、thread handoff、
OrderGateway 或 TCP，因此不能叫 wire-to-wire Tick-to-Trade。

### Q7：Order RTT 的起点为什么不在 TradeEngine？

本次要隔离网关到 exchange 再返回网关的 response RTT，所以起点放在 OrderGateway 内核 send
前。如果从 TradeEngine 入队开始，就会额外包含 TradeEngine→OrderGateway 的 queue wait，应该
定义为另一种策略端端延迟。

### Q8：如何确保返回的是同一笔订单？

除了每次只保留一个 outstanding request，还验证 application sequence、client id、client
order id 和 expected response type。错误响应不会被当作有效样本。

### Q9：为什么 NEW RTT 不代表完整 add latency？

项目源码在 `MEOrderBook::add()` 一开始就发送 ACCEPTED，之后才 checkForMatch 和 addOrder。
OrderServer 与 MatchingEngine 并发，所以 response 可能提前返回。完整 add latency要看 Matching
microbenchmark，而不是 NEW response RTT。

### Q10：为什么 max 比 P99.9 大很多？

max 只由单个样本决定，对 VM 调度、中断和 hypervisor 抢占极敏感。五轮 percentile 相对稳定，
而 max 波动到数十甚至上百微秒，符合云 VM tail 的特征。不能删除这些 outlier，但简历应优先
报告 P50/P99/P99.9，并披露 KVM 环境。

### Q11：这是 throughput 或高并发测试吗？

不是。当前三组业务 benchmark 主要测无排队或单 outstanding 的 latency。要测 saturation，
需要加入 offered-load generator、多个 outstanding order、完整 backpressure、队列深度和吞吐
统计，并输出 latency-vs-load 曲线。

## 14. 简历指标清单

### 现在可以写

- TSC 测量框架：序列化 RDTSCP、TSC_AUX migration detection、动态频率校准、overhead correction；
- Matching Engine / OrderBook add、cancel、single-level match、four-level sweep；
- decoded tick → first order LFQueue commit 的策略决策延迟；
- loopback OrderGateway↔Exchange response RTT；
- 五轮重复、每轮 100 万样本、离线 P50/P90/P95/P99/P99.9 统计；
- 非阻塞 TCP partial-write/EAGAIN correctness 修复。

### 不建议声称

- 生产级 HFT；
- 7 µs 跨机器交易所 RTT；
- 45 ns wire-to-wire Tick-to-Trade；
- 已完成多 client scaling 或饱和负载测试；
- NEW RTT 等于完整 OrderBook insertion；
- 云 VM tail 等于裸金属 tail。

## 15. 简历描述模板

### 综合版本

> 基于 C++20 构建低延迟交易系统性能测试框架，使用 LFENCE+RDTSCP+LFENCE、TSC_AUX
> migration detection、动态 TSC 校准、CLOCK_MONOTONIC_RAW 和预分配 recorder，对 5100 万个事件样本进行五轮可重复
> benchmark；测得 OrderBook add/cancel/match-one/sweep-four P50 分别为
> 77/95/134/479 ns，策略决策 Tick-to-Trade P50 为 41～45 ns，Application Tick-to-Kernel-Send
> P50/P99/P99.9 为 3.068/3.810/7.463 µs，并在阿里云 KVM 环境测得
> TCP loopback Order RTT P50/P99/P99.9 约为 7.39/11.02/14.89 µs。

### 工程优化版本

> 将 latency hot path 改为预分配原始 cycle 记录和测试结束后离线 percentile 统计，关闭 benchmark
> 日志与嵌套 instrumentation；修复非阻塞 TCP partial write/EAGAIN 导致的未发送数据丢失、
> 接收缓冲区重叠 memcpy 及组件线程未 join 等问题，并为关键组件增加物理核隔离。

### 面试时主动附带的限定

> Matching 和策略数据是 reduced-capacity、warm-cache 同步 microbenchmark；Order RTT 是单机
> loopback、单 outstanding request，运行于 8 核/16 vCPU KVM，因此用来展示项目内部组件性能和
> benchmark 方法，不宣称生产交易所的绝对网络延迟。

## 16. 60 秒面试口述版本

> 我没有直接使用项目原来把 RDTSC 差值写日志的方式，因为它没有序列化、写死频率，而且日志
> 会污染后续样本。我实现了 LFENCE+RDTSCP+LFENCE 的时间戳，利用 TSC_AUX 拒绝迁核样本，
> 启动时相对 CLOCK_MONOTONIC_RAW 校准 2.8 GHz TSC，并用预分配数组记录原始 cycle，结束后再
> 离线统计。基于真实源码我做了三层业务 benchmark：Matching Engine 同步处理、解码 tick 到
> 第一笔订单入队的策略决策延迟，以及经过 TCP、OrderServer、FIFOSequencer、MatchingEngine
> 再返回 OrderGateway 的 Order RTT。五轮每轮每场景 100 万样本，add/cancel/match-one/sweep-four
> 的 P50 为 77/95/134/479 ns，策略 P50 为 41～45 ns，loopback Order RTT P50 约 7.39 µs、
> P99 约 11.02 µs、P99.9 约 14.89 µs。所有正式样本无迁核、无无效和丢弃。我会明确说明
> microbenchmark、loopback 和 KVM 的边界，不把它包装成生产 HFT 延迟。

## 17. 为什么必须“先测试，再优化”

博客采用的思路不是先猜哪里慢，而是先在关键边界采样：一类使用 RDTSC 测函数或组件内部执行
时间，另一类使用绝对时间戳串起 Tick-to-Trade 路径；然后离线观察均值、分布和尖峰，最后只对
被数据证明重要的部分动手。本项目也应沿用这条主线，但使用本文第 3 节中更严谨的 TSC recorder
和 percentile 统计，而不是直接复用旧 benchmark 的平均 cycles。

完整闭环如下：

```text
1. 定义边界
   明确 timestamp A/B，以及包含和不包含的组件
        ↓
2. 建立 baseline
   Release -O3、固定 CPU、warm-up、五轮 × 100 万样本
        ↓
3. 离线分析
   P50/P90/P95/P99/P99.9、标准差、max、迁核/无效/丢弃样本
        ↓
4. 根据证据选择优化点
   日志、断言/对象池、数据结构、线程亲和性、快照突发等
        ↓
5. 单变量修改并复测
   同一台机器、同一 workload、同一 CPU、同一统计脚本
        ↓
6. 同时检查副作用
   RSS、吞吐、正确性、丢包、队列深度以及 tail latency
```

“优化后更快”至少应满足：五轮结果总体向好，而不是只挑最好的一轮；P99/P99.9 没有用更差的
tail 换来一点平均值；功能校验、`invalid_samples`、`migration_samples` 和 `dropped_samples`
仍然正常；内存占用没有恶化到让系统 swap 或 OOM。

### 17.1 当前五轮数据扮演什么角色

当前正式结果可以作为后续优化的 **before/baseline**：

| 优化对象 | 当前 baseline | 优化后的目标栏 | 状态 |
|---|---:|---:|---|
| OrderBook add P99 | 104.29 ns | 待复测 | 尚未形成 before/after |
| OrderBook cancel P99 | 106.43 ns | 待复测 | 尚未形成 before/after |
| match-one P99 | 169.29 ns | 待复测 | 尚未形成 before/after |
| sweep-four P99 | 547.86 ns | 待复测 | 尚未形成 before/after |
| maker Tick-to-Trade P99 | 47.86 ns | 待复测 | 尚未形成 before/after |
| taker Tick-to-Trade P99 | 44.29 ns | 待复测 | 尚未形成 before/after |
| NEW Order RTT P99 | 11.020 µs | 待复测 | 尚未形成 before/after |
| CANCEL Order RTT P99 | 11.013 µs | 待复测 | 尚未形成 before/after |

因此，现在可以说“完成了可信 baseline 并确定了下一步优化方法”，不能说“某项优化把 P99 从 X
降低到 Y”。只有再次以相同方法测得 after 数据后，才可以填写最后一列并写进简历。

## 18. 博客提出的优化、当前源码对应关系和实际含义

下面的“博客参考效果”来自博客作者当时的环境和旧 benchmark，**不是本次阿里云五轮实测**。
当前源码是否包含该实现，则是根据仓库文件逐项核对的结果。

| 优化方向 | 当前源码位置 | 具体做法 | 预期作用 | 当前证据/限制 |
|---|---|---|---|---|
| Release 构建 | `CMakeLists.txt`、运行脚本 | `Release`、`-O3 -DNDEBUG` | 内联、常量传播、消除调试开销 | 正式测试已验证编译命令含 `-O3 -DNDEBUG` |
| 热路径断言 | `common/mem_pool.h`、`common/opt_mem_pool.h` | 优化版在 `NDEBUG` 下移除 allocate/deallocate 的检查 | 减少分支和错误处理开销 | 博客旧测试约 343→44 cycles/op，约 7.8×；需用新 recorder 重测 |
| Logger 字符串入队 | `common/logging.h`、`common/opt_logging.h` | 原版逐字符入队；优化版把最长 256 字节字符串作为一个元素入队 | 大幅减少队列 push 次数 | 博客旧测试约 25757→466 cycles/op，约 55×；存在巨大内存代价，见 18.2 |
| 减少 hot-path 日志 | 各组件 `logger_->log(...)`；benchmark mode | Release 中关闭非必要日志，benchmark 中禁用被测路径日志 | 避免格式化、队列操作和 Logger thread 干扰 | 当前正式 benchmark 是隔离测量，不等于生产程序已全面移除日志 |
| 回调静态化 | `common/mcast_socket.h`、`common/tcp_socket.h`、`common/tcp_server.h`、`trading/strategy/trade_engine.h` | 用模板/CRTP 替代部分 `std::function` | 给编译器更多内联机会，减少间接调用 | 当前主代码仍有 `std::function`；尚无新旧 P99 对比 |
| 关键线程绑核 | 各 `start()` 线程与 benchmark 脚本 | MatchingEngine、TradeEngine、MDC、OrderGateway 等放到独立物理核 | 降低迁核、SMT 争用和调度抖动 | RTT 正式测试已分配四个物理核；这是测量控制，不是性能提升数据 |
| 交易簿索引结构 | `exchange/matcher/me_order_book.*`、`exchange/matcher/unordered_map_me_order_book.*` | 比较定长直接寻址 array 与 `std::unordered_map` | 在延迟、内存与扩展性之间取舍 | 博客旧测试 array 142650、unordered 152457 cycles/op，后者约高 6.9%；旧测试不能作为本机结论 |
| 分散快照突发 | `exchange/market_data/market_data_publisher.*` 等快照路径 | 不一次发送所有 ticker，可把 ticker 分散到多个时间片 | 降低周期性 burst、丢包和恢复等待 | 是系统设计建议，当前正式三组测试没有覆盖快照流量 |

### 18.1 Release 与 MemPool：为什么不是简单地“删掉所有检查”

`common/mem_pool.h` 的 `allocate()`、`deallocate()` 和空闲索引更新包含自定义 `ASSERT`；这个宏并
不会因为 `-DNDEBUG` 自动消失。`common/opt_mem_pool.h` 才显式使用
`#if !defined(NDEBUG)` 包住部分 hot-path 检查。

这项选择的本质是：

- Debug/测试构建保留边界、重复释放、pool exhaustion 等检查，尽快发现内存池损坏；
- Release hot path 移除已由离线测试覆盖的重复检查，降低每次 allocate/deallocate 的固定成本；
- 不能为了一个更漂亮的数字而移除所有保护。启动期配置校验、生产 telemetry 和故障快速停止仍
  有价值；更不能在尚无完整正确性测试时直接把检查全部删掉。

博客中 343→44 cycles/op 是方向性证据，不是当前云服务器的可引用结果。要形成自己的效果数字，
应把 `release_benchmark.cpp` 改成预热、逐样本记录、扣 overhead、迁核检测和五轮离线统计，再分别
编译原版与优化版。

### 18.2 Logger 优化为什么快，但不能直接照搬

`common/logging.h::Logger::pushValue(const char *)` 会遍历字符串，把每个字符分别包装成一个
`LogElement` 再 push 到 LFQueue。假设一条日志有 100 个字符，就可能产生约 100 次队列操作。
`common/opt_logging.h` 新增 `LogType::STRING` 和 `char s[256]`，将整段字符串放进一个元素，只做
一次 push，所以博客旧测试出现约 55× 的吞吐差异是可以解释的。

但这个优化把 **每个队列槽位** 都扩大了。在常见 x86-64 GCC ABI 下：

```text
原 LogElement       ≈ 16 B
OptLogElement       ≈ 264 B（包含固定 char[256]）
LOG_QUEUE_SIZE       = 8 * 1024 * 1024

原 Logger 队列      ≈ 128 MiB
优化版 Logger 队列  ≈ 2.06 GiB
```

也就是说，它可能用约 16.5 倍的每槽内存换取更少的 push。多个 Logger 时，4 GB/16 GB VM 很容易
产生 swap 或 OOM；这也是不能只看平均 cycles 的典型例子。正式方案更适合使用预分配 byte ring、
分块字符串池或固定的小块记录，并同时测：每条日志 cycles、P99/P99.9、producer drop、consumer
吞吐和 RSS。本文的 latency benchmark 直接关闭被测 hot path 日志，是为了避免测量污染，并不代表
`OptLogger` 已经无条件适合生产。

### 18.3 `std::function`、线程亲和性和快照

`std::function` 允许灵活注册回调，但可能产生间接调用；如果 callable 超过 small-buffer，还可能
涉及动态分配。对每个行情/订单事件都会经过的稳定回调，可以用模板或 CRTP 让具体类型在编译期
确定，再用汇编和 benchmark 验证是否真正内联。这里不能仅凭“虚调用一定慢”就写优化结论。

线程绑核的目标是减少迁核和竞争，而不是宣称某个固定 CPU 编号适合所有机器。博客给出的组件到
CPU 映射是示例；本次阿里云实例有 8 个物理核、16 个 SMT 线程，必须先从 `lscpu -e=CPU,CORE`
确认 sibling。最值得隔离的是 MatchingEngine、TradeEngine、MarketDataConsumer、OrderGateway/
OrderServer。异步 Logger 通常不必占最宝贵的隔离核，除非实测表明它跟不上并造成 queue pressure。

全量 snapshot 如果固定时刻为所有 ticker 一次性发送，会形成周期性 burst。把不同 ticker 的快照
错开能改善瞬时队列和网络压力，但平均消息率可能不变。验证时应记录每时间窗包数、队列 high-water
mark、丢包/重传次数和恢复时间，而不能只看一次 RTT。

## 19. `std::unordered_map` 与 `std::array` 哈希表的源码级对比

### 19.1 项目里的“array 哈希表”究竟是什么

`exchange/matcher/me_order.h` 中定义：

```cpp
using OrderHashMap = std::array<MEOrder *, ME_MAX_ORDER_IDS>;
using ClientOrderHashMap =
    std::array<OrderHashMap, ME_MAX_NUM_CLIENTS>;
using OrdersAtPriceHashMap =
    std::array<MEOrdersAtPrice *, ME_MAX_PRICE_LEVELS>;
```

它不是通用哈希表，而是 **direct-address table（直接寻址表）**：`client_id`、`order_id` 直接作为
数组下标，price 则由 `priceToIndex(price) = price % ME_MAX_PRICE_LEVELS` 映射到槽位。
`MEOrderBook` 的两个关键成员是：

- `cid_oid_to_order_`：`[client_id][order_id] → MEOrder*`；
- `price_orders_at_price_`：`price index → MEOrdersAtPrice*`。

直接寻址消除了 hash 计算、bucket 查找、node allocation 和大部分指针追逐，因此在 ID 稠密且边界
可控的教学撮合系统里通常更低延迟、更稳定。

### 19.2 两种实现逐项比较

| 维度 | `std::array` 直接寻址 | 当前 `std::unordered_map` 版本 |
|---|---|---|
| 查找路径 | 下标计算 + 一次数组读取 | hash + bucket + node/pointer traversal |
| 平均复杂度 | 真正固定 O(1) | 平均 O(1)，冲突/rehash 时不稳定 |
| cache locality | 表本身连续；访问稀疏大表仍会 cache/TLB miss | node 分散，通常 locality 较差 |
| 动态分配 | 启动时整体预分配，hot path 无 node allocation | 新 key 可能分配 node，rehash 可能搬 bucket |
| tail latency | 通常更可预测 | allocator、冲突和 rehash 会产生尖峰 |
| 内存 | 与最大 ID 空间成正比，稀疏时浪费巨大 | 与实际 key 数更接近，但每 node/bucket 有额外开销 |
| ID 要求 | 必须有严格、稠密、有界 ID | 可以支持稀疏和更大的 key 空间 |
| 启动/页错误 | 大表构造、清零、首次触页代价高 | 初始较轻，运行中逐步分配 |
| 正确性风险 | price 取模若同槽有不同活跃价格会冲突 | 正确使用完整 price key 可避免；但当前实现仍使用取模 key |
| 可扩展性 | 上限翻倍会直接放大预分配内存 | 更适合未知/稀疏规模，但需控制 rehash |

### 19.3 当前默认 array 配置为什么会吃掉大量内存

在默认常量 `ME_MAX_TICKERS=8`、`ME_MAX_NUM_CLIENTS=256`、
`ME_MAX_ORDER_IDS=1,048,576`，只算 `cid_oid_to_order_` 的指针槽位：

```text
每 ticker = 256 × 1,048,576 × 8 B = 2 GiB
8 tickers  = 16 GiB
```

这还没有包含订单池、价格档、队列、Logger 和其他进程数据。因此 array 的低查找延迟不是免费的，
默认配置在 16 GB 主机上就可能 OOM。我们的 Matching benchmark 通过编译期降低到 1 ticker、8
clients、65,536 order ids，使该表约为 4 MiB；这也是报告必须披露 reduced-capacity 的原因。

### 19.4 当前 unordered_map 版本不是“换容器就解决了”

`exchange/matcher/unordered_map_me_order_book.*` 确实实现了嵌套
`unordered_map<ClientId, unordered_map<OrderId, MEOrder *>>` 和价格表，但源码还有以下问题：

- 没有在启动时 `reserve()` 和设置 `max_load_factor()`，运行中可能分配和 rehash；
- 多处 `operator[]` 会在 key 不存在时隐式插入；
- cancel/remove 把 value 设为 `nullptr` 而不 `erase`，key/node 可能长期增长；
- `getOrdersAtPrice()` 先 `find()` 再 `at()`，相当于做了两次查找；
- cancel 里用 `client_id < map.size()` 判断 key，仍带有数组思维，不适合稀疏 map；
- price map 仍以 `price % ME_MAX_PRICE_LEVELS` 为 key，没有真正消除不同价格的 modulo alias。

所以不能根据博客约 6.9% 的平均差异就得出“unordered_map 只慢 7%，可以直接替换”。该旧
`benchmarks/hash_benchmark.cpp` 还存在三项测量限制：使用未序列化的旧式平均 RDTSC、没有
warm-up/percentile/迁核检查；循环处理 `loop_count` 个操作却除以 `loop_count * 2`，打印的绝对
cycles/op 会偏小一半；并且默认构造完整 MatchingEngine，可能仅 array 表就需要约 16 GiB。
相对比值仍有参考意义，但绝对数字不能用于简历，也不应在当前 16 GB 云主机上原样运行。

### 19.5 应该怎么选

推荐按 key 空间选择，而不是整个系统只用一种容器：

| 使用场景 | 推荐选择 | 原因 |
|---|---|---|
| ticker、side 等很小且固定的枚举 | `std::array` | 空间小、直接寻址、最稳定 |
| 已知上限且 ID 高度稠密的订单槽 | 预分配 array/slot table + generation | 无 hot-path allocation，可检测旧 ID |
| ID 很大、活跃订单很稀疏 | 启动时 reserve 的 flat/open-addressing hash map | 避免按最大 ID 分配数 GiB，locality 好于 node map |
| 有限 tick grid 且可证明不会 alias 的价格档 | array | 最快，但要验证边界和冲突 |
| 任意价格或活跃价格稀疏 | 预留容量的 flat hash/map + 有序价位结构 | 用完整 price key，避免 modulo correctness bug |

对这个项目的结论是：reduced-capacity microbenchmark 继续使用 array 是合理的；如果目标变成大量
client/ticker 扩展，默认二维巨型 array 必须重构。但首选也不是未经配置的嵌套
`std::unordered_map`，而是预分配的 slot table 或 reserve 后的开放寻址/flat hash，并保证 hot
path 不 rehash、不向通用 allocator 申请内存。

## 20. 如何做出可写进简历的优化效果

每一个候选优化都建立独立 A/B，其他条件保持一致：

| 实验 | A（before） | B（after） | 除 latency 外还要记录 |
|---|---|---|---|
| MemPool | `MemPool` Release | `OptMemPool` Release | 错误检查覆盖、RSS |
| Logger | 原逐字符 Logger | 分块/byte-ring Logger；`OptLogger` 仅作参照 | producer ops/s、drop、consumer ops/s、RSS |
| Callback | `std::function` | template/CRTP callback | binary size、是否内联、P99.9 |
| Order mapping | reduced-capacity array | reserved flat hash；可加标准 unordered 对照 | RSS、启动时间、rehash 次数、不同 occupancy |
| Thread placement | 不绑核/SMT sibling | 独立物理核 | context switch、migration、P99/P99.9/max |
| Snapshot | 同时 burst | ticker 错峰发送 | queue high-water、丢包、恢复时间 |

正式 hash 对比 workload 至少应覆盖：

1. 10%、50%、90% occupancy；
2. 顺序 ID 与随机稀疏 ID；
3. 50% NEW + 50% CANCEL，以及主动成交 workload；
4. 预热后每组至少 100 万个有效样本，五轮；
5. 每轮报告 P50/P90/P95/P99/P99.9/min/max/stddev 和 peak RSS；
6. 预先构造 request 数组，计时区间禁止日志、vector growth 和 allocator；
7. 统计结束前不排序 hot-path recorder，所有 percentile 离线生成。

结果表应使用这种格式，空白处只能由真实复测填写：

| 优化项 | 版本 | samples | P50 | P99 | P99.9 | RSS | 相对 P99 | 结论 |
|---|---|---:|---:|---:|---:|---:|---:|---|
| 示例：order lookup | array baseline | 5 × 1M | 待测 | 待测 | 待测 | 待测 | baseline | — |
| 示例：order lookup | reserved flat hash | 5 × 1M | 待测 | 待测 | 待测 | 待测 | `(B-A)/A` | — |

只有得到这张表之后，简历才能写：

> 基于五轮、每轮 N 个事件的同 workload A/B benchmark，将 XXX 的 P99 从 X 降至 Y（下降 Z%），
> 同时将/保持峰值 RSS 为 M MiB，并确保 migration、invalid 和 dropped samples 均为 0。

在获得 after 数据之前，当前更准确的表述是：

> 建立序列化 TSC、迁核检测、动态校准、预分配采样和离线 percentile 分析框架，对 Matching、
> Tick-to-Trade 与 Order RTT 共 4100 万事件形成可复现 baseline；通过源码分析识别 Logger 粒度、
> MemPool Release 检查、巨型直接寻址表和线程布局等优化候选，并设计单变量 A/B 验证方案。

## 21. 面试时如何讲这次“测试—优化”工作

可以按下面顺序回答：

1. **先讲边界**：Matching 是同步函数边界，TTT 是 decoded update 到第一笔订单 commit，RTT 是
   Gateway TCP send 到匹配 response；三者不能互相替代。
2. **再讲测量可信度**：Release/O3、独立物理核、warm-up、五轮、每轮 100 万、序列化 TSC、
   TSC_AUX 迁核检测、扣测量 overhead、离线 percentile。
3. **再讲 baseline**：引用第 1 节真实 P50/P99/P99.9，并说明 KVM、loopback、single outstanding、
   reduced-capacity 等限制。
4. **最后讲优化决策**：日志优化虽能减少 push，却可能把每 Logger 队列从约 128 MiB 放大到约
   2.06 GiB；array 延迟稳定，但默认 order pointer table 就约 16 GiB；因此必须同时看 tail、RSS
   和正确性，不能只追求一条漂亮的平均 cycles。

这比背诵“unordered_map 慢、array 快”更有说服力：真正的工程结论是数据结构必须匹配 key 的
密度和上限，所有优化都要在相同 workload 下以 P99/P99.9、内存和正确性共同验收。

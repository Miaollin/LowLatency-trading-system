# LowLatency Trading System：Order RTT Benchmark 复习与面试报告

> 项目级完整报告（包含 TSC、Matching、Tick-to-Trade 和 Order RTT）请优先阅读：
> `benchmarks/PERFORMANCE_BENCHMARK_REPORT.md`。本文仅保留 Order RTT 专项细节。

> 测试日期：2026-09-01  
> 测试平台：阿里云 KVM，Intel Xeon 6982P-C，8 个物理核 / 16 vCPU，16 GB RAM  
> 正式数据：`runs/order-rtt-run-02` ～ `runs/order-rtt-run-06`

## 1. 一页结论

本次测试测量了项目内一笔订单从 `OrderGateway` 实际进入内核 TCP 发送，到
`OrderGateway` 收到并验证对应响应的完整组件 RTT。

真实路径为：

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
MEOrderBook 响应路径
↓
MatchingEngine → OrderServer response LFQueue
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

Order RTT = B - A
```

正式测试共五轮，每轮每种场景 100 万个有效样本：

- `NEW → ACCEPTED`：总计 500 万个样本。
- `CANCEL → CANCELED`：总计 500 万个样本。
- 总计记录 1000 万个 RTT 样本。
- 所有正式轮次 `migration=0`、`invalid=0`、`dropped=0`。

五轮统计值的中位数如下，单位为微秒：

| 指标 | NEW → ACCEPTED | CANCEL → CANCELED |
|---|---:|---:|
| Mean | 7.527 µs | 7.584 µs |
| P50 | 7.386 µs | 7.391 µs |
| P90 | 8.209 µs | 8.297 µs |
| P95 | 8.692 µs | 8.859 µs |
| P99 | 11.020 µs | 11.013 µs |
| P99.9 | 14.793 µs | 14.889 µs |
| Min 的五轮中位值 | 6.364 µs | 6.283 µs |
| Max 的五轮中位值 | 41.353 µs | 49.329 µs |
| Standard deviation | 0.754 µs | 0.767 µs |

推荐简历重点报告：

```text
NEW RTT P50/P99/P99.9   = 7.39/11.02/14.79 µs
CANCEL RTT P50/P99/P99.9 = 7.39/11.01/14.89 µs
```

这些数字是单机 TCP loopback、单 outstanding request 的空载订单响应 RTT，不能描述成：

- 跨机器或交易所物理网络 RTT；
- 饱和负载下的 RTT；
- 生产级 HFT 延迟；
- 完整的 NEW OrderBook insertion latency。

## 2. 为什么原项目不能直接测出可信的 Order RTT

原来的 `T12_OrderGateway_TCP_write` 并不是内核 TCP 发送时间。它的实际终点只是
`TCPSocket::send()` 将订单 `memcpy` 到用户态发送缓冲区。

原路径实际上是：

```text
OrderGateway 读取 LFQueue
↓
TCPSocket::send
↓
memcpy 到 outbound_data_
↓
T12
```

它没有包含：

- 内核 `send()`；
- TCP loopback；
- OrderServer；
- FIFOSequencer；
- MatchingEngine / OrderBook；
- response 返回；
- OrderGateway response 解码和校验。

另外，原 `TCPSocket::sendAndRecv()` 在非阻塞 `send()` 返回 partial write 或
`EAGAIN` 后仍然会清空整个发送缓冲区，可能静默丢弃尚未发送的数据。因此正式 RTT
benchmark 前先修复了 TCP 发送正确性。

## 3. 本次实现涉及的真实源码

### 3.1 Benchmark workload

文件：`benchmarks/order_rtt_benchmark.cpp`

关键内容：

- `Harness`：创建真实的 `OrderServer`、`MatchingEngine` 和 `OrderGateway`。
- `Harness::roundTrip()`：提交订单、等待并验证对应 response。
- `Harness::cycle()`：执行一组 NEW/CANCEL，使每个样本开始前订单簿状态一致。
- `runScenario()`：执行 warm-up、正式采样并在结束后写 CSV。

### 3.2 起点：OrderGateway 真实内核发送

文件：`common/tcp_socket.cpp`  
函数：`Common::TCPSocket::sendAndRecv()`

在准备执行非阻塞内核 `send()` 前调用序列化 TSC：

```cpp
send_start = readTSC();
const auto n = ::send(...);
```

只有 `send()` 返回 `n > 0` 时，才将该时间戳通过 `send_observer_` 交给
`OrderGateway`。因此 EAGAIN 不会错误地启动一个 RTT 样本。

文件：`trading/order_gw/order_gateway.cpp`  
函数：`OrderGateway::observeKernelSend()`

该函数保存当前被测订单第一次成功 TCP send 对应的起点时间戳。

### 3.3 终点：OrderGateway 验证响应

文件：`trading/order_gw/order_gateway.cpp`  
函数：`OrderGateway::recvCallback()`

终点不是“收到任意 TCP 字节”，而是在完成以下校验之后读取：

- response `client_id` 与当前 client 相同；
- TCP 应用层 `seq_num` 与期望序号相同；
- response `client_order_id` 与被测订单相同；
- response type 是预期的 `ACCEPTED` 或 `CANCELED`。

终点位于 response 写入 TradeEngine 的 incoming LFQueue 之前，因此不包含 TradeEngine
消费 response 的延迟。

### 3.4 TSC 与原始样本记录

文件：`common/tsc_clock.h`

- 使用 `LFENCE; RDTSCP; LFENCE` 读取序列化时间戳。
- `RDTSCP` 同时读取 `TSC_AUX`，用于识别逻辑 CPU。
- TSC frequency 在启动阶段相对 `CLOCK_MONOTONIC_RAW` 校准。
- 本次各轮校准结果约为 `2.8 GHz`。
- 启动阶段测得一对时间戳读取的中位开销为 72 ticks，约 25.7 ns。

文件：`common/latency_recorder.h`

- 在测试开始前一次性分配固定容量数组并触碰内存页。
- hot path 只记录原始 tick 差值，不排序、不格式化、不执行文件 I/O。
- 如果起止 `TSC_AUX` 不相同，将该样本记为 migration 并拒绝。
- 测试结束后才转换成 ns 并输出 CSV。

### 3.5 Benchmark 构建配置

文件：`CMakeLists.txt`  
target：`order_rtt_benchmark`

关键定义：

```text
LLT_BENCHMARK_MODE=1
LLT_ORDER_RTT_BENCHMARK=1
LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK=1
LLT_TCP_BUFFER_SIZE=65536
LLT_ME_MAX_TICKERS=1
LLT_ME_MAX_NUM_CLIENTS=1
LLT_ME_MAX_ORDER_IDS=65536
LLT_ME_MAX_PRICE_LEVELS=256
LLT_ME_MAX_CLIENT_UPDATES=4096
LLT_ME_MAX_MARKET_UPDATES=4096
```

`LLT_BENCHMARK_MODE` 关闭异步 Logger 线程及日志文件写入，避免日志 I/O 污染
hot path。缩小的容量只作用于独立 benchmark target，不改变生产 executable 的默认容量。

### 3.6 自动化脚本

文件：`scripts/run_order_rtt_benchmark.sh`

脚本负责：

1. 使用 CMake Release 构建；
2. 保存编译命令，确认存在 `-O3 -DNDEBUG`；
3. 检查四个 CPU 是否属于不同物理核；
4. 启动 benchmark 并固定各组件 CPU；
5. 保存 OS、CPU、编译器、swap 和 TSC 元数据；
6. 离线计算 count、mean、percentile、min、max 和标准差。

## 4. 实际线程与 CPU 分配

服务器拓扑显示相邻的两个逻辑 CPU 是同一物理核上的 SMT siblings，例如 CPU 2/3
属于同一 core。因此正式测试没有采用 `2,3,4,5`，而是使用：

| 线程 | 逻辑 CPU | 物理 Core |
|---|---:|---:|
| Benchmark main / response consumer | CPU 2 | Core 1 |
| OrderGateway | CPU 4 | Core 2 |
| OrderServer / FIFOSequencer | CPU 6 | Core 3 |
| MatchingEngine / OrderBook | CPU 8 | Core 4 |

这样避免关键线程共享同一个物理核的执行资源。

Logger 在 benchmark mode 下不启动，因此没有 Logger CPU。

## 5. Workload 设计

### 5.1 通用条件

- client id：0；
- ticker id：0；
- side：BUY；
- price：100；
- quantity：10；
- TCP：`127.0.0.1` loopback；
- 每次只允许一个 outstanding request；
- 每轮 warm-up：100,000 次 cycle；
- 每轮正式样本：每种场景 1,000,000 个。

### 5.2 NEW → ACCEPTED

每个正式样本：

```text
测量 NEW
↓
等待 ACCEPTED
↓
等待 ADD market update，确认订单簿操作已推进
↓
发送不测量的 CANCEL 清理订单
↓
等待 CANCELED 和 CANCEL market update
```

### 5.3 CANCEL → CANCELED

每个正式样本：

```text
发送不测量的 NEW 建立订单
↓
等待 ACCEPTED 和 ADD market update
↓
测量 CANCEL
↓
等待 CANCELED
↓
等待 CANCEL market update，确认订单簿恢复为空
```

因此五轮正式测试除了记录 1000 万个 RTT 样本外，实际还执行了对应的未测建单/清理操作。

## 6. NEW 和 CANCEL 的源码语义差异

这是面试中必须主动说明的地方。

### NEW → ACCEPTED

在 `exchange/matcher/me_order_book.cpp` 的 `MEOrderBook::add()` 中，代码先调用
`MatchingEngine::sendClientResponse()` 发布 `ACCEPTED`，然后才执行：

```text
checkForMatch()
addOrder()
sendMarketUpdate(ADD)
```

由于 OrderServer 与 MatchingEngine 并发运行，OrderServer 可能在完整订单簿插入尚未结束时
就已经发送 ACCEPTED。因此：

> NEW RTT 是 NEW request 到 ACCEPTED response 的 RTT，不是完整 OrderBook add latency。

### CANCEL → CANCELED

在 `MEOrderBook::cancel()` 中，代码先：

```text
removeOrder()
sendMarketUpdate(CANCEL)
```

之后才调用 `sendClientResponse()` 发布 `CANCELED`。因此 CANCEL RTT 包含订单簿移除路径。

完整 add/cancel/match 函数性能应使用独立 Matching Engine / OrderBook microbenchmark，不应从
Order RTT 反推。

## 7. 五轮原始结果

以下单位均为微秒。正式轮次为 run-02～run-06；run-01 只有 10 万样本且 warm-up
只有 1 万，因此作为验证轮保留，不纳入正式五轮统计。

### 7.1 NEW → ACCEPTED

| Run | Mean | P50 | P90 | P95 | P99 | P99.9 | Min | Max | Stddev |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 02 | 7.623 | 7.408 | 8.487 | 8.906 | 11.091 | 14.845 | 6.308 | 34.407 | 0.782 |
| 03 | 7.566 | 7.396 | 8.209 | 8.692 | 11.020 | 14.793 | 6.364 | 113.672 | 0.754 |
| 04 | 7.527 | 7.308 | 8.389 | 8.823 | 11.171 | 14.880 | 6.220 | 41.353 | 0.801 |
| 05 | 7.502 | 7.344 | 8.093 | 8.607 | 10.554 | 14.721 | 6.372 | 65.998 | 0.715 |
| 06 | 7.482 | 7.386 | 7.696 | 8.165 | 10.973 | 14.407 | 6.458 | 33.391 | 0.616 |
| **五轮中位数** | **7.527** | **7.386** | **8.209** | **8.692** | **11.020** | **14.793** | **6.364** | **41.353** | **0.754** |

### 7.2 CANCEL → CANCELED

| Run | Mean | P50 | P90 | P95 | P99 | P99.9 | Min | Max | Stddev |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 02 | 7.674 | 7.428 | 8.701 | 9.151 | 11.223 | 14.927 | 6.379 | 148.792 | 0.844 |
| 03 | 7.587 | 7.404 | 8.297 | 8.859 | 11.013 | 14.899 | 6.283 | 42.054 | 0.767 |
| 04 | 7.584 | 7.333 | 8.626 | 9.056 | 11.170 | 14.889 | 6.231 | 71.582 | 0.841 |
| 05 | 7.506 | 7.329 | 8.148 | 8.764 | 10.688 | 14.791 | 6.265 | 43.722 | 0.746 |
| 06 | 7.496 | 7.391 | 7.701 | 8.255 | 10.960 | 14.601 | 6.579 | 49.329 | 0.641 |
| **五轮中位数** | **7.584** | **7.391** | **8.297** | **8.859** | **11.013** | **14.889** | **6.283** | **49.329** | **0.767** |

## 8. 如何解读结果

### 8.1 P50/P99/P99.9 比 max 更有意义

五轮的 P50、P99 和 P99.9 范围：

| 指标 | NEW 五轮范围 | CANCEL 五轮范围 |
|---|---:|---:|
| P50 | 7.308～7.408 µs | 7.329～7.428 µs |
| P99 | 10.554～11.171 µs | 10.688～11.223 µs |
| P99.9 | 14.407～14.880 µs | 14.601～14.927 µs |

这些 percentile 的跨轮变化较小，说明主体分布具有较好的重复性。

绝对 max 的范围明显更大：

```text
NEW max：33.391～113.672 µs
CANCEL max：42.054～148.792 µs
```

max 只由一个极端样本决定。在 KVM 云服务器上，它很容易受到 vCPU 调度、宿主机中断、
steal time 和其他租户影响。因此应完整保留 max，但简历主要报告 P50/P99/P99.9。

### 8.2 为什么 P99.9 明显高于 P99

该链路跨越三个忙轮询组件线程和两次 TCP loopback，任何一次线程调度、中断或 cache miss
都可能放大 tail。P99.9 约为 14.8 µs，而 P99 约为 11.0 µs，说明极少量样本进入了更慢的
调度/系统调用路径。

### 8.3 为什么云服务器不能代表裸金属 HFT

服务器运行在 KVM hypervisor 上。即使项目线程已经绑核，也只能固定到 guest vCPU，不能保证：

- 宿主机物理核完全独占；
- 无 hypervisor 抢占；
- 无 steal time；
- 中断不会落在关键物理核；
- TSC 虚拟化和频率行为与裸金属完全相同。

因此这组结果适合证明工程化测量能力和项目内部相对性能，不适合声称生产交易所/HFT
绝对延迟。

## 9. 本次额外修复

为了让 benchmark 的功能正确，完成了以下基础修复：

1. `TCPSocket` 保留 partial write 后尚未发送的数据；
2. `EAGAIN/EWOULDBLOCK` 不再清空发送缓冲区；
3. `EINTR` 自动重试；
4. 发送缓冲区空间不足时先 compact，并进行容量检查；
5. TCP 分帧后左移重叠内存由 `memcpy` 改为 `memmove`；
6. `OrderGateway`、`OrderServer`、`MatchingEngine` 使用 atomic run flag；
7. 组件停止时 join 并释放线程，避免线程访问已经析构的对象；
8. 修复线程创建工具捕获临时 lambda 引用的生命周期风险；
9. 三个关键组件支持指定独立 CPU；
10. benchmark target 关闭异步日志和大容量生产配置。

这些修复不仅是性能 instrumentation，也保证了 RTT workload 在非阻塞 TCP 下不会因为
partial send 静默破坏请求或响应。

## 10. 结果文件和查看命令

阿里云项目目录：

```text
/root/trading-system
```

每轮目录中包含：

```text
metadata.txt
order_rtt_new.csv
order_rtt_new.summary.txt
order_rtt_cancel.csv
order_rtt_cancel.summary.txt
```

查看第六轮：

```bash
cd /root/trading-system

cat runs/order-rtt-run-06/order_rtt_new.summary.txt
cat runs/order-rtt-run-06/order_rtt_cancel.summary.txt
```

检查样本是否有效：

```bash
grep -E 'scenario=|migration|invalid|dropped' \
  runs/order-rtt-run-06/metadata.txt
```

查看逐样本数据：

```bash
head runs/order-rtt-run-06/order_rtt_new.csv
```

CSV 字段：

| 字段 | 含义 |
|---|---|
| `sample` | 样本序号 |
| `raw_ticks` | 原始 TSC tick 差值 |
| `corrected_ticks` | 扣除 timestamp measurement overhead 后的 tick |
| `raw_ns` | 原始纳秒值 |
| `corrected_ns` | 扣除 measurement overhead 后的纳秒值 |

## 11. 面试常见问题

### Q1：你测的 Order RTT 到底是什么？

从 OrderGateway 第一次成功进入内核 TCP send 前开始，到响应通过 TCP 返回并由同一个
OrderGateway 验证 sequence、client id、client order id 和 response type 后结束。中间经过
OrderServer、FIFOSequencer、MatchingEngine 和 OrderBook response path。

### Q2：为什么不在 TradeEngine 把订单写入 LFQueue 时开始？

这次目标是隔离 OrderGateway 到 exchange 再返回网关的订单响应 RTT。如果从 TradeEngine
开始，就会额外包含 TradeEngine→OrderGateway 的 LFQueue 等待和线程调度。那是另一种更长的
端到端指标，应单独命名和测试。

### Q3：为什么起点不使用原来的 T12？

原 T12 只代表请求被复制到用户态 TCP buffer，没有发生真实内核 send。使用它会把网关内部
buffer 等待排除在定义之外，而且无法确认数据是否成功交给内核。

### Q4：如何关联 request 和 response？

每次只允许一个 outstanding request，同时仍然严格校验：

- 应用层 TCP sequence number；
- client id；
- client order id；
- NEW 对应 ACCEPTED、CANCEL 对应 CANCELED。

任何不匹配都会增加 protocol error 或导致本轮失败，而不会记录成有效 latency。

### Q5：为什么使用 RDTSCP，而不是 chrono？

TSC 读取成本低，适合 hot path。当前实现使用 `LFENCE; RDTSCP; LFENCE` 限制指令重排，
并读取 `TSC_AUX` 检测 CPU migration。频率只在启动阶段校准，hot path 不进行系统时钟调用。

### Q6：为什么需要减去 measurement overhead？

即使被测区间为空，两次序列化 TSC 读取本身也需要时间。程序在启动时测量大量空时间戳对并
取中位数，本次为 72 ticks，然后在离线输出 `corrected_ticks/corrected_ns` 时扣除。

### Q7：如何避免日志污染？

RTT target 定义 `LLT_BENCHMARK_MODE`，Logger 不启动后台线程、不分配生产级日志队列、
不写日志文件。hot path 只把 tick 写入预分配的 `LatencyRecorder`，CSV 和 percentile 都在
测试结束后处理。

### Q8：为什么每次只发一个 outstanding request？

这是 idle-path latency benchmark，目标是测没有排队情况下的组件基础 RTT。多个 outstanding
request 会引入 queueing delay，需要同时报告 offered load、吞吐、队列深度和响应关联方式，
属于后续 saturation benchmark。

### Q9：为什么 NEW 和 CANCEL 的 RTT 接近？

两者共享绝大部分 TCP、OrderServer、FIFO、MatchingEngine 和返回路径。并且项目的 NEW
`ACCEPTED` 在完整 `addOrder()` 前发布，而 CANCEL response 在 remove 后发布，所以不能简单根据
RTT 大小比较 add 和 cancel 的算法复杂度。

### Q10：为什么 max 达到 100 µs 以上？

max 是单个最慢样本，对 VM 调度、宿主机中断和 steal time 极其敏感。五轮 P99.9 很稳定，
但 max 波动大，这是云 VM latency benchmark 的典型现象。报告中保留 max，但主要使用
P50/P99/P99.9 描述分布。

### Q11：下一步如何扩展？

可以增加：

1. 多 outstanding request，并使用预分配数组按 order id 关联时间戳；
2. 固定 offered load 的吞吐/延迟曲线；
3. 找到饱和点并报告 queue depth、drop 和 backpressure；
4. 两台物理机之间使用 NIC hardware timestamp；
5. 裸金属、isolcpus、IRQ affinity 和关闭 SMT 条件下重复测试；
6. 使用 `perf stat/record` 分析 cache miss、branch miss 和 context switch。

## 12. 简历描述模板

推荐版本：

> 基于序列化 RDTSCP 和 TSC_AUX 实现低开销延迟测量框架，对
> OrderGateway→TCP→OrderServer→FIFOSequencer→MatchingEngine→OrderGateway 订单响应链路
> 进行 benchmark；在阿里云 8 核/16 vCPU KVM 环境完成 5 轮、每轮每种场景 100 万样本测试，
> 测得 NEW→ACCEPTED RTT P50/P99/P99.9 为 7.39/11.02/14.79 µs，
> CANCEL→CANCELED 为 7.39/11.01/14.89 µs，全部正式样本无 TSC migration、无无效或丢弃记录。

可以补充的工程点：

> 修复非阻塞 TCP partial write/EAGAIN 导致的未发送数据丢弃问题，使用预分配 recorder 将
> hot path 限制为原始 cycle 写入，并在测试结束后离线计算 P50/P90/P95/P99/P99.9 等统计量。

不建议写：x

```text
实现生产级 HFT 交易所，端到端延迟仅 7 µs。
```

原因是测试运行在 KVM、使用 loopback、只有一个 outstanding request，而且 NEW response
boundary 不包含完整 OrderBook insertion。

## 13. 30 秒面试口述版本

> 我为项目补充了一套真实的订单 RTT benchmark。起点不是原来的用户态 buffer memcpy，
> 而是 OrderGateway 第一次成功进入内核 send 前；终点是 response 回到 OrderGateway，并完成
> sequence、client id、order id 和 response type 校验之后。中间使用项目真实的 TCP、
> OrderServer、FIFOSequencer、MatchingEngine 和 OrderBook response path。计时采用
> LFENCE+RDTSCP+LFENCE，利用 TSC_AUX 拒绝迁核样本，原始 cycle 写入预分配数组，测试结束后
> 离线统计。我在阿里云 KVM 上用四个不同物理核完成五轮、每轮每种场景 100 万样本，
> NEW 和 CANCEL RTT 的 P50 都约 7.39 微秒，P99 约 11 微秒，P99.9 约 14.8 微秒。
> 我会明确说明这是单机 loopback 空载 RTT，不是生产交易所网络延迟。

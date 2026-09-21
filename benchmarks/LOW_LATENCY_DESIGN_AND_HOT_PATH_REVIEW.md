# LowLatency Trading System：低延迟设计、Hot Path 与改进路线

> 本文基于当前仓库真实源码总结项目如何降低延迟、哪些代码属于 hot path，以及哪些地方仍需
> 改进。本文不会把“支持某种优化”写成“正常运行已经启用”，也不会把 benchmark 专用实现
> 当成生产主程序的实际行为。

## 1. 一句话概括整个低延迟设计

这个项目的核心思路是：

> 把网络接入、撮合、行情发布、行情消费、策略和订单网关拆到不同 busy-loop 线程；用预分配
> 的环形队列跨线程传递固定大小消息；让 MatchingEngine 和 TradeEngine 各自单线程拥有核心
> 状态；再通过直接寻址数组、侵入式链表和对象池减少查找、锁竞争与运行时分配。

它已经包含很多典型低延迟教学设计，但当前还不是生产级 HFT 实现。主要原因包括：

- 正常模式下 hot path 日志非常多；
- 大部分正常运行线程没有真正绑核；
- `LFQueue` 没有满队列保护，并且共享计数器会造成缓存行竞争；
- 默认容器和网络 buffer 预分配过大；
- 价格索引存在 modulo collision 风险；
- 若干 `volatile bool` 跨线程读写不符合 C++ 并发规则；
- 网络协议、UDP 批量大小和异常输入处理不够健壮；
- 所有 ticker 共用一个 MatchingEngine 线程，吞吐存在单核上限。

因此最合适的项目定位是：

> 一个完整展示低延迟交易系统数据流、线程分层、订单簿和性能测量方法的教学型实现；可以用
> 正式 benchmark 证明特定组件延迟，但不能直接声称达到了生产 HFT 的绝对延迟或容量。

---

## 2. 什么是这个项目的 hot path

这里把“每条订单、每条行情或每条交易响应都会执行，并直接决定 latency”的代码称为 hot
path。偶尔运行的初始化、快照恢复和退出逻辑不属于稳定状态 hot path，但它们可能干扰尾延迟。

项目有三条最重要的业务路径。

### 2.1 Order request / matching / response 路径

```text
Trading::TradeEngine::sendClientRequest
  |
  v
ClientRequestLFQueue
  |
  v
Trading::OrderGateway::run
  |
  v
Common::TCPSocket::send + sendAndRecv
  |
  | TCP loopback / network
  v
Common::TCPServer / TCPSocket::sendAndRecv
  |
  v
Exchange::OrderServer::recvCallback
  |
  v
Exchange::FIFOSequencer::addClientRequest
  |
  v
Exchange::FIFOSequencer::sequenceAndPublish
  |
  v
ClientRequestLFQueue
  |
  v
Exchange::MatchingEngine::run
  |
  v
MatchingEngine::processClientRequest
  |
  +--> MEOrderBook::add / cancel
         +--> checkForMatch
         +--> match
         +--> addOrder / removeOrder
  |
  v
ClientResponseLFQueue
  |
  v
Exchange::OrderServer::run
  |
  v
TCPSocket::send + sendAndRecv
  |
  | TCP loopback / network
  v
Trading::OrderGateway::recvCallback
  |
  v
ClientResponseLFQueue
  |
  v
Trading::TradeEngine::run / onOrderUpdate
```

订单 RTT benchmark 的核心边界位于 Gateway 实际调用 kernel `send()` 之前，到 Gateway 收到、
校验相同 order ID 和预期 response type 为止。详见 `benchmarks/order_rtt_benchmark.cpp` 和
`trading/order_gw/order_gateway.cpp` 中 `armRTT()`、`observeKernelSend()`、`recvCallback()`。

### 2.2 Market data → strategy → order，也就是 Tick-to-Trade

```text
Exchange::MEOrderBook::add / cancel / match
  |
  v
Exchange::MatchingEngine::sendMarketUpdate
  |
  v
MEMarketUpdateLFQueue
  |
  v
Exchange::MarketDataPublisher::run
  |
  v
Common::McastSocket::send / sendAndRecv
  |
  | UDP multicast
  v
Trading::MarketDataConsumer::run
  |
  v
MarketDataConsumer::recvCallback
  |
  v
MEMarketUpdateLFQueue
  |
  v
Trading::TradeEngine::run / processMarketUpdate
  |
  v
MarketOrderBook::onMarketUpdate
  |
  +--> addOrder / removeOrder / qty modify
  +--> updateBBO
  |
  v
TradeEngine::onOrderBookUpdate 或 onTradeUpdate
  |
  +--> PositionKeeper
  +--> FeatureEngine
  +--> MarketMaker 或 LiquidityTaker
  +--> OrderManager
  +--> RiskManager
  |
  v
TradeEngine::sendClientRequest
  |
  v
ClientRequestLFQueue commit
```

正式 Tick-to-Trade microbenchmark 从 `TradeEngine::processMarketUpdateForBenchmark()` 读取序列化
TSC 开始，到 `TradeEngine::sendClientRequest()` 把第一条订单提交进 queue 后停止。它故意不包含
MarketDataConsumer、UDP 和 OrderGateway 网络发送，因此是组件内决策延迟，不是完整网络端到端。

### 2.3 Market data publish 路径

```text
MEOrderBook::add / cancel / match
  |
  v
MatchingEngine::sendMarketUpdate
  |
  v
MEMarketUpdateLFQueue
  |
  v
MarketDataPublisher::run
  |
  +--> 给 update 添加 incremental sequence
  +--> 写入 McastSocket outbound buffer
  +--> 写入 snapshot_md_updates_ queue
  |
  v
McastSocket::sendAndRecv -> UDP multicast
```

SnapshotSynthesizer 在独立线程消费 `snapshot_md_updates_`，维护快照状态并每 60 秒发布一次完整
snapshot。它不在撮合函数的同步调用栈里，但 MatchingEngine 发出的每条市场更新仍要先经过
MarketDataPublisher，再被复制一份到 snapshot queue。

---

## 3. Hot path 源码清单

### 3.1 Exchange 侧

| 优先级 | 文件与函数 | 为什么是 hot path |
|---|---|---|
| A | `common/tcp_socket.cpp`：`TCPSocket::sendAndRecv()` | 每轮非阻塞收发 TCP 数据 |
| A | `exchange/order_server/order_server.h`：`OrderServer::run()` | 轮询连接、收请求、发 response |
| A | `OrderServer::recvCallback()` | 解码请求、校验 client/sequence、进入 FIFOSequencer |
| A | `exchange/order_server/fifo_sequencer.h`：`addClientRequest()` | 每个请求保存 kernel receive timestamp |
| A | `FIFOSequencer::sequenceAndPublish()` | 按接收时间排序并提交 MatchingEngine queue |
| A | `exchange/matcher/matching_engine.h`：`run()` | 持续消费请求 queue |
| A | `MatchingEngine::processClientRequest()` | NEW/CANCEL 分发入口 |
| A | `exchange/matcher/me_order_book.cpp`：`add()`、`cancel()` | 权威订单状态变更 |
| A | `MEOrderBook::checkForMatch()`、`match()` | 撮合核心循环 |
| A | `me_order_book.h`：`addOrder()`、`removeOrder()` | 挂簿和撤单的链表修改 |
| A | `MatchingEngine::sendClientResponse()` | 每条 response 写 SPSC queue |
| A | `MatchingEngine::sendMarketUpdate()` | 每条 ADD/MODIFY/CANCEL/TRADE 写行情 queue |
| B | `exchange/market_data/market_data_publisher.cpp`：`run()` | 每条行情编号、拷贝、发布 |
| B | `common/mcast_socket.cpp`：`send()`、`sendAndRecv()` | 行情 buffer 与 UDP syscall |

### 3.2 Trading 侧

| 优先级 | 文件与函数 | 为什么是 hot path |
|---|---|---|
| A | `trading/market_data/market_data_consumer.cpp`：`run()` | busy poll 两个 multicast socket |
| A | `MarketDataConsumer::recvCallback()` | 解码、检测 sequence gap、写 TradeEngine queue |
| A | `trading/strategy/trade_engine.cpp`：`run()` | 消费 response 与 market update |
| A | `TradeEngine::processMarketUpdate()` | 本地订单簿入口 |
| A | `trading/strategy/market_order_book.cpp`：`onMarketUpdate()` | 重建本地 LOB、更新 BBO |
| A | `MarketOrderBook::addOrder()`、`removeOrder()`、`updateBBO()` | 每条行情对应的数据结构操作 |
| A | `TradeEngine::onOrderBookUpdate()` / `onTradeUpdate()` | 更新特征并调用策略 |
| A | `trading/strategy/feature_engine.h` | 计算 microprice 与 aggressive trade ratio |
| A | `market_maker.h` / `liquidity_taker.h` 回调 | 策略判断 |
| A | `trading/strategy/order_manager.h`：`moveOrder(s)` | 决定 new/cancel |
| A | `trading/strategy/risk_manager.h`：`checkPreTradeRisk()` | 下单前固定次数风控检查 |
| A | `TradeEngine::sendClientRequest()` | Tick-to-Trade 测量终点、订单 queue commit |
| A | `trading/order_gw/order_gateway.cpp`：`run()` | 从 queue 取请求并送入 TCP buffer |
| A | `OrderGateway::recvCallback()` | 校验 response 并写 TradeEngine queue |

### 3.3 会干扰 hot path，但不是核心业务计算

| 文件/组件 | 影响 |
|---|---|
| `common/logging.h`：`Logger::log()` | 正常模式几乎每个阶段都调用，producer 工作就在 hot path |
| `Logger::flushQueue()` | 独立线程做文件输出，但会竞争 CPU、内存带宽和 page cache |
| `SnapshotSynthesizer::run()` | 每条行情更新快照，周期性全量发布可形成 CPU/网络突发 |
| `MarketDataConsumer::checkSnapshotSync()` | 只在丢包恢复时运行，但会使用 map/vector 和多次日志 |

---

## 4. 已经采用的低延迟方法

## 4.1 单线程拥有核心状态，避免订单簿锁

Exchange 的所有 client requests 最终由同一个 `MatchingEngine::run()` 线程串行处理。它独占：

- `ticker_order_book_`；
- 每本 `MEOrderBook`；
- market order ID 生成；
- price-time priority 和撮合状态。

其他线程不能直接修改订单簿，只能通过 queue 输入/输出消息。这样做带来：

- 不需要给 `MEOrderBook` 加 mutex；
- 单个请求内状态变化顺序确定；
- 不会出现两个线程同时撮合同一 passive order；
- cache ownership 相对稳定；
- replay 和调试更容易。

Trading 端同样由 `TradeEngine::run()` 独占本地订单簿、FeatureEngine、PositionKeeper、RiskManager
和 OrderManager。MarketDataConsumer 和 OrderGateway 只负责 I/O，然后通过 queue 把消息交给
TradeEngine。

这是项目最重要的低延迟架构选择之一。它用串行状态机换取低同步开销和确定性。

限制是：所有 ticker 共用一个 MatchingEngine，所有策略事件也共用一个 TradeEngine。吞吐超过
单核处理能力后，queue 会积压，延迟会非线性上升。

## 4.2 组件线程之间使用预分配环形队列

源码：`common/lf_queue.h`，`Common::LFQueue<T>`。

队列构造时一次性创建 `std::vector<T> store_(num_elems)`。生产者：

```text
getNextToWriteTo()
写入固定槽位
updateWriteIndex()
```

消费者：

```text
getNextToRead()
读取固定槽位
updateReadIndex()
```

实际连接关系都是单生产者、单消费者风格：

| Queue | Producer | Consumer |
|---|---|---|
| Exchange request queue | OrderServer | MatchingEngine |
| Exchange response queue | MatchingEngine | OrderServer |
| Exchange market queue | MatchingEngine | MarketDataPublisher |
| Snapshot update queue | MarketDataPublisher | SnapshotSynthesizer |
| Trading request queue | TradeEngine | OrderGateway |
| Trading response queue | OrderGateway | TradeEngine |
| Trading market queue | MarketDataConsumer | TradeEngine |

收益：

- 不在每条消息上分配 queue node；
- 没有应用层 mutex/condition_variable；
- 固定大小 POD-like 消息直接复制；
- 线程之间通过所有权转移解耦；
- busy consumer 不需要被系统调用唤醒。

但是类名中的“LFQueue”不能被理解为通用 MPMC queue。当前实现没有多生产者 reservation，也
没有 slot sequence；它依赖当前架构中的 SPSC 使用方式。

## 4.3 Busy polling，避免阻塞和唤醒延迟

以下线程都在运行标志为 true 时持续循环，没有在空闲路径 `sleep()` 或等待条件变量：

- `OrderServer::run()`；
- `MatchingEngine::run()`；
- `MarketDataPublisher::run()`；
- `SnapshotSynthesizer::run()`；
- `MarketDataConsumer::run()`；
- `TradeEngine::run()`；
- `OrderGateway::run()`。

OrderServer 使用 `epoll_wait(..., timeout=0)`；TCP 和 multicast 数据读写使用 `MSG_DONTWAIT`。
优点是消息到达时线程通常已经在运行，避免睡眠线程的 scheduler wake-up latency。

代价是：

- 每个线程即使空闲也可能占满一个 CPU；
- 没绑核时会与其他线程争抢时间片；
- VM steal time、SMT sibling 和宿主机噪声会直接反映到 P99/P99.9；
- 多客户端会线性增加 busy-loop 线程数量。

Busy polling 只有与 CPU pinning、核心隔离和容量规划配套时才真正有效。

## 4.4 对象池避免稳定状态的通用堆分配

源码：`common/mem_pool.h`。

Exchange `MEOrderBook` 预分配：

- `MemPool<MEOrder>`；
- `MemPool<MEOrdersAtPrice>`。

Trading `MarketOrderBook` 预分配：

- `MemPool<MarketOrder>`；
- `MemPool<MarketOrdersAtPrice>`。

新增订单时使用 placement new 在池中已有地址构造对象；撤单或完全成交时只标记 block free。
收益包括：

- 避免每个 NEW/CANCEL 触发 malloc/free；
- 节点地址稳定，数组和侵入式链表可保存裸指针；
- 降低 allocator 锁竞争和不可预测慢路径；
- 容量和内存生命周期明确。

但 `MemPool::updateNextFreeIndex()` 会扫描下一个 free block，高占用或碎片化时最坏 O(P)。它是
预分配池，不是严格 O(1) free-list allocator。

## 4.5 直接寻址数组减少 hash 和树查找

Exchange：

```cpp
cid_oid_to_order_[client_id][client_order_id] -> MEOrder *
```

Trading：

```cpp
oid_to_order_[market_order_id] -> MarketOrder *
```

这让订单查找为 O(1)，没有 hash collision、rehash 或 tree walk。ticker 到 book、ticker 到
position/risk/config，以及 ticker+side 到 OMOrder 也大量使用 `std::array`。

这是适合“整数 ID 有严格上界且相对稠密”的设计。代价是 Exchange 默认二维表每 ticker 约
2 GiB，8 ticker 仅指针槽就约 16 GiB。它减少计算但严重增加 RSS、page fault、TLB 和内存压力。

## 4.6 侵入式循环双链表支持 FIFO 和常数时间摘链

`MEOrder` / `MarketOrder` 自身包含 `prev_order_`、`next_order_`；价格档节点自身包含
`prev_entry_`、`next_entry_`。

项目因此可以：

- `bids_by_price_` O(1) 得到最高 bid；
- `asks_by_price_` O(1) 得到最低 ask；
- `first_order` O(1) 得到下一 passive order；
- `first->prev` O(1) 得到 FIFO tail；
- 已知订单 O(1) 摘链；
- 价格档清空后 O(1) 切换到下一档。

需要准确说明：在已有价格档尾插是 O(1)，但新普通价格档需要扫描有序价格链，最坏 O(L)；
一次扫过 K 个 passive orders 的 aggressive order 为 O(K)。详见
`benchmarks/ORDER_BOOK_DATA_STRUCTURES_REVIEW.md`。

## 4.7 非阻塞 socket 与 TCP_NODELAY

源码：`common/socket_utils.h`。

`createSocket()` 会：

- 通过 `fcntl(..., O_NONBLOCK)` 设置非阻塞；
- 对 TCP 调用 `setsockopt(TCP_NODELAY)` 关闭 Nagle；
- 对 OrderServer TCP 开启 `SO_TIMESTAMP`；
- listening socket 使用 `SO_REUSEADDR`；
- multicast consumer 加入 multicast group。

关闭 Nagle 避免小订单消息等待合并。非阻塞 I/O 让 busy-loop 不会卡在 recv/send 上。
OrderServer 通过 `recvmsg()` 读取 `SCM_TIMESTAMP`，FIFOSequencer 使用 kernel receive timestamp
对同一轮多个连接收到的订单排序。

当前没有看到以下配置：

- `SO_RCVBUF` / `SO_SNDBUF` 显式调优；
- `SO_BUSY_POLL`；
- NIC RSS/RPS/XPS 配置；
- `recvmmsg()` / `sendmmsg()`；
- AF_XDP、DPDK 或其他 kernel bypass。

所以不能在简历中声称项目实现了 kernel bypass 或网卡级 low-latency tuning。

## 4.8 预分配 socket buffer，并允许一定程度批量发送

`TCPSocket` 默认预分配 64 MiB inbound 和 64 MiB outbound；`McastSocket` 也各预分配 64 MiB。
调用组件的 `send()` 先 `memcpy` 到用户态 outbound buffer，实际 kernel `::send()` 在后续
`sendAndRecv()` 中发生。

这样做：

- 避免每条消息创建临时动态 buffer；
- 可让连续多条消息在一次 syscall 中发送；
- 能处理 TCP partial write 和 EAGAIN，保留尚未发出的字节。

但它不是 zero-copy。消息通常经历：

```text
业务对象 -> LFQueue slot -> socket outbound vector -> kernel socket buffer
```

而且 64 MiB × 2 × 每个 socket 会造成很高的常驻内存。Mcast publisher 把 queue 中多条更新
拼入一个 outbound buffer 后一次 UDP `send()`；若积压过多，可能超过单个 UDP datagram 的
协议上限或 MTU，当前没有显式分包策略。

## 4.9 固定大小二进制消息，减少解析成本

`MEClientRequest`、`OMClientRequest`、`MEClientResponse`、`OMClientResponse`、
`MEMarketUpdate` 和 `MDPMarketUpdate` 使用固定字段，并通过 `#pragma pack(push, 1)` 去除 padding。

接收侧按 `sizeof(message)` 在字节 buffer 中逐条移动指针，用 `reinterpret_cast` 读取；不需要
JSON、文本 tokenization 或 schema lookup。sequence number 也直接包含在 wire message 中。

收益是格式简单、消息较小、解析指令少。限制是：

- packed field 可能产生未对齐访问；
- wire format 包含 `size_t`，32/64 位平台尺寸不同；
- 没有显式 endian conversion；
- enum 底层类型、版本兼容、长度和校验机制不完整；
- 当前协议适合同构机器上的教学环境，不是跨平台生产协议。

## 4.10 正常路径与行情恢复路径分离

MarketDataConsumer 正常收到连续 sequence 时，只做：

```text
检查 seq -> 自增 expected seq -> 固定消息复制到 LFQueue
```

只有出现 gap 才进入 recovery，使用两个 `std::map` 保存 snapshot/incremental update，再合并为
`std::vector`。把复杂动态容器放在异常路径而不是每条行情的稳定路径，是合理的延迟取舍。

Exchange 也用单独 SnapshotSynthesizer 线程维护和发布全量快照，使完整快照构建不在
MatchingEngine 同步调用栈内。

## 4.11 分支预测提示和 `noexcept`

`common/macros.h` 定义：

```cpp
LIKELY(x)   -> __builtin_expect(..., 1)
UNLIKELY(x) -> __builtin_expect(..., 0)
```

它被用于：

- queue 有数据；
- 订单可撤；
- 是否继续撮合；
- 风控失败；
- 策略价格/阈值分支；
- 行情 gap/recovery 等异常路径。

许多 hot functions 声明为 `noexcept`。这能表达“不把异常传播作为正常控制流”的意图，并可能
帮助优化，但项目并没有使用 `-fno-exceptions`，而且部分 `noexcept` 函数内部调用
`std::array::at()`；越界抛出时会直接 `std::terminate()`。`noexcept` 不能替代输入校验。

分支 hint 也不是自动优化保证。只有 profile 证明该分支分布稳定时才有价值，错误 hint 反而可能
影响代码布局。

## 4.12 异步 Logger 把文件 I/O 移出调用线程

每个 `Logger` 有一个预分配 `LFQueue<LogElement>` 和独立 `flushQueue()` 线程。业务线程调用
`log()` 时把 primitive value/字符写入 queue；Logger 线程再写 `ofstream`、flush 并 sleep 10 ms。

这确实避免业务线程直接等待磁盘。但不能把它描述为“日志不影响 hot path”，因为业务线程仍要：

- 调用 `getCurrentTimeStr()`；
- 经常构造 `toString()`、`stringstream` 和临时 `std::string`；
- 解析 format string；
- 原版 Logger 对字符串逐字符 push；
- 每个字符都更新 LFQueue 的 atomic index/count。

因此异步只移走了 consumer I/O，producer formatting 和入队成本仍在 hot path。

## 4.13 独立的正式测量路径

当前新增 benchmark 基础设施位于：

- `common/tsc_clock.h`；
- `common/latency_recorder.h`；
- `benchmarks/matching_latency_benchmark.cpp`；
- `benchmarks/tick_to_trade_benchmark.cpp`；
- `benchmarks/order_rtt_benchmark.cpp`。

正式 benchmark 使用 `LFENCE + RDTSCP + LFENCE`，记录 TSC_AUX 检测迁核，启动时相对
`CLOCK_MONOTONIC_RAW` 校准频率，并把 raw ticks 写到预分配数组。测试结束后才生成 CSV 和计算
percentile。

`LLT_BENCHMARK_MODE` 会禁用嵌套 `START_MEASURE/END_MEASURE` 和 Logger 文件线程，使被测路径不被
逐样本日志污染。这是可信 benchmark 的正确方向，但也意味着 microbenchmark 数字不等于正常
主程序开启完整日志时的延迟。

---

## 5. 哪些设计主要优化 latency，哪些主要保证正确性

| 设计 | 主要目标 | 对 latency 的作用 |
|---|---|---|
| 单线程拥有订单簿 | 顺序正确、无数据竞争 | 避免核心状态 mutex |
| SPSC 风格预分配 queue | 线程解耦 | 避免动态 node 和阻塞唤醒 |
| Busy polling | 快速响应 | 消除 sleep/wakeup，但消耗 CPU |
| MemPool | 稳定地址、容量受控 | 避免 hot-path malloc/free 抖动 |
| 直接寻址 array | 快速查找 | 固定下标，代价是大内存 |
| 侵入式环链 | price-time priority | O(1) 尾插、摘链、best 切换 |
| TCP_NODELAY | 避免小包等待 | 降低订单消息发送等待 |
| Kernel rx timestamp + FIFO sort | 跨连接公平排序 | 会增加 batch/sort 成本，不是纯延迟优化 |
| Incremental sequence | 检测行情 gap | 正常路径只有固定检查 |
| Snapshot recovery | 行情一致性 | 独立线程/异常路径，减少对稳定路径影响 |
| Async Logger | 可观测性 | 只移走磁盘 I/O，producer 仍然较重 |
| TSC recorder | 测量可信度 | benchmark hot path 仅固定数组写入 |

低延迟系统不能只追求更少 CPU cycles。FIFOSequencer、sequence check 和 snapshot 增加了工作量，
但它们承担公平性和恢复能力。正确的优化目标是在不破坏这些语义的前提下降低延迟。

---

## 6. 当前设计中会伤害 latency 的地方

## 6.1 正常模式日志是最明显的 hot-path 污染

正常构建没有因为 `NDEBUG` 自动移除 Logger。大量路径会先调用：

```cpp
getCurrentTimeStr(...)
request->toString()
logger.log(...)
```

`toString()` 广泛使用 `std::stringstream` 和动态字符串。原 Logger 又对字符串逐字符入队。
一条业务事件会在 OrderGateway、OrderServer、MatchingEngine、MarketDataPublisher、MDC、
TradeEngine、OrderBook、FeatureEngine、Strategy、OrderManager 等多个位置重复记录。

正式 benchmark 通过 `LLT_BENCHMARK_MODE` 关闭这些路径，所以当前 benchmark 是“算法/组件的
隔离延迟”，不是“生产日志全开时的运行延迟”。

## 6.2 Logger queue 自身巨大，并且没有满队列处理

正常 `LOG_QUEUE_SIZE = 8 * 1024 * 1024`。在常见 x86-64 ABI 下，原始 `LogElement` 约 16 bytes，
单个 Logger queue 约 128 MiB。

Exchange 有 main、OrderServer、MatchingEngine、MarketDataPublisher、SnapshotSynthesizer 等多个
Logger；每个 trading process 也有 main、TradeEngine、OrderGateway、MarketDataConsumer 多个
Logger。大 queue 会增加 RSS、page fault、swap/OOM 风险和内存带宽压力。

`LFQueue::getNextToWriteTo()` 不检查满队列。如果 producer 超过 Logger consumer，可能覆盖尚未读取
的槽位，`num_elements_` 还可能继续超过 capacity。这既是可观测性问题，也是正确性问题。

## 6.3 LFQueue 有多余共享原子操作和 false sharing 风险

`next_write_index_`、`next_read_index_`、`num_elements_` 连续放在类中。生产者和消费者都会修改
`num_elements_`，极可能使同一 cache line 在两个核心之间来回转移。

所有 atomic 默认使用 sequential consistency；对于严格 SPSC ring，通常可以使用独立 producer/
consumer sequence 配合 acquire/release，避免共享 size counter。队列还每次使用 `% store_.size()`，
当 capacity 是 2 的幂时可改用 bit mask。

## 6.4 默认线程实际没有绑核

`createAndStartThread(core_id, ...)` 支持 `pthread_setaffinity_np()`，但正常主程序：

- `exchange_main` 构造 MatchingEngine 和 OrderServer 时没有传 core ID，默认 `-1`；
- MarketDataPublisher 和 SnapshotSynthesizer 固定传 `-1`；
- `trading_main` 构造 OrderGateway 时没有传 core ID；
- TradeEngine 和 MarketDataConsumer 固定传 `-1`；
- Logger 线程也固定传 `-1`。

因此源码是“部分组件具备可选绑核接口”，不是“生产主程序已经绑核”。Order RTT benchmark
接受 CPU 参数并绑核，属于 benchmark harness 的行为。

## 6.5 多个运行标志使用 `volatile bool`

MarketDataPublisher、SnapshotSynthesizer、MarketDataConsumer 和 TradeEngine 等类使用
`volatile bool run_` 在主线程和 worker 线程之间读写。C++ 的 `volatile` 不提供原子性或 happens-
before；这构成 data race。

MatchingEngine、OrderServer、OrderGateway 已使用 `std::atomic<bool>`，应统一改成 atomic，并使用
明确的 acquire/release 或 relaxed 语义。

## 6.6 部分线程对象没有保存和 join

`createAndStartThread()` 返回 `new std::thread`。MatchingEngine、OrderServer、OrderGateway 保存
指针并在 stop 时 join/delete；但 TradeEngine、MarketDataConsumer、MarketDataPublisher、
SnapshotSynthesizer 的 start 路径没有保存返回值。

这会造成 thread object 泄漏，也让 stop/destructor 无法可靠等待 worker 完全退出。该问题主要是
生命周期正确性，但退出/重启和资源回收不确定也会影响 benchmark 可复现性。

## 6.7 对象池 free-slot 搜索不是严格 O(1)

`MemPool::deallocate()` 只把 block 标为 free；`allocate()` 之后的 `updateNextFreeIndex()` 线性寻找
下一个 free block。池高占用、回收位置分散时，扫描长度会进入 latency distribution 的尾部。

## 6.8 价格索引没有处理碰撞

`priceToIndex(price) = price % ME_MAX_PRICE_LEVELS`。价格 100 和 356 都落在槽位 100；当前没有
collision resolution，也没有检查槽位中 level 的 exact price。结果不仅是性能问题，更可能导致
错误 level、链表损坏或错误撮合。

## 6.9 Trading BBO 数量每次遍历最优档

`MarketOrderBook::updateBBO()` 读取 best price 是 O(1)，但会沿 best-level FIFO 累加所有订单数量。
同一 best price 有 M 个订单时为 O(M)，会直接进入 Tick-to-Trade latency。

## 6.10 FIFOSequencer 的排序是有界但非 O(1)

OrderServer 每轮读取多个 socket 后，`FIFOSequencer::sequenceAndPublish()` 对最多 1024 个 pending
requests 调用 `std::sort`，复杂度 O(B log B)。这提供跨连接时间排序，但增加批量等待与排序成本。

`SO_TIMESTAMP` 当前得到 `timeval`，精度为 microseconds 后再乘成 nanoseconds；多个请求可能拥有
相同 timestamp。`std::sort` 也不稳定，结构中没有显式 tie-breaker，因此同 timestamp 请求的
严格顺序没有定义。

## 6.11 大 socket buffer 带来内存与 page-fault 成本

每个 TCP/Mcast socket 默认都分配 128 MiB 用户态 buffer，即使该 socket 实际只发或只收。每增加
一个 TCP client，OrderServer 还会为 accepted socket 新建一对 64 MiB buffer。

大 buffer 可以容纳 burst，但并不自动降低延迟。若没有提前 fault-in，会在首次触页时产生 major/
minor page fault；大量 buffer 还会放大 RSS 和 swap 风险。

## 6.12 接收 buffer 使用 `memmove`/`memcpy` 压缩残余字节

OrderServer、OrderGateway 收到完整消息后用 `memmove` 把残余字节移到 buffer 开头；MDC 使用
`memcpy` 做类似处理。碎片化 TCP stream 下可能频繁移动数据。更适合的实现是 ring buffer 或
双指针 parser，只有跨环消息才做一次小拷贝。

MDC 对重叠区域使用 `memcpy`；如果 source/destination 区域重叠，C++ 语义未定义，应该使用
`memmove` 或改为 ring buffer。

## 6.13 多处 `std::function` 位于事件分发路径

Socket receive callback、TradeEngine strategy callbacks 使用 `std::function`。函数对象在启动时
绑定，稳定运行时通常不会重复分配，但每次事件仍可能有间接调用，并妨碍 inlining。

它是否是主要瓶颈必须通过 perf/call-graph 和 A/B benchmark 证明。不能只凭“虚调用一定慢”就
声称替换后获得优化。

## 6.14 所有 ticker 共用单个 MatchingEngine

`MatchingEngine::ticker_order_book_` 是 8 本 book 的指针数组，但只有一个 `run()` 线程消费所有
请求。优点是全局顺序清楚；缺点是不同 ticker 之间无法并行。

当负载接近单线程 service rate 时，根据排队论，即使单次撮合很快，queue waiting latency 也会
迅速上升。这也是项目当前不适合直接声称“高并发线性扩展”的根本原因之一。

## 6.15 正常 instrumentation 仍是逐样本日志

非 benchmark 模式的 `START_MEASURE/END_MEASURE` 会读取 TSC，然后把每个差值写 Logger。虽然当前
`rdtsc()` 已经调用序列化 `readTSC()`，宏仍只保存 ticks，不保留 TSC_AUX，且每条样本写日志。

用于正式 benchmark 时应该使用 `LatencyRecorder`：记录 start/end AUX、拒绝 migration、扣除
measurement overhead、动态校准频率并离线输出，而不是从正常业务日志中提取最终 percentile。

## 6.16 多 ticker 的 FeatureEngine 状态没有按 ticker 隔离

`TradeEngine` 拥有 8 本 `MarketOrderBook`，但 `FeatureEngine` 只有一份 `mkt_price_` 和一份
`agg_trade_qty_ratio_`，没有 `std::array<Feature, ME_MAX_TICKERS>`。任意 ticker 的行情都会覆盖这
两个值，随后另一 ticker 的 MarketMaker/LiquidityTaker 可能读取到前一个 ticker 的 feature。

这首先是多 ticker 正确性问题。修复时应按 ticker 保存 feature，并让 `getMktPrice(ticker_id)`、
`getAggTradeQtyRatio(ticker_id)` 显式带 ticker。按 ticker 的定长数组仍可保持 O(1) 访问。

`TradeEngine::last_event_time_` 也由 worker 更新、main thread 通过 `silentSeconds()` 读取，却不是
atomic；它应和 `run_` 一起纳入并发状态修复。

---

## 7. 推荐的未来改进路线

改进顺序应先保证正确性和可测量性，再优化 cycles。否则可能得到“更快但会丢数据”的结果。

## 7.1 第一阶段：正确性与 benchmark 基础

### 改进 1：把所有跨线程 `run_` 改为 atomic，并正确 join

位置：

- `exchange/market_data/market_data_publisher.h`；
- `exchange/market_data/snapshot_synthesizer.h`；
- `trading/market_data/market_data_consumer.h`；
- `trading/strategy/trade_engine.h`。

为每个 component 保存 `std::thread *` 或直接使用 `std::thread/std::jthread` 成员；stop 后 join，再
销毁依赖对象。

验收：ThreadSanitizer 生命周期测试、重复 start/stop 测试、退出时无泄漏。

### 改进 2：重写 LFQueue 为明确的 bounded SPSC ring

位置：`common/lf_queue.h`。

目标：

- producer/consumer counter 分离到不同 cache line；
- 使用 monotonically increasing sequence 或 head/tail；
- acquire/release publication；
- 写满时返回 false 或计数 drop/backpressure，绝不覆盖未读槽；
- 容量取 2 的幂，使用 mask；
- 明确文档声明 SPSC，禁止 MPMC 使用。

验收：长时间 wrap-around、producer faster、consumer faster、queue full、TSAN 和消息序列校验。

### 改进 3：修复订单簿索引正确性

位置：

- `exchange/matcher/me_order_book.cpp` 构造函数：正常模式也初始化 client/order slot；
- `me_order_book.h` 与 `market_order_book.h`：替换 `price % 256` 无冲突索引；
- 入口处校验 ticker/client/order/price；
- `MarketOrderBook::CLEAR` 清空 price slots 和 BBO。

同时把 `FeatureEngine` 的 feature state 改成按 ticker 存储，并修复 `last_event_time_` 的跨线程
同步。

价格结构可选：

- 已知有界 tick range：exact tick array + active bitmap；
- 宽价格范围：预分配 flat/open-address hash，key 必须包含 exact `(side, price)`；
- 活跃 level 很少：预分配树节点的 ordered tree。

### 改进 4：约束 UDP batch 大小并定义 wire protocol

位置：`common/mcast_socket.cpp`、`market_data_publisher.cpp`、消息头文件。

- 每个 datagram 限制条数/字节数；
- 按路径 MTU 决定 payload，必要时应用层分包；
- wire 字段使用固定宽度整数；
- 明确 network byte order、version、message length、type；
- 加入 malformed packet/partial frame 校验。

## 7.2 第二阶段：去除 hot-path 可观测性开销

### 改进 5：按级别编译掉详细日志

位置：`common/logging.h` 以及所有 event-level call site。

推荐把日志分为：

- `ERROR/WARN`：保留；
- `INFO`：生命周期和低频状态；
- `TRACE_EVENT`：订单/行情逐事件日志，仅诊断构建开启；
- `BENCHMARK`：只写固定二进制 sample/counter。

关键是让关闭级别在编译期消失，使 `toString()`、`stringstream` 和时间格式化连参数都不求值。

### 改进 6：使用固定二进制日志记录

若必须保留逐事件审计，可将固定结构写入 byte ring：

```text
timestamp + event_type + ids + numeric fields
```

后台线程再格式化成人类文本。不要把字符串拆为上百个 `LogElement`；也不要把每个 queue slot
直接扩大到 256 bytes、导致单 Logger 数 GiB。需要同时测 producer cycles、drop count、consumer
throughput 和 RSS。

### 改进 7：正式测量只写预分配 raw samples

继续沿用 `LatencyRecorder`，在目标 boundary 只执行：

```text
serialized timestamp -> fixed array write
```

测试结束后再做 TSC→ns、sort、percentile、CSV。正常 tracing 与 benchmark measurement 分离。

## 7.3 第三阶段：线程、CPU 与内存布局

### 改进 8：让生产 main 接受 CPU mapping

位置：`exchange_main.cpp`、`trading_main.cpp` 以及各 component constructor/start。

最低限度为：

```text
独立物理核：MatchingEngine
独立物理核：OrderServer
独立物理核：MarketDataPublisher
独立物理核：TradeEngine
独立物理核：MarketDataConsumer
独立物理核：OrderGateway
```

Logger 和 Snapshot 在有余量时放到非关键核，不能与 MatchingEngine/TradeEngine 的 SMT sibling
共享执行资源。CPU 编号必须根据 `lscpu -e`/NUMA topology 配置，不能照抄固定数字。

### 改进 9：预 fault、锁内存并控制 NUMA

预分配并不等于预触页。启动阶段应显式 touch queue、pool 和 socket buffer；在有权限和内存预算
时评估 `mlockall()`，避免 benchmark 中 page fault/swap。核心状态和使用它的线程应位于同一 NUMA
node。

关闭 swap 有助于避免灾难性延迟，但前提是先把默认巨型 array/logger/socket buffer 缩到合理
容量并保留系统安全余量。不能用“关 swap”掩盖内存设计过大。

### 改进 10：right-size 所有固定容量

把下列常量改为按 benchmark/部署配置生成：

- `ME_MAX_NUM_CLIENTS`；
- `ME_MAX_ORDER_IDS`；
- `ME_MAX_CLIENT_UPDATES` / `ME_MAX_MARKET_UPDATES`；
- Logger queue；
- TCP/Mcast inbound/outbound buffer。

只发不收或只收不发的 Mcast socket 不应无条件分配两个 64 MiB buffer。容量选择要基于最大 burst
和 backpressure 策略，而不是越大越低延迟。

## 7.4 第四阶段：核心算法和数据结构

### 改进 11：MemPool 使用 O(1) free-list

位置：`common/mem_pool.h`。

构造时建立 free index stack；allocate pop、deallocate push。单线程 book pool 无需锁。另需：

- 明确 exhaustion 行为；
- 可选 generation counter 检测 stale handle；
- benchmark fragmentation、高 occupancy 与 steady reuse。

### 改进 12：缓存 best-level aggregate quantity

位置：`trading/strategy/market_order.h`、`market_order_book.h/.cpp`。

在 `MarketOrdersAtPrice` 添加 `total_qty_`，ADD/MODIFY/CANCEL 时增量维护。这样 BBO price 和 qty
都能 O(1) 读取，减少同价格订单较多时的 Tick-to-Trade 尾延迟。

### 改进 13：评估 callback 静态分发

位置：TradeEngine 的三个 `std::function` callback，以及 socket callback。

可以用 template/variant/function pointer/直接 owner 调用做 A/B。只有 perf 显示间接调用和无法内联
占比显著时才值得修改；策略可插拔性也是有价值的工程属性。

### 改进 14：减少消息复制

当前小消息复制通常成本可控，不应盲目实现复杂 zero-copy。先用 perf 测 `memcpy` 和 cache miss。
若确实成为瓶颈，可以评估：

- queue slot 原地构造；
- 批量 descriptor；
- circular network buffer；
- sendmsg/iovec，避免先拼接部分 header/body；
- 生命周期可证明的 buffer ownership。

任何 zero-copy 都必须保证 producer 不会覆盖 consumer 仍在使用的数据。

## 7.5 第五阶段：网络与扩展性

### 改进 15：批量 syscall，但设置延迟上限

可评估 `recvmmsg()`/`sendmmsg()` 或 bounded batch。batch 可提高吞吐，却会让 batch 首条消息等待
更多事件。应同时报告：

- messages/sec；
- P50/P99/P99.9；
- batch size distribution；
- queue waiting time。

### 改进 16：按 ticker shard MatchingEngine

当单核饱和后，可按 ticker/partition 把请求路由到多个 MatchingEngine，每个 shard 仍单线程拥有
自己的 books。这样保留 book 内无锁语义。

需要重新设计：

- OrderServer 到 shard 的 queue；
- client response 的多 producer 汇聚；
- market update 全局 sequence 或 partition sequence；
- 跨 ticker 的风险/策略一致性；
- CPU/NUMA placement。

这是吞吐扩展方案，不保证单笔空载 latency 一定降低。

### 改进 17：只有证据充分时才考虑 kernel bypass

当前 loopback/TCP/UDP 系统调用版本适合教学和实习项目。只有 perf 显示内核网络栈是主要成本，并且
测试环境支持真实 NIC queue 隔离时，才考虑 AF_XDP/DPDK。否则实现复杂度会掩盖订单簿、排队和
策略本身的问题。

## 7.6 第六阶段：构建与编译优化

当前 `CMAKE_BUILD_TYPE=Release` 在现有 GCC 环境生成 `-O3 -DNDEBUG`，但项目没有显式启用：

- `-march=native` / 针对部署 CPU 的 `-march=`；
- LTO；
- PGO；
- `-fno-exceptions` / `-fno-rtti`。

可对 `-march`、LTO、PGO 做相同 workload A/B。不要直接把所有 flag 堆上去；每种构建必须记录
compiler、CPU、二进制 hash 和结果。只有清理所有可能抛出路径后，才讨论 `-fno-exceptions`。

---

## 8. 推荐的优化优先级

| 优先级 | 改进 | 原因 |
|---:|---|---|
| P0 | LFQueue 满队列保护 + 明确 SPSC 内存序 | 否则高负载可能覆盖数据 |
| P0 | atomic run flag + 保存/join 线程 | 消除 data race 和生命周期错误 |
| P0 | 修复 price collision、ID 校验、CLEAR | 防止错误订单簿状态 |
| P0 | UDP 分包与 wire-format 边界校验 | 防止 burst 下发送失败/协议不兼容 |
| P1 | 编译期关闭逐事件文本日志 | 最直接降低正常 hot-path 开销 |
| P1 | right-size array/logger/socket buffer | 避免 OOM、swap、page fault |
| P1 | 生产 main 支持绑核 | 降低迁核和 scheduler noise |
| P1 | SPSC cache-line 对齐和 acquire/release | 降低跨核 cache bouncing |
| P2 | MemPool free-list | 控制高占用尾延迟 |
| P2 | price-level cached quantity | 改善 Trading BBO/TTT 尾延迟 |
| P2 | circular receive buffer | 避免 memmove 与重叠 memcpy |
| P2 | bounded batch syscall | 提高吞吐，同时控制等待 |
| P3 | callback static dispatch | 必须由 profile 证明收益 |
| P3 | ticker sharding | 单核饱和后扩展吞吐 |
| P3 | LTO/PGO/CPU-specific build | 算法和正确性稳定后再调 |
| P4 | AF_XDP/DPDK | 只有真实网络 profiling 支持时考虑 |

---

## 9. 每项优化应该如何证明有效

不要同时修改五个变量再比较两个数字。每个优化做独立 A/B：

| 优化 | A | B | 主要指标 |
|---|---|---|---|
| Logging | 正常逐事件日志 | 编译期关闭 trace | P50/P99/P99.9、logger drops、RSS |
| SPSC queue | 当前三个 atomic | cache-line aligned SPSC | ns/op、throughput、cache misses |
| MemPool | 扫描空块 | free-list | occupancy 分层 P99/P99.9 |
| BBO | 遍历 best FIFO | cached total qty | 不同 M 的 TTT percentile |
| Price levels | 线性环链 | bitmap/tree/flat hash | existing/new-best/new-worst workloads |
| CPU placement | unpinned | 独立物理核 | migrations、context switches、tail |
| Buffers | 64 MiB 双向 | 按方向/负载配置 | RSS、faults、drops、tail |
| Batching | 单条/当前隐式 batch | bounded batch | throughput 和 per-message tail |
| Sharding | 单 ME | N ticker shards | saturation curve、queue depth、fairness |
| Compile | baseline O3 | LTO/PGO/target CPU | binary hash、cycles、percentiles |

每次测试至少保存：

- CPU 型号、物理核/SMT/NUMA；
- OS/kernel、compiler、CMake flags；
- git commit 或 source hash；
- 是否绑核、是否隔离、是否关闭 swap；
- warm-up、sample count、输入分布；
- sample count、mean、P50、P90、P95、P99、P99.9、min、max、stddev；
- migration/invalid/dropped samples；
- throughput、queue depth、RSS 和 page faults；
- 五轮原始 CSV，不只保存最好一轮。

Outlier 不应直接删除。先保留完整分布，再结合 context switch、migration、page fault、IRQ、VM steal
分析来源。若另给 trimmed statistics，必须同时报告原始结果和明确规则。

---

## 10. 当前可以安全写进简历的低延迟设计点

可以写：

> 基于 C++20 实现分层交易系统，将 OrderServer、MatchingEngine、MarketDataPublisher、
> MarketDataConsumer、TradeEngine 与 OrderGateway 拆分为独立事件循环，并通过预分配 SPSC 风格
> 环形队列传递固定大小消息，使订单簿与策略状态保持单线程所有权，避免核心状态锁竞争。

可以写：

> 使用直接寻址订单索引、预分配 MemPool 与两层侵入式循环双链表实现 price-time priority，支持
> O(1) 订单 ID 查找、已有价格档 FIFO 尾插、已知订单撤销和 best-price 访问；针对新价格档 O(L)
> 与多笔扫单 O(K) 分场景 benchmark。

可以写：

> 在网络层使用 non-blocking TCP/UDP、TCP_NODELAY、epoll zero-timeout polling、固定二进制消息和
> sequence-based recovery；使用 kernel receive timestamp 对同轮多连接请求进行 FIFO 排序。

可以写：

> 构建基于 LFENCE+RDTSCP+LFENCE、TSC_AUX 迁核检测、动态频率校准和预分配 sample recorder 的
> benchmark 框架，对 Matching、Tick-to-Trade 和 TCP loopback Order RTT 进行五轮 percentile 测试。

需要加限定语：

> benchmark 模式关闭逐事件日志，结果代表给定硬件、绑核和 workload 下的组件/loopback 数据，
> 不代表真实交易所网络或生产部署延迟。

不能写：

- “系统全链路 zero-copy”；
- “使用 DPDK/kernel bypass”；
- “所有订单簿操作 O(1)”；
- “生产模式全部线程绑核”；
- “LFQueue 支持任意 MPMC 且不会丢消息”；
- “项目支持 client 数量线性扩展”；
- “达到生产 HFT 纳秒级端到端”；
- “完整处理所有网络异常和行情恢复边界”。

---

## 11. 面试问答模板

### 问：这个项目为什么能做到低延迟？

> 我把低延迟来源分成架构、数据结构、内存和 I/O 四层。架构上，MatchingEngine 与 TradeEngine
> 单线程拥有核心状态，线程之间用预分配 SPSC queue 传消息，避免订单簿锁；数据结构上用直接
> 寻址、best pointer 和侵入式 FIFO 链表；内存上订单节点来自对象池；I/O 上用 non-blocking
> socket、TCP_NODELAY 和 busy polling。与此同时，我通过源码分析发现正常日志、未默认绑核、
> queue false sharing 和巨型预分配仍会影响尾延迟，所以用正式 benchmark 隔离测量并设计 A/B。

### 问：最关键的 hot path 是什么？

> Exchange 侧是 OrderServer recv/decode → FIFOSequencer → request queue → MatchingEngine →
> MEOrderBook add/cancel/match → response/market queues。Trading 侧是 multicast recv/decode → market
> queue → MarketOrderBook/BBO → FeatureEngine → strategy → RiskManager/OrderManager → request queue。
> OrderGateway 和两个方向的 socket 收发决定 RTT，Logger producer 虽不是业务逻辑，却也处于正常
> hot path。

### 问：为什么不直接多线程并发修改一本订单簿？

> 同一 order book 内的 price-time priority、部分成交和 level 删除有强顺序依赖。单线程 state
> ownership 避免细粒度锁和竞态，延迟更可预测。扩展时应该按 ticker shard，每个 shard 仍单线程，
> 而不是多个线程同时修改同一本 book。

### 问：预分配为什么不一定更快？

> 它避免 hot-path malloc 和地址变化，但容量过大会造成 RSS、page fault、TLB miss、NUMA remote
> access 甚至 swap。当前每 Logger 约 128 MiB queue、每 socket 约 128 MiB 双向 buffer，Exchange
> 的二维订单索引更大。因此容量必须结合实际 workload right-size 并预 fault，不能只说预分配越
> 多越低延迟。

### 问：你下一步先优化哪里？

> 先修 queue 满覆盖、run flag data race、thread join、price collision 和 UDP 分包等正确性；然后
> 编译期关闭逐事件文本日志、right-size buffer、生产绑核，并用同一 workload 五轮 A/B。之后再看
> perf 决定优化 SPSC cache line、MemPool free-list、BBO cached qty 或 ticker sharding。

---

## 12. 一页复习版

```text
低延迟架构：
I/O thread -> preallocated SPSC-style queue -> single-owner engine
Exchange book 只由 MatchingEngine 修改
Trading book/feature/risk/order state 只由 TradeEngine 修改

低延迟数据结构：
direct array lookup
intrusive circular doubly-linked price/FIFO lists
best bid/ask pointers
preallocated MemPool

低延迟 I/O：
O_NONBLOCK + MSG_DONTWAIT
TCP_NODELAY
epoll timeout=0 / busy loops
fixed packed binary messages
sequence number + snapshot recovery

真实 Hot Path：
OrderServer -> FIFOSequencer -> MatchingEngine -> MEOrderBook
MDC -> TradeEngine -> MarketOrderBook -> Feature -> Algo -> Risk/OM
OrderGateway + TCP send/recv
Logger producer 也会污染正常 hot path

当前主要问题：
normal-mode event logs very heavy
most production threads unpinned
LFQueue no full check + shared atomic count/false sharing
volatile cross-thread run flags + missing join
huge arrays/logger/socket buffers
price modulo collision
BBO qty O(M), new price level O(L), sweep K orders O(K)
single MatchingEngine for all tickers

改进顺序：
correctness -> measurement -> logging/buffer sizing -> CPU/NUMA
-> queue/pool/book optimization -> networking/batching -> sharding

验证方法：
one-variable A/B
warm-up + preallocated raw samples
P50/P99/P99.9 + throughput + queue depth + RSS/faults
five runs, keep all outliers, record environment
```

这个项目真正值得展示的不是“用了几个 low-latency 关键词”，而是能够从一条事件路径解释每次
线程切换、拷贝、查找、分配和系统调用发生在哪里，再用可复现 benchmark 判断哪一项真的值得
优化。

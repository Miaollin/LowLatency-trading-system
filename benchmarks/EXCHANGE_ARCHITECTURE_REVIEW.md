# LowLatency Trading System：Exchange 源码架构复习文档

> 用途：源码复习、面试准备、Exchange 数据结构与调用链梳理  
> 分析范围：当前仓库的 `exchange/`，以及它直接依赖的网络、队列、内存池实现  
> 原则：只描述真实源码；设计意图、当前实现和生产级限制分别说明  
> 配套性能数据见 `benchmarks/PERFORMANCE_BENCHMARK_REPORT.md`

## 阅读建议

第一次阅读先看第 1 节整体架构和第 5～8 节调用链；第二次再看第 3～4 节的成员变量与数据结构；
面试前重点复习第 9 节限制和第 10 节口述模板。

## 1. Exchange 整体实现思路

本章只描述当前仓库的真实实现。核心源码位于：

```text
exchange/exchange_main.cpp
exchange/order_server/*
exchange/matcher/*
exchange/market_data/*
common/tcp_server.*
common/tcp_socket.*
common/mcast_socket.*
common/lf_queue.h
common/mem_pool.h
```

### 1.1 一句话架构

Exchange 是一个“网络接入、确定顺序、单线程撮合、双通道行情发布”系统：

```text
Trading client / OrderGateway
            │ TCP：OMClientRequest
            ▼
     OrderServer thread
       │ 校验 client/sequence
       ▼
      FIFOSequencer
       │ 按软件接收时间排序
       ▼ ClientRequestLFQueue
   MatchingEngine thread
       │ 根据 ticker 分派
       ▼
      MEOrderBook
       ├──────── ClientResponseLFQueue ────────► OrderServer ──TCP──► client
       │
       └──────── MEMarketUpdateLFQueue ───────► MarketDataPublisher
                                                   ├─增量 UDP multicast
                                                   └─MDPMarketUpdateLFQueue
                                                          ▼
                                                SnapshotSynthesizer thread
                                                          └─快照 UDP multicast
```

OrderServer、MatchingEngine、MarketDataPublisher、SnapshotSynthesizer 分别运行自己的 busy-loop。
OrderBook 本身没有单独线程，它只由 MatchingEngine 线程调用。因此正常设计下每个 OrderBook 只有
一个写线程，不需要在订单簿内部加锁。

### 1.2 进程启动与对象所有权

`exchange/exchange_main.cpp::main()` 的启动顺序是：

1. 创建主 Logger，注册 `SIGINT` handler；
2. 在 main 栈上创建三条跨组件 LFQueue；
3. `new MatchingEngine(...)`，然后 `start()`；
4. `new MarketDataPublisher(...)`，其构造函数内部再创建 `SnapshotSynthesizer`，然后启动两个线程；
5. `new OrderServer(...)`，监听 loopback `12345` 并启动线程；
6. main 线程进入长期 sleep/log 循环。

三条核心队列的所有权和方向如下：

| 队列对象 | 元素类型 | 容量 | 唯一 producer | 唯一 consumer |
|---|---|---:|---|---|
| `client_requests` | `MEClientRequest` | `ME_MAX_CLIENT_UPDATES`，默认 262,144 | OrderServer/FIFOSequencer | MatchingEngine |
| `client_responses` | `MEClientResponse` | 默认 262,144 | MatchingEngine | OrderServer |
| `market_updates` | `MEMarketUpdate` | `ME_MAX_MARKET_UPDATES`，默认 262,144 | MatchingEngine | MarketDataPublisher |
| `snapshot_md_updates_` | `MDPMarketUpdate` | 默认 262,144 | MarketDataPublisher | SnapshotSynthesizer |

前三个队列对象由 `main()` 持有，各组件只保存指针；第四个由 MarketDataPublisher 直接持有。
这种 ownership 符合当前 `LFQueue` 实际适用的 SPSC 模式。

### 1.3 真实线程模型

| 线程 | 入口 | 主要工作 |
|---|---|---|
| main | `exchange_main.cpp::main()` | 构造组件、处理进程生命周期，本身不撮合 |
| OrderServer | `OrderServer::run()` | epoll、TCP 收发、请求校验、FIFO 排序、响应发送 |
| MatchingEngine | `MatchingEngine::run()` | 消费请求并同步执行 OrderBook 操作 |
| MarketDataPublisher | `MarketDataPublisher::run()` | 消费行情更新、增加全局序号、发送增量组播 |
| SnapshotSynthesizer | `SnapshotSynthesizer::run()` | 维护全量订单镜像，每 60 秒发布快照 |
| 5 个 Logger thread | `Logger::flushQueue()` | 分别写 main、OrderServer、Matching、Publisher、Snapshot 日志 |

因此正常 exchange 进程大约有 10 条线程：main + 4 条业务线程 + 5 条 Logger 线程。TCPServer、
TCPSocket 和 McastSocket 自己不创建线程。

当前 `exchange_main` 创建组件时都没有传具体 `core_id`，MarketDataPublisher 和
SnapshotSynthesizer 也硬编码用 `-1` 创建线程，所以正常启动的 exchange **默认没有绑核**；本次
Order RTT benchmark 的绑核是 benchmark harness 额外传入的配置，不能等同于生产主程序行为。

## 2. 网络和内部消息格式

### 2.1 请求消息

文件：`exchange/order_server/client_request.h`

`ClientRequestType` 当前只有：

- `NEW`：新订单；
- `CANCEL`：撤单；
- `INVALID`：默认/错误值；
- 没有实现网络请求类型 `MODIFY`。

`MEClientRequest` 是撮合侧真正消费的请求：

| 变量 | 作用 |
|---|---|
| `type_` | NEW 或 CANCEL |
| `client_id_` | 客户端标识，也是 OrderServer 数组下标 |
| `ticker_id_` | 标的标识，也是 MatchingEngine 订单簿数组下标 |
| `order_id_` | 客户端自己分配的 order id；撤单时用它定位订单 |
| `side_` | BUY/SELL |
| `price_` | 限价，`int64_t` |
| `qty_` | 委托数量，`uint32_t` |

`OMClientRequest` 是 TCP wire wrapper：

| 变量 | 作用 |
|---|---|
| `seq_num_` | 每个 client 独立递增的应用层请求序号，从 1 开始 |
| `me_client_request_` | 内嵌的撮合请求 |

### 2.2 响应消息

文件：`exchange/order_server/client_response.h`

`ClientResponseType` 包括 `ACCEPTED`、`CANCELED`、`FILLED`、`CANCEL_REJECTED`。

`MEClientResponse` 字段：

| 变量 | 作用 |
|---|---|
| `type_` | 响应类型 |
| `client_id_` | 响应属于哪个 client |
| `ticker_id_` | 标的 |
| `client_order_id_` | client 原始 order id，用于 client 侧关联 |
| `market_order_id_` | Exchange 为 NEW 分配的市场内部 order id |
| `side_`、`price_` | 订单方向和本次响应价格 |
| `exec_qty_` | 本次成交数量；ACCEPTED 时为 0 |
| `leaves_qty_` | 该订单剩余未成交数量 |

`OMClientResponse` 再增加 `seq_num_`。OrderServer 为每个 client 分别维护出站响应序号，然后将
`seq_num_ + MEClientResponse` 作为连续字节写入 TCP buffer。

### 2.3 行情消息

文件：`exchange/market_data/market_update.h`

`MarketUpdateType` 的实际语义：

| 类型 | 语义 |
|---|---|
| `ADD` | 一个新 market order 挂入公开订单簿 |
| `MODIFY` | 已有被动订单部分成交，更新剩余量 |
| `CANCEL` | 订单被撤销或被完全成交，从公开簿删除 |
| `TRADE` | 成交事件；当前 `order_id_` 为 INVALID |
| `CLEAR` | 快照中要求下游先清空某 ticker 的本地簿 |
| `SNAPSHOT_START/END` | 一轮快照的边界 |

`MEMarketUpdate` 字段：

| 变量 | 作用 |
|---|---|
| `type_` | 行情动作类型 |
| `order_id_` | market order id；快照 START/END 时复用来携带最后增量序号 |
| `ticker_id_` | 标的 |
| `side_`、`price_`、`qty_` | 订单/成交属性 |
| `priority_` | 同价位 FIFO priority；TRADE 等不需要时为 INVALID |

`MDPMarketUpdate` 是组播 wrapper：`seq_num_` 加一个 `MEMarketUpdate`。增量流使用全局递增序号；
每轮快照自己的包序号从 0 开始，而 START/END 内层的 `order_id_` 标记该快照覆盖到的增量序号。

### 2.4 packed wire format 的收益与限制

三个 header 都用 `#pragma pack(push, 1)`。在 x86-64、`sizeof(size_t)=8` 的当前 ABI 下，大小约为：

| 类型 | 大小 |
|---|---:|
| `MEClientRequest` | 30 B |
| `OMClientRequest` | 38 B |
| `MEClientResponse` | 42 B |
| `OMClientResponse` | 50 B |
| `MEMarketUpdate` | 34 B |
| `MDPMarketUpdate` | 42 B |

好处是消息固定长度、没有 padding，可以直接从 buffer 解析，避免逐字段序列化。限制是源码直接把
C++ 对象布局发到网络上，使用了 `size_t` 和本机字节序，没有 endian 转换、schema version、长度
字段和兼容性协议。因此它适合同构教学环境，不是跨架构、可演进的生产 wire protocol。

## 3. Exchange 各类职责和成员变量

### 3.1 `OrderServer`

文件：`exchange/order_server/order_server.h/.cpp`

职责：管理所有交易客户端 TCP 连接，把 wire request 转为内部 request；进行 client/socket 与序号
校验；将同一轮读到的请求按内核软件接收时间排序；把撮合响应封装序号后发回对应连接。

| 成员变量 | 作用 |
|---|---|
| `iface_`、`port_` | TCP listen 网卡和端口，主程序使用 `lo:12345` |
| `core_id_` | OrderServer thread 绑核目标；`-1` 表示不绑 |
| `outgoing_responses_` | MatchingEngine→OrderServer 的 response LFQueue |
| `run_` | 原子运行开关 |
| `thread_` | 保存业务线程并在 `stop()` 中 join |
| `time_str_` | 日志时间字符串复用 buffer |
| `logger_` | 写 `exchange_order_server.log` |
| `cid_next_outgoing_seq_num_` | `[client_id] → 下一个 response seq`，初值 1 |
| `cid_next_exp_seq_num_` | `[client_id] → 下一个期望 request seq`，初值 1 |
| `cid_tcp_socket_` | `[client_id] → 已绑定的 TCPSocket*`，首次请求时建立关系 |
| `tcp_server_` | listen、epoll、accept 和各连接的非阻塞收发 |
| `fifo_sequencer_` | 跨连接请求暂存、按接收时间排序、写 request LFQueue |

`recvCallback()` 会拒绝同一 client id 从另一条 socket 发送的消息，也会丢弃序号不等于期望值的
请求；源码中的 TODO 是以后发送正式 reject，目前只是写日志并 `continue`。

### 3.2 `FIFOSequencer`

文件：`exchange/order_server/fifo_sequencer.h`

职责：使不同 TCP 连接在一轮接收批次内按 `rx_time` 排序，再由单一 producer 顺序送给
MatchingEngine。

| 成员变量 | 作用 |
|---|---|
| `incoming_requests_` | 要写入的 ClientRequestLFQueue |
| `time_str_`、`logger_` | 日志状态；Logger 由 OrderServer 所有 |
| `RecvTimeClientRequest::recv_time_` | `recvmsg()` 读取到的 `SO_TIMESTAMP` 软件时间戳，单位 ns |
| `RecvTimeClientRequest::request_` | 对应的内部订单请求副本 |
| `pending_client_requests_` | 固定 1024 槽的待排序数组 |
| `pending_size_` | 当前有效槽数 |

`addClientRequest()` 只追加，不立刻发布；`sequenceAndPublish()` 使用 `std::sort` 升序排列有效区间，
然后依次写 LFQueue，最后把 `pending_size_` 清零。固定 array 避免排序前动态扩容，但每批上限 1024，
超过就 `FATAL`。

这里的 FIFO 是“软件 receive timestamp 排序”，不是交易所硬件 ingress timestamp。一个
`recvmsg()` buffer 内可能包含多条 TCP 请求，它们共享同一个 `rx_time`；`std::sort` 也不是 stable
sort，所以相同时间戳之间没有源码保证的稳定次序。这是教学式公平排序，不能声称严格的物理到达顺序。

### 3.3 `MatchingEngine`

文件：`exchange/matcher/matching_engine.h/.cpp`

职责：作为唯一订单状态写线程，消费排好序的请求，按 ticker 分派给 OrderBook，并把响应/行情写入
两条独立队列。

| 成员变量 | 作用 |
|---|---|
| `ticker_order_book_` | `[ticker_id] → MEOrderBook*`；构造时为每个 ticker 创建一个订单簿 |
| `incoming_requests_` | OrderServer→MatchingEngine 请求队列 |
| `outgoing_ogw_responses_` | MatchingEngine→OrderServer 响应队列 |
| `outgoing_md_updates_` | MatchingEngine→MarketDataPublisher 行情队列 |
| `run_` | 原子运行开关 |
| `core_id_` | MatchingEngine 绑核目标，正常构建中存在 |
| `thread_` | 线程对象，`stop()` 中 join |
| `time_str_`、`logger_` | 日志状态，输出 `exchange_matching_engine.log` |

`processClientRequest()` 只做两级分派：先用 `ticker_id_` 取得 book，再按 NEW/CANCEL 调用
`MEOrderBook::add()` 或 `cancel()`。所有 ticker 共用这一条 MatchingEngine thread，好处是状态更新
顺序清楚且无锁；代价是所有标的共享单核吞吐上限。

`sendClientResponse()` 和 `sendMarketUpdate()` 都把栈上/复用对象复制到对应 LFQueue 的当前写槽，
然后 commit 写索引。它们不会直接执行 TCP 或 UDP 系统调用。

### 3.4 `MEOrder`

文件：`exchange/matcher/me_order.h`

它既表示一张活跃订单，也充当“同价位 FIFO 双向循环链表”的节点：

| 成员变量 | 作用 |
|---|---|
| `ticker_id_` | 所属 ticker |
| `client_id_` | 订单所有者 |
| `client_order_id_` | client 分配的 id |
| `market_order_id_` | Exchange/OrderBook 分配的 id |
| `side_`、`price_`、`qty_` | 订单方向、限价和当前剩余量 |
| `priority_` | 同价位进入顺序，从 1 递增 |
| `prev_order_`、`next_order_` | 同价位环形双向链表的前后节点；head 的 prev 是 tail |

使用侵入式链表指针意味着不需要额外 `std::list` node allocation；拿到 `MEOrder*` 后，可以 O(1)
从价位队列中删除，并能 O(1) 追加到同价位尾部。

### 3.5 `MEOrdersAtPrice`

它表示一个价格档，同时充当“按价格排序的价格档双向循环链表”节点：

| 成员变量 | 作用 |
|---|---|
| `side_`、`price_` | 该档方向和价格 |
| `first_me_order_` | 同价位最早进入、应最先成交的订单 |
| `prev_entry_`、`next_entry_` | 同 side 相邻价格档；整条 price list 是环 |

`bids_by_price_` 永远指向最高买价，后继逐档变差；`asks_by_price_` 永远指向最低卖价，后继逐档
变差。因此判断能否成交只需看 head，不需要每次搜索最优价。

### 3.6 `MEOrderBook`

文件：`exchange/matcher/me_order_book.h/.cpp`

职责：维护一个 ticker 的 price-time-priority 限价簿；处理 NEW、CANCEL 和连续撮合；生成交易响应
与公开行情事件。

| 成员变量 | 作用 |
|---|---|
| `ticker_id_` | 本订单簿对应的 ticker |
| `matching_engine_` | 回调父 MatchingEngine，用于发布 response/update |
| `cid_oid_to_order_` | `[client_id][client_order_id] → MEOrder*` 直接寻址表，用于撤单定位 |
| `orders_at_price_pool_` | 预分配 `MEOrdersAtPrice`，容量为最大价位数 |
| `bids_by_price_`、`asks_by_price_` | 最优买/卖档指针 |
| `price_orders_at_price_` | `[price % ME_MAX_PRICE_LEVELS] → MEOrdersAtPrice*` |
| `order_pool_` | 预分配 `MEOrder`，每个 book 容量为 `ME_MAX_ORDER_IDS` |
| `client_response_` | 复用的响应 scratch object；发送时复制进 queue |
| `market_update_` | 复用的行情 scratch object；发送时复制进 queue |
| `next_market_order_id_` | 该 ticker 内部下一个 market order id，从 1 开始 |
| `time_str_`、`logger_` | 日志状态；复用 MatchingEngine 的 Logger 指针 |

关键私有方法：

| 方法 | 作用和复杂度 |
|---|---|
| `generateNewMarketOrderId()` | 每个 book 单调递增分配 market id，O(1) |
| `priceToIndex()`/`getOrdersAtPrice()` | price 取模后数组查找，O(1) |
| `getNextPriority()` | 读取同价位 tail priority + 1，O(1) |
| `addOrdersAtPrice()` | 写 price index，并在线性扫描后插入有序 price ring，最坏 O(价位数) |
| `removeOrdersAtPrice()` | 从 price ring 与 price index 删除空档，O(1) |
| `addOrder()` | 新档则创建 price node；已有档则 append 到 order ring tail，通常 O(1) |
| `removeOrder()` | 从 order ring、client/order index 和 pool 删除，O(1) |
| `checkForMatch()` | 从对手最优档连续撮合，O(实际命中的订单/价位数) |
| `match()` | 完成一对主动/被动订单的一次 fill，并生成消息 |

### 3.7 `MarketDataPublisher`

文件：`exchange/market_data/market_data_publisher.h/.cpp`

职责：消费 MatchingEngine 生成的无序号内部行情，为增量流增加统一 sequence，发 UDP multicast，
同时把相同更新送给 SnapshotSynthesizer。

| 成员变量 | 作用 |
|---|---|
| `next_inc_seq_num_` | 下一条增量行情序号，从 1 开始 |
| `outgoing_md_updates_` | MatchingEngine→Publisher 内部队列 |
| `snapshot_md_updates_` | Publisher→SnapshotSynthesizer 队列，元素已带增量序号 |
| `run_` | Publisher loop 开关，当前是 `volatile bool` |
| `time_str_`、`logger_` | 输出 `exchange_market_data_publisher.log` |
| `incremental_socket_` | 发布 `233.252.14.3:20001` 的 McastSocket |
| `snapshot_synthesizer_` | 自己创建和管理的 SnapshotSynthesizer 指针 |

对于每条内部 update，`run()` 先把 `next_inc_seq_num_` 和 `MEMarketUpdate` 依次复制到 multicast
outbound buffer，再将同一内容复制到 snapshot queue，最后递增 sequence。循环排空本批 queue 后
才调用 `incremental_socket_.sendAndRecv()` 执行 UDP send。

### 3.8 `SnapshotSynthesizer`

文件：`exchange/market_data/snapshot_synthesizer.h/.cpp`

职责：根据增量流维护一份“每个 ticker 的所有活跃 market order”镜像，每 60 秒发布一次完整快照，
供 MarketDataConsumer 启动或发现序号 gap 时恢复。

| 成员变量 | 作用 |
|---|---|
| `snapshot_md_updates_` | Publisher→Snapshot 的 MDP update queue |
| `logger_` | 输出 `exchange_snapshot_synthesizer.log` |
| `run_` | Snapshot loop 开关，当前是 `volatile bool` |
| `time_str_` | 日志时间 buffer |
| `snapshot_socket_` | 发布 `233.252.14.1:20000` 的 McastSocket |
| `ticker_orders_` | `[ticker_id][market_order_id] → MEMarketUpdate*` 的全量快照索引 |
| `last_inc_seq_num_` | 已应用到镜像的最后一条增量序号 |
| `last_snapshot_time_` | 上次快照发布时间，单位 ns |
| `order_pool_` | 快照镜像中活跃订单的 MEMarketUpdate 对象池，容量默认 1,048,576 |

`addToSnapshot()` 的状态转换：

- ADD：从 pool 分配一条消息并放到 `[ticker][market_order_id]`；
- MODIFY：原地修改 qty/price；
- CANCEL：归还 pool 并清空 index；
- TRADE：不直接修改镜像，因为后续 MODIFY/CANCEL 才表达可见订单簿变化；
- 每条消息最后校验增量 sequence 必须恰好 `last + 1`。

`publishSnapshot()` 发送：

```text
SNAPSHOT_START(last_inc_seq_num)
  CLEAR(ticker 0)
  该 ticker 的所有活跃 ADD
  CLEAR(ticker 1)
  该 ticker 的所有活跃 ADD
  ...
SNAPSHOT_END(last_inc_seq_num)
```

### 3.9 `UnorderedMapMEOrderBook` 的实际地位

`exchange/matcher/unordered_map_me_order_book.*` 是与 array 版本做比较的替代实现，成员含义基本相同，
主要把两个 index 改成：

```cpp
std::unordered_map<ClientId, std::unordered_map<OrderId, MEOrder *> >
std::unordered_map<Price, MEOrdersAtPrice *>
```

但是生产 `MatchingEngine` 构造函数始终 `new MEOrderBook(...)`，并没有配置项切换到 unordered 版本；
后者目前只由 `benchmarks/hash_benchmark.cpp` 直接构造。因此不能说当前 exchange 运行时使用了
`std::unordered_map`。其具体缺陷和选型对比见性能总报告第 19 节。

## 4. 基础设施类和数据结构为什么这样选

### 4.1 `LFQueue<T>`：预分配环形队列

文件：`common/lf_queue.h`

成员变量：

| 变量 | 作用 |
|---|---|
| `store_` | 构造时一次性 `vector<T>(capacity)`，保存所有槽位 |
| `next_write_index_` | producer 下一写槽，环形取模 |
| `next_read_index_` | consumer 下一读槽 |
| `num_elements_` | 当前元素数，决定是否可读 |

producer 使用 `getNextToWriteTo()` 取得槽指针，直接赋值后 `updateWriteIndex()` commit；consumer
取得只读指针，处理后 `updateReadIndex()` 释放。选择它是为了避免互斥锁、每消息 malloc 和节点指针
追逐。

源码上的限制是：没有检查 queue full，也没有 backpressure/drop policy；如果 producer 追上
consumer，会覆盖尚未处理的槽位，而且 `num_elements_` 仍可能继续增长。它也不是通用 MPMC queue，
当前正确性依赖每条队列只有一个 producer 和一个 consumer。

### 4.2 `MemPool<T>`：预分配对象池

文件：`common/mem_pool.h`

| 变量 | 作用 |
|---|---|
| `ObjectBlock::object_` | 原地存放 T，使返回地址也能反推 block index |
| `ObjectBlock::is_free_` | 当前槽是否空闲 |
| `store_` | 一次性创建全部 ObjectBlock 的 vector |
| `next_free_index_` | 下一次 allocation 开始搜索的位置 |

`allocate()` 使用 placement new，不进入通用 allocator；`deallocate()` 只把 `is_free_` 设回 true，
不调用 T 析构。好处是 allocation 地址稳定、无碎片、延迟相对可控。限制是容量固定、池满即
FATAL；`updateNextFreeIndex()` 在碎片较多时可能线性扫描很多已占用槽，因此并非严格 O(1)。

### 4.3 两层侵入式循环双向链表

OrderBook 同时维护：

```text
price ring：best price → 次优 price → ... → tail → best price
order ring：first FIFO order → 第二张 → ... → tail → first order
```

选择循环双链表的原因：

- `best` 指针直接给出最优价和应先成交的订单；
- 同价位 append tail 为 O(1)；
- client/order array 已经找到订单指针后，中间撤单为 O(1)；
- 删除最后一个订单时可以同时 O(1) 拆掉 price node。

代价是 price level 第一次插入仍需在有序 ring 上线性寻找位置，而且大量 pointer chasing 的 cache
locality 不如连续数组。

### 4.4 TCP、epoll 与应用序号

`TCPServer` 保存：listener TCPSocket、`epoll_fd_`、固定 1024 个 event 槽、receive/send socket
vector，以及两个 `std::function` callback。新连接会设置 non-blocking 和 `TCP_NODELAY`。

`TCPSocket` 每条连接默认预分配：

```text
outbound_data_ = 64 MiB
inbound_data_  = 64 MiB
```

`next_send_index_`/`next_send_valid_index_` 支持 partial write 和 EAGAIN 后续传；
`next_rcv_valid_index_` 保存尚未组成完整固定消息的尾部。OrderServer socket 还打开 `SO_TIMESTAMP`，
通过 `recvmsg()` ancillary data 取得软件接收时间，供 FIFOSequencer 排序。

TCP 已经保证字节可靠有序，但应用序号还能检测程序自身 framing/路由 bug。代价是每连接 128 MiB
buffer 过大，且 `TCPServer::sendAndRecv()` 每轮线性遍历 receive socket vector；客户端数量增加时会
出现 O(client count) 的扫描成本，因此当前实现并不适合作为多 client 扩展性结论。

### 4.5 UDP multicast 与双流恢复

增量组播允许一份行情被多个 client 接收，Exchange 不需要为每个订阅者维护连接和逐个发送；代价
是 UDP 会丢包。项目通过两条流处理：

- incremental：低延迟实时 ADD/MODIFY/CANCEL/TRADE，全局 sequence 检测 gap；
- snapshot：周期性完整订单镜像，下游发现 gap 后把完整 snapshot 与其后的增量拼接恢复。

`McastSocket` 同样预分配 64 MiB inbound + 64 MiB outbound buffer。它会把多次 `send()` 先拼入
buffer，`sendAndRecv()` 时调用一次 UDP `send()`。这减少系统调用，但当前实现没有按 UDP 最大报文
长度主动分包，也没有严谨处理 send failure/partial result；大 burst 时是需要修正的工程边界。

## 5. NEW 订单的完整函数调用与数据传递链

下面从 Trading 侧已经产生 `MEClientRequest` 开始，便于把 Exchange 边界讲完整。

### 5.1 网络接入和排序

```text
TradeEngine / OrderManager
  │ 写 Trading 进程的 ClientRequestLFQueue
  ▼
Trading::OrderGateway::run()
  │ tcp_socket_.send(seq_num)
  │ tcp_socket_.send(MEClientRequest)
  ▼
Common::TCPSocket::sendAndRecv()
  │ 非阻塞 ::send()，TCP_NODELAY
  ▼ TCP：OMClientRequest
Common::TCPServer::poll()
  ▼
Common::TCPServer::sendAndRecv()
  ▼
Common::TCPSocket::sendAndRecv()
  │ recvmsg() + SO_TIMESTAMP
  ▼
Exchange::OrderServer::recvCallback(socket, rx_time)
  │ 固定长度解析 OMClientRequest
  │ 校验 client→socket、request seq
  ▼
FIFOSequencer::addClientRequest(rx_time, MEClientRequest)
  ▼ 本轮所有 receive socket 处理结束
OrderServer::recvFinishedCallback()
  ▼
FIFOSequencer::sequenceAndPublish()
  │ std::sort(rx_time)
  ▼
exchange_main 的 ClientRequestLFQueue
```

需要注意，`TCPSocket::send()` 只是 memcpy 到用户态 outbound buffer。真正的 `::send()` 在下一次
`sendAndRecv()` 执行；Order RTT benchmark 的起点正是放在这次成功 kernel send 之前，而不是放在
buffer copy 之前。

### 5.2 MatchingEngine 分派

```text
MatchingEngine::run()
  │ incoming_requests_->getNextToRead()
  ▼
MatchingEngine::processClientRequest()
  │ ticker_order_book_[request.ticker_id_]
  ├─ NEW    → MEOrderBook::add(...)
  └─ CANCEL → MEOrderBook::cancel(...)
```

### 5.3 NEW 没有成交、直接挂簿

```text
MEOrderBook::add()
  │
  ├─ generateNewMarketOrderId()
  │
  ├─ MatchingEngine::sendClientResponse(ACCEPTED)
  │      └─ commit ClientResponseLFQueue
  │
  ├─ checkForMatch()
  │      └─ 对手盘为空或价格不交叉 → 原 qty 返回
  │
  ├─ getNextPriority(price)
  │
  ├─ order_pool_.allocate(...)
  │
  ├─ addOrder(order)
  │      ├─ 已有价位：append 到 order FIFO tail
  │      └─ 新价位：orders_at_price_pool_.allocate()
  │                    → addOrdersAtPrice() 插入有序 price ring
  │      └─ cid_oid_to_order_[client][client_order_id] = order
  │
  └─ MatchingEngine::sendMarketUpdate(ADD)
         └─ commit MEMarketUpdateLFQueue
```

最容易说错的一点：`ACCEPTED` 在 `checkForMatch()`、`order_pool_.allocate()` 和 `addOrder()` **之前**
就写入 response queue。由于 OrderServer 与 MatchingEngine 并发，client 可能在订单完成撮合/入簿前
收到 ACCEPTED。因此 NEW Order RTT 测的是 acceptance response path，不是完整 add-to-book 延迟。

## 6. CANCEL 的函数调用链

网络、序号检查、FIFO 和 MatchingEngine 分派与 NEW 相同，进入 book 后：

```text
MEOrderBook::cancel(client_id, client_order_id, ticker_id)
  │
  ├─ cid_oid_to_order_[client_id][client_order_id]
  │
  ├─ 找不到
  │    └─ 构造 CANCEL_REJECTED
  │         └─ sendClientResponse()
  │
  └─ 找到 MEOrder*
       ├─ 构造 CANCELED response
       ├─ 构造 CANCEL market update
       ├─ removeOrder(order)
       │    ├─ 同价位只有一单 → removeOrdersAtPrice()
       │    │    ├─ 从 price ring 摘除
       │    │    ├─ price index 清空
       │    │    └─ price node 归还 pool
       │    ├─ 同价位多单 → 从 order ring 摘除
       │    ├─ client/order index 清空
       │    └─ MEOrder 归还 pool
       ├─ sendMarketUpdate(CANCEL)
       └─ sendClientResponse(CANCELED)
```

与 NEW 的响应时点不同：有效 CANCEL 是先真正删除订单并提交 market update，再提交 CANCELED
response。无效撤单只产生 `CANCEL_REJECTED`，没有行情更新。

## 7. 撮合成功时的函数调用链和消息顺序

### 7.1 价格和时间优先规则

`checkForMatch()` 的判断：

```text
incoming BUY：只看 asks_by_price_->first_me_order_
              BUY limit price >= best ask 才成交

incoming SELL：只看 bids_by_price_->first_me_order_
               SELL limit price <= best bid 才成交
```

best price 指针实现价格优先；每个价位的 `first_me_order_` 实现时间优先。一次主动订单可在 while
循环中依次吃掉同价多张订单和多个价格档。

### 7.2 每一次 passive fill 的真实顺序

```text
MEOrderBook::add(aggressive order)
  ▼
MEOrderBook::checkForMatch()
  ▼
MEOrderBook::match(..., passive_order, &aggressive_leaves)
  │
  ├─ fill_qty = min(aggressive_leaves, passive_order.qty)
  ├─ aggressive_leaves -= fill_qty
  ├─ passive_order.qty -= fill_qty
  │
  ├─ sendClientResponse(FILLED for aggressive client)
  ├─ sendClientResponse(FILLED for passive client)
  ├─ sendMarketUpdate(TRADE)
  │
  ├─ passive qty == 0
  │    ├─ sendMarketUpdate(CANCEL passive market order)
  │    └─ removeOrder(passive_order)
  │
  └─ passive qty > 0
       └─ sendMarketUpdate(MODIFY passive remaining qty)
```

当 `checkForMatch()` 循环结束：

- 主动单 `leaves_qty == 0`：完全成交，不会进入 order pool，也不会发布主动单 ADD；
- 主动单 `leaves_qty > 0`：为剩余量创建 MEOrder，挂到自己的 limit price，发布 ADD；
- 无论是否成交，NEW 一进入 `add()` 就已经先发布一条 ACCEPTED。

### 7.3 一张主动单扫四档时的事件形态

假设 BUY 40@103 扫 SELL 100/101/102/103，每档 10：

```text
Response queue:
ACCEPTED(aggressive)
FILLED(aggressive, 10, leaves 30)
FILLED(passive@100, 10, leaves 0)
FILLED(aggressive, 10, leaves 20)
FILLED(passive@101, 10, leaves 0)
FILLED(aggressive, 10, leaves 10)
FILLED(passive@102, 10, leaves 0)
FILLED(aggressive, 10, leaves 0)
FILLED(passive@103, 10, leaves 0)

Market-update queue:
TRADE@100, CANCEL(passive@100)
TRADE@101, CANCEL(passive@101)
TRADE@102, CANCEL(passive@102)
TRADE@103, CANCEL(passive@103)
```

因为主动单完全成交，所以没有它自己的 ADD。响应队列和行情队列内部各自保持 producer 的写入
顺序，但它们由不同 consumer/thread/network channel 发送，不能假设 client response 与 multicast
update 之间存在全局到达顺序。

### 7.4 `MODIFY` 在这里指什么

网络 order request 没有 MODIFY 类型。行情里的 `MarketUpdateType::MODIFY` 仅表示一张被动订单被
部分成交后，公开簿中的剩余 `qty_` 改变；不是 client 主动修改订单价格/数量。

## 8. Response 和 Market Data 两条输出链

### 8.1 Client response 返回链

```text
MEOrderBook
  ▼ MatchingEngine::sendClientResponse()
ClientResponseLFQueue
  ▼ OrderServer::run()
cid_next_outgoing_seq_num_[client]++
  ▼ TCPSocket::send(seq) + send(MEClientResponse)
TCPSocket outbound buffer
  ▼ 下一轮 TCPServer::sendAndRecv() / ::send()
TCP
  ▼ Trading::OrderGateway::recvCallback()
校验 client_id + response sequence
  ▼ Trading 进程的 ClientResponseLFQueue
TradeEngine / OrderManager
```

TCP 提供可靠字节流；应用序号按 client 独立维护。OrderServer 根据响应内的 `client_id_` 查
`cid_tcp_socket_`，把响应路由回首次绑定该 client 的连接。

### 8.2 Incremental market-data 链

```text
MEOrderBook
  ▼ MatchingEngine::sendMarketUpdate()
MEMarketUpdateLFQueue（还没有网络 sequence）
  ▼ MarketDataPublisher::run()
赋 next_inc_seq_num_
  ├─ McastSocket outbound buffer
  │    ▼ UDP multicast 233.252.14.3:20001
  │  Trading::MarketDataConsumer::recvCallback()
  │    ├─ sequence 连续 → MEMarketUpdateLFQueue → TradeEngine
  │    └─ 发现 gap → 进入 snapshot recovery
  │
  └─ MDPMarketUpdateLFQueue
       ▼ SnapshotSynthesizer::addToSnapshot()
       维护 Exchange 侧全量镜像
```

### 8.3 Snapshot 恢复链

```text
SnapshotSynthesizer 每 60 秒 publishSnapshot()
  ▼ UDP multicast 233.252.14.1:20000
MarketDataConsumer 在启动/增量 gap 后订阅 snapshot stream
  │
  ├─ std::map 暂存并排序完整 snapshot 消息
  ├─ std::map 暂存 recovery 期间的 incremental 消息
  ├─ 校验 SNAPSHOT_START → 连续 local seq → SNAPSHOT_END
  ├─ 从 SNAPSHOT_END.order_id_ + 1 衔接增量 sequence
  └─ 将 CLEAR/ADD snapshot + 后续 incrementals 依次写给 TradeEngine
```

这里的两个 `std::map` 在 Trading::MarketDataConsumer 中，不在 Exchange hot matching path；它们
优先解决乱序拼接与 gap recovery，而不是追求单次查找的极低延迟。

## 9. 当前实现的重要假设与源码限制

这些限制不否定教学价值，但面试中应主动区分“实现思路”和“生产级保证”。

### 9.1 输入边界和直接寻址

- 多处在验证前就用 `client_id`、`ticker_id`、`order_id` 访问 `.at()` 或 `[]`；恶意/错误 wire
  message 可能导致越界、异常或 `noexcept` terminate；
- `priceToIndex(price) = price % 256` 假设价格非负且不会有两个活跃价格映射到同一槽；源码没有
  collision resolution；
- 相同 client/order id 的重复 NEW 没有显式拒绝，可能覆盖 direct index 而旧节点仍留在 book；
- `checkForMatch()` 的 `new_market_order_id` 参数类型写成 `Qty` 而不是 `OrderId`，当前小 ID 下不暴露，
  但 market id 超过 32 位会截断；
- 非 benchmark 构造中 `cid_oid_to_order_` 没有显式 `fill(nullptr)`，指针槽不能被视为已可靠初始化。

### 9.2 队列与过载

- LFQueue 没有 full check；系统缺少清晰的 backpressure、reject 或 drop policy；
- FIFO pending array 满 1024 会直接 `FATAL`；
- Snapshot pool 只有 `ME_MAX_ORDER_IDS` 个对象，是所有 ticker 共用，而索引空间允许每 ticker 都有
  这么多 order id；总活跃订单超过 pool 容量会退出；
- 所有 ticker 共用一条 MatchingEngine thread，高负载时最终会形成单核饱和与 queueing latency。

### 9.3 并发和生命周期

- MatchingEngine、OrderServer 使用 `std::atomic<bool>` 并 join thread；
- MarketDataPublisher、SnapshotSynthesizer 使用 `volatile bool`，启动后没有保存返回的 thread 指针，
  stop 时不能 join；在 C++ memory model 下 `volatile` 不是线程同步原语；
- Publisher 在 `outgoing_md_updates_->updateReadIndex()` 后仍通过原 slot pointer 复制消息到 snapshot
  queue；producer 理论上已可复用该槽，严格实现应先拷贝局部值再释放 read index；
- response queue 与 market-data queue 是独立并发通道，只能保证各自的内部顺序，不能提供跨通道
  atomic commit 或统一全序。

### 9.4 网络与扩展性

- wire struct 没有 byte-order/schema/version 处理；
- TCPServer 会线性遍历保存的 socket vector，每个连接又有 128 MiB 用户态 buffer；
- McastSocket 把批量消息拼成一个 UDP send，但没有显式 datagram 分片上限和完善错误处理；
- FIFO 使用内核软件时间戳，不是 NIC hardware timestamp；相同 timestamp 没有稳定 tie-breaker；
- busy-spin 有利于低 wake-up latency，但会长期消耗 CPU；正常 exchange_main 又没有默认绑核/隔离。

### 9.5 快照成本

默认 `ticker_orders_` 是 `8 × 1,048,576` 个指针，约 64 MiB。每 60 秒发布快照时即使活跃订单很
少，也会扫描全部约 838 万个槽，再集中发送所有活跃订单。这会产生周期性 CPU/cache 与网络 burst，
正是性能总报告第 18 节“按 ticker 错峰快照”优化建议的源码依据。

## 10. Exchange 架构的面试总结模板

> Exchange 采用 OrderServer、MatchingEngine、MarketDataPublisher 和 SnapshotSynthesizer 四条
> 业务线程。订单经非阻塞 TCP 进入 OrderServer，使用每 client 应用序号校验，并由 FIFOSequencer
> 按 SO_TIMESTAMP 软件接收时间排序后写入 SPSC 环形队列；单线程 MatchingEngine 按 ticker 分派到
> OrderBook，因此簿内无需加锁。OrderBook 用 client/order 直接寻址表完成 O(1) 撤单定位，用两层
> 侵入式循环双链表实现 price-time priority，并用预分配 MemPool 避免撮合 hot path 动态分配。
> 撮合结果分别写 response 和 market-update 队列：response 经 TCP 回到指定 client，增量行情加
> 全局序号后通过 UDP multicast 广播，同时由 SnapshotSynthesizer 维护镜像并周期发布恢复快照。
> 我也识别了它的教学项目边界，包括巨型 array 内存、price modulo collision、LFQueue 无 full
> backpressure、软件时间戳公平性、快照全表扫描和部分线程生命周期问题，因此不会把它描述成
> 已具备生产交易所的容错与扩展能力。

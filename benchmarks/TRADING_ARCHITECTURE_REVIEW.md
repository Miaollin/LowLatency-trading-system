# LowLatency Trading System：Trading 源码架构复习文档

> 用途：源码复习、面试准备、Trading 数据结构与调用链梳理  
> 分析范围：当前仓库 `trading/`，以及它直接依赖的 Exchange 消息、LFQueue 和网络封装  
> 原则：只描述真实源码；设计意图、当前行为和生产级限制分别说明  
> 配套文档：`benchmarks/EXCHANGE_ARCHITECTURE_REVIEW.md`、
> `benchmarks/PERFORMANCE_BENCHMARK_REPORT.md`

## 阅读建议

第一次阅读先看第 1 节整体架构，以及第 6～10 节调用链；第二次阅读第 3～5 节的类、成员变量和
数据结构；面试前重点复习第 11 节源码限制和第 12 节口述模板。

## 1. Trading 整体实现思路

核心源码：

```text
trading/trading_main.cpp
trading/market_data/market_data_consumer.*
trading/order_gw/order_gateway.*
trading/strategy/trade_engine.*
trading/strategy/market_order_book.*
trading/strategy/feature_engine.h
trading/strategy/market_maker.*
trading/strategy/liquidity_taker.*
trading/strategy/order_manager.*
trading/strategy/risk_manager.*
trading/strategy/position_keeper.h
```

### 1.1 一句话架构

每个 `trading_main` 进程代表一个独立 client。它同时接收 Exchange 的公开行情和自己的私有订单
响应，在 TradeEngine 单线程中维护本地订单簿、特征、持仓、风险和订单状态，再把策略生成的订单
交给 OrderGateway 通过 TCP 发送。

```text
Exchange incremental/snapshot UDP multicast
                  │
                  ▼
       MarketDataConsumer thread
       │ sequence/gap recovery/解码
       ▼ MEMarketUpdateLFQueue
           TradeEngine thread
           │
           ├─ MarketOrderBook：本地公开订单簿镜像、BBO
           ├─ PositionKeeper：position/PnL/volume
           ├─ FeatureEngine：fair price、aggressive trade ratio
           ├─ MarketMaker 或 LiquidityTaker：策略决策
           ├─ RiskManager：新单前风险检查
           └─ OrderManager：每 ticker/side 的私有订单状态
                         │
                         ▼ ClientRequestLFQueue
                 OrderGateway thread
                         │ sequence + TCP
                         ▼
                 Exchange OrderServer
                         │
                         ▼ TCP response
                 OrderGateway thread
                         │ ClientResponseLFQueue
                         └──────────────► TradeEngine thread
```

这套分层把职责拆成：

- MarketDataConsumer 只负责 market-data transport、序号和恢复；
- OrderGateway 只负责 order transport、framing 和序号；
- TradeEngine 是策略状态的唯一串行执行者；
- MarketOrderBook、FeatureEngine、PositionKeeper、RiskManager、OrderManager 都在 TradeEngine
  线程中同步调用，不各自启动线程；
- 策略类通过三个 callback 接入 TradeEngine，不直接操作 socket。

### 1.2 队列所有权

`trading_main.cpp::main()` 在栈上创建三条队列：

| 队列 | 元素 | producer | consumer | 方向 |
|---|---|---|---|---|
| `client_requests` | `MEClientRequest` | TradeEngine | OrderGateway | 策略订单出站 |
| `client_responses` | `MEClientResponse` | OrderGateway | TradeEngine | 私有订单响应入站 |
| `market_updates` | `MEMarketUpdate` | MarketDataConsumer | TradeEngine | 已解码公开行情入站 |

它们使用同一个 `Common::LFQueue<T>` 模板，但和 exchange_main 中的三条 queue 是不同进程、不同
对象；中间通过 TCP/UDP 连接，不是共享内存。

### 1.3 真实线程模型

| 线程 | 入口 | 作用 |
|---|---|---|
| main | `trading_main.cpp::main()` | 解析配置、创建组件；RANDOM 模式还直接产生随机订单 |
| TradeEngine | `TradeEngine::run()` | 串行处理 response 和 market update，并运行策略 |
| OrderGateway | `OrderGateway::run()` | 非阻塞 TCP 发送订单、接收响应 |
| MarketDataConsumer | `MarketDataConsumer::run()` | 轮询增量/快照 multicast socket |
| 4 个 Logger thread | `Logger::flushQueue()` | 分别写 main、TradeEngine、OrderGateway、MDC 日志 |

一个普通 trading 进程约有 8 条线程：main + 3 条业务线程 + 4 条 Logger 线程。策略、OrderManager、
RiskManager、PositionKeeper、FeatureEngine 和各 MarketOrderBook 都没有单独线程。

正常 `trading_main` 创建组件时没有传 CPU id，TradeEngine 和 MarketDataConsumer 也硬编码
`createAndStartThread(-1, ...)`，因此默认不绑核。benchmark 中的绑核不能自动代表正常主程序。

## 2. 启动、配置和三种 AlgoType

### 2.1 启动顺序

`trading/trading_main.cpp::main()` 的真实顺序：

1. 从 `argv[1]` 读取 `client_id`，并用它 `srand(client_id)`；
2. 从 `argv[2]` 解析 `RANDOM`、`MAKER` 或 `TAKER`；
3. 创建 main Logger 和三条 LFQueue；
4. 每 5 个参数解析一个 ticker 的 `TradeEngineCfg`；
5. 创建并启动 TradeEngine；
6. 创建 OrderGateway，连接 `127.0.0.1:12345`，然后启动；
7. 创建 MarketDataConsumer，订阅增量 `233.252.14.3:20001`，然后启动；
8. 等待 10 秒后设置 `last_event_time_`；
9. RANDOM 在 main thread 生成订单；MAKER/TAKER 等待异步行情触发；
10. 连续 60 秒没有 response/update 后依次停止组件并退出。

`createAndStartThread()` 内部每次创建线程后还固定 sleep 1 秒，所以启动是刻意错开的，但这不是
严格的 ready barrier。

### 2.2 每个 ticker 的配置

文件：`common/types.h`

```cpp
struct RiskCfg {
  Qty max_order_size_;
  Qty max_position_;
  double max_loss_;
};

struct TradeEngineCfg {
  Qty clip_;
  double threshold_;
  RiskCfg risk_cfg_;
};
```

| 字段 | MAKER 中的意义 | TAKER 中的意义 |
|---|---|---|
| `clip_` | 每侧希望维持的被动订单数量 | 触发后发送的主动订单数量 |
| `threshold_` | fair price 与 BBO 的最小绝对价差阈值 | aggressive trade qty / 对手 BBO qty 阈值 |
| `max_order_size_` | 单笔订单数量上限 | 同左 |
| `max_position_` | 假设新单完全成交后的绝对持仓上限 | 同左 |
| `max_loss_` | 允许的 PnL 下限，脚本传负数，例如 -100 | 同左 |

配置保存在 `TradeEngineCfgHashMap = std::array<TradeEngineCfg, ME_MAX_TICKERS>`。命令行未提供的
ticker 使用默认 0 配置，因此策略新单通常会被 `max_order_size_=0` 拒绝。

### 2.3 三种模式

| AlgoType | 实现位置 | 行为 |
|---|---|---|
| `MAKER` | `MarketMaker` | 本地簿变化时计算 fair price，在 bid/ask 各管理一张被动单 |
| `TAKER` | `LiquidityTaker` | 收到 TRADE 时计算 aggressive trade ratio，超过阈值后追击主动方向 |
| `RANDOM` | `trading_main.cpp` | main thread 直接随机 NEW/CANCEL，不经过 OrderManager 和 RiskManager |

RANDOM 没有单独策略类。它每次 NEW 后把请求存入 `std::vector`，随机挑历史请求改成 CANCEL；可能
重复撤同一订单，也可能撤已经成交/撤销的订单。它适合制造教学流量，不是受风险管理的策略。

## 3. 核心类职责和成员变量

### 3.1 `TradeEngine`

文件：`trading/strategy/trade_engine.h/.cpp`

职责：Trading 侧的中央协调器和状态 owner。在一条线程中消费订单响应与行情，调用本地 OrderBook、
持仓、特征、策略、风险和订单管理，并把新请求写给 OrderGateway。

| 成员变量 | 作用 |
|---|---|
| `client_id_` | 当前 trading 进程对应的 client id |
| `ticker_order_book_` | `[ticker_id] → MarketOrderBook*`；构造时为全部 ticker 创建本地簿 |
| `outgoing_ogw_requests_` | TradeEngine→OrderGateway 的 request LFQueue |
| `incoming_ogw_responses_` | OrderGateway→TradeEngine 的 response LFQueue |
| `incoming_md_updates_` | MarketDataConsumer→TradeEngine 的 market-update LFQueue |
| `last_event_time_` | 最后处理 response/update 的纳秒时间，用于 60 秒 silent exit |
| `run_` | 主循环开关，当前类型为 `volatile bool` |
| `time_str_` | 日志时间字符串复用 buffer |
| `logger_` | 写 `trading_engine_<client>.log`，也被内部策略组件共享 |
| `feature_engine_` | 计算 fair market price 和 aggressive trade ratio |
| `position_keeper_` | 每 ticker 的 position/PnL/volume |
| `risk_manager_` | 使用 PositionKeeper 数据执行 pre-trade risk |
| `order_manager_` | 维护每 ticker、每 side 的一张策略订单及状态机 |
| `mm_algo_` | MAKER 时创建，否则为空 |
| `taker_algo_` | TAKER 时创建，否则为空 |
| `algoOnOrderBookUpdate_` | 本地簿变化 callback；构造策略时被覆盖 |
| `algoOnTradeUpdate_` | TRADE callback；构造策略时被覆盖 |
| `algoOnOrderUpdate_` | 私有订单响应 callback；构造策略时被覆盖 |

仅 benchmark 构建还有：

| 成员 | 作用 |
|---|---|
| `ttt_recorder_` | 当前 Tick-to-Trade recorder |
| `ttt_start_` | 调用 `processMarketUpdate()` 前的 TSC |
| `ttt_measurement_active_` | 是否等待第一笔 request commit |
| `ttt_endpoint_reached_` | 本次 update 是否产生订单 |

`run()` 每轮先把 `incoming_ogw_responses_` 全部排空，再排空 `incoming_md_updates_`。因此 response
处理优先；如果 response 持续不断，market data 理论上可能被延后。

### 3.2 `MarketDataConsumer`

文件：`trading/market_data/market_data_consumer.h/.cpp`

职责：订阅增量 multicast；校验 sequence；发现 gap 后订阅 snapshot，把完整快照和 gap 后的增量
拼接成连续事件；最后只把内部 `MEMarketUpdate` 写给 TradeEngine。

| 成员变量 | 作用 |
|---|---|
| `next_exp_inc_seq_num_` | 下一条期望的增量 sequence，初值 1 |
| `incoming_md_updates_` | MarketDataConsumer→TradeEngine 的输出 LFQueue |
| `run_` | busy-loop 开关，当前为 `volatile bool` |
| `time_str_`、`logger_` | 日志状态，输出 `trading_market_data_consumer_<client>.log` |
| `incremental_mcast_socket_` | 常驻订阅增量流 |
| `snapshot_mcast_socket_` | 只在 recovery 时初始化并订阅快照流 |
| `in_recovery_` | 是否正在使用 snapshot + incremental 恢复 |
| `iface_` | 组播网卡，主程序使用 `lo` |
| `snapshot_ip_`、`snapshot_port_` | 快照地址 `233.252.14.1:20000` |
| `snapshot_queued_msgs_` | `snapshot local seq → update` 的有序 `std::map` |
| `incremental_queued_msgs_` | recovery 期间 `incremental seq → update` 的有序 `std::map` |

`recvCallback()` 通过传入 socket 的 fd 判断消息来自增量还是快照。稳态增量 sequence 正确时直接
写 LFQueue；第一次发现 gap 时调用 `startSnapshotSync()`，之后两个 channel 都先进入 map，再由
`checkSnapshotSync()` 判断能否拼成连续历史。

### 3.3 `OrderGateway`

文件：`trading/order_gw/order_gateway.h/.cpp`

职责：消费 TradeEngine 的内部请求，添加应用层请求序号并写入 TCP；解析 Exchange 响应，校验
client id/response sequence 后写给 TradeEngine。

| 成员变量 | 作用 |
|---|---|
| `client_id_` | 此 Gateway 代表的 client |
| `ip_`、`iface_`、`port_` | Exchange OrderServer 地址和本地接口 |
| `core_id_` | Gateway thread 的可选绑核目标 |
| `outgoing_requests_` | TradeEngine→Gateway request LFQueue |
| `incoming_responses_` | Gateway→TradeEngine response LFQueue |
| `run_` | 原子运行开关 |
| `thread_` | 线程对象；`stop()` 中 join |
| `time_str_`、`logger_` | 日志状态，输出 `trading_order_gateway_<client>.log` |
| `next_outgoing_seq_num_` | 下一条 TCP request sequence，从 1 开始 |
| `next_exp_seq_num_` | 下一条期望的 response sequence，从 1 开始 |
| `tcp_socket_` | 与 Exchange OrderServer 的一条非阻塞 TCP 连接 |

Order RTT benchmark 条件编译字段 `rtt_order_id_`、`rtt_expected_type_`、`rtt_recorder_`、
`rtt_start_`、`rtt_armed_`、`rtt_started_`、`rtt_completed_`、`rtt_protocol_errors_` 只服务于单笔
请求/响应相关性和测量，不属于正常策略状态。

### 3.4 `MarketOrder`

文件：`trading/strategy/market_order.h`

它表示从公开 market data 重建的一张订单，同时是同价位侵入式 FIFO 链表节点：

| 成员变量 | 作用 |
|---|---|
| `order_id_` | Exchange 分配的 market order id，不是本 client 的 order id |
| `side_`、`price_`、`qty_` | 方向、价格、当前公开剩余数量 |
| `priority_` | Exchange 发布的同价位优先级 |
| `prev_order_`、`next_order_` | 同价位循环双向链表指针 |

### 3.5 `MarketOrdersAtPrice` 与 `BBO`

`MarketOrdersAtPrice` 表示一个价格档：

| 成员变量 | 作用 |
|---|---|
| `side_`、`price_` | 价格档方向与价格 |
| `first_mkt_order_` | 此价位 priority 最早的订单 |
| `prev_entry_`、`next_entry_` | 同一 side 上按价格排序的循环双向链表 |

`BBO` 是策略和持仓只需读取的 top-of-book 摘要：

| 成员变量 | 作用 |
|---|---|
| `bid_price_`、`ask_price_` | best bid/best ask |
| `bid_qty_`、`ask_qty_` | 最优价位上所有订单的总数量 |

### 3.6 `MarketOrderBook`

文件：`trading/strategy/market_order_book.h/.cpp`

职责：根据 Exchange 的 ADD/MODIFY/CANCEL/CLEAR 更新，为一个 ticker 维护本地 full-depth order-by-
order book 和 BBO；TRADE 不直接改簿，而是通知 TradeEngine。

| 成员变量 | 作用 |
|---|---|
| `ticker_id_` | 此本地簿对应的 ticker |
| `trade_engine_` | 回调父 TradeEngine 的指针 |
| `oid_to_order_` | `[market_order_id] → MarketOrder*` 直接寻址表 |
| `orders_at_price_pool_` | 预分配价格档节点，容量 `ME_MAX_PRICE_LEVELS` |
| `bids_by_price_`、`asks_by_price_` | 最优买/卖价位 head |
| `price_orders_at_price_` | `[price % ME_MAX_PRICE_LEVELS] → MarketOrdersAtPrice*` |
| `order_pool_` | 预分配 MarketOrder，容量 `ME_MAX_ORDER_IDS` |
| `bbo_` | 当前 top-of-book cache |
| `time_str_`、`logger_` | 日志状态，复用 TradeEngine Logger |

私有方法与复杂度：

| 方法 | 作用 |
|---|---|
| `priceToIndex()`/`getOrdersAtPrice()` | price 取模后的 O(1) 查找 |
| `addOrdersAtPrice()` | 将新档插入按优劣排序的 price ring，最坏 O(价位数) |
| `removeOrdersAtPrice()` | O(1) 删除空价格档 |
| `addOrder()` | 新价位建档；已有价位 append FIFO tail |
| `removeOrder()` | 从 order ring、order-id index 和 pool 删除，O(1) |
| `updateBBO()` | 获取 best price，并遍历最优档全部订单求总 qty |

它和 Exchange MEOrderBook 使用相同的两层链表思想，但它不撮合，只重放公开事件；索引 key 是
market order id，也不需要 client/order 二维映射。

### 3.7 `FeatureEngine`

文件：`trading/strategy/feature_engine.h`

职责：计算两个简单特征并立即供策略 callback 使用。

| 成员变量 | 作用 |
|---|---|
| `time_str_`、`logger_` | 日志状态，Logger 由 TradeEngine 所有 |
| `mkt_price_` | 基于 BBO 数量加权的 fair/micro price |
| `agg_trade_qty_ratio_` | 最近 TRADE 数量与被攻击一侧 BBO 数量之比 |

公式：

```text
mkt_price = (bid_price × ask_qty + ask_price × bid_qty)
            / (bid_qty + ask_qty)

aggressive BUY ratio  = trade_qty / ask_qty
aggressive SELL ratio = trade_qty / bid_qty
```

两个 feature 是单个成员，不是 `[ticker]` 数组。当前安全性依赖“更新 feature 后立刻在同一
TradeEngine 调用栈中执行该 ticker 的策略”，不适合异步保存后跨 ticker 使用。

### 3.8 `MarketMaker`

文件：`trading/strategy/market_maker.h/.cpp`

职责：响应本地 OrderBook 更新，根据 FeatureEngine fair price 选择 bid/ask quote，并委托
OrderManager 执行 cancel/new。

| 成员变量 | 作用 |
|---|---|
| `feature_engine_` | 只读 fair price 来源 |
| `order_manager_` | 管理每侧实际策略订单 |
| `time_str_`、`logger_` | 日志状态 |
| `ticker_cfg_` | 按 ticker 保存 clip、threshold 和风险配置的副本 |

目标价逻辑：

```text
fair - best_bid >= threshold ? quote best_bid : quote best_bid - 1
best_ask - fair >= threshold ? quote best_ask : quote best_ask + 1
```

阈值是绝对 price unit，不是百分比。MarketMaker 的 `onTradeUpdate()` 只记录日志，不下单；它会在
TRADE 后续的 MODIFY/CANCEL 改变本地簿时重新报价。

### 3.9 `LiquidityTaker`

文件：`trading/strategy/liquidity_taker.h/.cpp`

职责：只在 TRADE 事件上判断主动成交强度，超过 threshold 后沿原主动方向发送可穿过当前 BBO 的
订单。

| 成员变量 | 作用 |
|---|---|
| `feature_engine_` | 只读 aggressive trade ratio 来源 |
| `order_manager_` | 发送/撤销主动订单 |
| `time_str_`、`logger_` | 日志状态 |
| `ticker_cfg_` | 每 ticker clip、threshold、risk 配置副本 |

```text
收到 aggressive BUY TRADE  → BUY clip @ current best ask，SELL target=INVALID
收到 aggressive SELL TRADE → SELL clip @ current best bid，BUY target=INVALID
```

`onOrderBookUpdate()` 不做交易。由于 Exchange 先发布 TRADE，再发布被动单 MODIFY/CANCEL，Taker
计算 ratio 时使用的是成交发生前/尚未应用随后簿变化的 BBO。

### 3.10 `OMOrder` 与 `OrderManager`

文件：`trading/strategy/om_order.h`、`order_manager.h/.cpp`

`OMOrder` 表示策略自己的一张私有订单：

| 成员变量 | 作用 |
|---|---|
| `ticker_id_`、`order_id_` | ticker 与 client order id |
| `side_`、`price_`、`qty_` | 订单属性和当前 leaves qty |
| `order_state_` | INVALID/PENDING_NEW/LIVE/PENDING_CANCEL/DEAD |

`OrderManager` 成员：

| 成员变量 | 作用 |
|---|---|
| `trade_engine_` | 用来调用 `sendClientRequest()` |
| `risk_manager_` | 新单前风险检查的 const reference |
| `time_str_`、`logger_` | 日志状态 |
| `ticker_side_order_` | `[ticker_id][side] → OMOrder`，每 ticker 每 side 只管理一张 |
| `next_order_id_` | 本 client 下一 client order id，从 1 开始 |

`sideToIndex()` 的实际映射是 SELL→0、INVALID→1、BUY→2、MAX→3；array 长度为 4，真正策略使用
SELL 和 BUY 两个槽。

状态机：

```text
INVALID/DEAD
    │ risk ALLOWED + newOrder()
    ▼
PENDING_NEW
    │ ACCEPTED
    ▼
  LIVE
    │ 目标价格变化或目标变 INVALID → cancelOrder()
    ▼
PENDING_CANCEL
    │ CANCELED
    ▼
  DEAD

任意有效订单收到 FILLED：更新 qty；leaves_qty==0 → DEAD
```

`moveOrder()` 在 LIVE 时只比较 price，不比较 qty；需要移动时只发 CANCEL，不在同一次调用里立即发
replacement。等 CANCELED 后状态变 DEAD，还要下一次策略事件才会重新 NEW。

### 3.11 `RiskInfo` 与 `RiskManager`

文件：`trading/strategy/risk_manager.h/.cpp`

`RiskInfo`：

| 成员变量 | 作用 |
|---|---|
| `position_info_` | 指向 PositionKeeper 中稳定的该 ticker PositionInfo |
| `risk_cfg_` | 该 ticker 的风险阈值副本 |

检查顺序：

```text
qty > max_order_size                         → ORDER_TOO_LARGE
abs(position + signed qty) > max_position    → POSITION_TOO_LARGE
total_pnl < max_loss                         → LOSS_TOO_LARGE
否则                                         → ALLOWED
```

`RiskManager` 自身只有 `time_str_`、共享 `logger_` 和
`ticker_risk_ = std::array<RiskInfo, ME_MAX_TICKERS>`。构造时把每个 RiskInfo 的 position pointer
连到 PositionKeeper，并复制对应 ticker 的 risk config。

### 3.12 `PositionInfo` 与 `PositionKeeper`

文件：`trading/strategy/position_keeper.h`

`PositionInfo` 字段：

| 成员变量 | 作用 |
|---|---|
| `position_` | 净持仓；BUY fill 增加，SELL fill 减少 |
| `real_pnl_` | 已平仓部分实现盈亏 |
| `unreal_pnl_` | 当前未平仓盈亏 |
| `total_pnl_` | `real_pnl_ + unreal_pnl_` |
| `open_vwap_` | BUY/SELL 两侧未平仓成本累积值；使用时除以绝对持仓得到均价 |
| `volume_` | 累计成交量 |
| `bbo_` | 指向对应 MarketOrderBook 内部 BBO，用于 mark-to-market |

`addFill()` 处理加仓、减仓、平仓和反手；`updateBBO()` 使用 bid/ask 中间价更新未实现盈亏。

`PositionKeeper` 成员为 `time_str_`、共享 `logger_` 和
`ticker_position_ = std::array<PositionInfo, ME_MAX_TICKERS>`，提供按 ticker 的 `addFill()`、
`updateBBO()`、`getPositionInfo()` 与汇总输出。

## 4. 数据结构选型与原因

### 4.1 `std::array` 直接寻址

Trading hot path 大量使用编译期定长 array：

| 映射 | 类型 | 原因 |
|---|---|---|
| ticker→MarketOrderBook | `std::array<MarketOrderBook*>` | ticker 小而有界，O(1) 分派 |
| market order id→order | `std::array<MarketOrder*>` | O(1) MODIFY/CANCEL 定位，无 hash/node allocation |
| price slot→price level | `std::array<MarketOrdersAtPrice*>` | O(1) 价格档查找 |
| ticker→position/risk/config | 多个 `std::array` | 对象连续、地址稳定、无动态扩容 |
| ticker→side→OMOrder | 二维 `std::array` | 每 ticker/side 只有一个策略订单 |

收益是低固定开销、无 rehash、tail 更稳定。代价是按最大 ID 空间预分配，ID 稀疏时浪费内存；price
使用 modulo 还要求不会发生冲突。

默认每个 MarketOrderBook 的 `oid_to_order_` 有 1,048,576 个指针，约 8 MiB；8 个 ticker 约
64 MiB。它比 Exchange 的 client×order 二维表小很多，但仍是按上限而非活跃量付费。

### 4.2 两层侵入式循环双向链表

本地簿的数据关系：

```text
bids_by_price_ → 最优买价 → 次优买价 → ... → 回到最优买价
asks_by_price_ → 最优卖价 → 次优卖价 → ... → 回到最优卖价

每个 price node：first order → 第二张 → ... → tail → first order
```

`MarketOrder`/`MarketOrdersAtPrice` 自己携带 prev/next，不需要 `std::list` 额外分配 node。已通过
order-id array 找到订单后，撤销/完全成交更新可以 O(1) 摘链；同价新单 O(1) append tail。
新价格档仍需线性扫描 price ring 才能找到排序位置。

### 4.3 `MemPool`

每个 MarketOrderBook 在启动时预分配：

- `MemPool<MarketOrder>(ME_MAX_ORDER_IDS)`；
- `MemPool<MarketOrdersAtPrice>(ME_MAX_PRICE_LEVELS)`。

ADD 使用 placement new，CANCEL/CLEAR 只把槽标回 free，避免 steady-state `new/delete`。代价是启动
内存大、池满直接 FATAL，并且碎片化后 `updateNextFreeIndex()` 可能线性扫描。

在常见 x86-64 ABI 下，`MarketOrder` 对应 ObjectBlock 约 64 B；8 个 ticker、每个 1M 槽的订单池
粗略约 512 MiB。这个估算应在目标机用 `sizeof`/RSS 再确认。

### 4.4 LFQueue

三条 queue 都是构造时预分配的 vector ring，通过 atomic read/write index 和 element count 协调。
选择它是为了避免 mutex、条件变量和每消息 allocation。

当前实现没有 full check/backpressure，producer 追上 consumer 时可能覆盖未读数据；所以它适合当前
一个 producer、一个 consumer且负载可控的教学路径，不是通用 MPMC 队列。

### 4.5 Recovery 为什么使用 `std::map`

MarketDataConsumer 只有在 sequence gap/recovery 时才使用两个 `std::map<size_t, update>`。选择有序
map 是为了：消息可能来自两个 channel 且顺序不齐，按 sequence 遍历即可检测缺口并拼接。

其 node allocation、O(logN) 和较差 locality 不适合 steady-state tick hot path，但 recovery 本身是
异常/慢路径，优先保证顺序和实现清晰是合理取舍。恢复完成时的 `final_events` 使用 `std::vector`，
同样会动态分配。

### 4.6 `std::function` callback

TradeEngine 先把三个 callback 指向 default logging handler；构造 MarketMaker/LiquidityTaker 时，
策略构造函数再用捕获 `this` 的 lambda 覆盖：

```text
algoOnOrderBookUpdate_
algoOnTradeUpdate_
algoOnOrderUpdate_
```

优点是 TradeEngine 不需要在每次事件中 switch AlgoType，策略接口清楚。代价是间接调用、可能阻碍
内联；如果要进一步优化，应和 template/CRTP 版本做相同 workload P99/P99.9 A/B，而不是直接假设
替换一定更快。

### 4.7 每 ticker/side 只保存一张 OMOrder

二维 array 让 MarketMaker 的目标很明确：一个 bid quote、一个 ask quote；状态机也能阻止 pending
期间重复发单。它不支持多层报价、订单梯队、多张同向主动单或复杂 parent/child order，因此这是
简化策略管理的选型，不是通用 OMS。

### 4.8 每个 trading 进程的大致预分配成本

按默认常量和典型 x86-64 大小粗略估算：

```text
4 个 Logger queue                         ≈ 512 MiB
8 个 MarketOrder pool                     ≈ 512 MiB
8 个 market-order pointer table           ≈  64 MiB
OrderGateway 的 TCP inbound/outbound       ≈ 128 MiB
MDC 两个 McastSocket 的 inbound/outbound   ≈ 256 MiB
三条 LFQueue                              ≈  27 MiB
-------------------------------------------------------
仅主要预分配合计                           ≈ 1.46 GiB
```

还没有包含 price pool、线程栈、map/vector recovery、程序和 allocator overhead。因此 4 GB VM 启动
多个 trading client 很容易 OOM；这也是“一个进程一个 client”的当前模型不适合直接做大规模 client
扩展性测试的源码原因。

## 5. Market Data 接收与 Snapshot Recovery 链路

### 5.1 稳态增量路径

```text
Exchange MarketDataPublisher
  ▼ UDP multicast：MDPMarketUpdate(seq + MEMarketUpdate)
Common::McastSocket::sendAndRecv()
  ▼
MarketDataConsumer::recvCallback(socket)
  │ 固定长度解析 MDPMarketUpdate
  │ request.seq == next_exp_inc_seq_num_ ?
  │ 是：next_exp_inc_seq_num_++
  ▼
incoming_md_updates_->getNextToWriteTo()
  │ copy MEMarketUpdate
  ▼
LFQueue::updateWriteIndex()
  ▼
TradeEngine::run()
```

MarketDataConsumer 去掉 network wrapper 和 sequence，只把 `MEMarketUpdate` 交给策略线程。因此本地
MarketOrderBook 不感知 UDP socket，也不负责 gap detection。

### 5.2 发现 gap 后的恢复路径

```text
recvCallback(): incoming seq != expected seq
  ▼
in_recovery_ = true
  ▼
startSnapshotSync()
  ├─ 清空 snapshot/incremental maps
  ├─ 初始化 snapshot socket
  └─ join 233.252.14.1:20000

恢复期间：
snapshot message    → snapshot_queued_msgs_[snapshot_seq]
incremental message → incremental_queued_msgs_[incremental_seq]
                          ▼
                   checkSnapshotSync()
```

`checkSnapshotSync()` 要求：

1. snapshot 第一条必须是 `SNAPSHOT_START`；
2. snapshot local sequence 必须从 0 连续；
3. 最后一条必须是 `SNAPSHOT_END`；
4. 读取 END 的 `order_id_`，令下一增量序号为 `last_snapshot_inc_seq + 1`；
5. 跳过过旧 incremental，剩余 incremental 必须连续；
6. 将 snapshot 中的 CLEAR/ADD 与其后 incrementals 写入 TradeEngine queue；
7. 清空 maps、退出 recovery、leave snapshot stream。

## 6. 本地 MarketOrderBook 收到更新后的调用链

共同入口：

```text
TradeEngine::run()
  ▼
TradeEngine::processMarketUpdate(update)
  │ 校验 ticker_id
  ▼
ticker_order_book_[ticker_id]->onMarketUpdate(update)
```

### 6.1 ADD

```text
MarketOrderBook::onMarketUpdate(ADD)
  ├─ 预先判断此次更新是否影响 best bid/ask
  ├─ order_pool_.allocate(order_id, side, price, qty, priority)
  ├─ addOrder()
  │    ├─ 新价位：price_pool.allocate() → addOrdersAtPrice()
  │    ├─ 已有价位：append 到 FIFO tail
  │    └─ oid_to_order_[market_order_id] = order
  ├─ updateBBO(affected side)
  └─ TradeEngine::onOrderBookUpdate(...)
```

### 6.2 MODIFY

```text
onMarketUpdate(MODIFY)
  ├─ order = oid_to_order_[market_order_id]
  ├─ order->qty_ = update.qty_
  ├─ 若订单位于 best level：重新遍历该档聚合 BBO qty
  └─ TradeEngine::onOrderBookUpdate(...)
```

Exchange 当前只用 MODIFY 表示被动订单部分成交后的 leaves qty；Trading 侧不实现 client modify
request，也不在 MODIFY 中移动 price/side。

### 6.3 CANCEL

```text
onMarketUpdate(CANCEL)
  ├─ order = oid_to_order_[market_order_id]
  ├─ removeOrder(order)
  │    ├─ 同价位最后一张 → removeOrdersAtPrice()
  │    ├─ 否则从 order ring 摘除
  │    ├─ oid index 清空
  │    └─ order 归还 pool
  ├─ 更新受影响 side 的 BBO
  └─ TradeEngine::onOrderBookUpdate(...)
```

### 6.4 TRADE

```text
onMarketUpdate(TRADE)
  └─ TradeEngine::onTradeUpdate(update, book)
       └─ return
```

TRADE 本身不修改本地 book，也不调用 `onOrderBookUpdate()`。原因是 Exchange 会在 TRADE 后继续发
被动订单的 MODIFY 或 CANCEL，那条消息才表达公开簿的实际状态变化。

### 6.5 CLEAR

```text
onMarketUpdate(CLEAR)
  ├─ 遍历 oid_to_order_，归还全部 MarketOrder
  ├─ oid_to_order_.fill(nullptr)
  ├─ 遍历 bid/ask price ring，归还全部 price node
  ├─ bids_by_price_ = asks_by_price_ = nullptr
  ├─ updateBBO(bid_updated, ask_updated)
  └─ TradeEngine::onOrderBookUpdate(...)
```

CLEAR 来自 snapshot recovery，每个 ticker 一条。当前实现存在 BBO 和 price index 未完全清理问题，
见第 11 节。

## 7. MarketMaker：行情到第一笔订单的调用链

Maker 由 ADD/MODIFY/CANCEL/CLEAR 等 order-book change 触发：

```text
MarketOrderBook::onMarketUpdate()
  ▼ 更新本地 book 和 BBO
TradeEngine::onOrderBookUpdate(ticker, price, side, book)
  ├─ position_keeper_.updateBBO(ticker, bbo)
  ├─ feature_engine_.onOrderBookUpdate(...)
  │    └─ 计算 mkt_price/fair price
  └─ algoOnOrderBookUpdate_(...)
       ▼
     MarketMaker::onOrderBookUpdate()
       ├─ 读取 BBO、fair price、clip、threshold
       ├─ 计算 bid target / ask target
       └─ OrderManager::moveOrders(ticker, bid, ask, clip)
            ├─ moveOrder(BUY)
            │    ├─ LIVE 且 price 不同 → cancelOrder()
            │    └─ INVALID/DEAD → RiskManager::checkPreTradeRisk()
            │                         └─ ALLOWED → newOrder()
            └─ moveOrder(SELL)：相同逻辑
                         ▼
              TradeEngine::sendClientRequest()
                         ▼
              ClientRequestLFQueue commit
```

本项目 Tick-to-Trade maker benchmark 的边界正是：调用 `processMarketUpdate()` 前，到以上第一次
`sendClientRequest()` 完成 LFQueue commit。它不包含 MarketDataConsumer、UDP、OrderGateway 或
TCP。

## 8. LiquidityTaker：成交行情到订单的调用链

```text
MarketOrderBook::onMarketUpdate(TRADE)
  ▼
TradeEngine::onTradeUpdate(update, book)
  ├─ feature_engine_.onTradeUpdate()
  │    └─ trade qty / 对手 BBO qty
  └─ algoOnTradeUpdate_(...)
       ▼
     LiquidityTaker::onTradeUpdate()
       ├─ ratio >= threshold ?
       ├─ aggressive BUY → desired BUY @ best ask
       ├─ aggressive SELL → desired SELL @ best bid
       └─ OrderManager::moveOrders()
            ▼ risk/new/cancel 状态机
       TradeEngine::sendClientRequest()
            ▼
       ClientRequestLFQueue commit
```

Taker benchmark 的边界同样从 decoded TRADE 进入 TradeEngine 到第一笔 request commit，不包含网络。

由于 Exchange 对一笔成交的行情顺序是 `TRADE → MODIFY/CANCEL passive order`，Taker 看到 TRADE 时
本地簿仍是更新前状态；Maker 忽略 TRADE，等后续 MODIFY/CANCEL 后再重新定价。

## 9. 策略订单发送、接收响应和订单状态链

### 9.1 NEW 发送路径

```text
OrderManager::moveOrder()
  ▼ RiskManager::checkPreTradeRisk()
OrderManager::newOrder()
  ├─ 构造 MEClientRequest{NEW, client_id, ticker, next_order_id, ...}
  ├─ TradeEngine::sendClientRequest()
  │    └─ copy 到 ClientRequestLFQueue + commit
  ├─ OMOrder = PENDING_NEW
  └─ next_order_id_++
       ▼
OrderGateway::run()
  ├─ 从 LFQueue 读取请求
  ├─ tcp_socket_.send(next_outgoing_seq_num_)
  ├─ tcp_socket_.send(MEClientRequest)
  ├─ 释放 queue slot
  └─ next_outgoing_seq_num_++
       ▼ 下一轮 TCPSocket::sendAndRecv()
TCP ::send() → Exchange OrderServer
```

`TCPSocket::send()` 只是 memcpy 到预分配 outbound buffer；真正 kernel `::send()` 在
`sendAndRecv()` 中执行。

### 9.2 CANCEL 发送路径

```text
OrderManager::moveOrder()
  │ LIVE 且目标 price 不同/INVALID
  ▼
OrderManager::cancelOrder(order)
  ├─ 使用原 client order id 构造 CANCEL
  ├─ TradeEngine::sendClientRequest()
  └─ order_state = PENDING_CANCEL
       ▼
OrderGateway → TCP → Exchange
```

项目没有 client MODIFY；改价使用 cancel，等状态变 DEAD 后由后续策略事件再 NEW。

### 9.3 Response 接收路径

```text
Exchange OrderServer
  ▼ TCP：OMClientResponse(seq + MEClientResponse)
TCPSocket::sendAndRecv()
  ▼
OrderGateway::recvCallback(socket, rx_time)
  ├─ 固定长度解析
  ├─ 校验 response.client_id == 本 client
  ├─ 校验 response.seq == next_exp_seq_num_
  ├─ next_exp_seq_num_++
  └─ copy MEClientResponse 到 ClientResponseLFQueue
       ▼
TradeEngine::run()
  ▼
TradeEngine::onOrderUpdate(response)
```

### 9.4 ACCEPTED/CANCELED/FILLED 的处理顺序

```text
TradeEngine::onOrderUpdate(response)
  │
  ├─ response.type == FILLED
  │    └─ PositionKeeper::addFill()
  │         └─ position / realized / unrealized / total PnL / volume
  │
  └─ algoOnOrderUpdate_(response)
       └─ MarketMaker::onOrderUpdate() 或 LiquidityTaker::onOrderUpdate()
            └─ OrderManager::onOrderUpdate()
                 ├─ ACCEPTED → LIVE
                 ├─ CANCELED → DEAD
                 ├─ FILLED   → qty=leaves；0 时 DEAD
                 └─ CANCEL_REJECTED/INVALID → 当前代码不改变状态
```

因此 FILLED 到达时，PositionKeeper 先更新，OrderManager 后更新。RiskManager 持有 PositionInfo 指针，
下一次新单 risk check 能看到新的持仓。

## 10. 撮合成功后 Trading 侧的双通道数据链

同一笔撮合会从 Exchange 发出两类消息：私有 response 走 TCP，公开行情走 UDP multicast。

```text
                         Exchange MEOrderBook::match()
                         /                         \
                        /                           \
       FILLED response /                             \ TRADE + MODIFY/CANCEL
                      ▼                               ▼
             OrderServer/TCP               MarketDataPublisher/UDP
                      ▼                               ▼
             OrderGateway thread              MDC thread
                      ▼                               ▼
          ClientResponseLFQueue             MarketUpdateLFQueue
                      \                               /
                       \                             /
                        ▼                           ▼
                              TradeEngine thread
```

私有链最终更新：

```text
PositionKeeper → OrderManager 的私有订单 state/qty
```

公开链最终更新：

```text
TRADE → FeatureEngine/Taker
MODIFY/CANCEL → MarketOrderBook/BBO → Position mark-to-market/FeatureEngine/Maker
```

两条 LFQueue、两个网络协议、多个线程彼此独立。虽然 Exchange 在 `match()` 中有固定 enqueue 顺序，
Trading 端也不能假设 FILLED 一定早于或晚于 TRADE/MODIFY/CANCEL。TradeEngine 只在当前 loop 中先
排空 response queue，再排空 market-data queue；这不是跨网络通道的全局 sequencing guarantee。

### 10.1 自己的订单在两个状态模型中的区别

- `OMOrder`：私有订单状态，用 client order id，通过 ACCEPTED/CANCELED/FILLED 更新；
- `MarketOrder`：公开簿镜像，用 Exchange market order id，通过 ADD/MODIFY/CANCEL 更新。

收到 ACCEPTED 不会直接向本地 MarketOrderBook 插入订单；必须等公开 ADD multicast 到达。同样，
FILLED 先更新自己的 PositionInfo，公开簿则等对应 MODIFY/CANCEL 更新。这种分离符合真实交易系统
“private order feed + public market-data feed”的基本形态。

## 11. 当前实现的重要假设和源码限制

### 11.1 MarketOrderBook 正确性边界

- `priceToIndex(price) = price % ME_MAX_PRICE_LEVELS` 没有 collision resolution；相差 256 的活跃价格
  会占同一槽；负价格也会造成非法下标；
- MODIFY/CANCEL 直接取得 `oid_to_order_[order_id]` 后解引用，没有检查消息是否重复或 order 是否
  存在；
- CLEAR 归还所有 price node，但没有 `price_orders_at_price_.fill(nullptr)`，会留下指向已释放 pool
  slot 的旧指针；
- CLEAR 的 `side_` 为 INVALID，进入前计算出的 `bid_updated/ask_updated` 都是 false，因此清空 book
  后 `updateBBO(false,false)` 不会清空旧 BBO；若 snapshot 某侧没有随后 ADD，stale BBO 会保留；
- `updateBBO()` 每次更新最优档时遍历该价位所有订单求和，不是严格 O(1)；热点价位订单很多时会
  放大 Tick-to-Trade 延迟；
- public order id array 要求每 ticker 的 market id 小于 `ME_MAX_ORDER_IDS`。

### 11.2 OrderManager 状态机边界

- 每 ticker/side 只跟踪一张订单，不支持多层报价和多张并发订单；
- `onOrderUpdate()` 仅按 ticker+side 选择 OMOrder，没有验证 response 的 client order id 是否等于
  当前 `OMOrder::order_id_`，迟到/异常 response 可能更新错误对象；
- Exchange 的 CANCEL_REJECTED 使用 `Side::INVALID`，当前代码会索引 INVALID side 槽且不修改状态，
  原订单可能永久停在 PENDING_CANCEL；
- LIVE 且 price 相同但 qty 不同时不会调整数量，和函数注释“price and quantity”不一致；
- cancel-replace 不是连续操作：CANCELED 后需要下一条行情/策略事件才会发 replacement；
- 没有 reject-new response 类型，也没有超时和重连后的订单状态恢复。

### 11.3 RiskManager 的覆盖范围

当前 risk 只检查：单笔 qty、当前净持仓加本单 qty、当前 total PnL。它没有计算：

- live/pending order 的潜在持仓；
- 双边同时成交后的最大暴露；
- notional、price band、order rate、message rate；
- 跨 ticker/组合风险、信用额度或 kill switch；
- risk reservation 和并发订单释放。

`max_loss_` 是 PnL 下限，必须传负数；`scripts/run_clients.sh` 使用 -100、-300 等。若误传正数，
初始 PnL 0 就会满足 `0 < max_loss`，所有订单被判为 LOSS_TOO_LARGE。

### 11.4 Position 和 Feature 边界

- `PositionInfo::open_vwap_` 没有显式 `{}` 初始化；按标准 C++ 不能依赖其元素自动为 0，第一次
  `+= fill notional` 可能基于未初始化值，应改为 `open_vwap_{};`；
- `volume_` 是 `uint32_t`，长时间累计可能溢出；
- fill 后的 unrealized PnL 暂时用 fill price 计算，要等 BBO update 才按 mid mark；
- FeatureEngine 的两个值全 ticker 共用，不保存每 ticker 历史；
- `Feature_INVALID` 是 NaN，但代码用 `feature != Feature_INVALID` 检查。NaN 与自身比较也不相等，
  所以这个判断恒为 true；正确判断应使用 `std::isnan()`/`std::isfinite()`；
- aggressive trade ratio 只检查 BBO price 有效，没有检查对应 BBO qty 是否为 0。

### 11.5 并发和生命周期

- OrderGateway 使用 atomic run flag，并保存/join thread；
- TradeEngine 和 MarketDataConsumer 使用 `volatile bool`，且没有保存
  `createAndStartThread()` 返回的 thread pointer，无法 join；`volatile` 不是线程同步；
- Trading main 停止顺序是 TradeEngine→MDC→Gateway，但 `TradeEngine::stop()` 在 producer 尚未停止
  时等待两个输入 queue 为空；持续流量下可能一直等；
- TradeEngine 固定先 drain response 再 drain market data，极端 response load 可能造成行情饥饿；
- callback 使用 `std::function`；灵活但有间接调用成本。

### 11.6 MarketDataConsumer 恢复和网络边界

- 稳态 buffer 处理完后使用 `memcpy(dst, src, remaining)` 移动重叠区域，标准 C++ 中这是未定义行为，
  应像 OrderGateway 一样使用 `memmove`；
- snapshot/incremental maps 和 `final_events` 会在 recovery 动态分配，大快照可能显著占内存；
- `McastSocket` 没有严谨处理 UDP send/receive error 和 datagram 边界；
- wire struct 直接依赖本机 packed C++ layout、`size_t` 和字节序，没有 schema/version；
- 每个 McastSocket 即使只接收也同时预分配 64 MiB inbound 和 64 MiB outbound；
- steady-state 只有 sequence gap 检测，没有 checksum、冗余 A/B feed 或重传请求。

### 11.7 RANDOM 模式不能代表策略/Risk benchmark

RANDOM 直接调用 `TradeEngine::sendClientRequest()`，绕过 FeatureEngine、OrderManager 和 RiskManager；
它还在 main thread 中产生请求。因此用 RANDOM 测得的订单率/延迟不能代表 MAKER/TAKER 的完整策略
路径，也不能证明 pre-trade risk 已覆盖这些订单。

## 12. 面试总结模板

> Trading 侧采用一个 client 一个进程，拆为 MarketDataConsumer、TradeEngine 和 OrderGateway 三条
> 业务线程。MarketDataConsumer 订阅 incremental multicast，通过 sequence 检测丢包，并用 snapshot
> 加 gap 后 incrementals 恢复，再把解码事件写入 SPSC queue。TradeEngine 是策略状态的唯一串行
> owner：它维护每 ticker 的 order-by-order 本地簿和 BBO，依次更新 PositionKeeper、FeatureEngine，
> 再通过 callback 调用 MarketMaker 或 LiquidityTaker。策略不直接发网络包，而是由 OrderManager
> 管理每 ticker/side 的订单状态，通过 RiskManager 检查单笔数量、持仓和 PnL 后，把请求写给
> OrderGateway；Gateway 添加应用序号并通过非阻塞 TCP 发往 Exchange。订单簿使用直接寻址 array、
> 预分配 MemPool 和两层侵入式循环双链表，减少 hot-path allocation，并支持 O(1) order lookup 与
> 摘链。私有 FILLED response 和公开 TRADE/MODIFY/CANCEL 是两条独立通道，前者更新持仓和私有订单
> 状态，后者更新公开簿和策略特征，源码没有跨通道全序保证。

面试中还应主动说明：这是教学型单进程 client 架构，RiskManager 没有 outstanding exposure，OMS
每侧只管理一张订单，本地簿存在 price modulo collision/CLEAR 清理问题，MarketData recovery 使用
动态容器，部分线程生命周期也需要修正；因此可以展示低延迟分层和 benchmark 方法，但不声称已经
达到生产级交易客户端的可靠性与风控完整度。

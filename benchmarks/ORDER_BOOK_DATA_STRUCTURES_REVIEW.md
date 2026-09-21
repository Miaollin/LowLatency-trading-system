# LowLatency Trading System：订单簿数据结构与复杂度源码解析

> 本文只分析当前仓库真实源码，不把设计意图当成已经实现的能力。重点文件是
> `exchange/matcher/me_order.h`、`exchange/matcher/me_order_book.h/.cpp`、
> `trading/strategy/market_order.h`、`trading/strategy/market_order_book.h/.cpp` 和
> `common/mem_pool.h`。

## 1. 先给出结论

这个项目的订单簿不是单独依靠一种“哈希表”完成的，而是组合了三类结构：

1. **直接寻址数组**：根据 client/order ID 或 market order ID 直接取得订单指针；
2. **两层侵入式循环双向链表**：外层维护有序价格档，内层维护同价格订单的 FIFO；
3. **预分配对象池**：保存订单节点和价格档节点，避免在正常热路径中调用通用堆分配器。

简化后的 Exchange 订单簿如下：

```text
cid_oid_to_order_[client_id][client_order_id]
                         |
                         +--------------------------+
                                                    v
                                              +-----------+
                                              |  MEOrder  |
                                              +-----------+
                                                    ^
                                                    |
price_orders_at_price_[price % 256]                 |
                         |                          |
                         v                          |
                  +-----------------+               |
bids_by_price_ -->| MEOrdersAtPrice |--first_order-+
                  +-----------------+
                      |         |
                      | price   +--> 同价格订单循环双链表（FIFO）
                      |
                      +------------> 价格档循环双链表（BUY 降序）

asks_by_price_ --------------------> 价格档循环双链表（SELL 升序）
```

因此，复杂度必须分操作说明：

| 操作 | 当前源码复杂度 | 说明 |
|---|---:|---|
| 根据 client ID + client order ID 查 Exchange 订单 | O(1) | 二维直接寻址数组 |
| 根据 market order ID 查 Trading 订单 | O(1) | 一维直接寻址数组 |
| 根据价格取得价格档槽位 | O(1) | `price % ME_MAX_PRICE_LEVELS`，但没有冲突处理 |
| 取得 best bid / best ask | O(1) | `bids_by_price_` / `asks_by_price_` |
| 在已有价格档尾部追加订单 | O(1) | 循环双链表的 `head->prev` 就是尾节点 |
| 创建第一个价格档 | O(1) | 节点自环并设置 best 指针 |
| 插入一个新的最优价格档 | O(1) | 可直接插在 best 前面 |
| 插入一个新的普通/最差价格档 | O(L) 最坏 | 必须沿价格档链表查找有序位置，L 为该侧价格档数量 |
| 根据 ID 撤销订单 | O(1) | 直接查指针，再从双链表摘链 |
| 删除已知空价格档 | O(1) | 双向链表摘链 |
| 撮合一个 passive order | O(1) | 不计输出队列阻塞 |
| 一个 aggressive order 撮合 K 个 passive orders | O(K) | `checkForMatch()` 循环 K 次 |
| 计算某一价格的下一 FIFO priority | O(1) | `first->prev` 直接得到尾订单 |
| Trading 端取得 BBO 价格 | O(1) | best 指针 |
| Trading 端计算 BBO 总数量 | O(M) | 遍历最优档的 M 个订单求和 |
| CLEAR Trading 订单簿 | O(MAX_ORDER_IDS + L) | 先扫描整个 ID 数组，再回收价格档 |

面试时最准确的说法是：

> 项目利用直接寻址数组和侵入式循环双链表，把订单 ID 查询、已有价格档的 FIFO
> 追加、已知订单撤销以及 best price 访问做成了 O(1)；新价格档仍需在线性有序链表中
> 寻找插入位置，最坏为 O(L)，一次扫过 K 个挂单的撮合为 O(K)。

不能说“订单簿所有插入和撮合都是 O(1)”。

---

## 2. 两套订单簿分别做什么

项目中有两套形状相似、用途不同的订单簿。

| 订单簿 | 文件 | 数据来源 | 用途 |
|---|---|---|---|
| `Exchange::MEOrderBook` | `exchange/matcher/me_order_book.h/.cpp` | 客户端 NEW/CANCEL 请求 | 权威订单状态、价格时间优先撮合、生成 response 和 market update |
| `Trading::MarketOrderBook` | `trading/strategy/market_order_book.h/.cpp` | Exchange 发布的 `MEMarketUpdate` | 客户端本地市场视图、维护 BBO、通知策略 |

二者结构相似，但订单索引的 key 不同：

- Exchange 必须根据 `(client_id, client_order_id)` 撤单，所以使用二维数组；
- Trading 收到的市场数据携带全市场 `market_order_id`，所以使用一维数组。

每个 ticker 单独拥有一本订单簿。Exchange 在 `matching_engine.h` 中使用
`std::array<MEOrderBook *, ME_MAX_TICKERS>`；Trading 在 `market_order_book.h` 中使用
`std::array<MarketOrderBook *, ME_MAX_TICKERS>`。ticker ID 本身也可作为数组下标，因此
从 ticker 到订单簿同样是 O(1)。

---

## 3. Exchange 订单和价格档节点

### 3.1 `MEOrder`

源码：`exchange/matcher/me_order.h`，结构体 `Exchange::MEOrder`。

| 字段 | 作用 |
|---|---|
| `ticker_id_` | 合约/证券 ID |
| `client_id_` | 订单属于哪个客户端 |
| `client_order_id_` | 客户端自己的订单 ID，用于请求和撤单 |
| `market_order_id_` | Matching Engine 分配的全市场订单 ID，用于行情更新 |
| `side_` | BUY 或 SELL |
| `price_` | 限价 |
| `qty_` | 当前剩余数量；部分成交时原地减少 |
| `priority_` | 同价格档中的时间优先序号 |
| `prev_order_` | 同价格前一个订单 |
| `next_order_` | 同价格后一个订单 |

`MEOrder` 同时是业务对象和链表节点，这叫**侵入式数据结构**。项目没有再分配
`std::list` node，也不需要从 list node 间接找到订单对象。

### 3.2 `MEOrdersAtPrice`

源码：`exchange/matcher/me_order.h`，结构体 `Exchange::MEOrdersAtPrice`。

| 字段 | 作用 |
|---|---|
| `side_` | 这个价格档属于买侧还是卖侧 |
| `price_` | 价格档价格 |
| `first_me_order_` | 该价格 FIFO 中最早的订单，也是下一次优先成交的订单 |
| `prev_entry_` | 同一侧上一个价格档 |
| `next_entry_` | 同一侧下一个价格档 |

`MEOrdersAtPrice` 也是侵入式价格档链表的节点。

### 3.3 一本 `MEOrderBook` 的主要成员

源码：`exchange/matcher/me_order_book.h`，类 `Exchange::MEOrderBook`。

| 成员 | 类型/含义 |
|---|---|
| `cid_oid_to_order_` | `array<array<MEOrder *>>`，从 client/order ID 直接定位订单 |
| `orders_at_price_pool_` | 最多 `ME_MAX_PRICE_LEVELS` 个价格档节点的对象池 |
| `bids_by_price_` | 买侧最优价节点；买价链从高到低 |
| `asks_by_price_` | 卖侧最优价节点；卖价链从低到高 |
| `price_orders_at_price_` | 价格槽位到价格档指针的数组 |
| `order_pool_` | 最多 `ME_MAX_ORDER_IDS` 个订单节点的对象池 |
| `client_response_` | 复用的客户端响应临时对象 |
| `market_update_` | 复用的市场数据更新临时对象 |
| `next_market_order_id_` | 单 ticker 递增的 market order ID |

---

## 4. Trading 本地订单簿的数据结构

### 4.1 `MarketOrder`

源码：`trading/strategy/market_order.h`。

`MarketOrder` 保存 `order_id_`、`side_`、`price_`、`qty_`、`priority_`，以及
`prev_order_` 和 `next_order_`。它不保存 client ID 和 client order ID，因为这是公共市场
视图，不是某个客户端自己的委托记录。

### 4.2 `MarketOrdersAtPrice`

它保存价格档的 `side_`、`price_`、FIFO 头指针 `first_mkt_order_`，以及价格档链表的
`prev_entry_`、`next_entry_`。

### 4.3 `MarketOrderBook`

源码：`trading/strategy/market_order_book.h/.cpp`。

| 成员 | 作用 |
|---|---|
| `oid_to_order_` | `market_order_id -> MarketOrder *` 的直接寻址数组 |
| `orders_at_price_pool_` | 价格档对象池 |
| `bids_by_price_` / `asks_by_price_` | best bid / best ask 指针 |
| `price_orders_at_price_` | 价格到价格档的槽位数组 |
| `order_pool_` | 本地市场订单对象池 |
| `bbo_` | 当前 best bid/offer 价格及该档总量 |
| `trade_engine_` | 订单簿变化后通知策略所在的 TradeEngine |

---

## 5. 为什么使用两层循环双向链表

### 5.1 第一层：价格档链表

买侧和卖侧是两条独立的循环双向链表：

```text
BUY，最优到最差：
bids_by_price_
      |
      v
   105 <==> 104 <==> 101
    ^                 |
    +=================+

SELL，最优到最差：
asks_by_price_
      |
      v
   106 <==> 108 <==> 110
    ^                 |
    +=================+
```

关键不变量是：

- `bids_by_price_` 永远指向最高买价；
- `asks_by_price_` 永远指向最低卖价；
- `next_entry_` 从更优价格走向更差价格；
- 只有一个价格档时，`prev_entry_` 和 `next_entry_` 都指向自己；
- 最后一个节点的 `next_entry_` 回到 best，best 的 `prev_entry_` 是最差价格档。

这样设计的收益是：

- 读取 best bid/ask 只读一个指针，为 O(1)；
- 撮合清空一个最优价格档后，`best = best->next_entry_` 即可得到下一个档，为 O(1)；
- 已知价格档节点时，删除只修改相邻两个节点，为 O(1)。

代价是：

- 没有树结构帮助查找一个新价格的排序位置；
- 新的非最优价格档需要沿链表扫描；
- 指针不变量多，写错一个链接就可能破坏整条环。

### 5.2 第二层：同价格订单 FIFO

每个价格档内部又是一条循环双向链表：

```text
first_me_order_
      |
      v
  order A <==> order B <==> order C
     ^                         |
     +=========================+

A 最早，优先成交
C 最新，A->prev_order_ 直接指向 C
```

关键不变量是：

- `first_me_order_`/`first_mkt_order_` 指向最老订单；
- `first->prev_order_` 是最新订单，即尾节点；
- 单个订单时 `order->prev_order_ == order->next_order_ == order`；
- 新订单总是追加到尾部，保持 price-time priority。

为什么不额外保存 `tail`？因为循环双链表已经让 `head->prev` 等价于 tail。一本订单簿
不需要为每个价格档再增加一个尾指针。

---

## 6. O(1) 订单查找是怎么实现的

### 6.1 Exchange：二维直接寻址

类型定义位于 `exchange/matcher/me_order.h`：

```cpp
using OrderHashMap = std::array<MEOrder *, ME_MAX_ORDER_IDS>;
using ClientOrderHashMap = std::array<OrderHashMap, ME_MAX_NUM_CLIENTS>;
```

虽然源码注释把它叫作 HashMap，但它实际上不是传统哈希表。查找没有计算 hash，也没有
bucket、链式冲突或开放寻址：

```text
地址 = 数组首地址
     + client_id * sizeof(OrderHashMap)
     + client_order_id * sizeof(MEOrder *)
```

`MEOrderBook::cancel()` 的核心查找为：

```cpp
auto &client_orders = cid_oid_to_order_.at(client_id);
MEOrder *order = client_orders.at(order_id);
```

因此撤单不需要遍历价格档，也不需要遍历同价格 FIFO。找到 `MEOrder *` 后，它自身带有
`prev_order_` 和 `next_order_`，可直接摘链。

为什么 key 中需要 client ID？因为两个客户端都可能使用 `client_order_id == 100`。
二维数组把二者放到不同区域：

```text
client 7, order 100  -> cid_oid_to_order_[7][100]
client 9, order 100  -> cid_oid_to_order_[9][100]
```

### 6.2 Trading：一维直接寻址

类型定义位于 `trading/strategy/market_order.h`：

```cpp
using OrderHashMap = std::array<MarketOrder *, ME_MAX_ORDER_IDS>;
```

Exchange 生成的 market order ID 被直接用作下标：

```cpp
auto order = oid_to_order_.at(market_update->order_id_);
```

所以 `MODIFY` 能直接修改 `qty_`，`CANCEL` 能直接获得待删除节点，都是 O(1)。

### 6.3 为什么这种查找快

在 ID 稠密、上界可控的前提下，直接寻址有以下特点：

- 固定次数的下标计算；
- 没有 hash 运算和冲突探测；
- 没有 rehash；
- 不需要单独分配哈希节点；
- 数组槽位连续，访问模式比 node-based `std::unordered_map` 更简单；
- 延迟更可预测。

但这是用内存换时间。按 x86-64 一个指针 8 bytes 估算，默认常量来自
`common/types.h`：

```text
ME_MAX_NUM_CLIENTS = 256
ME_MAX_ORDER_IDS   = 1,048,576

Exchange 每个 ticker 的二维索引：
256 * 1,048,576 * 8 bytes = 2 GiB

8 个 ticker：
2 GiB * 8 = 16 GiB

Trading 每个 ticker 的一维索引：
1,048,576 * 8 bytes = 8 MiB

8 个 ticker：
8 MiB * 8 = 64 MiB
```

这里还没有计算订单池、队列、logger 和其他组件。因此这种设计只有在 ID 范围较小或
内存预算充足时才合适。benchmark target 把 clients 和 order IDs 缩小，正是为了避免
默认二维数组的多 GiB 占用。

### 6.4 直接寻址的使用前提

必须保证：

- ID 非负且可直接转为数组下标；
- `client_id < ME_MAX_NUM_CLIENTS`；
- `order_id < ME_MAX_ORDER_IDS`；
- ID 空间不能无限稀疏；
- 相同 `(client_id, client_order_id)` 在旧订单仍存在时不能重复使用。

当前 `MEOrderBook::cancel()` 检查了 client ID 是否越界，但没有在调用 `.at(order_id)`
之前单独检查 order ID。越界会抛出 `std::out_of_range`；函数又声明为 `noexcept`，最终会
`std::terminate()`。这不是可以忽略的业务校验。

另外，正常配置的 `MEOrderBook` 构造函数只对 `price_orders_at_price_` 执行了
`fill(nullptr)`；`cid_oid_to_order_` 只在 `LLT_BENCHMARK_MODE` 下显式清零。指针数组未初始化
就参与撤单查找存在正确性风险。要将它作为可靠实现使用，应在正常模式也初始化，或重新
设计这个过大的索引结构。

---

## 7. O(1) 价格档查找：实现与限制

Exchange 和 Trading 使用相同实现：

```cpp
index = price % ME_MAX_PRICE_LEVELS;
orders_at_price = price_orders_at_price_.at(index);
```

默认 `ME_MAX_PRICE_LEVELS = 256`，所以价格查找只需取模和数组读取，在指令数量意义上为
O(1)。价格档数组只保存 256 个指针，在 64 位系统约为 2 KiB/订单簿。

但是，当前实现**不是完整的哈希表**，因为没有解决冲突，也没有校验槽位中节点的
`price_` 是否等于查询价格。

例如：

```text
100 % 256 = 100
356 % 256 = 100
```

如果价格 100 和 356 同时存在，它们会写入同一个槽位。后写入者覆盖前者的查找入口；
价格链表中的旧节点仍可能存在。随后 `getOrdersAtPrice(100)` 甚至可能返回价格 356 的节点。

所以真实结论是：

> 价格槽位访问是 O(1)，但只有在所有同时活跃价格对 256 取模后互不冲突的输入约束下，
> 才能作为正确的 exact-price lookup。

源码还把 `Price` 定义为 `int64_t`。负价格取模后仍可能为负，转换为 `std::array::at()`
需要的无符号下标时会越界；在 `noexcept` 函数中同样可能导致进程终止。业务入口需要保证
价格有效，或者索引函数需要显式标准化并处理冲突。

买卖两侧还共享同一个 `price_orders_at_price_`。正常未交叉限价簿通常不会长期保留完全
相同价格的 bid 和 ask，但数据结构本身没有用 side 区分 key。稳健实现应该至少以
`(side, price)` 为 key，或者使用两个独立的侧别索引。

---

## 8. 新订单插入的完整过程

Exchange 的入口是 `MEOrderBook::add()`，调用链为：

```text
MEOrderBook::add(...)
  |
  +--> generateNewMarketOrderId()
  |
  +--> MatchingEngine::sendClientResponse(ACCEPTED)
  |
  +--> checkForMatch(...)
  |      |
  |      +--> 零到多次 match(...)
  |
  +--> 若 leaves_qty > 0
         |
         +--> getNextPriority(price)
         +--> order_pool_.allocate(...)
         +--> addOrder(order)
         +--> MatchingEngine::sendMarketUpdate(ADD)
```

也就是说，订单并不是一进入 `add()` 就挂簿。它会先吃掉能够成交的反向订单，只有剩余量
大于零时才创建 resting order。

### 8.1 已有价格档：严格的常数次指针修改

`MEOrderBook::addOrder()` 先通过价格槽位得到 `orders_at_price`。若该档已存在：

```cpp
first = orders_at_price->first_me_order_;
tail  = first->prev_order_;

tail->next_order_  = order;
order->prev_order_ = tail;
order->next_order_ = first;
first->prev_order_ = order;
```

插入前：

```text
first=A

A <==> B <==> C
^           |
+===========+

A->prev = C
```

插入 D 后：

```text
first=A

A <==> B <==> C <==> D
^                 |
+=================+

A->prev = D
```

无论这个价格档有 1 个还是 100 万个订单，都只需要固定次数的读写，所以链表追加是 O(1)。
随后：

```cpp
cid_oid_to_order_[client_id][client_order_id] = order;
```

也是 O(1)。因此，在忽略对象池寻找空闲块的情况下，**向已有价格档追加挂单**的订单簿
结构操作为 O(1)。

### 8.2 新的第一个价格档

若该价格不存在，先让唯一订单自环：

```cpp
order->next_order_ = order;
order->prev_order_ = order;
```

再从 `orders_at_price_pool_` 取得一个 `MEOrdersAtPrice`，把它写进价格槽位。若该侧此前为空：

```cpp
bids_by_price_ 或 asks_by_price_ = new_level;
new_level->prev_entry_ = new_level;
new_level->next_entry_ = new_level;
```

这也是 O(1)。

### 8.3 新的最优价格档

已有 best 时，如果新 BUY 价格高于 best bid，或者新 SELL 价格低于 best ask，
`addOrdersAtPrice()` 可判断新节点应插在 best 之前，并更新 best 指针。

```text
原 BUY：105(best) <=> 103 <=> 101
插入 107：

107(new best) <=> 105 <=> 103 <=> 101
```

这个分支不需要沿链表查找，链表部分为 O(1)。

### 8.4 新的普通或最差价格档：不是 O(1)

如果新价格不优于 best，`addOrdersAtPrice()` 会从 best 开始沿 `next_entry_` 扫描，直到找到
正确有序位置或回到 best：

```text
BUY：110(best) -> 109 -> 107 -> 104 -> 100 -> 回到 110
插入 105：
检查 110、109、107、104，找到 107 与 104 之间的位置
```

扫描为 O(L)，其中 L 是当前这一侧的价格档数。找到目标后，真正的双链表插入只有固定四次
链接修改，是 O(1)；但整个“创建新价格档”的操作仍然是 O(L)。

默认最多只有 `ME_MAX_PRICE_LEVELS = 256` 个价格档，所以这是一个有固定上限的线性扫描。
工程上可能足够快，但算法复杂度不能因此写成 O(1)。benchmark 应分别测：

- existing-level append；
- new best level；
- new middle level；
- new worst level。

只测 existing-level append，不能代表所有 ADD 情况。

---

## 9. Priority 为什么可以 O(1) 生成

`MEOrderBook::getNextPriority(price)` 的实现是：

```cpp
if (!orders_at_price)
    return Priority{1};

return orders_at_price->first_me_order_
                      ->prev_order_
                      ->priority_ + 1;
```

因为 `first_me_order_->prev_order_` 就是 FIFO 尾订单，所以不需要遍历整档。新价格从 priority 1
开始，已有价格取尾订单 priority 加一。

Priority 的真正作用是让 Exchange 发出的 ADD/MODIFY market update 携带原始队列顺序，
Trading 端可以按相同顺序重建订单簿。当前 Trading `addOrder()` 实际按 market update 到达
顺序追加，并没有按 `priority_` 搜索插入；这依赖市场数据在进入它前已被 FIFOSequencer
排序并按正确序列交付。

---

## 10. 撤单为什么是 O(1)

Exchange 撤单调用链：

```text
MEOrderBook::cancel(client_id, order_id, ticker_id)
  |
  +--> cid_oid_to_order_[client_id][order_id]  // O(1)
  |
  +--> removeOrder(exchange_order)
  |      |
  |      +--> getOrdersAtPrice(order->price_) // O(1)，受价格冲突前提限制
  |      +--> 从 FIFO 双链表摘链           // O(1)
  |      +--> 若该档已空，摘掉价格档        // O(1)
  |      +--> 清空 ID 槽位并归还对象池       // O(1)
  |
  +--> sendMarketUpdate(CANCEL)
  +--> sendClientResponse(CANCELED)
```

### 10.1 删除非唯一订单

已知待删节点 `X`：

```cpp
before = X->prev_order_;
after  = X->next_order_;
before->next_order_ = after;
after->prev_order_  = before;
```

如果 `X` 正好是 FIFO 头，再执行：

```cpp
orders_at_price->first_me_order_ = after;
```

删除前：

```text
A <==> X <==> C
```

删除后：

```text
A <========> C
```

不需要搜索 X，因为直接寻址表已经给出了 `MEOrder *`。

### 10.2 删除价格档中唯一订单

代码用 `order->prev_order_ == order` 判断唯一节点。此时调用
`removeOrdersAtPrice(side, price)`：

- 价格档是该侧唯一档：把相应 best 指针设为 `nullptr`；
- 否则让前后价格档互相链接；
- 如果删的是 best，让 best 指向 `next_entry_`；
- 清空价格槽位；
- 把价格档节点还给 `orders_at_price_pool_`。

这些都是固定次数操作，所以在已经得到正确价格档指针的前提下为 O(1)。

---

## 11. 撮合时如何利用这些结构

源码：`exchange/matcher/me_order_book.cpp` 中
`checkForMatch()`、`match()` 和 `removeOrder()`。

### 11.1 买单打卖盘

```text
incoming BUY(price=P, qty=Q)
  |
  +--> asks_by_price_                         // O(1) 取得最低卖价档
          |
          +--> first_me_order_                // O(1) 取得该档最老订单
                  |
                  +--> P < ask_price ? stop
                  |
                  +--> match(...)
                          |
                          +--> fill_qty = min(leaves, passive.qty)
                          +--> 更新双方剩余量
                          +--> 发布两条 FILLED response
                          +--> 发布 TRADE market update
                          +--> passive 全成：CANCEL update + removeOrder()
                          +--> passive 部成：MODIFY update
```

卖单打买盘完全对称，从 `bids_by_price_` 和该档 `first_me_order_` 开始。

### 11.2 为什么每次选择下一笔订单是 O(1)

- 最优反向价格由 best 指针给出；
- 最早 passive order 由价格档 head 指针给出；
- 全成后从 FIFO 摘链为 O(1)；
- 如果价格档清空，best 指向价格档的 `next_entry_` 为 O(1)。

因此撮合单个 passive order 的订单簿结构操作是 O(1)。

### 11.3 为什么整个 aggressive order 不是 O(1)

`checkForMatch()` 使用 `while (leaves_qty && opposite_best)`。如果一个大单扫过 K 个 passive
orders，就会调用 K 次 `match()`，生成相应响应和行情事件，所以总复杂度为 O(K)。

例如 SWEEP4 benchmark 的含义正是一个 aggressive order 连续成交 4 个 passive orders：

```text
timestamp A
   |
   +--> match passive #1
   +--> match passive #2
   +--> match passive #3
   +--> match passive #4
   |
timestamp B

latency = B - A
```

它测的是一次多笔扫单，不是一次哈希查找，也不应期待与单笔撮合相同延迟。

---

## 12. Trading 如何根据行情重建订单簿

入口是 `MarketOrderBook::onMarketUpdate()`：

### 12.1 ADD

```text
MEMarketUpdate::ADD
  |
  +--> order_pool_.allocate(MarketOrder)
  +--> addOrder(order)
  |      +--> 价格槽位查询
  |      +--> 已有档：O(1) 追加到 FIFO
  |      +--> 新档：创建 price level，可能 O(L) 查排序位置
  |      +--> oid_to_order_[market_order_id] = order
  +--> updateBBO(...)
  +--> TradeEngine::onOrderBookUpdate(...)
```

### 12.2 MODIFY

```text
MEMarketUpdate::MODIFY
  |
  +--> oid_to_order_[market_order_id] // O(1)
  +--> order->qty_ = update.qty_      // O(1)
  +--> updateBBO(...)                 // 最优档数量可能 O(M)
```

### 12.3 CANCEL

```text
MEMarketUpdate::CANCEL
  |
  +--> oid_to_order_[market_order_id] // O(1)
  +--> removeOrder(order)             // O(1)
  +--> updateBBO(...)                 // 最优档数量可能 O(M)
```

### 12.4 TRADE

TRADE update 不直接增加、修改或删除 `MarketOrder`，而是调用：

```cpp
trade_engine_->onTradeUpdate(market_update, this);
return;
```

对应 passive order 的剩余量变化由随后发布的 MODIFY 或 CANCEL update 表达。

### 12.5 CLEAR

CLEAR 会扫描整个 `oid_to_order_`，归还非空订单对象，再遍历 bid/ask 价格档归还价格节点。
因此不是 O(1)。按默认容量，即使活跃订单很少也会扫描 1,048,576 个槽位。

当前 CLEAR 分支还有两个值得修正的问题：

1. 它没有对 `price_orders_at_price_` 再次执行 `fill(nullptr)`，槽位可能留下指向已回收节点的
   悬空指针；
2. `bid_updated` 和 `ask_updated` 根据 update side 计算，而 CLEAR 的 side 通常不是 BUY/SELL，
   因此 `updateBBO(false, false)` 不会清除旧 BBO。

这两点是源码行为，面试中不能把 CLEAR 描述为已经完整、可靠地重置所有索引和 BBO。

---

## 13. BBO 为什么不完全是 O(1)

`MarketOrderBook::updateBBO()` 取得 best price 很快：

```cpp
bbo_.bid_price_ = bids_by_price_->price_;
bbo_.ask_price_ = asks_by_price_->price_;
```

但计算 best-level quantity 时，会从 `first_mkt_order_` 开始遍历同档环链并累加每个订单的
`qty_`。如果 best level 有 M 个订单，该侧更新复杂度就是 O(M)。

所以不能说“BBO 更新整体 O(1)”。准确说法是：

- BBO price lookup 为 O(1)；
- BBO aggregate quantity 为 O(M)。

若要让完整 BBO 更新为 O(1)，可以在 `MarketOrdersAtPrice` 增加：

```cpp
Qty total_qty_;
```

并保持以下不变量：

- ADD：`total_qty_ += new_qty`；
- MODIFY：先保存 old qty，再 `total_qty_ += new_qty - old_qty`；
- CANCEL：`total_qty_ -= canceled_qty`；
- 新档：`total_qty_ = first_order.qty_`；
- 删除空档：回收该字段所在节点。

之后 `updateBBO()` 只读 `best_level->total_qty_` 即可。但这属于建议修改，当前源码尚未实现。

---

## 14. MemPool 做了什么，是否严格 O(1)

源码：`common/mem_pool.h`，模板类 `Common::MemPool<T>`。

构造时使用：

```cpp
std::vector<ObjectBlock> store_(num_elems, {T(), true});
```

一次性创建固定数量的 `ObjectBlock`：

```cpp
struct ObjectBlock {
    T object_;
    bool is_free_;
};
```

### 14.1 allocate

`allocate()`：

1. 读取 `store_[next_free_index_]`；
2. 在 `object_` 原有地址上执行 placement new；
3. 把 `is_free_` 设为 false；
4. 调用 `updateNextFreeIndex()` 寻找下一个空块。

这样做避免了每个 ADD 都调用通用 `new`/`malloc`，而且节点地址在池生命周期内稳定。稳定地址
是双链表和直接寻址数组能长期保存裸指针的基础。

### 14.2 deallocate

`deallocate()` 根据对象地址计算它在 `store_` 中的下标，只把 `is_free_` 改回 true，不释放
底层 vector，也不调用对象析构函数。

### 14.3 为什么不能无条件说 allocate 为 O(1)

`updateNextFreeIndex()` 使用 while 循环跳过已占用块，必要时绕回数组首部。池连续增长、下一格
恰好空闲时接近常数成本；但在高占用或碎片化情况下可能扫描多个 block，最坏 O(P)，P 为池
容量。

所以准确表述是：

> 对象池把运行时系统堆分配替换为预分配存储和 placement new，减少抖动并保证指针稳定；
> 当前 free-slot 搜索是线性扫描，分配操作并非数学意义上的严格 O(1)。

如果需要严格常数时间，可以维护空闲索引栈/free list：

```text
allocate: pop free index      O(1)
deallocate: push free index   O(1)
```

单线程订单簿不需要为这条 free list 加锁。

---

## 15. 为什么不用 `std::map`、`std::list` 或只用 `unordered_map`

### 15.1 当前 array + intrusive list

优点：

- ID 直接查询和已知节点删除 O(1)；
- best price 访问 O(1)；
- 同价格尾插 O(1)；
- 无 rehash；
- 订单/价格节点来自预分配池；
- 侵入式节点避免 `std::list` 的独立 node allocation；
- 延迟路径较短且容易控制容量。

缺点：

- Exchange 二维数组内存巨大；
- ID 必须受固定上界约束；
- price modulo 没有冲突解决；
- 新价格档排序查找为 O(L)；
- 裸指针环链容易被错误更新破坏；
- 对象池满时通过 ASSERT 失败，不能弹性增长。

### 15.2 `std::unordered_map`

仓库确实存在 `exchange/matcher/unordered_map_me_order_book.h/.cpp`，并由
`benchmarks/hash_benchmark.cpp` 构造测试，但正常 `MatchingEngine` 使用的是 `MEOrderBook`，
不是这个 unordered-map 版本。

一般而言，`unordered_map` 平均查询 O(1)，但有：

- bucket 数组和 node 分配；
- hash 与冲突探测/链遍历；
- load factor 变化；
- rehash 时的大延迟尖峰；
- node-based 实现的缓存局部性通常不如连续数组；
- 最坏查找 O(N)。

当前仓库的 unordered-map 版本也不是可直接替换的生产实现：

- 没有看到预先 `reserve()` 和固定 `max_load_factor()`；
- `operator[]` 会隐式插入；
- cancel/remove 后把 value 设为 `nullptr`，没有 erase key；
- 价格 key 仍然使用 `price % 256`，并未真正解决价格冲突；
- cancel 用 `client_id < cid_oid_to_order_.size()` 判断 client 是否存在，对稀疏 unordered_map
  并不正确。

因此不能依据 `hash_benchmark` 就宣称 unordered_map 方案功能等价或已经完成公平对比。

### 15.3 `std::map`

`std::map<Price, Level>` 可用红黑树保持价格有序：

- 插入/查找/删除价格档 O(log L)；
- best 可从 begin/rbegin 取得，但需要正确处理两侧排序；
- 不存在 modulo collision；
- 通常有节点分配和多次指针跳转。

它比当前线性价格链更通用、正确性约束更少，但延迟常数和缓存局部性不一定适合本项目的
低延迟教学目标。

### 15.4 更稳妥的可选设计

若要继续优化而又保留固定容量思想，可以考虑：

1. **订单 ID**：保留直接寻址，但缩小 ID 空间；或按 client 动态创建、预留容量的扁平哈希表；
2. **价格查找**：使用 `(side, exact_price)` 的预留开放寻址 hash，必须比较完整 key；
3. **有界 tick 价格**：把合法价格映射为连续 tick index，使用 level array + active bitmap；
4. **价格范围很宽**：使用预分配节点的平衡树/radix tree；
5. **对象池**：使用 O(1) free-list，而不是扫描 `is_free_`。

选型取决于最大订单数、价格 tick 范围、活跃价格档数和内存预算，不能只看 Big-O。

---

## 16. 源码中的关键不变量

调试或写单元测试时，应检查以下不变量。

### 16.1 订单节点

对每个 active order：

```text
order->next_order_->prev_order_ == order
order->prev_order_->next_order_ == order
```

若是该档唯一节点：

```text
order->next_order_ == order
order->prev_order_ == order
```

### 16.2 价格档节点

对每个 active level：

```text
level->next_entry_->prev_entry_ == level
level->prev_entry_->next_entry_ == level
```

每个 level 必须至少有一个 order；空档应立即删除。

### 16.3 排序

从 best 沿 `next_entry_`：

```text
BUY:  price 单调下降
SELL: price 单调上升
```

### 16.4 FIFO

从 `first_order` 沿 `next_order_`：

```text
priority 单调增加
```

### 16.5 多入口一致性

Exchange 的 active order 同时能从以下两条路径到达：

```text
cid_oid_to_order_[client][client_order]

price slot -> price level -> FIFO order ring
```

二者必须指向同一个对象。Trading 则要求 `oid_to_order_[market_order_id]` 与价格档 FIFO 一致。
撤单后，ID 槽位必须为 nullptr，环链中也不能再出现该节点。

### 16.6 推荐增加的测试

- 单节点档增加第二个订单，再依次删除 head/tail；
- 删除中间订单；
- 删除 best level 的最后一个订单；
- 删除非 best level 的最后一个订单；
- 插入新 best、新 middle、新 worst；
- 一个 aggressive order 跨多个订单和多个价格档；
- 两个价格满足 `p1 % 256 == p2 % 256`；
- order ID/client ID 边界与越界；
- Trading CLEAR 后重新 ADD；
- MemPool 回收、绕回和耗尽。

碰撞用例目前应该暴露失败，而不是被写成“通过”；它用于证明现有数据结构的输入约束。

---

## 17. 面试中如何讲“为什么是 O(1)”

可以按下面的顺序回答。

### 问：撤单为什么是 O(1)？

> Exchange 以 `(client_id, client_order_id)` 直接下标访问二维指针数组，先在 O(1) 得到
> `MEOrder *`。订单对象本身带前后指针，所以不需要扫描同价格订单，固定修改相邻节点就能
> 摘链；如果这是价格档最后一单，价格档自身也是双向链表节点，同样固定修改指针。因此，
> 在 ID 和价格槽位合法且无碰撞的前提下，订单簿撤单结构操作是 O(1)。

### 问：插入为什么是 O(1)？

不能直接回答“所有插入都是 O(1)”。应答：

> 同价格档追加是 O(1)，因为循环双链表用 `head->prev` 直接得到尾节点；创建第一档和插入
> 新 best 也可以 O(1)。但插入新的普通价格档时，要从 best 沿有序价格链寻找位置，最坏是
> O(L)。

### 问：撮合为什么快？

> `bids_by_price_`/`asks_by_price_` 直接指向最优价格档，价格档的 `first_me_order_` 直接
> 指向 FIFO 首单，所以选择下一笔 passive order 是 O(1)。每成交一个订单是 O(1)，但一次
> 扫过 K 个订单的 aggressive request 总成本是 O(K)。

### 问：为什么不用 unordered_map？

> 当前 ID 是有界整数，所以直接寻址省去 hash、冲突探测、node allocation 和 rehash，延迟
> 更确定。代价是内存与 ID 上界约束。仓库虽然有 unordered-map 对照实现，但它只用于旧的
> hash benchmark，而且没有 reserve、完整冲突处理和等价的删除逻辑，不能作为生产级对照。

### 问：这个实现有什么不足？

> 最大问题是 Exchange 二维 ID 数组默认可达每 ticker 2 GiB，价格索引只做 `price % 256`
> 而没有冲突处理；新价格档是线性搜索，BBO 数量也需要遍历 best level。对象池避免通用堆
> 分配但空闲槽搜索并非严格 O(1)。这些是后续优化和 benchmark 应覆盖的重点。

---

## 18. 一页复习版

```text
订单查找：
Exchange: [client_id][client_order_id] -> MEOrder*       O(1)
Trading : [market_order_id]           -> MarketOrder*    O(1)

价格查找：
[price % 256] -> OrdersAtPrice*                         O(1)
但当前没有 collision handling，必须说明输入约束/缺陷

价格结构：
BUY  环：最高价 -> 低价                                       
SELL 环：最低价 -> 高价
best price O(1)，已知 level 删除 O(1)，新普通 level 插入 O(L)

同价订单结构：
循环双向 FIFO，first 是最老订单，first->prev 是最新订单
尾插 O(1)，已知订单摘链 O(1)，下一 priority O(1)

撮合：
best opposite level -> first FIFO order                 O(1)
单个 passive match                                     O(1)
扫 K 个 passive orders                                  O(K)

内存：
订单/level 节点由 MemPool 预分配，避免 hot path malloc
但 updateNextFreeIndex 可能扫描，allocate 最坏不是 O(1)

Trading BBO：
best price O(1)，best-level aggregate qty O(M)
```

最终应记住：这里的性能来自多种结构各司其职，而不是某个“万能 O(1) 哈希表”。直接寻址负责
按 ID 找对象；价格档环链负责 best 和顺序；订单环链负责 FIFO 和摘链；对象池负责稳定地址和
减少动态分配。任何简历或面试描述都应同时说明它们的输入边界和非 O(1) 路径。

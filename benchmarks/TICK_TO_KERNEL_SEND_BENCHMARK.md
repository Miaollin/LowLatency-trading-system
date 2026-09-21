# Application Tick-to-Kernel-Send Benchmark

这组 benchmark 测量真实项目线程与 socket 路径中的 application tick-to-trade latency。它不是
wire-to-wire latency，也不包含交易所撮合和订单响应。

## 1. 测量边界

```text
MarketDataConsumer 的 UDP recv() 返回
timestamp A = CLOCK_MONOTONIC_RAW
        |
        v
解析 MDPMarketUpdate、检查 incremental sequence
        |
        v
MDC -> TradeEngine 的 LFQueue 等待
        |
        v
TradeEngine 更新本地订单簿、计算 feature
        |
        v
LiquidityTaker 策略、RiskManager、OrderManager
        |
        v
TradeEngine -> OrderGateway 的 LFQueue 等待
        |
        v
OrderGateway 序列化、写 TCPSocket outbound buffer
        |
        v
完整 pending TCP outbound buffer 被内核接受
timestamp B = CLOCK_MONOTONIC_RAW

latency = B - A
```

因此它包含行情解码和排序检查、两个跨线程队列、策略决策、风险检查、订单管理、序列化以及
TCP send syscall；不包含 UDP 报文到达网卡至 `recv()` 返回之前的时间、真实网络传输、交易所
撮合或订单响应。

起点在 `common/mcast_socket.cpp` 的 `McastSocket::sendAndRecv()` 中，成功 `recv()` 返回后立刻
记录。终点在 `common/tcp_socket.cpp` 的 `TCPSocket::sendAndRecv()` 中，订单网关通过一次或
多次 `send()` 将当时完整 pending outbound buffer 交给内核后记录。原始样本由
`common/latency_recorder.h` 的
`NanosecondLatencyRecorder` 写入 CSV。

使用 `CLOCK_MONOTONIC_RAW` 是因为起点和终点属于不同线程、可能运行在不同 CPU 上，不能用
原项目只检查单次读数 TSC_AUX 的方式判断跨线程迁核。脚本仍会把五个线程绑定到五个不同物理核。

## 2. 工作负载与正确性约束

- client 固定为 `TAKER`，ticker 为 0；
- 先通过真实 UDP multicast 添加 bid 100@99 和 ask 100@101；
- 每轮发送一条 BUY TRADE 100@101，触发策略发送 NEW BUY 10@101；
- 同一时间只允许一条 trigger outstanding，因此 tick 和订单具有确定的一一对应关系；
- TCP sink 解码并验证消息类型、client、ticker、side、price、qty 和 sequence；
- 每轮测量结束后，直接向 TradeEngine response queue 注入 ACCEPTED 和 CANCELED，以重置订单
  状态；这两条响应明确位于测量区间之外；
- warm-up 样本不写 CSV；正式样本预分配后只写内存，CSV 和 percentile 都在测试结束后生成；
- benchmark target 使用较小的 queue/socket 容量，避免教学项目默认 64 MiB socket buffer 影响
  小内存环境；正常 executable 的容量没有改变。

## 3. 编译与单轮验证

```bash
cmake -DCMAKE_BUILD_TYPE=Release -G Ninja -S . -B cmake-build-release
cmake --build cmake-build-release --target tick_to_kernel_send_benchmark -j 4

WARMUP=100 bash scripts/run_tick_to_kernel_send_benchmark.sh \
  0 2 4 6 8 1000 runs/tick-to-kernel-send-validation \
  19111 22111 22110
```

脚本参数依次为：main/generator CPU、MarketDataConsumer CPU、TradeEngine CPU、OrderGateway
CPU、TCP sink CPU、正式样本数、输出目录、order TCP port、incremental UDP port、snapshot UDP
port。`WARMUP` 通过环境变量设置。

当前阿里云实例为 16 个 logical CPU、8 个 physical core，SMT sibling 分别为 0/1、2/3、
4/5、6/7、8/9、10/11、12/13、14/15。因此使用 0、2、4、6、8，而不是 0、1、2、3、4。
脚本会读取 `lscpu`，拒绝重复 CPU 和可识别的 SMT sibling 冲突。

## 4. 正式五轮测试

```bash
cd /root/trading-system

for run in 01 02 03 04 05; do
  WARMUP=100000 bash scripts/run_tick_to_kernel_send_benchmark.sh \
    0 2 4 6 8 1000000 "runs/tks-optimized-run-${run}" \
    19131 22131 22130
done
```

若端口已被其他进程占用，换一组三个端口并保证五轮一致。运行前避免同时启动
`exchange_main`、`trading_main` 或其他 CPU 密集任务。

每轮输出：

```text
runs/tks-optimized-run-01/metadata.txt
runs/tks-optimized-run-01/tick_to_kernel_send.csv
runs/tks-optimized-run-01/tick_to_kernel_send.summary.txt
```

查看五轮统计：

```bash
for run in 01 02 03 04 05; do
  echo "===== run ${run} ====="
  cat "runs/tks-optimized-run-${run}/tick_to_kernel_send.summary.txt"
done
```

统计包含 sample count、mean、median/P50、P90、P95、P99、P99.9、min、max 和 standard
deviation。简历中建议报告五轮 P50/P99/P99.9 的中位数，同时保留各轮原始 CSV 和
`metadata.txt`，不要挑选最好的一轮。

## 5. 优化与边界修正

第一版五轮结果暴露出一个 C++ 日志陷阱：`Logger::log()` 在 benchmark mode 中虽然是空函数，
但调用前仍会求值 `getCurrentTimeStr()` 和 `MDPMarketUpdate::toString()`。后者使用
`std::stringstream`，会进行字符串格式化并可能动态分配。这些工作发生在起点之后，污染了
software path latency。

修正版进行了三项改动：

1. 在 `LLT_BENCHMARK_MODE` 下从调用点编译掉 `McastSocket` 和 `MarketDataConsumer` 正常收包
   路径中的日志表达式，而不只是让 `Logger::log()` 函数体为空；
2. 在行情 LFQueue commit 前设置测量 probe，再在 commit 后单独发布 drain 状态，消除消费者先
   处理、probe 后设置的理论竞态；
3. 仅在完整 pending TCP outbound buffer 被内核接受时记录终点，正确处理 TCP partial write。

后两项是正确性修正：新终点不会比“第一次正数 `send()`”更早，probe 前移还会在 queue commit
前增加少量工作，因此它们不能解释 latency 大幅下降；下降主要来自移除日志参数构造。不过这不是
只改变单一变量的实验，报告时应将它描述为“hot-path logging elimination + measurement boundary
hardening”的组合修改。

## 6. 优化前后五轮结果

环境为阿里云 KVM、8 个物理核/16 vCPU、GCC 11.4.0、Release `-O3 -DNDEBUG`。每轮先执行
100,000 次 warm-up，再记录 1,000,000 个正式样本。单位均为微秒（us）。

优化前 baseline 的日志文件写入已经关闭，但日志参数仍被 eager evaluation；单位均为微秒：

| run | mean | P50 | P90 | P95 | P99 | P99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 01 | 10.351 | 10.238 | 10.624 | 10.811 | 14.721 | 17.767 | 230.318 |
| 02 | 10.369 | 10.266 | 10.643 | 10.822 | 14.700 | 18.292 | 404.405 |
| 03 | 10.285 | 10.170 | 10.549 | 10.735 | 14.672 | 18.165 | 220.792 |
| 04 | 10.290 | 10.185 | 10.559 | 10.739 | 14.660 | 17.491 | 262.041 |
| 05 | 10.305 | 10.196 | 10.590 | 10.785 | 14.584 | 17.489 | 258.374 |
| **五轮中位数** | **10.305** | **10.196** | **10.590** | **10.785** | **14.672** | **17.767** | **258.374** |

五轮合并 5,000,000 个样本后的统计为：

| metric | corrected latency |
| --- | ---: |
| sample count | 5,000,000 |
| mean | 10.320 us |
| P50 | 10.211 us |
| P90 | 10.597 us |
| P95 | 10.784 us |
| P99 | 14.670 us |
| P99.9 | 18.016 us |
| min | 6.760 us |
| max | 404.405 us |
| standard deviation | 1.127 us |

五轮的 `invalid_samples`、`dropped_samples` 和 `protocol_errors` 均为 0。P50 的轮间范围只有
10.170–10.266 us，P99 为 14.584–14.721 us，说明主体分布重复性较好。数百微秒级 max 不应
删除或包装掉；它反映 KVM 宿主机调度、中断或其他系统噪声，因此面试时应同时报告 P99/P99.9
并说明 max 的运行环境限制。

修正后使用相同 CPU、workload、warm-up 和样本数重新测试：

| run | mean | P50 | P90 | P95 | P99 | P99.9 | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 01 | 3.066 | 3.060 | 3.362 | 3.446 | 3.799 | 7.577 | 17.877 |
| 02 | 3.260 | 3.268 | 3.529 | 3.614 | 4.012 | 7.584 | 17.358 |
| 03 | 2.940 | 2.932 | 3.203 | 3.280 | 3.658 | 7.268 | 17.999 |
| 04 | 3.090 | 3.078 | 3.383 | 3.480 | 3.819 | 7.463 | 17.199 |
| 05 | 3.066 | 3.068 | 3.344 | 3.419 | 3.810 | 7.357 | 18.397 |
| **五轮中位数** | **3.066** | **3.068** | **3.362** | **3.446** | **3.810** | **7.463** | **17.877** |

修正版五轮合并 5,000,000 样本：

| metric | corrected latency |
| --- | ---: |
| sample count | 5,000,000 |
| mean | 3.085 us |
| P50 | 3.074 us |
| P90 | 3.403 us |
| P95 | 3.497 us |
| P99 | 3.839 us |
| P99.9 | 7.451 us |
| min | 1.937 us |
| max | 18.397 us |
| standard deviation | 0.368 us |

按五轮中位数比较，mean/P50/P90/P95/P99/P99.9 分别下降
70.24%/69.91%/68.25%/68.05%/74.03%/58.00%。修正版五轮也全部满足
`invalid_samples=0`、`dropped_samples=0`、`protocol_errors=0`。

## 7. 简历口径与限制

合适表述：

> 在 8 物理核/16 vCPU 阿里云 KVM 环境中，使用 CLOCK_MONOTONIC_RAW 对 UDP 行情接收完成至
> OrderGateway TCP send 返回的跨线程 application tick-to-trade 路径进行五轮、共 500 万
> 样本 benchmark；通过在调用点编译期移除 eager 日志参数构造并加固 partial-write/事件关联
> 边界，将五轮中位 P50/P99/P99.9 从 10.196/14.672/17.767 us 降至
> 3.068/3.810/7.463 us，其中 P99 降低 74.03%，并保存 CPU 拓扑、Release 编译选项和原始
> CSV 以保证可复现。

不要把该指标写成 wire-to-wire、硬件时间戳、NIC latency 或生产级 HFT latency。KVM 调度、
steal time、虚拟网络和共享宿主机都会显著影响 tail latency。

## 8. T0–T6 分段诊断

分段诊断使用独立的 `tick_to_kernel_send_stage_benchmark` target，不替代上面的正式 T0→T6
benchmark：

```text
T0  McastSocket 的 UDP recv() 返回
T1  MDC 完成解码/sequence/queue-slot copy，准备 commit
T2  TradeEngine 从行情 LFQueue 取得事件
T3  策略产生订单并准备 commit 到订单 LFQueue
T4  OrderGateway 从订单 LFQueue 取得订单
T5  TCPSocket 第一次 send syscall 之前
T6  完整 pending outbound buffer 被内核接受
```

对应区间：

```text
MDC processing                 = T1 - T0
MDC -> TradeEngine handoff     = T2 - T1
book/feature/strategy/risk/OM  = T3 - T2
TradeEngine -> Gateway handoff = T4 - T3
Gateway staging/loop wait      = T5 - T4
TCP send                       = T6 - T5
```

正式诊断运行五轮，每轮 10,000 warm-up、100,000 样本：

```bash
WARMUP=10000 bash scripts/run_tick_to_kernel_send_stage_benchmark.sh \
  0 2 4 6 8 100000 runs/tks-stage-run-01 \
  19151 22151 22150
```

五轮各自统计值的中位数，单位 ns：

| stage | mean | P50 | P99 | P99.9 |
| --- | ---: | ---: | ---: | ---: |
| MDC processing | 97.236 | 96 | 133 | 167 |
| MDC -> TradeEngine handoff | 223.456 | 207 | 511 | 735.005 |
| strategy path | 134.814 | 127 | 271 | 352 |
| TradeEngine -> Gateway handoff | 399.605 | 374 | 540 | 556 |
| Gateway staging/loop wait | 402.276 | 394 | 521 | 606 |
| TCP send | 2,005.457 | 1,999 | 2,555 | 5,797.009 |
| diagnostic total | 3,360.072 | 3,355 | 4,180.010 | 7,921.007 |

合并 500,000 个诊断样本后，按各阶段 mean 占总 mean 的比例：

| stage | mean share |
| --- | ---: |
| MDC processing | 2.72% |
| MDC -> TradeEngine handoff | 7.46% |
| strategy path | 4.87% |
| TradeEngine -> Gateway handoff | 11.81% |
| Gateway staging/loop wait | 12.44% |
| TCP send | 60.70% |

结论是当前最大阶段为普通 TCP `send()` syscall；项目内最直接可改的部分则是 Gateway loop wait
以及两个跨线程 handoff。handoff 数值包含消费者轮询相位、跨核 cache coherence 和诊断 observer，
不能称为 LFQueue 单次操作成本。

诊断版增加了五个额外 `CLOCK_MONOTONIC_RAW` 边界和 observer/atomic 状态转换：合并样本的 total
P50 为 3.323 us，而无分段 instrumentation 的正式 P50 为 3.074 us，约增加 0.249 us（8.1%）。
因此简历必须继续引用正式结果 3.068/3.810/7.463 us；分段数据只用于瓶颈定位。所有五轮均为
`invalid=0`、`dropped=0`、`protocol_errors=0`、`stage_invalid=0`、`stage_dropped=0`。

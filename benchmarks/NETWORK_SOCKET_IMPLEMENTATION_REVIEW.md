# LowLatency Trading System：Socket、TCP、UDP Multicast 源码详解

> 这是一份面向网络基础初学者的源码复习文档。目标不是讲完整的《计算机网络》课程，而是让你
> 能够顺着当前项目代码解释：订单和行情怎样从一个线程/进程到达另一个线程/进程、每个 socket
> 系统调用在做什么、为什么这样设计，以及当前实现还缺少哪些生产能力。

## 1. 先看项目的真实网络拓扑

当前 `exchange_main` 和 `trading_main` 默认都使用 Linux loopback 接口 `lo`，也就是通信仍经过
操作系统网络栈，但不会真正经过物理网卡和交换机。

### 1.1 订单通道：TCP

配置位置：

- `exchange/exchange_main.cpp`：OrderServer 在 `lo:12345` 监听；
- `trading/trading_main.cpp`：OrderGateway 连接 `127.0.0.1:12345`。

```text
Trading process                                      Exchange process

TradeEngine
    |
    | ClientRequestLFQueue
    v
OrderGateway
    |
    | TCP: 127.0.0.1:临时端口 -> 127.0.0.1:12345
    v
OrderServer
    |
    | ClientRequestLFQueue
    v
MatchingEngine


MatchingEngine
    |
    | ClientResponseLFQueue
    v
OrderServer
    |
    | 同一条 TCP 连接反向发送
    v
OrderGateway
    |
    | ClientResponseLFQueue
    v
TradeEngine
```

TCP 同时承载两个方向：

- client → exchange：NEW/CANCEL；
- exchange → client：ACCEPTED/CANCELED/FILLED/CANCEL_REJECTED。

### 1.2 行情通道：UDP multicast

配置位置：`exchange/exchange_main.cpp` 和 `trading/trading_main.cpp`。

```text
Incremental channel：233.252.14.3:20001
Snapshot channel   ：233.252.14.1:20000
Interface          ：lo
```

```text
MatchingEngine
    |
    | MEMarketUpdateLFQueue
    v
MarketDataPublisher
    |
    | UDP multicast incremental
    | 233.252.14.3:20001
    +----------------------------+----------------------------+
                                 |                            |
                                 v                            v
                      MarketDataConsumer A         MarketDataConsumer B

SnapshotSynthesizer
    |
    | UDP multicast snapshot
    | 233.252.14.1:20000
    +----------------------------+----------------------------+
```

增量行情持续发送；完整 snapshot 每 60 秒构造一次，主要用于客户端发现增量 sequence gap 后恢复。

---

## 2. 初学者必须先理解的网络概念

## 2.1 Socket 是什么

Socket 可以先理解成“进程访问操作系统网络能力的一个句柄”。程序调用：

```cpp
int fd = socket(...);
```

Linux 返回一个整数 file descriptor，例如：

```text
fd = 7
```

它不是网络数据本身，而是一个索引。内核根据 fd 找到对应的 socket 状态，包括：

- 使用 TCP 还是 UDP；
- 本地/远端 IP 和端口；
- 接收与发送队列；
- 当前连接状态；
- socket options；
- 尚未被应用读取的数据。

项目在 `TCPSocket::socket_fd_`、`McastSocket::socket_fd_`、`TCPServer::epoll_fd_` 中保存这些 fd。

## 2.2 IP、端口和连接

IP 用来找到一台主机或一个网络接口，端口用来找到主机上的具体服务。

```text
127.0.0.1:12345
^^^^^^^^^ ^^^^^
   IP      port
```

`127.0.0.1` 表示本机 loopback。`lo` 是 Linux 中对应的接口名。

一条 TCP 连接通常由四元组唯一识别：

```text
源 IP + 源端口 + 目标 IP + 目标端口
```

例如：

```text
127.0.0.1:47120 -> 127.0.0.1:12345
```

12345 是 OrderServer 的固定监听端口；47120 通常是内核给 OrderGateway 选择的临时端口。

## 2.3 TCP 是可靠、有序的字节流

TCP 提供：

- 可靠传输：丢失的数据会重传；
- 有序：应用最终按发送顺序读到字节；
- 双向：一条连接可以同时发送和接收；
- 拥塞控制和流量控制。

但 TCP **没有应用消息边界**。

发送方执行两次：

```text
send("ABC")
send("DEFG")
```

接收方可能看到：

```text
recv -> "ABCDEFG"
```

也可能看到：

```text
recv #1 -> "A"
recv #2 -> "BCDE"
recv #3 -> "FG"
```

所以不能假设“一次 send 对应一次 recv”。项目通过固定消息长度和 inbound buffer 解决 TCP 半包/
粘包问题。

## 2.4 UDP 是一条一条的数据报

UDP 不建立可靠连接，不保证：

- 一定到达；
- 到达顺序；
- 不重复；
- 自动重传。

但 UDP 保留 datagram 边界：一次 `send()` 形成一个 datagram，接收方的一次 `recv()` 读取一个
datagram；如果接收 buffer 太小，剩余部分不会像 TCP 那样留给下一次读取，而会被截断。

UDP 少了 TCP 的确认、重传和 head-of-line blocking，适合低延迟的一对多行情发布，但应用必须
自己检测丢包并恢复。

## 2.5 Unicast 和 Multicast

Unicast 是一对一：

```text
Sender -> Receiver A
```

Multicast 是发布者发到一个组地址，多个订阅者加入同一组后都能收到：

```text
                         -> Receiver A
Publisher -> Group IP   -> Receiver B
                         -> Receiver C
```

项目的 `233.252.14.1` 和 `233.252.14.3` 属于 IPv4 multicast 地址。客户端通过
`IP_ADD_MEMBERSHIP` 加入组。

## 2.6 用户态 buffer 和内核 socket buffer 不是一回事

项目中 `std::vector<char>` 是进程自己的用户态内存：

```text
业务对象
   |
   | memcpy
   v
TCPSocket::outbound_data_       用户态
   |
   | ::send() 系统调用
   v
Kernel socket send buffer       内核态
   |
   v
TCP/IP stack -> loopback/NIC -> network
```

接收方向相反：

```text
network/loopback
   |
   v
Kernel socket receive buffer    内核态
   |
   | recv()/recvmsg() 系统调用
   v
TCPSocket::inbound_data_        用户态
   |
   v
解析成 OMClientRequest/Response
```

源码里的 64 MiB vector 并不是 `SO_SNDBUF` 或 `SO_RCVBUF`。当前项目没有显式设置内核 socket
buffer 大小。

## 2.7 什么是系统调用

普通 C++ 函数运行在用户态，不能直接操作网卡和 TCP 状态。`socket()`、`bind()`、`listen()`、
`accept()`、`connect()`、`send()`、`recv()`、`epoll_wait()` 等调用会进入内核。

系统调用比普通函数更重，因此低延迟程序通常会：

- 减少调用次数；
- 合理 batch；
- 使用非阻塞模式；
- 避免线程睡眠/唤醒；
- 但不能无限 batch，因为第一条消息会等待后面的消息。

---

## 3. 项目的网络消息格式

源码：

- `exchange/order_server/client_request.h`；
- `exchange/order_server/client_response.h`；
- `exchange/market_data/market_update.h`。

这些结构位于 `#pragma pack(push, 1)` 中，编译器不会在字段之间插入 padding。

### 3.1 订单请求

```cpp
struct MEClientRequest {
    ClientRequestType type_;
    ClientId client_id_;
    TickerId ticker_id_;
    OrderId order_id_;
    Side side_;
    Price price_;
    Qty qty_;
};

struct OMClientRequest {
    size_t seq_num_;
    MEClientRequest me_client_request_;
};
```

`MEClientRequest` 是内部业务消息；加上 TCP 应用层 sequence 后形成 wire message
`OMClientRequest`。

### 3.2 订单响应

```cpp
struct OMClientResponse {
    size_t seq_num_;
    MEClientResponse me_client_response_;
};
```

响应包含 client order ID 和 market order ID，使客户端可以把 exchange response 关联回自己的
订单。

### 3.3 行情消息

```cpp
struct MDPMarketUpdate {
    size_t seq_num_;
    MEMarketUpdate me_market_update_;
};
```

`MEMarketUpdate` 的 type 可能是：

- `ADD`；
- `MODIFY`；
- `CANCEL`；
- `TRADE`；
- `CLEAR`；
- `SNAPSHOT_START`；
- `SNAPSHOT_END`。

### 3.4 当前 64 位构建中的理论尺寸

根据当前类型和 pack(1)，在 `size_t=8` 的 64 位构建中：

| 结构 | 字段相加大小 |
|---|---:|
| `MEClientRequest` | 30 bytes |
| `OMClientRequest` | 8 + 30 = 38 bytes |
| `MEClientResponse` | 42 bytes |
| `OMClientResponse` | 8 + 42 = 50 bytes |
| `MEMarketUpdate` | 34 bytes |
| `MDPMarketUpdate` | 8 + 34 = 42 bytes |

这不是一个跨平台协议保证，因为 wire message 使用了 `size_t`，32 位机器上 sequence 字段会变成
4 bytes。代码也没有把整数转换为 network byte order。因此当前协议适合同构机器上的教学运行，
不能直接当作生产跨平台协议。

---

## 4. `SocketCfg` 和 `createSocket()` 做了什么

源码：`common/socket_utils.h`。

`SocketCfg` 描述一次 socket 创建：

```cpp
struct SocketCfg {
    std::string ip_;
    std::string iface_;
    int port_;
    bool is_udp_;
    bool is_listening_;
    bool needs_so_timestamp_;
};
```

## 4.1 根据 interface 找 IP

如果 `ip_` 为空，`getIfaceIP(iface)` 使用：

```text
getifaddrs()
遍历系统网络接口
找到名称等于 iface 的 IPv4 地址
getnameinfo() 转成字符串
```

OrderServer 传入空 IP 和 `lo`，因此监听 loopback 地址。

## 4.2 `getaddrinfo()`

代码使用：

```cpp
getaddrinfo(ip, port, &hints, &result);
```

作用是把字符串 IP/端口转换成内核 socket API 使用的二进制地址结构。项目指定：

- `AF_INET`：IPv4；
- TCP 使用 `SOCK_STREAM/IPPROTO_TCP`；
- UDP 使用 `SOCK_DGRAM/IPPROTO_UDP`；
- `AI_NUMERICHOST/AI_NUMERICSERV`：只接受数字 IP/port，不做 DNS 查询；
- server/listener 增加 `AI_PASSIVE`。

不做 DNS 能避免启动时的名称解析依赖，但它不影响每条消息 hot path，因为解析只在初始化发生。

## 4.3 `socket()`

```cpp
socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol)
```

向内核创建 TCP 或 UDP socket，返回 fd。

## 4.4 设置 non-blocking

`setNonBlocking(fd)` 通过：

```cpp
fcntl(fd, F_GETFL, 0);
fcntl(fd, F_SETFL, flags | O_NONBLOCK);
```

把 socket 改成非阻塞。之后没有数据时 `recv()` 不会一直睡眠，而是立即返回 `-1`，并设置：

```text
errno = EAGAIN 或 EWOULDBLOCK
```

这不是异常，而是告诉程序“现在没有数据，下次再试”。

## 4.5 TCP_NODELAY

所有 TCP socket 都调用：

```cpp
setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, ...)
```

TCP 的 Nagle 算法可能暂时合并小数据，减少包数量。订单消息很小，低延迟系统通常更关心立刻发送，
因此关闭 Nagle。

注意：`TCP_NODELAY` 不意味着每次项目的 `TCPSocket::send()` 都立即发送。项目先把消息复制进用户态
outbound buffer，实际 `::send()` 要等到下一次 `sendAndRecv()`。

## 4.6 Client 的 `connect()`

当 `is_listening_ == false`，`createSocket()` 调用内核：

```cpp
connect(fd, remote_address, address_length);
```

TCP non-blocking connect 可能返回：

```text
-1, errno = EINPROGRESS
```

表示连接正在建立，而不是失败。当前源码把它视为允许结果，但没有再通过 `EPOLLOUT` 和
`getsockopt(SO_ERROR)` 确认连接最终成功，也没有 reconnect state machine。这是后续需要完善的地方。

UDP publisher 也会调用 `connect()`，但 UDP connect 不进行 TCP 三次握手。它只是给 socket 设置默认
目标地址，使后续可以直接调用 `send()` 而不必每次提供 `sockaddr`。

## 4.7 Server 的 `bind()` 和 `listen()`

当 `is_listening_ == true`：

```text
SO_REUSEADDR
    ↓
bind(local IP, port)
    ↓
TCP 才执行 listen(backlog=1024)
```

`bind()` 表示“这个进程占用并接收发往该本地地址/端口的数据”。

`listen()` 只用于 TCP，把 socket 变成 listener。`backlog=1024` 表示内核最多允许一定数量尚未被
应用 accept 的连接排队；它不是最多只能服务 1024 个已连接客户端。

UDP receiver 只需要 bind，不需要 listen/accept。

## 4.8 `SO_TIMESTAMP`

`TCPSocket::connect()` 构造配置时把 `needs_so_timestamp` 固定为 true。内核在接收数据时通过
`recvmsg()` control message 返回 `SCM_TIMESTAMP`。

这里有一个必须根据源码谨慎说明的细节：`OrderServer` 的 listener 在 `createSocket()` 中显式开启了
`SO_TIMESTAMP`，但 `TCPServer::poll()` 对 `accept()` 返回的新连接只显式调用了
`setNonBlocking()` 和 `disableNagle()`，没有再次调用 `setSOTimestamp()`。socket option 是否按目标
平台预期继承不应靠猜测；正式 benchmark 应在每个 accepted fd 上显式开启该选项，并检查每次
`recvmsg()` 是否真的收到了 `SCM_TIMESTAMP`。否则 `kernel_time` 会保持为 0，FIFOSequencer 的排序
依据就不可信。

源码得到的是 `timeval`：

```text
seconds + microseconds
```

再乘法转换成名为 `Nanos` 的整数。数值单位变成 ns，但原始时间戳精度仍是 microsecond，不会因为
乘以 1000 就真正获得 nanosecond 精度。

## 4.9 Multicast join

`Common::join()` 调用：

```cpp
setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, ...)
```

告诉内核“我要接收发往这个 multicast group 的 datagram”。当前 `ip_mreq` 的本地接口地址设置为
`INADDR_ANY`，没有使用传入 `iface` 的明确 IPv4 地址；多网卡机器上可能选错接口，生产实现应使用
`ip_mreqn` 或指定 interface address/index。

---

## 5. `TCPSocket` 的成员分别做什么

源码：`common/tcp_socket.h/.cpp`。

| 成员 | 作用 |
|---|---|
| `socket_fd_` | 内核 socket fd；析构时 close |
| `outbound_data_` | 用户态待发送字节 buffer，默认 64 MiB |
| `next_send_index_` | 已经发送到内核的前缀长度 |
| `next_send_valid_index_` | outbound buffer 中有效数据的结尾 |
| `inbound_data_` | 用户态已接收但可能尚未完整解析的字节 buffer，默认 64 MiB |
| `next_rcv_valid_index_` | inbound buffer 中有效字节数 |
| `socket_attrib_` | 提供给 `recvmsg()` 的地址结构 |
| `recv_callback_` | 收到字节后通知 OrderServer 或 OrderGateway |
| `send_observer_` | 仅 Order RTT benchmark 使用，记录实际 kernel send 前的 TSC |
| `logger_` | 网络日志 |

一个 `TCPSocket` 构造时默认分配：

```text
64 MiB outbound + 64 MiB inbound = 128 MiB 用户态 buffer
```

每接受一个新客户端，OrderServer 都会 `new TCPSocket`，也会增加约 128 MiB buffer。这是项目多
client 时内存快速增长的重要原因。

---

## 6. `TCPSocket::send()` 并不是真正的网络发送

项目方法：

```cpp
TCPSocket::send(const void* data, size_t len)
```

只做：

```cpp
memcpy(outbound_data_.data() + next_send_valid_index_, data, len);
next_send_valid_index_ += len;
```

例如 OrderGateway 发送请求时：

```cpp
tcp_socket_.send(&next_outgoing_seq_num_, sizeof(next_outgoing_seq_num_));
tcp_socket_.send(client_request, sizeof(MEClientRequest));
```

buffer 变成：

```text
+----------------+------------------------------+
| sequence: 8 B  | MEClientRequest: 30 B        |
+----------------+------------------------------+
|<----------- OMClientRequest: 38 B ----------->|
```

这两次调用没有发生两次网络 syscall，只发生两次用户态 memcpy。

如果 buffer 尾部空间不足、前面已有发送完成的数据，代码使用 `memmove()` 把仍未发送的数据压到
buffer 开头，再追加新消息。

真正进入内核的是：

```cpp
::send(socket_fd_, pointer, pending_bytes,
       MSG_DONTWAIT | MSG_NOSIGNAL);
```

它位于 `TCPSocket::sendAndRecv()`。

`MSG_NOSIGNAL` 表示对端关闭导致写失败时不要给进程发送 SIGPIPE，避免整个进程被信号终止。

---

## 7. 为什么 TCP `send()` 可能只发送一部分

TCP 是字节流。假设 outbound 中有 100 bytes，但内核发送缓冲区当前只能接受 40 bytes：

```text
::send(..., 100) -> 返回 40
```

剩下 60 bytes 仍然需要应用以后再发送。

当前 `TCPSocket::sendAndRecv()` 使用：

```text
next_send_index_       已发送到哪里
next_send_valid_index_ 有效数据结束在哪里
```

发送过程：

```text
while next_send_index < next_send_valid_index
    ::send(未发送区间)

    n > 0:
        next_send_index += n
        继续尝试

    EINTR:
        系统调用被信号打断，重试

    EAGAIN/EWOULDBLOCK:
        内核暂时放不下，保留剩余字节，下次循环再发

全部完成：
    send_index = valid_index = 0
```

这是正确处理 non-blocking TCP partial write 的关键。不能在 EAGAIN 时直接清空 outbound buffer，
否则会丢订单字节并破坏后续 frame 边界。

---

## 8. `TCPSocket::sendAndRecv()` 如何接收数据

接收侧不是普通 `recv()`，而是：

```cpp
recvmsg(socket_fd_, &msg, MSG_DONTWAIT);
```

使用 `recvmsg()` 是因为除了数据本身，还要读取 `SCM_TIMESTAMP` control message。

### 8.1 iovec 指向 inbound 尾部

```cpp
iovec iov{
    inbound_data_.data() + next_rcv_valid_index_,
    TCPBufferSize - next_rcv_valid_index_
};
```

新收到的字节追加在尚未解析数据的后面：

```text
inbound buffer
+------------------+-------------------+--------------------+
| 上次残留的半条消息 | 本次 recv 新增字节 | 尚未使用的空间       |
+------------------+-------------------+--------------------+
                   ^
                   next_rcv_valid_index before recv
```

`read_size > 0` 后：

```cpp
next_rcv_valid_index_ += read_size;
recv_callback_(this, kernel_time);
```

具体解析交给 OrderServer 或 OrderGateway。

### 8.2 non-blocking 没数据时

`recvmsg()` 通常返回 `-1`，errno 为 EAGAIN/EWOULDBLOCK。当前函数不做额外处理，直接继续发送路径
并返回 false。busy-loop 很快会再次调用。

### 8.3 当前没有完整处理的结果

- `recvmsg() == 0`：TCP peer 正常关闭；
- `ECONNRESET` 等不可恢复错误；
- inbound buffer 已满；
- control message 不是第一个或有多个 cmsg；
- reconnect/connection cleanup。

生产实现应把这些结果纳入 connection state machine。

---

## 9. TCP 半包和粘包在项目里怎么处理

### 9.1 半包

`OMClientRequest` 理论长度 38 bytes。第一次只收到 20 bytes：

```text
next_rcv_valid_index = 20
20 < sizeof(OMClientRequest)
```

`OrderServer::recvCallback()` 不解析，把 20 bytes 留在 inbound。下次再收到 18 bytes：

```text
20 + 18 = 38
```

这时可以解析一条完整请求。

### 9.2 粘包

一次 `recvmsg()` 收到 76 bytes：

```text
76 / 38 = 2 条完整 OMClientRequest
```

回调使用循环：

```cpp
for (i = 0;
     i + sizeof(OMClientRequest) <= valid_bytes;
     i += sizeof(OMClientRequest)) {
    // 解析一条
}
```

### 9.3 解析后保留残余字节

处理完所有完整 frame 后：

```cpp
memmove(buffer.begin(), buffer.begin() + i, valid_bytes - i);
valid_bytes -= i;
```

OrderServer 和 OrderGateway 对 TCP 使用 `memmove` 是因为 source/destination 可能重叠。

这种固定长度 framing 简单快速，但真实协议通常还会有：

```text
magic | version | message_type | length | sequence | payload | checksum
```

以便校验损坏、支持多消息类型和协议升级。

---

## 10. `TCPServer` 如何监听多个 client

源码：`common/tcp_server.h/.cpp`。

## 10.1 Listener socket

`TCPServer::listen()`：

```text
epoll_create
    ↓
listener_socket.connect(..., is_listening=true)
    ↓
socket + nonblocking + TCP_NODELAY + SO_REUSEADDR
    ↓
bind + listen
    ↓
epoll_ctl(ADD listener)
```

这里的 `TCPSocket::connect()` 名字容易误导：当 `is_listening=true` 时，它内部并不会主动连接远端，
而是创建并绑定 listener。

## 10.2 epoll 是什么

如果 server 有很多 fd，逐个询问“你有数据吗”会浪费工作。epoll 允许把 fd 注册到内核：

```text
epoll_ctl：把感兴趣的 fd 加入列表
epoll_wait：询问哪些 fd 出现事件
```

项目注册：

```cpp
EPOLLET | EPOLLIN
```

- `EPOLLIN`：有数据可读或 listener 有新连接；
- `EPOLLET`：edge-triggered，只在状态从无数据变为有数据时通知边沿。

`epoll_wait(..., timeout=0)` 不阻塞。没有事件也立即返回，OrderServer 继续 busy polling。

## 10.3 `accept()`

listener 出现 EPOLLIN 后：

```text
accept(listener_fd)
    ↓
获得一个代表该客户端连接的新 fd
    ↓
设置 O_NONBLOCK 和 TCP_NODELAY
    ↓
new TCPSocket(logger)
    ↓
把新 fd 放入对象
    ↓
设置 recv_callback
    ↓
加入 epoll 和 receive_sockets_
```

listener fd 只负责接受新连接；每个已连接 client 都有单独 connection fd。

### 10.4 当前 epoll 使用方式的特点

一旦 socket 因 EPOLLIN 被加入 `receive_sockets_`，它会一直留在 vector。每轮
`TCPServer::sendAndRecv()` 都遍历所有 receive sockets，并对每个调用一次 non-blocking recv/send。

因此当前实现并不是“只处理本轮 epoll 返回的 ready sockets”，而是：

```text
epoll 用于发现/登记 socket
+
之后持续 busy poll 所有已登记连接
```

client 少时简单直接；client 多时每轮 O(number_of_connections)，空 socket 也产生 recvmsg syscall。

`addToEpollList()` 只注册 `EPOLLIN`，没有注册 `EPOLLOUT`，所以 `send_sockets_` 正常情况下不会因为
可写事件而被加入；不过 accepted socket 已在 `receive_sockets_`，其 `sendAndRecv()` 同时负责发送，
response 仍能通过该路径发出。

---

## 11. 一条 NEW 订单是怎样通过 TCP 到 Exchange 的

### 11.1 TradeEngine 写内部 queue

`TradeEngine::sendClientRequest()`：

```text
MEClientRequest
    ↓ copy
ClientRequestLFQueue slot
    ↓ updateWriteIndex
OrderGateway 可见
```

这一步没有网络 syscall。

### 11.2 OrderGateway 取出请求

`OrderGateway::run()` 是 busy-loop：

```cpp
while (run_) {
    tcp_socket_.sendAndRecv();

    for (request in outgoing_requests_) {
        tcp_socket_.send(&seq, sizeof(seq));
        tcp_socket_.send(request, sizeof(MEClientRequest));
    }
}
```

注意顺序：它先调用一次 `sendAndRecv()`，再把新 queue 消息复制进 outbound。因此这些刚写入
outbound 的请求通常在下一轮 `sendAndRecv()` 才调用 kernel `::send()`。因为线程不睡眠，这是一轮
busy-loop 的等待，而不是固定毫秒级 timer。

OrderGateway 为每个请求增加：

```cpp
next_outgoing_seq_num_++;
```

### 11.3 内核 TCP/loopback

```text
OrderGateway user buffer
    ↓ ::send
Gateway kernel TCP send buffer
    ↓ TCP/loopback stack
OrderServer kernel TCP receive buffer
    ↓ recvmsg
OrderServer TCPSocket inbound buffer
```

在当前 `lo` 配置中没有经过物理网卡，但仍经过 socket、TCP 和调度路径。

### 11.4 OrderServer 解析和校验

`OrderServer::recvCallback()` 对每条完整 `OMClientRequest`：

1. 从 payload 读取 `client_id_`；
2. 第一次见到该 client 时记录 `client_id -> socket`；
3. 检查以后该 client 是否仍使用同一 socket；
4. 检查 request sequence 是否等于 expected；
5. expected sequence 加一；
6. 把 kernel receive time 和 `MEClientRequest` 放进 FIFOSequencer。

源码对 client ID 越界没有在数组访问前完整校验，恶意/损坏输入可能越界；生产协议必须先做
边界校验再索引。

### 11.5 FIFOSequencer

`FIFOSequencer::addClientRequest()` 把请求暂存在 1024 槽固定数组：

```text
{kernel_receive_time, request}
```

TCPServer 读完本轮所有 socket 后调用 `recvFinishedCallback()`，再由：

```cpp
std::sort(pending.begin(), pending.begin() + pending_size)
```

按 receive timestamp 排序，依次写入 MatchingEngine 的 request LFQueue。

这试图让多个 TCP client 的订单按内核接收时间进入撮合，而不是按 server 遍历 socket 的顺序。

限制是：`SCM_TIMESTAMP` 是 microsecond 精度；同一次 `recvmsg()` 中解析出的多条 request 共用同一个
`rx_time`。相同 timestamp 又没有显式 tie-breaker，`std::sort` 不是 stable sort，所以完全相同时间
下的严格顺序没有定义。

---

## 12. Exchange response 怎样返回客户端

MatchingEngine 把 `MEClientResponse` 写入 `ClientResponseLFQueue`。OrderServer 的 run loop 随后：

1. 根据 `client_response->client_id_` 找到 TCP socket；
2. 取得该 client 的 `next_outgoing_seq_num`；
3. 先把 sequence 复制到 socket outbound；
4. 再把 `MEClientResponse` 复制到 outbound；
5. 消费 response queue；
6. sequence 加一。

和 Gateway 一样，`OrderServer::run()` 在本轮较早位置已经执行过 `tcp_server_.sendAndRecv()`，所以
刚进入 outbound 的 response 通常等下一轮 loop 才执行 kernel send。

客户端 `OrderGateway::recvCallback()`：

```text
检查完整 OMClientResponse
    ↓
验证 response.client_id == 当前 client
    ↓
验证 response.seq == expected_seq
    ↓
expected_seq++
    ↓
MEClientResponse 写入 TradeEngine queue
```

TCP 自己已经保证字节有序，应用 sequence 主要用于发现编码、连接映射或应用逻辑错误；当前发现
错误时只记录并跳过，没有 session recovery/replay。

---

## 13. Kernel receive timestamp 为什么存在

如果 OrderServer 依次遍历 socket A、B：

```text
实际上 B 的订单先到内核
但代码先读 A，再读 B
```

若直接按遍历顺序发布，可能违背“谁先到达 exchange 就先处理”的意图。

项目使用：

```text
SO_TIMESTAMP
    ↓
recvmsg ancillary/control data
    ↓
SCM_TIMESTAMP timeval
    ↓
FIFOSequencer sort
```

这解决的是跨 socket 的近似公平排序，不是网络 latency 计时器。

如果需要更高精度，可评估 `SO_TIMESTAMPNS` 或硬件 timestamping，但真实公平顺序还需要定义：

- timestamp 相同怎样打破平局；
- 一个 TCP segment 中多条订单怎样排序；
- 不同 NIC RX queue 的时钟是否一致；
- timestamp 位于网络栈哪个位置。

---

## 14. `McastSocket` 如何实现行情 UDP multicast

源码：`common/mcast_socket.h/.cpp`。

成员结构与 TCPSocket 相似：

- `socket_fd_`；
- 64 MiB outbound；
- 64 MiB inbound；
- valid index；
- receive callback。

## 14.1 Publisher socket

MarketDataPublisher 调用：

```cpp
incremental_socket_.init(group_ip, iface, port, false);
```

`is_listening=false`，UDP socket 被 `connect()` 到 multicast group。UDP 没有建立可靠连接，connect
只保存默认目标。

SnapshotSynthesizer 对 snapshot group 进行相同初始化。

## 14.2 Subscriber socket

MarketDataConsumer 对 incremental：

```text
创建 UDP socket
设置 O_NONBLOCK
设置 SO_REUSEADDR
bind(INADDR_ANY, 20001)
IP_ADD_MEMBERSHIP(233.252.14.3)
```

snapshot socket 在构造时只设置 callback。只有检测到 incremental gap、进入 recovery 后，
`startSnapshotSync()` 才 init/bind/join snapshot group。

## 14.3 UDP 发送

`McastSocket::send()` 与 TCP wrapper 一样，只先 memcpy 到用户 buffer。

`McastSocket::sendAndRecv()` 中：

```cpp
::send(fd, outbound_data, next_send_valid_index,
       MSG_DONTWAIT | MSG_NOSIGNAL);
next_send_valid_index = 0;
```

UDP 的一次 `::send()` 就是一整个 datagram。因此 outbound 中如果拼了 100 条 update，这 100 条会
尝试放在同一个 UDP datagram 中，而不是自动变成 100 个独立 datagram。

IPv4 UDP payload 有协议大小上限，真实以太网 MTU 通常还远小于这个上限。当前代码没有限制一个
datagram 包含多少 update，也不检查 `send()` 是否失败后重试；无论结果如何都会把 valid index
清零。这是 burst 下的丢行情风险。

## 14.4 UDP 接收

```cpp
recv(fd,
     inbound_data + valid_index,
     remaining_capacity,
     MSG_DONTWAIT);
```

收到后调用 `MarketDataConsumer::recvCallback()`。回调按照固定的
`sizeof(MDPMarketUpdate)` 连续解析 datagram payload。

处理完成后，MDC 使用 `memcpy(destination=buffer start, source=buffer+i, remaining)` 搬移残余数据。
若区域重叠，`memcpy` 的行为未定义，应改为 `memmove`。不过 UDP 本身保留 datagram 边界，更稳妥
的设计是一次验证并解析完整 datagram，而不是把它当作 TCP 字节流累积。

---

## 15. Incremental market data 怎样发布

入口：`MarketDataPublisher::run()`。

```text
MatchingEngine
    ↓ MEMarketUpdateLFQueue
MarketDataPublisher::run
    ↓ 取一条 update
McastSocket::send(&next_inc_seq_num, 8)
    ↓
McastSocket::send(&MEMarketUpdate, 34)
    ↓
复制 MDPMarketUpdate 到 snapshot_md_updates_ queue
    ↓
next_inc_seq_num++
    ↓
继续 drain queue
    ↓
McastSocket::sendAndRecv()
    ↓
一次 UDP send 发出当前整个 outbound buffer
```

每条 update 在逻辑上由：

```text
sequence + MEMarketUpdate
```

组成，与 packed `MDPMarketUpdate` 的内存布局相同。

Publisher 会先 drain 当前 queue，再发送 buffer。这是一种隐式 batching：

- 优点：减少 UDP syscall 数；
- 缺点：batch 第一条会等待 drain，积压过多时 datagram 过大；
- 当前没有最大 batch count/bytes 或最大等待时间。

---

## 16. MarketDataConsumer 怎样检测丢包

MDC 保存：

```cpp
size_t next_exp_inc_seq_num_ = 1;
```

正常情况：

```text
收到 seq=1，expected=1 -> 接受，expected=2
收到 seq=2，expected=2 -> 接受，expected=3
收到 seq=3，expected=3 -> 接受，expected=4
```

丢包：

```text
expected=4
收到 seq=5
```

MDC 发现：

```cpp
request->seq_num_ != next_exp_inc_seq_num_
```

于是设置 `in_recovery_=true`，加入 snapshot multicast，并把后续 incremental/snapshot 消息分别
存入两个 `std::map<sequence, update>`。

这就是 UDP 自身不可靠时由应用层 sequence 检测 gap。

---

## 17. Snapshot recovery 怎样工作

源码：

- `exchange/market_data/snapshot_synthesizer.cpp`；
- `trading/market_data/market_data_consumer.cpp`。

## 17.1 Exchange 构造 snapshot

SnapshotSynthesizer 独立消费每条 incremental update，维护当前所有 live orders。每约 60 秒发布：

```text
SNAPSHOT_START
    order_id 字段临时携带：构建本 snapshot 时最后使用的 incremental seq

Ticker 0: CLEAR
Ticker 0: ADD each live order

Ticker 1: CLEAR
Ticker 1: ADD each live order
...

SNAPSHOT_END
    order_id 同样携带 last incremental seq
```

Snapshot 自己的 datagram record sequence 从 0 开始，用于检查 snapshot 是否完整。

## 17.2 Client 合并 snapshot 与 incremental

MDC 的 `checkSnapshotSync()` 要求：

1. snapshot 第一条是 `SNAPSHOT_START`；
2. snapshot record sequence 从 0 连续；
3. 最后一条是 `SNAPSHOT_END`；
4. 读出 snapshot 对应的 `last_incremental_seq`；
5. 从 `last_incremental_seq + 1` 开始检查缓存 incremental 是否连续；
6. 把 snapshot 内容和后续 incremental 合并；
7. 依次写入 TradeEngine 的 market update queue；
8. 退出 recovery 并关闭 snapshot socket。

直观例子：

```text
客户端缺失 incremental 100
已经收到/缓存 incremental 101、102、103

收到一份 snapshot，说明它包含截至 incremental 101 的状态

那么：
应用完整 snapshot
丢弃已经被 snapshot 覆盖的 incremental <= 101
接着应用 incremental 102、103
下一条 expected = 104
```

这是一种 snapshot + incremental catch-up 模型。

当前 recovery 路径使用 `std::map`、`std::vector`、字符串日志和动态分配。它不是低延迟路径，但属于
异常恢复路径；这种取舍可以接受，前提是 gap 不频繁且恢复正确。

---

## 18. 为什么订单用 TCP，行情用 UDP multicast

| 对比 | 订单 TCP | 行情 UDP multicast |
|---|---|---|
| 通信模式 | client 与 exchange 一对一 | publisher 对多个 subscriber |
| 可靠性 | TCP 自动重传、有序 | 可能丢失、乱序、重复 |
| 消息边界 | 没有，是字节流 | 有 datagram 边界 |
| 丢包影响 | TCP 重传，可能产生 head-of-line delay | 后续消息仍可能到达 |
| 应用恢复 | 当前只校验 sequence | sequence gap + snapshot recovery |
| 适合内容 | NEW/CANCEL/response，不能随便丢 | 全市场实时 feed，一次发给多个 client |

面试时可以说：

> 订单通道选择 TCP，是因为 NEW/CANCEL 和成交响应不能无声丢失，需要可靠有序的双向连接；行情
> 选择 UDP multicast，是为了让 exchange 一次发布、多客户端同时接收，并避免一个慢客户端阻塞
> 所有人。UDP 丢包由应用 sequence 检测，再使用 snapshot 加后续 incremental 恢复。

不要说 UDP 一定比 TCP 快。实际延迟取决于负载、内核、NIC、丢包、重传、batch 和实现。这里的
选择首先是通信语义不同。

---

## 19. 这套 socket 实现如何追求低延迟

| 方法 | 源码位置 | 作用 |
|---|---|---|
| Non-blocking fd | `setNonBlocking()` | recv/send 不等待 |
| Busy polling | 各 `run()` | 避免条件变量或阻塞 I/O 唤醒 |
| TCP_NODELAY | `disableNagle()` | 避免小订单等待 Nagle 合并 |
| Fixed binary struct | request/response/update headers | 避免文本序列化和解析 |
| Packed layout | `#pragma pack(1)` | 减少 padding/wire bytes |
| Preallocated buffers | `TCPSocket`/`McastSocket` constructor | 避免每条消息分配网络 buffer |
| Buffer batching | `send()` 先 append | 减少系统调用，但必须限制 batch |
| SPSC-style LFQueue | I/O thread ↔ engine thread | 网络与业务线程解耦，无 mutex |
| Kernel timestamp | `SO_TIMESTAMP` + `recvmsg()` | 跨 client 近似到达顺序 |
| Sequence numbers | OM/MDP message | 检测应用顺序错误和 UDP gap |
| Snapshot thread | `SnapshotSynthesizer` | 恢复功能不直接进入撮合调用栈 |

但正常模式的大量网络日志仍在 hot path 构造字符串、格式化时间并逐字符入队。异步 Logger 只把
磁盘写移出 I/O 线程，没有消除 producer logging cost。

---

## 20. Order RTT benchmark 中 socket 边界是什么

源码：

- `benchmarks/order_rtt_benchmark.cpp`；
- `trading/order_gw/order_gateway.cpp`；
- `common/tcp_socket.cpp`。

起点不是 `TradeEngine::sendClientRequest()`，而是 Gateway 调用 kernel `::send()` 前读取 TSC：

```text
TSC start
   ↓
OrderGateway kernel send
   ↓ TCP loopback
OrderServer recv/decode
   ↓
FIFOSequencer
   ↓
MatchingEngine + MEOrderBook
   ↓
OrderServer response send
   ↓ TCP loopback
OrderGateway recv/decode
   ↓
验证 order_id 和 expected response type
   ↓
TSC end
```

它包含 Gateway/OrderServer 两侧 socket、两个方向 loopback TCP、sequencer、LFQueue 与 MatchingEngine；
不包含策略产生订单前的时间，也不包含真实物理网卡、交换机或远端网络。

benchmark 保持一个 outstanding request，目的是准确关联请求/response 并测 latency，不是吞吐或高
并发 in-flight benchmark。

---

## 21. 当前 socket 实现的源码问题

以下结论基于当前代码，不代表所有问题在本地 loopback 小负载下都会立即出现。

### 21.1 协议直接发送 C++ 内存布局

问题：

- 使用 `size_t`；
- 没有 byte-order conversion；
- 没有 magic/version/length/checksum；
- pack 结构可能产生未对齐访问；
- 不同编译器/架构之间不保证协议一致。

改进：定义固定宽度 wire types，显式 encode/decode，并做版本和长度校验。

### 21.2 TCP connection state 不完整

当前没有：

- non-blocking connect completion 检查；
- heartbeat/timeout；
- peer close (`recv=0`) 处理；
- fatal socket error 分类；
- reconnect；
- session login/resync；
- client disconnect 后从 vector/epoll/map 清理。

### 21.3 EPOLLET 没有按惯例单次事件 drain 到 EAGAIN

Edge-triggered epoll 通常要求收到事件后循环 recv，直到 EAGAIN。当前一次 `sendAndRecv()` 只调用
一次 `recvmsg()`，但外层又 busy poll 所有 `receive_sockets_`，所以会在后续 loop 继续读取。功能上
小负载可工作，但模型混合且每轮遍历所有连接，不利于大 client 数扩展。

### 21.4 `send_sockets_` 与 epoll interest 不一致

代码处理 `EPOLLOUT`，但 `epoll_ctl` 只注册 `EPOLLIN|EPOLLET`。当前 response 依靠
`receive_sockets_` 中每个 `TCPSocket::sendAndRecv()` 同时执行 send；`send_sockets_` 设计没有真正
闭环。

### 21.5 固定 `events_[1024]` 与 max_events

`TCPServer::poll()` 把：

```cpp
1 + send_sockets_.size() + receive_sockets_.size()
```

传给 `epoll_wait` 作为 `maxevents`，但实际数组固定只有 1024 个。连接数量增长后必须把 maxevents
限制到数组容量或使用动态、预分配且一致的数组。

### 21.6 64 MiB 双向用户 buffer 太大

一个 TCP connection 约 128 MiB。一个只发送的 multicast socket 仍同时分配 inbound/outbound；只
接收的 socket 也一样。它增加：

- RSS；
- 首次触页延迟；
- OOM/swap 风险；
- 多 client 扩展成本。

应该按连接方向和实际最大 burst right-size，并加入高水位与 backpressure。

### 21.7 UDP batch 没有 datagram 上限与失败处理

Publisher drain 整个 queue 后一次 UDP send，可能超过 datagram/MTU 限制。`McastSocket::send()` 先
memcpy 再 ASSERT，也就是说超容量时检查发生得太晚；send 失败后仍清零 buffer。

应先检查长度，设置 `MAX_UPDATES_PER_DATAGRAM`，验证返回值，并记录/drop/retry policy。

### 21.8 Queue 没有 full detection

网络线程向 engine queue 写入时不检查是否已满。突发流量可能覆盖未消费消息。socket backpressure
和应用 queue backpressure 必须联动。

### 21.9 多播接口没有明确绑定到传入 iface

`join()` 使用 `INADDR_ANY` 选择 interface。在多网卡服务器上应显式指定 interface index/address，
并设置 publisher 的 `IP_MULTICAST_IF`。

### 21.10 `getaddrinfo()` 结果没有释放

`createSocket()` 没有调用 `freeaddrinfo(result)`，还会遍历所有 result 而不在成功后 break。当前只在
启动时调用，虽然不属于 per-message hot path，仍应修复资源和多地址处理逻辑。

### 21.11 输入校验不足

网络收到的 client/ticker/order ID 会被用于数组索引；当前很多路径依赖 `.at()`、ASSERT 或直接
下标，没有完整的 malformed input reject。`noexcept` 中 `.at()` 越界会 terminate。

生产 server 必须把网络数据视为不可信输入，先校验完整 frame 和所有字段，再访问内部状态。

### 21.12 时间戳精度与平局

`SO_TIMESTAMP` 是 microsecond timeval；同一个 recv 批次的所有请求共用时间戳。需要显式 tie-breaker，
例如 receive batch sequence、connection sequence 或稳定排序规则。

另外，listener 虽然启用了 `SO_TIMESTAMP`，accepted socket 没有在源码中显式重新启用。正式测试前
应给 accepted fd 调用 `setSOTimestamp(fd)`，并为缺失 `SCM_TIMESTAMP` 增加计数/报错，不能把
`kernel_time == 0` 当作正常到达时间参与排序。

---

## 22. 推荐的修改顺序

### P0：先保证不丢、不乱、不越界

1. 定义固定宽度、带 version/length/type/sequence 的 wire protocol；
2. 所有字段先校验再访问数组；
3. 修复 bounded SPSC queue 的 full/backpressure；
4. 完成 TCP connect/disconnect/error/reconnect state machine；
5. 限制 UDP datagram 大小并处理 send failure；
6. 修复 epoll event array 上限和 socket 移除；
7. 显式设置 multicast interface；
8. MDC 残余复制改 `memmove` 或 datagram parser。

### P1：降低稳定状态延迟和内存

1. 编译期关闭 per-message 文本日志；
2. 按方向缩小 socket user buffers；
3. 对 buffer/queue 预 fault，避免测试期 page fault；
4. OrderServer、OrderGateway、MDC 分配独立物理核；
5. 使用 cache-line separated SPSC head/tail；
6. 用 ring receive buffer 避免频繁 memmove。

### P2：有 profiling 证据后优化 syscall

1. `recvmmsg()/sendmmsg()`；
2. bounded batch count 和最大等待时间；
3. `sendmsg()` + iovec 避免 sequence/payload 拼接；
4. 调整 `SO_RCVBUF/SO_SNDBUF`；
5. 评估 `SO_BUSY_POLL`、RSS/RPS/XPS；
6. 只有真实 NIC 成为瓶颈时考虑 AF_XDP/DPDK。

任何 batch 优化都必须同时测 throughput 和 P99/P99.9，因为 batch 提高吞吐时可能恶化第一条消息
的等待时间。

---

## 23. 面试高频问题

### Q1：项目里的 socket 架构是什么？

> 订单通道使用 non-blocking TCP。客户端 OrderGateway 与 Exchange OrderServer 建立双向连接，
> 通过固定长度 packed request/response 和每 client sequence 传输订单。行情使用 UDP multicast，
> Exchange 发布 incremental 和 snapshot 两个 channel，MarketDataConsumer 用 sequence 检测 gap，
> 再用 snapshot 加缓存 incremental 恢复。

### Q2：为什么订单不用 UDP？

> NEW/CANCEL 和成交响应不能无声丢失，TCP 提供可靠、有序、双向字节流，项目无需自己实现订单
> 重传。但 TCP 丢包时会重传并产生 head-of-line blocking，所以低延迟并不等于所有场景都更快。

### Q3：为什么行情使用 UDP multicast？

> 同一份行情要发送给多个客户端。Multicast 允许 exchange 发布一次，由多个订阅者接收，不需要
> 为每个 client 维护一份 TCP 发送流，也不会因为一个 client 的 TCP 重传阻塞其他 client。代价是
> UDP 会丢包，因此项目增加 sequence 和 snapshot recovery。

### Q4：项目如何处理 TCP 半包和粘包？

> TCP 是字节流，没有消息边界。TCPSocket 把 recvmsg 得到的字节追加到预分配 inbound buffer；
> OrderServer/OrderGateway 只有在 valid bytes 至少达到固定 message size 时才解析，并循环处理多条
> 完整消息，最后把不足一条的残余字节移动到开头等待下次 recv。

### Q5：项目的 `send()` 为什么不一定真的发出？

> `TCPSocket::send()` 是项目封装，只 memcpy 到用户态 outbound buffer。实际系统调用是
> `TCPSocket::sendAndRecv()` 中的 `::send()`。Non-blocking TCP 可能 partial write 或 EAGAIN，代码用
> send index 保留剩余字节，下次继续发送。

### Q6：EAGAIN 是错误吗？

> 对 non-blocking socket，EAGAIN/EWOULDBLOCK 表示现在不能继续读写，不是连接失败。程序应该保留
> 状态，等下次 ready 或下一轮 polling 再试，不能清空尚未发送的数据。

### Q7：为什么关闭 Nagle？

> 订单消息小且关注单笔 latency。TCP_NODELAY 避免 TCP 为了合并小包而等待。但应用自己的 batching
> 仍可能产生等待，关闭 Nagle 不等于调用 wrapper send 后立即上网。

### Q8：epoll 和 busy polling 是否矛盾？

> 不矛盾。epoll 是 ready-fd 发现机制，timeout=0 时不会阻塞；busy polling 是外层线程不断调用它。
> 当前实现发现连接后又每轮遍历全部 receive sockets，所以是 epoll registration 加全连接 busy
> polling 的混合模式，client 多时需要改进。

### Q9：sequence number 有什么用？

> TCP 上用于校验应用层顺序和 session 状态；UDP 上用于发现丢包、乱序或重复。MDC 发现 incremental
> gap 后进入 snapshot recovery，再从 snapshot 所对应的最后 incremental sequence 后继续追赶。

### Q10：这个网络实现是不是 zero-copy？

> 不是。消息会从业务对象复制到 LFQueue slot，再复制到 socket outbound vector，然后由 send 复制/
> 提交给内核。它通过预分配避免动态分配，但没有实现 zero-copy 或 kernel bypass。

### Q11：为什么 Order RTT 不是实际互联网/交易所 RTT？

> 当前 benchmark 使用 127.0.0.1/lo，包含本机 TCP 网络栈、OrderGateway、OrderServer、
> FIFOSequencer、queue 和 MatchingEngine，但不经过物理 NIC、交换机、跨机链路和真实交易所。因此
> 应称 TCP loopback application RTT。

### Q12：你会优先优化什么？

> 先处理协议边界、queue full、TCP 断线、UDP datagram 上限和输入校验，保证不丢不乱；然后关闭
> hot-path 文本日志、缩小 64 MiB 双向 buffer、绑核并优化 SPSC cache layout；最后根据 perf 结果
> 决定是否做 recvmmsg/sendmmsg、socket tuning 或 kernel bypass。

---

## 24. 一页复习版

```text
Socket：
进程访问内核网络栈的 fd

TCP：
可靠、有序、双向字节流
没有消息边界，需要处理半包/粘包
订单 NEW/CANCEL/response 使用 TCP

UDP multicast：
不保证到达/顺序/不重复，保留 datagram 边界
一次发布，多个 subscriber 接收
行情使用 incremental + snapshot 两条 channel

Project TCP path：
TradeEngine queue
 -> OrderGateway user buffer
 -> kernel send
 -> OrderServer recvmsg
 -> FIFOSequencer
 -> MatchingEngine
 -> response queue
 -> OrderServer send
 -> OrderGateway recv
 -> TradeEngine queue

Project market-data path：
MatchingEngine queue
 -> MarketDataPublisher
 -> UDP multicast incremental
 -> MarketDataConsumer
 -> sequence check
 -> TradeEngine queue

Loss recovery：
incremental gap
 -> join snapshot group
 -> validate complete snapshot
 -> apply snapshot
 -> append cached incrementals after snapshot last seq

Low-latency methods：
O_NONBLOCK / MSG_DONTWAIT
TCP_NODELAY
timeout=0 busy polling
fixed packed binary messages
preallocated user buffers
SPSC-style LFQueue
kernel receive timestamp

Important distinction：
TCPSocket::send() = user-space memcpy
::send()            = kernel syscall

Current limits：
loopback, not real NIC
not zero-copy / not kernel bypass
ABI-dependent raw structs
no complete reconnect/error state machine
UDP batch can exceed datagram size
64 MiB inbound + 64 MiB outbound per socket
queue has no full handling
normal hot-path logging is heavy
```

面试时最重要的不是背系统调用名称，而是能够解释两条选择：订单为什么需要 TCP 的可靠有序语义，
行情为什么选择 UDP multicast 并通过 sequence + snapshot 弥补不可靠性；再沿源码说明数据怎样从
业务对象经过 queue、用户态 buffer、内核 socket buffer 到达对端。

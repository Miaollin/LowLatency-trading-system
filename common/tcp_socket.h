#pragma once

#include <functional>

#include "socket_utils.h"
#include "logging.h"
#if defined(LLT_ORDER_RTT_BENCHMARK)
#include "tsc_clock.h"
#endif

namespace Common {
  /// Size of our send and receive buffers in bytes.
#ifndef LLT_TCP_BUFFER_SIZE
#define LLT_TCP_BUFFER_SIZE (64 * 1024 * 1024)
#endif
  constexpr size_t TCPBufferSize = LLT_TCP_BUFFER_SIZE;

  struct TCPSocket {
    explicit TCPSocket(Logger &logger)
        : logger_(logger) {
      outbound_data_.resize(TCPBufferSize);
      inbound_data_.resize(TCPBufferSize);
    }

    ~TCPSocket();

    /// Create TCPSocket with provided attributes to either listen-on / connect-to.
    auto connect(const std::string &ip, const std::string &iface, int port, bool is_listening) -> int;

    /// Called to publish outgoing data from the buffers as well as check for and callback if data is available in the read buffers.
    auto sendAndRecv() noexcept -> bool;

    /// Write outgoing data to the send buffers.
    auto send(const void *data, size_t len) noexcept -> void;

    auto closeSocket() noexcept -> void;

    /// Deleted default, copy & move constructors and assignment-operators.
    TCPSocket() = delete;

    TCPSocket(const TCPSocket &) = delete;

    TCPSocket(const TCPSocket &&) = delete;

    TCPSocket &operator=(const TCPSocket &) = delete;

    TCPSocket &operator=(const TCPSocket &&) = delete;

    /// File descriptor for the socket.
    int socket_fd_ = -1;
    bool closed_ = false;

    /// Send and receive buffers and trackers for read/write indices.
    std::vector<char> outbound_data_;
    size_t next_send_index_ = 0;
    size_t next_send_valid_index_ = 0;
    std::vector<char> inbound_data_;
    size_t next_rcv_valid_index_ = 0;

    /// Socket attributes.
    struct sockaddr_in socket_attrib_{};

    /// Function wrapper to callback when there is data to be processed.
    std::function<void(TCPSocket *s, Nanos rx_time)> recv_callback_ = nullptr;
#if defined(LLT_ORDER_RTT_BENCHMARK)
    /// Benchmark-only observer. The stamp is taken immediately before the
    /// successful kernel send whose byte count is provided to the callback.
    std::function<void(const TSCStamp &, size_t)> send_observer_ = nullptr;
#endif
#if defined(LLT_TICK_TO_KERNEL_SEND_BENCHMARK)
    /// Benchmark-only observer invoked after the complete pending outbound
    /// buffer has been accepted by the kernel, including partial writes.
    std::function<void(std::uint64_t, size_t)> send_complete_observer_ = nullptr;
#endif
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
    /// Diagnostic-only timestamp immediately before the first send syscall for
    /// the currently pending outbound buffer.
    std::function<void(std::uint64_t)> send_start_observer_ = nullptr;
#endif

    std::string time_str_;
    Logger &logger_;
  };
}

#include "tcp_socket.h"

#include <cstring>

namespace Common {
  TCPSocket::~TCPSocket() {
    closeSocket();
  }

  /// Create TCPSocket with provided attributes to either listen-on / connect-to.
  auto TCPSocket::connect(const std::string &ip, const std::string &iface, int port, bool is_listening) -> int {
    // Note that needs_so_timestamp=true for FIFOSequencer.
    const SocketCfg socket_cfg{ip, iface, port, false, is_listening, true};
    socket_fd_ = createSocket(logger_, socket_cfg);
    closed_ = (socket_fd_ < 0);

    socket_attrib_.sin_addr.s_addr = INADDR_ANY;
    socket_attrib_.sin_port = htons(port);
    socket_attrib_.sin_family = AF_INET;

    return socket_fd_;
  }

  auto TCPSocket::sendAndRecv() noexcept -> bool {
    if (closed_ || socket_fd_ < 0) {
      return false;
    }

    bool received = false;
    while (!closed_) {
      if (next_rcv_valid_index_ == TCPBufferSize) {
#if !defined(LLT_BENCHMARK_MODE)
        logger_.log("%:% %() % TCP inbound buffer full socket:%\n", __FILE__, __LINE__, __FUNCTION__,
                    Common::getCurrentTimeStr(&time_str_), socket_fd_);
#endif
        break;
      }

      char ctrl[CMSG_SPACE(sizeof(struct timeval))]{};

      iovec iov{inbound_data_.data() + next_rcv_valid_index_, TCPBufferSize - next_rcv_valid_index_};
      msghdr msg{&socket_attrib_, sizeof(socket_attrib_), &iov, 1, ctrl, sizeof(ctrl), 0};

      // Drain non-blocking reads until the kernel reports no more data.
      const auto read_size = recvmsg(socket_fd_, &msg, MSG_DONTWAIT);

      if (read_size > 0) {
        received = true;
        next_rcv_valid_index_ += read_size;

        Nanos kernel_time = 0;
        timeval time_kernel;
        const auto cmsg = CMSG_FIRSTHDR(&msg);
        if (cmsg != nullptr && cmsg->cmsg_level == SOL_SOCKET &&
            cmsg->cmsg_type == SCM_TIMESTAMP &&
            cmsg->cmsg_len == CMSG_LEN(sizeof(time_kernel))) {
          memcpy(&time_kernel, CMSG_DATA(cmsg), sizeof(time_kernel));
          kernel_time = time_kernel.tv_sec * NANOS_TO_SECS + time_kernel.tv_usec * NANOS_TO_MICROS; // convert timestamp to nanoseconds.
        }

#if !defined(LLT_BENCHMARK_MODE)
        const auto user_time = getCurrentNanos();
        logger_.log("%:% %() % read socket:% len:% utime:% ktime:% diff:%\n", __FILE__, __LINE__, __FUNCTION__,
                    Common::getCurrentTimeStr(&time_str_), socket_fd_, next_rcv_valid_index_, user_time, kernel_time, (user_time - kernel_time));
#endif
        if (recv_callback_ != nullptr) {
          recv_callback_(this, kernel_time);
        }
        continue;
      }

      if (read_size == 0) {
#if !defined(LLT_BENCHMARK_MODE)
        logger_.log("%:% %() % peer closed socket:%\n", __FILE__, __LINE__, __FUNCTION__,
                    Common::getCurrentTimeStr(&time_str_), socket_fd_);
#endif
        closeSocket();
        break;
      }

      if (errno == EINTR) {
        continue;
      }

      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        break;
      }

#if !defined(LLT_BENCHMARK_MODE)
      logger_.log("%:% %() % recvmsg failed socket:% error:%\n", __FILE__, __LINE__, __FUNCTION__,
                  Common::getCurrentTimeStr(&time_str_), socket_fd_, std::strerror(errno));
#endif
      closeSocket();
      break;
    }

    while (!closed_ && next_send_index_ < next_send_valid_index_) {
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
      if (next_send_index_ == 0 && send_start_observer_ != nullptr) {
        send_start_observer_(monotonicRawNanos());
      }
#endif
#if defined(LLT_ORDER_RTT_BENCHMARK)
      TSCStamp send_start{};
      if (send_observer_ != nullptr) {
        send_start = readTSC();
      }
#endif
      const auto n = ::send(socket_fd_, outbound_data_.data() + next_send_index_,
                            next_send_valid_index_ - next_send_index_,
                            MSG_DONTWAIT | MSG_NOSIGNAL);
      if (n > 0) {
#if defined(LLT_TICK_TO_KERNEL_SEND_BENCHMARK)
        const auto send_complete_ns = monotonicRawNanos();
#endif
#if defined(LLT_ORDER_RTT_BENCHMARK)
        if (send_observer_ != nullptr) {
          send_observer_(send_start, static_cast<size_t>(n));
        }
#endif
        next_send_index_ += static_cast<size_t>(n);
#if defined(LLT_TICK_TO_KERNEL_SEND_BENCHMARK)
        if (send_complete_observer_ != nullptr &&
            next_send_index_ == next_send_valid_index_) {
          send_complete_observer_(send_complete_ns, static_cast<size_t>(n));
        }
#endif
#if !defined(LLT_BENCHMARK_MODE)
        logger_.log("%:% %() % send socket:% len:%\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_), socket_fd_, n);
#endif
        continue;
      }
      if (n < 0 && errno == EINTR) {
        continue;
      }
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        break;
      }
      closeSocket();
      break;
    }
    if (next_send_index_ == next_send_valid_index_) {
      next_send_index_ = 0;
      next_send_valid_index_ = 0;
    }

    return received;
  }

  /// Write outgoing data to the send buffers.
  auto TCPSocket::send(const void *data, size_t len) noexcept -> void {
    if (closed_) {
      return;
    }
    if (next_send_valid_index_ + len > outbound_data_.size() && next_send_index_ > 0) {
      const auto pending = next_send_valid_index_ - next_send_index_;
      memmove(outbound_data_.data(), outbound_data_.data() + next_send_index_, pending);
      next_send_index_ = 0;
      next_send_valid_index_ = pending;
    }
    ASSERT(next_send_valid_index_ + len <= outbound_data_.size(),
           "TCPSocket outbound buffer capacity exceeded.");
    memcpy(outbound_data_.data() + next_send_valid_index_, data, len);
    next_send_valid_index_ += len;
  }

  auto TCPSocket::closeSocket() noexcept -> void {
    if (socket_fd_ >= 0) {
      close(socket_fd_);
      socket_fd_ = -1;
    }
    closed_ = true;
  }
}

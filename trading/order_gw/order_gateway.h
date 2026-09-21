#pragma once

#include <functional>

#include "common/thread_utils.h"
#include "common/macros.h"
#include "common/tcp_server.h"
#if defined(LLT_ORDER_RTT_BENCHMARK)
#include "common/latency_recorder.h"
#endif

#include "exchange/order_server/client_request.h"
#include "exchange/order_server/client_response.h"

namespace Trading {
  class OrderGateway {
  public:
    OrderGateway(ClientId client_id,
                 Exchange::ClientRequestLFQueue *client_requests,
                 Exchange::ClientResponseLFQueue *client_responses,
                 std::string ip, const std::string &iface, int port,
                 int core_id = -1);

    ~OrderGateway() {
      stop();
    }

    /// Start and stop the order gateway main thread.
    auto start() {
      run_.store(true, std::memory_order_release);
      ASSERT(tcp_socket_.connect(ip_, iface_, port_, false) >= 0,
             "Unable to connect to ip:" + ip_ + " port:" + std::to_string(port_) + " on iface:" + iface_ + " error:" + std::string(std::strerror(errno)));
      thread_ = Common::createAndStartThread(core_id_, "Trading/OrderGateway", [this]() { run(); });
      ASSERT(thread_ != nullptr, "Failed to start OrderGateway thread.");
    }

    auto stop() -> void {
      run_.store(false, std::memory_order_release);
      if (thread_ != nullptr) {
        if (thread_->joinable()) {
          thread_->join();
        }
        delete thread_;
        thread_ = nullptr;
      }
    }

#if defined(LLT_ORDER_RTT_BENCHMARK)
    /// Arm exactly one correlated request/response RTT sample. The benchmark
    /// keeps one request outstanding, but still validates both order id and
    /// response type before recording the end timestamp.
    auto armRTT(OrderId order_id, Exchange::ClientResponseType expected_type,
                Common::LatencyRecorder *recorder) noexcept -> bool;
    [[nodiscard]] auto rttCompleted() const noexcept -> bool {
      return rtt_completed_.load(std::memory_order_acquire);
    }
    [[nodiscard]] auto rttProtocolErrors() const noexcept -> std::uint64_t {
      return rtt_protocol_errors_.load(std::memory_order_relaxed);
    }
#endif

#if defined(LLT_TICK_TO_KERNEL_SEND_BENCHMARK)
    /// Observe successful TCP send completion without changing the production
    /// build or the normal hot path.
    auto setSendCompleteObserver(
        std::function<void(std::uint64_t, size_t)> observer) -> void {
      tcp_socket_.send_complete_observer_ = std::move(observer);
    }
#endif

#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
    auto setOrderDequeueObserver(std::function<void(std::uint64_t)> observer) -> void {
      order_dequeue_observer_ = std::move(observer);
    }

    auto setSendStartObserver(std::function<void(std::uint64_t)> observer) -> void {
      tcp_socket_.send_start_observer_ = std::move(observer);
    }
#endif

    /// Deleted default, copy & move constructors and assignment-operators.
    OrderGateway() = delete;

    OrderGateway(const OrderGateway &) = delete;

    OrderGateway(const OrderGateway &&) = delete;

    OrderGateway &operator=(const OrderGateway &) = delete;

    OrderGateway &operator=(const OrderGateway &&) = delete;

  private:
    const ClientId client_id_;

    /// Exchange's order server's TCP server address.
    std::string ip_;
    const std::string iface_;
    const int port_ = 0;
    const int core_id_ = -1;

    /// Lock free queue on which we consume client requests from the trade engine and forward them to the exchange's order server.
    Exchange::ClientRequestLFQueue *outgoing_requests_ = nullptr;

    /// Lock free queue on which we write client responses which we read and processed from the exchange, to be consumed by the trade engine.
    Exchange::ClientResponseLFQueue *incoming_responses_ = nullptr;

    std::atomic<bool> run_{false};
    std::thread *thread_ = nullptr;

    std::string time_str_;
    Logger logger_;

    /// Sequence numbers to track the sequence number to set on outgoing client requests and expected on incoming client responses.
    size_t next_outgoing_seq_num_ = 1;
    size_t next_exp_seq_num_ = 1;

    /// TCP connection to the exchange's order server.
    Common::TCPSocket tcp_socket_;

#if defined(LLT_ORDER_RTT_BENCHMARK)
    OrderId rtt_order_id_ = OrderId_INVALID;
    Exchange::ClientResponseType rtt_expected_type_ = Exchange::ClientResponseType::INVALID;
    Common::LatencyRecorder *rtt_recorder_ = nullptr;
    Common::TSCStamp rtt_start_{};
    std::atomic<bool> rtt_armed_{false};
    bool rtt_started_ = false; // only read/written by the gateway thread
    std::atomic<bool> rtt_completed_{false};
    std::atomic<std::uint64_t> rtt_protocol_errors_{0};
#endif

#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
    std::function<void(std::uint64_t)> order_dequeue_observer_ = nullptr;
#endif

  private:
    /// Main thread loop - sends out client requests to the exchange and reads and dispatches incoming client responses.
    auto run() noexcept -> void;

    /// Callback when an incoming client response is read, we perform some checks and forward it to the lock free queue connected to the trade engine.
    auto recvCallback(TCPSocket *socket, Nanos rx_time) noexcept -> void;

#if defined(LLT_ORDER_RTT_BENCHMARK)
    auto observeKernelSend(const Common::TSCStamp &start, size_t bytes_sent) noexcept -> void;
#endif
  };
}

#include "order_gateway.h"

namespace Trading {
  OrderGateway::OrderGateway(ClientId client_id,
                             Exchange::ClientRequestLFQueue *client_requests,
                             Exchange::ClientResponseLFQueue *client_responses,
                             std::string ip, const std::string &iface, int port,
                             int core_id)
      : client_id_(client_id), ip_(ip), iface_(iface), port_(port), core_id_(core_id), outgoing_requests_(client_requests), incoming_responses_(client_responses),
      logger_("trading_order_gateway_" + std::to_string(client_id) + ".log"), tcp_socket_(logger_) {
    tcp_socket_.recv_callback_ = [this](auto socket, auto rx_time) { recvCallback(socket, rx_time); };
#if defined(LLT_ORDER_RTT_BENCHMARK)
    tcp_socket_.send_observer_ = [this](const auto &start, const auto bytes_sent) {
      observeKernelSend(start, bytes_sent);
    };
#endif
  }

#if defined(LLT_ORDER_RTT_BENCHMARK)
  auto OrderGateway::armRTT(const OrderId order_id,
                            const Exchange::ClientResponseType expected_type,
                            Common::LatencyRecorder *recorder) noexcept -> bool {
    if (recorder == nullptr || rtt_armed_.load(std::memory_order_acquire)) {
      return false;
    }
    rtt_order_id_ = order_id;
    rtt_expected_type_ = expected_type;
    rtt_recorder_ = recorder;
    rtt_started_ = false;
    rtt_completed_.store(false, std::memory_order_relaxed);
    rtt_armed_.store(true, std::memory_order_release);
    return true;
  }

  auto OrderGateway::observeKernelSend(const Common::TSCStamp &start,
                                       const size_t bytes_sent) noexcept -> void {
    (void) bytes_sent;
    if (rtt_armed_.load(std::memory_order_acquire) && !rtt_started_) {
      rtt_start_ = start;
      rtt_started_ = true;
    }
  }
#endif

  /// Main thread loop - sends out client requests to the exchange and reads and dispatches incoming client responses.
  auto OrderGateway::run() noexcept -> void {
#if !defined(LLT_BENCHMARK_MODE)
    logger_.log("%:% %() %\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_));
#endif
    while (run_.load(std::memory_order_relaxed)) {
      tcp_socket_.sendAndRecv();

      for(auto client_request = outgoing_requests_->getNextToRead(); client_request; client_request = outgoing_requests_->getNextToRead()) {
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
        if (order_dequeue_observer_ != nullptr) {
          order_dequeue_observer_(Common::monotonicRawNanos());
        }
#endif
        TTT_MEASURE(T11_OrderGateway_LFQueue_read, logger_);

#if !defined(LLT_BENCHMARK_MODE)
        logger_.log("%:% %() % Sending cid:% seq:% %\n", __FILE__, __LINE__, __FUNCTION__,
                    Common::getCurrentTimeStr(&time_str_), client_id_, next_outgoing_seq_num_, client_request->toString());
#endif
        START_MEASURE(Trading_TCPSocket_send);
        tcp_socket_.send(&next_outgoing_seq_num_, sizeof(next_outgoing_seq_num_));
        tcp_socket_.send(client_request, sizeof(Exchange::MEClientRequest));
        END_MEASURE(Trading_TCPSocket_send, logger_);
        outgoing_requests_->updateReadIndex();
        TTT_MEASURE(T12_OrderGateway_TCP_write, logger_);

        next_outgoing_seq_num_++;
      }
    }
  }

  /// Callback when an incoming client response is read, we perform some checks and forward it to the lock free queue connected to the trade engine.
  auto OrderGateway::recvCallback(TCPSocket *socket, Nanos rx_time) noexcept -> void {
    TTT_MEASURE(T7t_OrderGateway_TCP_read, logger_);

    START_MEASURE(Trading_OrderGateway_recvCallback);
#if !defined(LLT_BENCHMARK_MODE)
    logger_.log("%:% %() % Received socket:% len:% %\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_), socket->socket_fd_, socket->next_rcv_valid_index_, rx_time);
#else
    (void) rx_time;
#endif

    if (socket->next_rcv_valid_index_ >= sizeof(Exchange::OMClientResponse)) {
      size_t i = 0;
      for (; i + sizeof(Exchange::OMClientResponse) <= socket->next_rcv_valid_index_; i += sizeof(Exchange::OMClientResponse)) {
        auto response = reinterpret_cast<const Exchange::OMClientResponse *>(socket->inbound_data_.data() + i);
#if !defined(LLT_BENCHMARK_MODE)
        logger_.log("%:% %() % Received %\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_), response->toString());
#endif

        if(response->me_client_response_.client_id_ != client_id_) { // this should never happen unless there is a bug at the exchange.
          logger_.log("%:% %() % ERROR Incorrect client id. ClientId expected:% received:%.\n", __FILE__, __LINE__, __FUNCTION__,
                      Common::getCurrentTimeStr(&time_str_), client_id_, response->me_client_response_.client_id_);
          continue;
        }
        if(response->seq_num_ != next_exp_seq_num_) { // this should never happen since we use a reliable TCP protocol, unless there is a bug at the exchange.
          logger_.log("%:% %() % ERROR Incorrect sequence number. ClientId:%. SeqNum expected:% received:%.\n", __FILE__, __LINE__, __FUNCTION__,
                      Common::getCurrentTimeStr(&time_str_), client_id_, next_exp_seq_num_, response->seq_num_);
          continue;
        }

        ++next_exp_seq_num_;

#if defined(LLT_ORDER_RTT_BENCHMARK)
        if (rtt_armed_.load(std::memory_order_acquire) &&
            response->me_client_response_.client_order_id_ == rtt_order_id_) {
          if (rtt_started_ &&
              response->me_client_response_.type_ == rtt_expected_type_) {
            const auto end = Common::readTSC();
            rtt_recorder_->record(rtt_start_, end);
            rtt_completed_.store(true, std::memory_order_release);
            rtt_armed_.store(false, std::memory_order_release);
          } else {
            rtt_protocol_errors_.fetch_add(1, std::memory_order_relaxed);
          }
        }
#endif

        auto next_write = incoming_responses_->getNextToWriteTo();
        *next_write = std::move(response->me_client_response_);
        incoming_responses_->updateWriteIndex();
        TTT_MEASURE(T8t_OrderGateway_LFQueue_write, logger_);
      }
      memmove(socket->inbound_data_.data(), socket->inbound_data_.data() + i,
              socket->next_rcv_valid_index_ - i);
      socket->next_rcv_valid_index_ -= i;
    }
    END_MEASURE(Trading_OrderGateway_recvCallback, logger_);
  }
}

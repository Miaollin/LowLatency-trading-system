#include "order_server.h"

namespace Exchange {
  OrderServer::OrderServer(ClientRequestLFQueue *client_requests, ClientResponseLFQueue *client_responses,
                           const std::string &iface, int port, int core_id)
      : iface_(iface), port_(port), core_id_(core_id), outgoing_responses_(client_responses), logger_("exchange_order_server.log"),
        tcp_server_(logger_), fifo_sequencer_(client_requests, &logger_) {
    cid_next_outgoing_seq_num_.fill(1);
    cid_next_exp_seq_num_.fill(1);
    cid_tcp_socket_.fill(nullptr);

    tcp_server_.recv_callback_ = [this](auto socket, auto rx_time) { recvCallback(socket, rx_time); };
    tcp_server_.recv_finished_callback_ = [this]() { recvFinishedCallback(); };
  }

  OrderServer::~OrderServer() {
    stop();
  }

  /// Start and stop the order server main thread.
  auto OrderServer::start() -> void {
    run_.store(true, std::memory_order_release);
    tcp_server_.listen(iface_, port_);

    thread_ = Common::createAndStartThread(core_id_, "Exchange/OrderServer", [this]() { run(); });
    ASSERT(thread_ != nullptr, "Failed to start OrderServer thread.");
  }

  auto OrderServer::stop() -> void {
    run_.store(false, std::memory_order_release);
    if (thread_ != nullptr) {
      if (thread_->joinable()) {
        thread_->join();
      }
      delete thread_;
      thread_ = nullptr;
    }
  }
}

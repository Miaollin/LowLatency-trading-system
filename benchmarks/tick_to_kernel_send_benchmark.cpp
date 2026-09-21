#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/latency_recorder.h"
#include "common/mcast_socket.h"
#include "common/tcp_server.h"
#include "common/thread_utils.h"
#include "exchange/order_server/client_request.h"
#include "exchange/order_server/client_response.h"
#include "trading/market_data/market_data_consumer.h"
#include "trading/order_gw/order_gateway.h"
#include "trading/strategy/trade_engine.h"

namespace {

  constexpr Common::ClientId CLIENT_ID = 0;
  constexpr Common::TickerId TICKER_ID = 0;
  constexpr Common::OrderId BID_ORDER_ID = 1;
  constexpr Common::OrderId ASK_ORDER_ID = 2;
  constexpr Common::Price BID_PRICE = 99;
  constexpr Common::Price ASK_PRICE = 101;
  constexpr Common::Qty BOOK_QTY = 100;
  constexpr Common::Qty CLIP_QTY = 10;
  constexpr std::size_t QUEUE_CAPACITY = 4096;
  constexpr std::uint64_t TIMEOUT_NS = 5'000'000'000ULL;

  struct Options {
    std::size_t warmup = 10'000;
    std::size_t samples = 100'000;
    std::filesystem::path output_dir = "runs/tick-to-kernel-send";
    std::string iface = "lo";
    std::string order_ip = "127.0.0.1";
    std::string incremental_ip = "233.252.14.33";
    std::string snapshot_ip = "233.252.14.31";
    int order_port = 19101;
    int incremental_port = 22101;
    int snapshot_port = 22100;
    int mdc_cpu = -1;
    int trade_engine_cpu = -1;
    int gateway_cpu = -1;
    int sink_cpu = -1;
  };

  [[nodiscard]] bool parseSize(const std::string_view text, std::size_t& value) {
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
  }

  [[nodiscard]] bool parseInt(const std::string_view text, int& value) {
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
  }

  [[nodiscard]] bool validPort(const int port) {
    return port > 0 && port <= 65535;
  }

  [[nodiscard]] bool parseOptions(const int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
      const std::string_view argument(argv[i]);
      if (argument == "--warmup" && i + 1 < argc) {
        if (!parseSize(argv[++i], options.warmup)) return false;
      } else if (argument == "--samples" && i + 1 < argc) {
        if (!parseSize(argv[++i], options.samples) || options.samples == 0) return false;
      } else if (argument == "--output-dir" && i + 1 < argc) {
        options.output_dir = argv[++i];
      } else if (argument == "--iface" && i + 1 < argc) {
        options.iface = argv[++i];
      } else if (argument == "--order-ip" && i + 1 < argc) {
        options.order_ip = argv[++i];
      } else if (argument == "--incremental-ip" && i + 1 < argc) {
        options.incremental_ip = argv[++i];
      } else if (argument == "--snapshot-ip" && i + 1 < argc) {
        options.snapshot_ip = argv[++i];
      } else if (argument == "--order-port" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.order_port) || !validPort(options.order_port)) return false;
      } else if (argument == "--incremental-port" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.incremental_port) || !validPort(options.incremental_port)) return false;
      } else if (argument == "--snapshot-port" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.snapshot_port) || !validPort(options.snapshot_port)) return false;
      } else if (argument == "--mdc-cpu" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.mdc_cpu)) return false;
      } else if (argument == "--trade-engine-cpu" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.trade_engine_cpu)) return false;
      } else if (argument == "--gateway-cpu" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.gateway_cpu)) return false;
      } else if (argument == "--sink-cpu" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.sink_cpu)) return false;
      } else {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] Common::TradeEngineCfgHashMap makeConfig() {
    Common::TradeEngineCfgHashMap config{};
    config[TICKER_ID].clip_ = CLIP_QTY;
    config[TICKER_ID].threshold_ = 0.5;
    config[TICKER_ID].risk_cfg_.max_order_size_ = 1'000;
    config[TICKER_ID].risk_cfg_.max_position_ = 1'000'000;
    config[TICKER_ID].risk_cfg_.max_loss_ = -1'000'000'000.0;
    return config;
  }

  [[nodiscard]] std::uint64_t measureClockOverhead(const std::size_t samples = 100'000) {
    std::vector<std::uint64_t> values;
    values.reserve(samples);
    for (std::size_t i = 0; i < samples; ++i) {
      const auto start = Common::monotonicRawNanos();
      const auto end = Common::monotonicRawNanos();
      if (end >= start) values.push_back(end - start);
    }
    if (values.empty()) return 0;
    const auto middle = values.begin() + static_cast<std::ptrdiff_t>(values.size() / 2);
    std::nth_element(values.begin(), middle, values.end());
    return *middle;
  }

#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
  struct StageSample {
    std::uint64_t udp_recv_complete_ns = 0;
    std::uint64_t md_publish_boundary_ns = 0;
    std::uint64_t trade_engine_dequeue_ns = 0;
    std::uint64_t order_publish_boundary_ns = 0;
    std::uint64_t order_gateway_dequeue_ns = 0;
    std::uint64_t tcp_send_start_ns = 0;
    std::uint64_t tcp_send_complete_ns = 0;
  };

  class StageLatencyRecorder final {
  public:
    explicit StageLatencyRecorder(const std::size_t capacity)
        : samples_(capacity) {}

    void reset() noexcept {
      size_ = 0;
      invalid_count_ = 0;
      dropped_count_ = 0;
    }

    void record(const StageSample& sample) noexcept {
      if (!(sample.udp_recv_complete_ns <= sample.md_publish_boundary_ns &&
            sample.md_publish_boundary_ns <= sample.trade_engine_dequeue_ns &&
            sample.trade_engine_dequeue_ns <= sample.order_publish_boundary_ns &&
            sample.order_publish_boundary_ns <= sample.order_gateway_dequeue_ns &&
            sample.order_gateway_dequeue_ns <= sample.tcp_send_start_ns &&
            sample.tcp_send_start_ns <= sample.tcp_send_complete_ns)) {
        ++invalid_count_;
        return;
      }
      if (size_ == samples_.size()) {
        ++dropped_count_;
        return;
      }
      samples_[size_++] = sample;
    }

    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] std::size_t invalidCount() const noexcept { return invalid_count_; }
    [[nodiscard]] std::size_t droppedCount() const noexcept { return dropped_count_; }

    [[nodiscard]] bool writeCSV(const std::string& filename) const {
      std::ofstream output(filename);
      if (!output.is_open()) return false;
      output << "sample,t0_udp_recv_complete_ns,t1_md_publish_boundary_ns,"
                "t2_trade_engine_dequeue_ns,t3_order_publish_boundary_ns,"
                "t4_order_gateway_dequeue_ns,t5_tcp_send_start_ns,"
                "t6_tcp_send_complete_ns,mdc_processing_ns,md_to_trade_queue_ns,"
                "strategy_ns,trade_to_gateway_queue_ns,gateway_wait_ns,"
                "tcp_send_ns,total_ns\n";
      for (std::size_t i = 0; i < size_; ++i) {
        const auto& s = samples_[i];
        output << i << ','
               << s.udp_recv_complete_ns << ','
               << s.md_publish_boundary_ns << ','
               << s.trade_engine_dequeue_ns << ','
               << s.order_publish_boundary_ns << ','
               << s.order_gateway_dequeue_ns << ','
               << s.tcp_send_start_ns << ','
               << s.tcp_send_complete_ns << ','
               << s.md_publish_boundary_ns - s.udp_recv_complete_ns << ','
               << s.trade_engine_dequeue_ns - s.md_publish_boundary_ns << ','
               << s.order_publish_boundary_ns - s.trade_engine_dequeue_ns << ','
               << s.order_gateway_dequeue_ns - s.order_publish_boundary_ns << ','
               << s.tcp_send_start_ns - s.order_gateway_dequeue_ns << ','
               << s.tcp_send_complete_ns - s.tcp_send_start_ns << ','
               << s.tcp_send_complete_ns - s.udp_recv_complete_ns << '\n';
      }
      return static_cast<bool>(output);
    }

  private:
    std::vector<StageSample> samples_;
    std::size_t size_ = 0;
    std::size_t invalid_count_ = 0;
    std::size_t dropped_count_ = 0;
  };
#endif

  class TickToSendProbe final {
  public:
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
    enum class State : std::uint8_t {
      IDLE, ARMED, MD_PUBLISHED, TRADE_ENGINE_DEQUEUED,
      ORDER_PUBLISHED, ORDER_GATEWAY_DEQUEUED, SEND_STARTED, COMPLETE
    };
#else
    enum class State : std::uint8_t { IDLE, ARMED, TICK_SEEN, COMPLETE };
#endif

    [[nodiscard]] bool arm(const std::size_t sequence,
                           Common::NanosecondLatencyRecorder* recorder
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
                           , StageLatencyRecorder* stage_recorder
#endif
                           ) noexcept {
      if (state_.load(std::memory_order_acquire) != State::IDLE) return false;
      expected_sequence_ = sequence;
      recorder_ = recorder;
      start_ns_ = 0;
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
      stage_recorder_ = stage_recorder;
      current_stage_sample_ = {};
#endif
      state_.store(State::ARMED, std::memory_order_release);
      return true;
    }

    void observeTick(const Exchange::MDPMarketUpdate& update,
                     const std::uint64_t recv_complete_ns) noexcept {
      if (state_.load(std::memory_order_acquire) != State::ARMED ||
          update.seq_num_ != expected_sequence_) {
        return;
      }
      start_ns_ = recv_complete_ns;
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
      current_stage_sample_.udp_recv_complete_ns = recv_complete_ns;
      current_stage_sample_.md_publish_boundary_ns = Common::monotonicRawNanos();
      state_.store(State::MD_PUBLISHED, std::memory_order_release);
#else
      state_.store(State::TICK_SEEN, std::memory_order_release);
#endif
    }

#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
    void observeMarketDequeue(const std::uint64_t timestamp) noexcept {
      transition(State::MD_PUBLISHED, State::TRADE_ENGINE_DEQUEUED,
                 current_stage_sample_.trade_engine_dequeue_ns, timestamp);
    }

    void observeOrderPublish(const std::uint64_t timestamp) noexcept {
      transition(State::TRADE_ENGINE_DEQUEUED, State::ORDER_PUBLISHED,
                 current_stage_sample_.order_publish_boundary_ns, timestamp);
    }

    void observeOrderGatewayDequeue(const std::uint64_t timestamp) noexcept {
      transition(State::ORDER_PUBLISHED, State::ORDER_GATEWAY_DEQUEUED,
                 current_stage_sample_.order_gateway_dequeue_ns, timestamp);
    }

    void observeSendStart(const std::uint64_t timestamp) noexcept {
      transition(State::ORDER_GATEWAY_DEQUEUED, State::SEND_STARTED,
                 current_stage_sample_.tcp_send_start_ns, timestamp);
    }
#endif

    void observeSendComplete(const std::uint64_t send_complete_ns,
                             const std::size_t bytes_sent) noexcept {
      (void) bytes_sent;
      const auto state = state_.load(std::memory_order_acquire);
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
      if (state == State::SEND_STARTED) {
        current_stage_sample_.tcp_send_complete_ns = send_complete_ns;
        if (recorder_ != nullptr) recorder_->record(start_ns_, send_complete_ns);
        if (stage_recorder_ != nullptr) stage_recorder_->record(current_stage_sample_);
        state_.store(State::COMPLETE, std::memory_order_release);
      } else if (state != State::IDLE) {
        unexpected_send_count_.fetch_add(1, std::memory_order_relaxed);
      }
#else
      if (state == State::TICK_SEEN) {
        if (recorder_ != nullptr) {
          recorder_->record(start_ns_, send_complete_ns);
        }
        state_.store(State::COMPLETE, std::memory_order_release);
      } else if (state == State::ARMED) {
        unexpected_send_count_.fetch_add(1, std::memory_order_relaxed);
      }
#endif
    }

    [[nodiscard]] bool completed() const noexcept {
      return state_.load(std::memory_order_acquire) == State::COMPLETE;
    }

    [[nodiscard]] bool reset() noexcept {
      auto expected = State::COMPLETE;
      return state_.compare_exchange_strong(expected, State::IDLE,
                                            std::memory_order_acq_rel);
    }

    [[nodiscard]] std::uint64_t unexpectedSendCount() const noexcept {
      return unexpected_send_count_.load(std::memory_order_relaxed);
    }

  private:
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
    void transition(const State expected, const State next,
                    std::uint64_t& destination,
                    const std::uint64_t timestamp) noexcept {
      const auto current = state_.load(std::memory_order_acquire);
      if (current == State::IDLE) return;
      if (current != expected) {
        unexpected_send_count_.fetch_add(1, std::memory_order_relaxed);
        return;
      }
      destination = timestamp;
      state_.store(next, std::memory_order_release);
    }
#endif
    std::atomic<State> state_{State::IDLE};
    std::size_t expected_sequence_ = 0;
    std::uint64_t start_ns_ = 0;
    Common::NanosecondLatencyRecorder* recorder_ = nullptr;
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
    StageLatencyRecorder* stage_recorder_ = nullptr;
    StageSample current_stage_sample_{};
#endif
    std::atomic<std::uint64_t> unexpected_send_count_{0};
  };

  class OrderSink final {
  public:
    OrderSink(const std::string& iface, const int port, const int core_id)
        : logger_("tick_to_kernel_send_sink.log"), server_(logger_),
          iface_(iface), port_(port), core_id_(core_id) {
      server_.recv_callback_ = [this](auto socket, auto) { recvCallback(socket); };
      server_.recv_finished_callback_ = []() {};
    }

    ~OrderSink() { stop(); }

    void start() {
      server_.listen(iface_, port_);
      run_.store(true, std::memory_order_release);
      thread_ = Common::createAndStartThread(core_id_, "Benchmark/OrderSink", [this]() { run(); });
      ASSERT(thread_ != nullptr, "Failed to start benchmark order sink.");
    }

    void stop() {
      run_.store(false, std::memory_order_release);
      if (thread_ != nullptr) {
        if (thread_->joinable()) thread_->join();
        delete thread_;
        thread_ = nullptr;
      }
    }

    [[nodiscard]] std::uint64_t receivedCount() const noexcept {
      return received_count_.load(std::memory_order_acquire);
    }

    [[nodiscard]] Exchange::MEClientRequest lastRequest() const noexcept {
      return last_request_;
    }

    [[nodiscard]] std::uint64_t protocolErrors() const noexcept {
      return protocol_errors_.load(std::memory_order_relaxed);
    }

  private:
    void run() noexcept {
      while (run_.load(std::memory_order_relaxed)) {
        server_.poll();
        server_.sendAndRecv();
      }
    }

    void recvCallback(Common::TCPSocket* socket) noexcept {
      std::size_t offset = 0;
      while (offset + sizeof(Exchange::OMClientRequest) <=
             socket->next_rcv_valid_index_) {
        const auto* request = reinterpret_cast<const Exchange::OMClientRequest*>(
            socket->inbound_data_.data() + offset);
        if (request->seq_num_ != next_expected_sequence_ ||
            request->me_client_request_.client_id_ != CLIENT_ID) {
          protocol_errors_.fetch_add(1, std::memory_order_relaxed);
        } else {
          ++next_expected_sequence_;
        }
        last_request_ = request->me_client_request_;
        received_count_.fetch_add(1, std::memory_order_release);
        offset += sizeof(Exchange::OMClientRequest);
      }
      const auto remaining = socket->next_rcv_valid_index_ - offset;
      std::memmove(socket->inbound_data_.data(),
                   socket->inbound_data_.data() + offset, remaining);
      socket->next_rcv_valid_index_ = remaining;
    }

    Common::Logger logger_;
    Common::TCPServer server_;
    const std::string iface_;
    const int port_;
    const int core_id_;
    std::atomic<bool> run_{false};
    std::thread* thread_ = nullptr;
    std::size_t next_expected_sequence_ = 1;
    Exchange::MEClientRequest last_request_{};
    std::atomic<std::uint64_t> received_count_{0};
    std::atomic<std::uint64_t> protocol_errors_{0};
  };

  class Harness final {
  public:
    explicit Harness(const Options& options)
        : options_(options), config_(makeConfig()),
          sink_(options.iface, options.order_port, options.sink_cpu),
          engine_(CLIENT_ID, Common::AlgoType::TAKER, config_,
                  &outgoing_requests_, &incoming_responses_,
                  &incoming_market_updates_, options.trade_engine_cpu),
          gateway_(CLIENT_ID, &outgoing_requests_, &unused_gateway_responses_,
                   options.order_ip, options.iface, options.order_port,
                   options.gateway_cpu),
          consumer_(CLIENT_ID, &incoming_market_updates_, options.iface,
                    options.snapshot_ip, options.snapshot_port,
                    options.incremental_ip, options.incremental_port,
                    options.mdc_cpu),
          publisher_socket_(publisher_logger_) {
      consumer_.setIncrementalReceiveObserver(
          [this](const auto& update, const auto recv_ns) {
            probe_.observeTick(update, recv_ns);
          });
      consumer_.setIncrementalPublishObserver([this](const auto sequence) {
        last_published_sequence_.store(sequence, std::memory_order_release);
      });
      gateway_.setSendCompleteObserver(
          [this](const auto send_ns, const auto bytes) {
            probe_.observeSendComplete(send_ns, bytes);
          });
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
      engine_.setMarketDequeueObserver([this](const auto timestamp) {
        probe_.observeMarketDequeue(timestamp);
      });
      engine_.setOrderPublishObserver([this](const auto timestamp) {
        probe_.observeOrderPublish(timestamp);
      });
      gateway_.setOrderDequeueObserver([this](const auto timestamp) {
        probe_.observeOrderGatewayDequeue(timestamp);
      });
      gateway_.setSendStartObserver([this](const auto timestamp) {
        probe_.observeSendStart(timestamp);
      });
#endif
      ASSERT(publisher_socket_.init(options.incremental_ip, options.iface,
                                    options.incremental_port, false) >= 0,
             "Unable to create benchmark market-data publisher socket.");

      sink_.start();
      engine_.start();
      gateway_.start();
      consumer_.start();
    }

    ~Harness() {
      consumer_.stop();
      engine_.stop();
      gateway_.stop();
      sink_.stop();
    }

    Harness(const Harness&) = delete;
    Harness& operator=(const Harness&) = delete;

    [[nodiscard]] bool initializeBook() {
      const Exchange::MEMarketUpdate bid{Exchange::MarketUpdateType::ADD,
          BID_ORDER_ID, TICKER_ID, Common::Side::BUY, BID_PRICE, BOOK_QTY, 1};
      const Exchange::MEMarketUpdate ask{Exchange::MarketUpdateType::ADD,
          ASK_ORDER_ID, TICKER_ID, Common::Side::SELL, ASK_PRICE, BOOK_QTY, 1};
      return publishAndDrain(bid) && publishAndDrain(ask);
    }

    [[nodiscard]] bool cycle(Common::NanosecondLatencyRecorder* recorder
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
                             , StageLatencyRecorder* stage_recorder
#endif
                             ) {
      if (incoming_market_updates_.size() != 0 || outgoing_requests_.size() != 0 ||
          incoming_responses_.size() != 0) {
        return false;
      }

      const auto sequence = next_market_sequence_;
      const auto expected_received = sink_.receivedCount() + 1;
      if (!probe_.arm(sequence, recorder
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
                      , stage_recorder
#endif
                      )) return false;

      const Exchange::MEMarketUpdate trade{Exchange::MarketUpdateType::TRADE,
          Common::OrderId_INVALID, TICKER_ID, Common::Side::BUY, ASK_PRICE,
          BOOK_QTY, Common::Priority_INVALID};
      publish(trade);

      if (!waitUntil([this]() { return probe_.completed(); }) ||
          !waitUntil([this, expected_received]() {
            return sink_.receivedCount() >= expected_received;
          })) {
        return false;
      }

      const auto request = sink_.lastRequest();
      const bool valid_request =
          request.type_ == Exchange::ClientRequestType::NEW &&
          request.client_id_ == CLIENT_ID && request.ticker_id_ == TICKER_ID &&
          request.side_ == Common::Side::BUY && request.price_ == ASK_PRICE &&
          request.qty_ == CLIP_QTY;
      if (!valid_request || !probe_.reset()) return false;

      publishResponse({Exchange::ClientResponseType::ACCEPTED, CLIENT_ID,
                       TICKER_ID, request.order_id_, request.order_id_,
                       request.side_, request.price_, 0, request.qty_});
      publishResponse({Exchange::ClientResponseType::CANCELED, CLIENT_ID,
                       TICKER_ID, request.order_id_, request.order_id_,
                       request.side_, request.price_, 0, request.qty_});
      return waitUntil([this]() { return incoming_responses_.size() == 0; });
    }

    [[nodiscard]] std::uint64_t protocolErrors() const noexcept {
      return sink_.protocolErrors() + probe_.unexpectedSendCount();
    }

  private:
    template<typename Predicate>
    [[nodiscard]] bool waitUntil(Predicate predicate) const noexcept {
      const auto start = Common::monotonicRawNanos();
      std::size_t polls = 0;
      while (!predicate()) {
        ++polls;
        if ((polls & 0xffffU) == 0U &&
            Common::monotonicRawNanos() - start > TIMEOUT_NS) {
          return false;
        }
      }
      return true;
    }

    void publish(const Exchange::MEMarketUpdate& update) {
      const Exchange::MDPMarketUpdate wire_update{next_market_sequence_++, update};
      publisher_socket_.send(&wire_update, sizeof(wire_update));
      publisher_socket_.sendAndRecv();
    }

    [[nodiscard]] bool publishAndDrain(const Exchange::MEMarketUpdate& update) {
      const auto sequence = next_market_sequence_;
      publish(update);
      return waitUntil([this, sequence]() {
               return last_published_sequence_.load(std::memory_order_acquire) >= sequence;
             }) &&
             waitUntil([this]() { return incoming_market_updates_.size() == 0; });
    }

    void publishResponse(const Exchange::MEClientResponse& response) noexcept {
      *incoming_responses_.getNextToWriteTo() = response;
      incoming_responses_.updateWriteIndex();
    }

    const Options options_;
    Common::TradeEngineCfgHashMap config_;
    Exchange::ClientRequestLFQueue outgoing_requests_{QUEUE_CAPACITY};
    Exchange::ClientResponseLFQueue incoming_responses_{QUEUE_CAPACITY};
    Exchange::ClientResponseLFQueue unused_gateway_responses_{QUEUE_CAPACITY};
    Exchange::MEMarketUpdateLFQueue incoming_market_updates_{QUEUE_CAPACITY};
    TickToSendProbe probe_;
    std::atomic<std::size_t> last_published_sequence_{0};
    OrderSink sink_;
    Trading::TradeEngine engine_;
    Trading::OrderGateway gateway_;
    Trading::MarketDataConsumer consumer_;
    Common::Logger publisher_logger_{"tick_to_kernel_send_publisher.log"};
    Common::McastSocket publisher_socket_;
    std::size_t next_market_sequence_ = 1;
  };

} // namespace

int main(const int argc, char** argv) {
  Options options;
  if (!parseOptions(argc, argv, options)) {
    std::cerr << "Usage: " << argv[0]
              << " [--warmup N] [--samples N] [--output-dir DIR]"
                 " [--iface IFACE] [--order-ip IP]"
                 " [--order-port PORT] [--incremental-ip IP]"
                 " [--incremental-port PORT] [--snapshot-ip IP]"
                 " [--snapshot-port PORT] [--mdc-cpu N]"
                 " [--trade-engine-cpu N] [--gateway-cpu N] [--sink-cpu N]\n";
    return EXIT_FAILURE;
  }

  std::filesystem::create_directories(options.output_dir);
  const auto clock_overhead_ns = measureClockOverhead();
  Common::NanosecondLatencyRecorder recorder(options.samples);
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
  StageLatencyRecorder stage_recorder(options.samples);
#endif

  std::cout
      << "benchmark=application_tick_to_kernel_send\n"
      << "scenario=taker_trade_to_complete_tcp_buffer_drain\n"
      << "boundary=start immediately after MarketDataConsumer UDP recv() returns; "
         "end immediately after OrderGateway fully drains its pending TCP buffer\n"
      << "includes=incremental decode/sequence check,MDC-to-TradeEngine queue wait,"
         "book/feature/strategy/risk,TradeEngine-to-OrderGateway queue wait,TCP send syscall\n"
      << "excludes=physical NIC/wire,UDP receive syscall before return,exchange matching/response\n"
      << "clock=CLOCK_MONOTONIC_RAW\n"
      << "clock_overhead_ns=" << clock_overhead_ns << '\n'
      << "outstanding_triggers=1\n"
      << "warmup=" << options.warmup << '\n'
      << "requested_samples=" << options.samples << '\n'
      << "hot_path_logging=compile_time_disabled\n"
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
      << "diagnostic_stage_timestamps=enabled\n";
#else
      << "diagnostic_stage_timestamps=disabled\n";
#endif

  Harness harness(options);
  if (!harness.initializeBook()) {
    std::cerr << "Unable to initialize the benchmark BBO over multicast.\n";
    return EXIT_FAILURE;
  }

  for (std::size_t i = 0; i < options.warmup; ++i) {
    if (!harness.cycle(nullptr
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
                       , nullptr
#endif
                       )) {
      std::cerr << "Warm-up failed at iteration " << i << ".\n";
      return EXIT_FAILURE;
    }
  }

  recorder.reset();
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
  stage_recorder.reset();
#endif
  for (std::size_t i = 0; i < options.samples; ++i) {
    if (!harness.cycle(&recorder
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
                       , &stage_recorder
#endif
                       )) {
      std::cerr << "Measurement failed at iteration " << i << ".\n";
      return EXIT_FAILURE;
    }
  }

  const auto output = options.output_dir / "tick_to_kernel_send.csv";
  if (!recorder.writeCSV(output.string(), clock_overhead_ns)) {
    std::cerr << "Unable to write " << output << ".\n";
    return EXIT_FAILURE;
  }

#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
  const auto stage_output = options.output_dir / "tick_to_kernel_send_stages.csv";
  if (!stage_recorder.writeCSV(stage_output.string())) {
    std::cerr << "Unable to write " << stage_output << ".\n";
    return EXIT_FAILURE;
  }
#endif

  std::cout << "recorded_samples=" << recorder.size() << '\n'
            << "invalid_samples=" << recorder.invalidCount() << '\n'
            << "dropped_samples=" << recorder.droppedCount() << '\n'
            << "protocol_errors=" << harness.protocolErrors() << '\n'
            << "output=" << output << '\n'
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
            << "stage_recorded_samples=" << stage_recorder.size() << '\n'
            << "stage_invalid_samples=" << stage_recorder.invalidCount() << '\n'
            << "stage_dropped_samples=" << stage_recorder.droppedCount() << '\n'
            << "stage_output=" << stage_output << '\n';
#else
            ;
#endif

  return recorder.size() == options.samples && recorder.invalidCount() == 0 &&
                 recorder.droppedCount() == 0 && harness.protocolErrors() == 0
#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
                 && stage_recorder.size() == options.samples &&
                 stage_recorder.invalidCount() == 0 &&
                 stage_recorder.droppedCount() == 0
#endif
             ? EXIT_SUCCESS
             : EXIT_FAILURE;
}

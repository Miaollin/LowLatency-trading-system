#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>

#include "common/latency_recorder.h"
#include "common/thread_utils.h"
#include "exchange/matcher/matching_engine.h"
#include "exchange/order_server/order_server.h"
#include "trading/order_gw/order_gateway.h"

namespace {

  constexpr Common::ClientId CLIENT_ID = 0;
  constexpr Common::TickerId TICKER_ID = 0;
  constexpr Common::Side ORDER_SIDE = Common::Side::BUY;
  constexpr Common::Price ORDER_PRICE = 100;
  constexpr Common::Qty ORDER_QTY = 10;
  constexpr std::size_t QUEUE_CAPACITY = 4096;
  constexpr std::uint64_t TIMEOUT_NS = 5'000'000'000ULL;

  struct Options {
    std::size_t warmup = 10'000;
    std::size_t samples = 100'000;
    std::string scenario = "all";
    std::filesystem::path output_dir = "runs/order-rtt";
    std::string ip = "127.0.0.1";
    std::string iface = "lo";
    int port = 19001;
    int gateway_cpu = -1;
    int order_server_cpu = -1;
    int matching_cpu = -1;
  };

  [[nodiscard]] bool parseSize(const std::string_view text, std::size_t& value) {
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
  }

  [[nodiscard]] bool parseInt(const std::string_view text, int& value) {
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
  }

  [[nodiscard]] bool parseOptions(const int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
      const std::string_view argument(argv[i]);
      if (argument == "--warmup" && i + 1 < argc) {
        if (!parseSize(argv[++i], options.warmup)) return false;
      } else if (argument == "--samples" && i + 1 < argc) {
        if (!parseSize(argv[++i], options.samples) || options.samples == 0) return false;
      } else if (argument == "--scenario" && i + 1 < argc) {
        options.scenario = argv[++i];
        if (options.scenario != "all" && options.scenario != "new" &&
            options.scenario != "cancel") return false;
      } else if (argument == "--output-dir" && i + 1 < argc) {
        options.output_dir = argv[++i];
      } else if (argument == "--ip" && i + 1 < argc) {
        options.ip = argv[++i];
      } else if (argument == "--iface" && i + 1 < argc) {
        options.iface = argv[++i];
      } else if (argument == "--port" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.port) || options.port <= 0 || options.port > 65535) return false;
      } else if (argument == "--gateway-cpu" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.gateway_cpu)) return false;
      } else if (argument == "--order-server-cpu" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.order_server_cpu)) return false;
      } else if (argument == "--matching-cpu" && i + 1 < argc) {
        if (!parseInt(argv[++i], options.matching_cpu)) return false;
      } else {
        return false;
      }
    }
    return true;
  }

  class Harness final {
  public:
    explicit Harness(const Options& options)
        : order_server_(&exchange_requests_, &exchange_responses_, options.iface,
                        options.port, options.order_server_cpu),
          matching_engine_(&exchange_requests_, &exchange_responses_,
                           &market_updates_, options.matching_cpu),
          order_gateway_(CLIENT_ID, &client_requests_, &client_responses_,
                         options.ip, options.iface, options.port,
                         options.gateway_cpu) {
      order_server_.start();
      matching_engine_.start();
      order_gateway_.start();
    }

    ~Harness() {
      order_gateway_.stop();
      order_server_.stop();
      matching_engine_.stop();
    }

    Harness(const Harness&) = delete;
    Harness& operator=(const Harness&) = delete;

    [[nodiscard]] bool cycle(const Common::OrderId order_id,
                             const bool measure_new,
                             const bool measure_cancel,
                             Common::LatencyRecorder* recorder) noexcept {
      const Exchange::MEClientRequest add{Exchange::ClientRequestType::NEW,
          CLIENT_ID, TICKER_ID, order_id, ORDER_SIDE, ORDER_PRICE, ORDER_QTY};
      if (!roundTrip(add, Exchange::ClientResponseType::ACCEPTED,
                     measure_new ? recorder : nullptr) ||
          !waitMarketUpdate(Exchange::MarketUpdateType::ADD)) {
        return false;
      }

      const Exchange::MEClientRequest cancel{Exchange::ClientRequestType::CANCEL,
          CLIENT_ID, TICKER_ID, order_id, ORDER_SIDE, ORDER_PRICE, ORDER_QTY};
      return roundTrip(cancel, Exchange::ClientResponseType::CANCELED,
                       measure_cancel ? recorder : nullptr) &&
             waitMarketUpdate(Exchange::MarketUpdateType::CANCEL);
    }

    [[nodiscard]] std::uint64_t protocolErrors() const noexcept {
      return order_gateway_.rttProtocolErrors();
    }

  private:
    [[nodiscard]] bool timedOut(const std::uint64_t start_ns,
                                std::size_t& polls) const noexcept {
      ++polls;
      return (polls & 0xffffU) == 0U &&
             Common::monotonicRawNanos() - start_ns > TIMEOUT_NS;
    }

    [[nodiscard]] bool roundTrip(const Exchange::MEClientRequest& request,
                                 const Exchange::ClientResponseType expected,
                                 Common::LatencyRecorder* recorder) noexcept {
      if (client_requests_.size() != 0 || client_responses_.size() != 0) {
        return false;
      }
      if (recorder != nullptr &&
          !order_gateway_.armRTT(request.order_id_, expected, recorder)) {
        return false;
      }

      *client_requests_.getNextToWriteTo() = request;
      client_requests_.updateWriteIndex();

      const auto wait_start = Common::monotonicRawNanos();
      std::size_t polls = 0;
      const Exchange::MEClientResponse* response = nullptr;
      while ((response = client_responses_.getNextToRead()) == nullptr) {
        if (timedOut(wait_start, polls)) return false;
      }

      const bool valid = response->client_id_ == CLIENT_ID &&
                         response->ticker_id_ == TICKER_ID &&
                         response->client_order_id_ == request.order_id_ &&
                         response->type_ == expected;
      client_responses_.updateReadIndex();
      return valid && (recorder == nullptr || order_gateway_.rttCompleted());
    }

    [[nodiscard]] bool waitMarketUpdate(const Exchange::MarketUpdateType expected) noexcept {
      const auto wait_start = Common::monotonicRawNanos();
      std::size_t polls = 0;
      const Exchange::MEMarketUpdate* update = nullptr;
      while ((update = market_updates_.getNextToRead()) == nullptr) {
        if (timedOut(wait_start, polls)) return false;
      }
      const bool valid = update->ticker_id_ == TICKER_ID && update->type_ == expected;
      market_updates_.updateReadIndex();
      return valid;
    }

    Exchange::ClientRequestLFQueue client_requests_{QUEUE_CAPACITY};
    Exchange::ClientResponseLFQueue client_responses_{QUEUE_CAPACITY};
    Exchange::ClientRequestLFQueue exchange_requests_{QUEUE_CAPACITY};
    Exchange::ClientResponseLFQueue exchange_responses_{QUEUE_CAPACITY};
    Exchange::MEMarketUpdateLFQueue market_updates_{QUEUE_CAPACITY};
    Exchange::OrderServer order_server_;
    Exchange::MatchingEngine matching_engine_;
    Trading::OrderGateway order_gateway_;
  };

  [[nodiscard]] Common::OrderId nextOrderId(std::size_t& sequence) noexcept {
    constexpr auto usable_ids = Common::ME_MAX_ORDER_IDS - 1;
    return static_cast<Common::OrderId>((sequence++ % usable_ids) + 1);
  }

  [[nodiscard]] bool runScenario(Harness& harness,
                                 const std::string_view name,
                                 const bool measure_new,
                                 Common::LatencyRecorder& recorder,
                                 const Options& options,
                                 const std::uint64_t overhead_ticks,
                                 const double tsc_hz,
                                 std::size_t& order_sequence) {
    for (std::size_t i = 0; i < options.warmup; ++i) {
      if (!harness.cycle(nextOrderId(order_sequence), false, false, nullptr)) {
        std::cerr << name << " warm-up failed at iteration " << i << ".\n";
        return false;
      }
    }

    recorder.reset();
    for (std::size_t i = 0; i < options.samples; ++i) {
      if (!harness.cycle(nextOrderId(order_sequence), measure_new, !measure_new,
                         &recorder)) {
        std::cerr << name << " measurement failed at iteration " << i << ".\n";
        return false;
      }
    }
    if (recorder.size() != options.samples) {
      std::cerr << name << " recorded " << recorder.size() << " of "
                << options.samples << " requested samples.\n";
      return false;
    }

    const auto output = options.output_dir / ("order_rtt_" + std::string(name) + ".csv");
    if (!recorder.writeCSV(output.string(), overhead_ticks, tsc_hz)) {
      std::cerr << "Failed to write " << output << ".\n";
      return false;
    }
    std::cout << "scenario=" << name << " samples=" << recorder.size()
              << " migrations=" << recorder.migrationCount()
              << " invalid=" << recorder.invalidCount()
              << " dropped=" << recorder.droppedCount()
              << " output=" << output << '\n';
    return true;
  }

} // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parseOptions(argc, argv, options)) {
    std::cerr << "Usage: " << argv[0]
              << " [--scenario all|new|cancel] [--warmup N] [--samples N]"
                 " [--output-dir DIR] [--ip IP] [--iface IFACE] [--port PORT]"
                 " [--gateway-cpu N] [--order-server-cpu N] [--matching-cpu N]\n";
    return EXIT_FAILURE;
  }

  std::filesystem::create_directories(options.output_dir);
  const auto tsc_hz = Common::calibrateTSCHz();
  const auto overhead_ticks = Common::measureTSCOverhead();
  if (tsc_hz <= 0.0) {
    std::cerr << "TSC calibration failed.\n";
    return EXIT_FAILURE;
  }

  std::cout << "clock=" << (Common::HAS_HARDWARE_TSC ? "x86_tsc" : "steady_clock")
            << " tsc_hz=" << tsc_hz
            << " measurement_overhead_ticks=" << overhead_ticks << '\n';
  std::cout << "boundary=OrderGateway_pre_kernel_send_to_validated_correlated_response"
            << " outstanding_requests=1 transport=TCP_loopback\n";

  Harness harness(options);
  Common::LatencyRecorder recorder(options.samples);
  std::size_t order_sequence = 0;
  bool ok = true;
  if (options.scenario == "all" || options.scenario == "new") {
    ok = runScenario(harness, "new", true, recorder, options,
                     overhead_ticks, tsc_hz, order_sequence) && ok;
  }
  if (options.scenario == "all" || options.scenario == "cancel") {
    ok = runScenario(harness, "cancel", false, recorder, options,
                     overhead_ticks, tsc_hz, order_sequence) && ok;
  }
  if (harness.protocolErrors() != 0) {
    std::cerr << "RTT correlation/protocol errors=" << harness.protocolErrors() << '\n';
    ok = false;
  }
  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}

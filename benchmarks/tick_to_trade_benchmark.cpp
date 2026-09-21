#include <array>
#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>

#include "common/latency_recorder.h"
#include "trading/strategy/trade_engine.h"

namespace {

  static_assert(Common::ME_MAX_TICKERS >= 1);
  static_assert(Common::ME_MAX_NUM_CLIENTS >= 1);
  static_assert(Common::ME_MAX_ORDER_IDS >= 4);

  constexpr Common::ClientId CLIENT_ID = 0;
  constexpr Common::TickerId TICKER_ID = 0;
  constexpr Common::OrderId BID_MARKET_ORDER_ID = 1;
  constexpr Common::OrderId ASK_MARKET_ORDER_ID = 2;
  constexpr Common::Price BID_PRICE = 99;
  constexpr Common::Price ASK_PRICE = 101;
  constexpr Common::Qty BOOK_QTY = 100;
  constexpr Common::Qty CLIP_QTY = 10;
  constexpr std::size_t QUEUE_CAPACITY = 16;

  struct Options {
    std::size_t warmup = 100'000;
    std::size_t samples = 1'000'000;
    std::string scenario = "all";
    std::filesystem::path output_dir = "runs/tick-to-trade";
  };

  [[nodiscard]] Common::TradeEngineCfgHashMap makeConfig() {
    Common::TradeEngineCfgHashMap config{};
    config[TICKER_ID].clip_ = CLIP_QTY;
    config[TICKER_ID].threshold_ = 0.5;
    config[TICKER_ID].risk_cfg_.max_order_size_ = 1'000;
    config[TICKER_ID].risk_cfg_.max_position_ = 1'000'000;
    config[TICKER_ID].risk_cfg_.max_loss_ = -1'000'000'000.0;
    return config;
  }

  struct Harness {
    Common::TradeEngineCfgHashMap config{makeConfig()};
    Exchange::ClientRequestLFQueue outgoing_requests{QUEUE_CAPACITY};
    Exchange::ClientResponseLFQueue incoming_responses{QUEUE_CAPACITY};
    Exchange::MEMarketUpdateLFQueue incoming_market_updates{QUEUE_CAPACITY};
    Trading::TradeEngine engine;

    explicit Harness(const Common::AlgoType algo_type)
        : engine(CLIENT_ID, algo_type, config, &outgoing_requests,
                 &incoming_responses, &incoming_market_updates) {
    }
  };

  [[nodiscard]] bool parseSize(const std::string_view text, std::size_t& value) {
    const auto* begin = text.data();
    const auto* end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
  }

  [[nodiscard]] bool validScenario(const std::string_view scenario) {
    return scenario == "all" || scenario == "maker_book_to_first_order" ||
           scenario == "taker_trade_to_order";
  }

  [[nodiscard]] bool parseOptions(const int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
      const std::string_view argument(argv[i]);
      if (argument == "--warmup" && i + 1 < argc) {
        if (!parseSize(argv[++i], options.warmup)) {
          return false;
        }
      } else if (argument == "--samples" && i + 1 < argc) {
        if (!parseSize(argv[++i], options.samples) || options.samples == 0) {
          return false;
        }
      } else if (argument == "--scenario" && i + 1 < argc) {
        options.scenario = argv[++i];
        if (!validScenario(options.scenario)) {
          return false;
        }
      } else if (argument == "--output-dir" && i + 1 < argc) {
        options.output_dir = argv[++i];
      } else {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] Exchange::MEMarketUpdate marketUpdate(
      const Exchange::MarketUpdateType type,
      const Common::OrderId order_id,
      const Common::Side side,
      const Common::Price price,
      const Common::Qty qty,
      const Common::Priority priority = 1) noexcept {
    return {type, order_id, TICKER_ID, side, price, qty, priority};
  }

  void acknowledgeAndCancel(Trading::TradeEngine& engine,
                            const Exchange::MEClientRequest& request) noexcept {
    const Exchange::MEClientResponse accepted{
        Exchange::ClientResponseType::ACCEPTED, CLIENT_ID, TICKER_ID,
        request.order_id_, request.order_id_, request.side_, request.price_, 0,
        request.qty_};
    engine.onOrderUpdate(&accepted);

    const Exchange::MEClientResponse canceled{
        Exchange::ClientResponseType::CANCELED, CLIENT_ID, TICKER_ID,
        request.order_id_, request.order_id_, request.side_, request.price_, 0,
        request.qty_};
    engine.onOrderUpdate(&canceled);
  }

  template<std::size_t Count>
  [[nodiscard]] bool consumeRequests(Harness& harness,
                                     std::array<Exchange::MEClientRequest, Count>& requests) noexcept {
    std::size_t index = 0;
    while (const auto* request = harness.outgoing_requests.getNextToRead()) {
      if (index == requests.size()) {
        return false;
      }
      requests[index++] = *request;
      harness.outgoing_requests.updateReadIndex();
    }
    return index == requests.size();
  }

  [[nodiscard]] bool validNewRequest(const Exchange::MEClientRequest& request,
                                     const Common::Side side,
                                     const Common::Price price) noexcept {
    return request.type_ == Exchange::ClientRequestType::NEW &&
           request.client_id_ == CLIENT_ID && request.ticker_id_ == TICKER_ID &&
           request.side_ == side && request.price_ == price &&
           request.qty_ == CLIP_QTY;
  }

  [[nodiscard]] bool initializeBook(Harness& harness,
                                    const bool maker) noexcept {
    const auto bid = marketUpdate(Exchange::MarketUpdateType::ADD,
                                  BID_MARKET_ORDER_ID, Common::Side::BUY,
                                  BID_PRICE, BOOK_QTY);
    const auto ask = marketUpdate(Exchange::MarketUpdateType::ADD,
                                  ASK_MARKET_ORDER_ID, Common::Side::SELL,
                                  ASK_PRICE, BOOK_QTY);
    harness.engine.processMarketUpdate(&bid);
    harness.engine.processMarketUpdate(&ask);

    if (!maker) {
      return harness.outgoing_requests.size() == 0;
    }

    std::array<Exchange::MEClientRequest, 2> initial_quotes{};
    if (!consumeRequests(harness, initial_quotes) ||
        !validNewRequest(initial_quotes[0], Common::Side::BUY, BID_PRICE) ||
        !validNewRequest(initial_quotes[1], Common::Side::SELL, ASK_PRICE)) {
      return false;
    }
    for (const auto& request : initial_quotes) {
      acknowledgeAndCancel(harness.engine, request);
    }
    return true;
  }

  [[nodiscard]] bool runMaker(Harness& harness,
                              const std::size_t iterations,
                              Common::LatencyRecorder* recorder) noexcept {
    for (std::size_t i = 0; i < iterations; ++i) {
      const auto new_qty = BOOK_QTY + static_cast<Common::Qty>(i & 1U);
      const auto update = marketUpdate(Exchange::MarketUpdateType::MODIFY,
                                       BID_MARKET_ORDER_ID, Common::Side::BUY,
                                       BID_PRICE, new_qty);
      const auto endpoint_reached = recorder != nullptr
          ? harness.engine.processMarketUpdateForBenchmark(&update, recorder)
          : (harness.engine.processMarketUpdate(&update), true);
      if (!endpoint_reached) {
        return false;
      }

      std::array<Exchange::MEClientRequest, 2> quotes{};
      if (!consumeRequests(harness, quotes) ||
          !validNewRequest(quotes[0], Common::Side::BUY, BID_PRICE) ||
          !validNewRequest(quotes[1], Common::Side::SELL, ASK_PRICE)) {
        return false;
      }
      for (const auto& request : quotes) {
        acknowledgeAndCancel(harness.engine, request);
      }
    }
    return true;
  }

  [[nodiscard]] bool runTaker(Harness& harness,
                              const std::size_t iterations,
                              Common::LatencyRecorder* recorder) noexcept {
    const auto trade = marketUpdate(Exchange::MarketUpdateType::TRADE,
                                    Common::OrderId_INVALID, Common::Side::BUY,
                                    ASK_PRICE, BOOK_QTY,
                                    Common::Priority_INVALID);
    for (std::size_t i = 0; i < iterations; ++i) {
      const auto endpoint_reached = recorder != nullptr
          ? harness.engine.processMarketUpdateForBenchmark(&trade, recorder)
          : (harness.engine.processMarketUpdate(&trade), true);
      if (!endpoint_reached) {
        return false;
      }

      std::array<Exchange::MEClientRequest, 1> request{};
      if (!consumeRequests(harness, request) ||
          !validNewRequest(request[0], Common::Side::BUY, ASK_PRICE)) {
        return false;
      }
      acknowledgeAndCancel(harness.engine, request[0]);
    }
    return true;
  }

  using ScenarioRunner = bool (*)(Harness&, std::size_t,
                                  Common::LatencyRecorder*) noexcept;

  [[nodiscard]] bool runScenario(const std::string_view name,
                                 const Common::AlgoType algo_type,
                                 const bool maker,
                                 const ScenarioRunner runner,
                                 Common::LatencyRecorder& recorder,
                                 const Options& options,
                                 const std::uint64_t overhead_ticks,
                                 const double tsc_hz) {
    Harness harness(algo_type);
    if (!initializeBook(harness, maker)) {
      std::cerr << "Book/strategy initialization failed for " << name << ".\n";
      return false;
    }
    if (!runner(harness, options.warmup, nullptr)) {
      std::cerr << "Workload validation failed during " << name << " warm-up.\n";
      return false;
    }

    recorder.reset();
    if (!runner(harness, options.samples, &recorder)) {
      std::cerr << "Workload validation failed during " << name << " measurement.\n";
      return false;
    }

    const auto filename = options.output_dir / ("ttt_" + std::string(name) + ".csv");
    if (!recorder.writeCSV(filename.string(), overhead_ticks, tsc_hz)) {
      std::cerr << "Unable to write " << filename << ".\n";
      return false;
    }

    std::cout << "scenario=" << name << '\n'
              << "requested_samples=" << options.samples << '\n'
              << "recorded_samples=" << recorder.size() << '\n'
              << "migration_samples=" << recorder.migrationCount() << '\n'
              << "invalid_samples=" << recorder.invalidCount() << '\n'
              << "dropped_samples=" << recorder.droppedCount() << '\n'
              << "output=" << filename << '\n';
    return recorder.droppedCount() == 0 && recorder.invalidCount() == 0;
  }

} // namespace

int main(const int argc, char** argv) {
  Options options;
  if (!parseOptions(argc, argv, options)) {
    std::cerr << "Usage: " << argv[0]
              << " [--scenario all|maker_book_to_first_order|taker_trade_to_order]"
              << " [--warmup N] [--samples N] [--output-dir DIR]\n";
    return EXIT_FAILURE;
  }

  std::filesystem::create_directories(options.output_dir);
  const auto tsc_hz = Common::calibrateTSCHz();
  if (tsc_hz <= 0.0) {
    std::cerr << "Unable to calibrate the timestamp frequency.\n";
    return EXIT_FAILURE;
  }
  const auto overhead_ticks = Common::measureTSCOverhead();
  Common::LatencyRecorder recorder(options.samples);

  std::cout << "benchmark=strategy_tick_to_first_order\n"
            << "boundary=start immediately before TradeEngine processes one decoded market update; "
               "end immediately after the first ClientRequest LFQueue commit\n"
            << "excludes=market-data network/decode, input queue wait, OrderGateway and TCP send\n"
            << "clock=" << (Common::HAS_HARDWARE_TSC ? "x86_tsc" : "steady_clock_fallback") << '\n'
            << "tsc_hz=" << tsc_hz << '\n'
            << "measurement_overhead_ticks=" << overhead_ticks << '\n'
            << "warmup=" << options.warmup << '\n'
            << "max_tickers=" << Common::ME_MAX_TICKERS << '\n'
            << "max_clients=" << Common::ME_MAX_NUM_CLIENTS << '\n'
            << "max_order_ids=" << Common::ME_MAX_ORDER_IDS << '\n'
            << "max_price_levels=" << Common::ME_MAX_PRICE_LEVELS << '\n'
            << "hot_path_logging=disabled\n";

  bool success = true;
  if (options.scenario == "all" || options.scenario == "maker_book_to_first_order") {
    success = runScenario("maker_book_to_first_order", Common::AlgoType::MAKER,
                          true, runMaker, recorder, options, overhead_ticks,
                          tsc_hz) && success;
  }
  if (options.scenario == "all" || options.scenario == "taker_trade_to_order") {
    success = runScenario("taker_trade_to_order", Common::AlgoType::TAKER,
                          false, runTaker, recorder, options, overhead_ticks,
                          tsc_hz) && success;
  }

  return success ? EXIT_SUCCESS : EXIT_FAILURE;
}

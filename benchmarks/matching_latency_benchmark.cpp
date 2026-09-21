#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>

#include "common/latency_recorder.h"
#include "exchange/matcher/matching_engine.h"

namespace {

  static_assert(Common::ME_MAX_TICKERS >= 1);
  static_assert(Common::ME_MAX_NUM_CLIENTS >= 2);
  static_assert(Common::ME_MAX_ORDER_IDS >= 32);
  static_assert(Common::ME_MAX_PRICE_LEVELS >= 4);

  constexpr Common::TickerId TICKER_ID = 0;
  constexpr Common::ClientId PASSIVE_CLIENT_ID = 0;
  constexpr Common::ClientId AGGRESSIVE_CLIENT_ID = 1;
  constexpr Common::Price BASE_PRICE = 100;
  constexpr Common::Qty ORDER_QTY = 10;
  constexpr std::size_t QUEUE_CAPACITY = 64;

  struct Options {
    std::size_t warmup = 100'000;
    std::size_t samples = 1'000'000;
    std::string scenario = "all";
    std::filesystem::path output_dir = "runs/matching";
  };

  struct Harness {
    Exchange::ClientRequestLFQueue requests{QUEUE_CAPACITY};
    Exchange::ClientResponseLFQueue responses{QUEUE_CAPACITY};
    Exchange::MEMarketUpdateLFQueue market_updates{QUEUE_CAPACITY};
    Exchange::MatchingEngine engine{&requests, &responses, &market_updates};
  };

  [[nodiscard]] bool parseSize(const std::string_view text, std::size_t& value) {
    const auto* begin = text.data();
    const auto* end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
  }

  [[nodiscard]] bool validScenario(const std::string_view scenario) {
    return scenario == "all" || scenario == "add" || scenario == "cancel" ||
           scenario == "match_one" || scenario == "sweep4";
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

  template<typename Queue>
  [[nodiscard]] std::size_t drain(Queue& queue) noexcept {
    std::size_t count = 0;
    while (queue.getNextToRead() != nullptr) {
      queue.updateReadIndex();
      ++count;
    }
    return count;
  }

  [[nodiscard]] bool drainExpected(Harness& harness,
                                   const std::size_t expected_responses,
                                   const std::size_t expected_market_updates) noexcept {
    const auto responses = drain(harness.responses);
    const auto market_updates = drain(harness.market_updates);
    return responses == expected_responses && market_updates == expected_market_updates;
  }

  [[nodiscard]] Exchange::MEClientRequest newOrder(const Common::ClientId client_id,
                                                    const Common::OrderId order_id,
                                                    const Common::Side side,
                                                    const Common::Price price,
                                                    const Common::Qty qty = ORDER_QTY) noexcept {
    return {Exchange::ClientRequestType::NEW, client_id, TICKER_ID, order_id,
            side, price, qty};
  }

  [[nodiscard]] Exchange::MEClientRequest cancelOrder(const Common::ClientId client_id,
                                                       const Common::OrderId order_id) noexcept {
    return {Exchange::ClientRequestType::CANCEL, client_id, TICKER_ID, order_id,
            Common::Side::INVALID, Common::Price_INVALID, Common::Qty_INVALID};
  }

  inline void process(Harness& harness,
                      const Exchange::MEClientRequest& request,
                      Common::LatencyRecorder* recorder) noexcept {
    if (recorder == nullptr) {
      harness.engine.processClientRequest(&request);
      return;
    }

    const auto start = recorder->start();
    harness.engine.processClientRequest(&request);
    recorder->stopAndRecord(start);
  }

  [[nodiscard]] Common::OrderId rotatingOrderId(const std::size_t iteration) noexcept {
    constexpr auto RESERVED_IDS = std::size_t{16};
    const auto usable_ids = Common::ME_MAX_ORDER_IDS - RESERVED_IDS;
    return 1 + static_cast<Common::OrderId>(iteration % usable_ids);
  }

  [[nodiscard]] bool runAdd(Harness& harness,
                            const std::size_t iterations,
                            Common::LatencyRecorder* recorder) noexcept {
    for (std::size_t i = 0; i < iterations; ++i) {
      const auto order_id = rotatingOrderId(i);
      const auto add = newOrder(PASSIVE_CLIENT_ID, order_id, Common::Side::BUY, BASE_PRICE);
      process(harness, add, recorder);
      if (!drainExpected(harness, 1, 1)) {
        return false;
      }

      const auto cancel = cancelOrder(PASSIVE_CLIENT_ID, order_id);
      process(harness, cancel, nullptr);
      if (!drainExpected(harness, 1, 1)) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] bool runCancel(Harness& harness,
                               const std::size_t iterations,
                               Common::LatencyRecorder* recorder) noexcept {
    for (std::size_t i = 0; i < iterations; ++i) {
      const auto order_id = rotatingOrderId(i);
      const auto add = newOrder(PASSIVE_CLIENT_ID, order_id, Common::Side::BUY, BASE_PRICE);
      process(harness, add, nullptr);
      if (!drainExpected(harness, 1, 1)) {
        return false;
      }

      const auto cancel = cancelOrder(PASSIVE_CLIENT_ID, order_id);
      process(harness, cancel, recorder);
      if (!drainExpected(harness, 1, 1)) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] bool runMatchOne(Harness& harness,
                                 const std::size_t iterations,
                                 Common::LatencyRecorder* recorder) noexcept {
    for (std::size_t i = 0; i < iterations; ++i) {
      const auto passive_order_id = rotatingOrderId(i);
      const auto passive = newOrder(PASSIVE_CLIENT_ID, passive_order_id,
                                    Common::Side::SELL, BASE_PRICE);
      process(harness, passive, nullptr);
      if (!drainExpected(harness, 1, 1)) {
        return false;
      }

      const auto aggressive = newOrder(AGGRESSIVE_CLIENT_ID,
                                       Common::ME_MAX_ORDER_IDS - 1,
                                       Common::Side::BUY, BASE_PRICE);
      process(harness, aggressive, recorder);
      if (!drainExpected(harness, 3, 2)) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] bool runSweepFour(Harness& harness,
                                  const std::size_t iterations,
                                  Common::LatencyRecorder* recorder) noexcept {
    constexpr std::size_t LEVEL_COUNT = 4;
    for (std::size_t i = 0; i < iterations; ++i) {
      const auto usable_base_ids = Common::ME_MAX_ORDER_IDS - 16;
      const auto base_order_id = 1 + static_cast<Common::OrderId>((i * LEVEL_COUNT) % usable_base_ids);

      for (std::size_t level = 0; level < LEVEL_COUNT; ++level) {
        const auto passive = newOrder(
            PASSIVE_CLIENT_ID, base_order_id + level, Common::Side::SELL,
            BASE_PRICE + static_cast<Common::Price>(level));
        process(harness, passive, nullptr);
      }
      if (!drainExpected(harness, LEVEL_COUNT, LEVEL_COUNT)) {
        return false;
      }

      const auto aggressive = newOrder(
          AGGRESSIVE_CLIENT_ID, Common::ME_MAX_ORDER_IDS - 1,
          Common::Side::BUY, BASE_PRICE + LEVEL_COUNT - 1,
          ORDER_QTY * LEVEL_COUNT);
      process(harness, aggressive, recorder);
      if (!drainExpected(harness, 1 + 2 * LEVEL_COUNT, 2 * LEVEL_COUNT)) {
        return false;
      }
    }
    return true;
  }

  using ScenarioRunner = bool (*)(Harness&, std::size_t, Common::LatencyRecorder*) noexcept;

  [[nodiscard]] bool runScenario(const std::string_view name,
                                 const ScenarioRunner runner,
                                 Harness& harness,
                                 Common::LatencyRecorder& recorder,
                                 const Options& options,
                                 const std::uint64_t overhead_ticks,
                                 const double tsc_hz) {
    recorder.reset();
    if (!runner(harness, options.warmup, nullptr)) {
      std::cerr << "Workload validation failed during " << name << " warm-up.\n";
      return false;
    }
    if (!runner(harness, options.samples, &recorder)) {
      std::cerr << "Workload validation failed during " << name << " measurement.\n";
      return false;
    }

    const auto filename = options.output_dir / ("matching_" + std::string(name) + ".csv");
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
              << " [--scenario all|add|cancel|match_one|sweep4]"
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

  std::cout << "benchmark=matching_engine_process_client_request\n"
            << "boundary=start immediately before MatchingEngine::processClientRequest; "
               "end immediately after return\n"
            << "includes=MEOrderBook processing plus client-response and market-update queue writes\n"
            << "clock=" << (Common::HAS_HARDWARE_TSC ? "x86_tsc" : "steady_clock_fallback") << '\n'
            << "tsc_hz=" << tsc_hz << '\n'
            << "measurement_overhead_ticks=" << overhead_ticks << '\n'
            << "warmup=" << options.warmup << '\n'
            << "max_tickers=" << Common::ME_MAX_TICKERS << '\n'
            << "max_clients=" << Common::ME_MAX_NUM_CLIENTS << '\n'
            << "max_order_ids=" << Common::ME_MAX_ORDER_IDS << '\n'
            << "max_price_levels=" << Common::ME_MAX_PRICE_LEVELS << '\n'
            << "hot_path_logging=disabled\n";

  Harness harness;
  Common::LatencyRecorder recorder(options.samples);
  bool success = true;

  if (options.scenario == "all" || options.scenario == "add") {
    success = runScenario("add", runAdd, harness, recorder, options,
                          overhead_ticks, tsc_hz) && success;
  }
  if (options.scenario == "all" || options.scenario == "cancel") {
    success = runScenario("cancel", runCancel, harness, recorder, options,
                          overhead_ticks, tsc_hz) && success;
  }
  if (options.scenario == "all" || options.scenario == "match_one") {
    success = runScenario("match_one", runMatchOne, harness, recorder, options,
                          overhead_ticks, tsc_hz) && success;
  }
  if (options.scenario == "all" || options.scenario == "sweep4") {
    success = runScenario("sweep4", runSweepFour, harness, recorder, options,
                          overhead_ticks, tsc_hz) && success;
  }

  return success ? EXIT_SUCCESS : EXIT_FAILURE;
}

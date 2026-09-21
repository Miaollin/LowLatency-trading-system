#include <charconv>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>

#include "common/latency_recorder.h"

namespace {

  struct Options {
    std::size_t warmup = 100'000;
    std::size_t samples = 1'000'000;
    std::string output = "tsc_latency.csv";
  };

  [[nodiscard]] bool parseSize(const std::string_view text, std::size_t& value) {
    const auto* begin = text.data();
    const auto* end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
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
      } else if (argument == "--output" && i + 1 < argc) {
        options.output = argv[++i];
      } else {
        return false;
      }
    }
    return true;
  }

  // A tiny deterministic operation that the compiler cannot remove. This
  // executable validates the timing framework; real benchmarks should replace
  // only this operation while retaining the same recording structure.
  inline void measuredOperation(std::uint64_t& state) noexcept {
    state = state * 2862933555777941757ULL + 3037000493ULL;
#if defined(__GNUC__) || defined(__clang__)
    __asm__ __volatile__("" : "+r"(state) : : "memory");
#endif
  }

} // namespace

int main(const int argc, char** argv) {
  Options options;
  if (!parseOptions(argc, argv, options)) {
    std::cerr << "Usage: " << argv[0]
              << " [--warmup N] [--samples N] [--output FILE]\n";
    return EXIT_FAILURE;
  }

  std::uint64_t state = 1;
  for (std::size_t i = 0; i < options.warmup; ++i) {
    measuredOperation(state);
  }

  const auto tsc_hz = Common::calibrateTSCHz();
  if (tsc_hz <= 0.0) {
    std::cerr << "Unable to calibrate the timestamp frequency.\n";
    return EXIT_FAILURE;
  }

  const auto overhead_ticks = Common::measureTSCOverhead();
  Common::LatencyRecorder recorder(options.samples);

  for (std::size_t i = 0; i < options.samples; ++i) {
    const auto start = recorder.start();
    measuredOperation(state);
    recorder.stopAndRecord(start);
  }

  if (!recorder.writeCSV(options.output, overhead_ticks, tsc_hz)) {
    std::cerr << "Unable to write " << options.output << ".\n";
    return EXIT_FAILURE;
  }

  std::cout << "clock=" << (Common::HAS_HARDWARE_TSC ? "x86_tsc" : "steady_clock_fallback")
            << '\n'
            << "tsc_hz=" << tsc_hz << '\n'
            << "measurement_overhead_ticks=" << overhead_ticks << '\n'
            << "requested_samples=" << options.samples << '\n'
            << "recorded_samples=" << recorder.size() << '\n'
            << "migration_samples=" << recorder.migrationCount() << '\n'
            << "invalid_samples=" << recorder.invalidCount() << '\n'
            << "dropped_samples=" << recorder.droppedCount() << '\n'
            << "output=" << options.output << '\n'
            << "checksum=" << state << '\n';

  return recorder.droppedCount() == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

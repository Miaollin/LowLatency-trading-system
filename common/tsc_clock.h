#pragma once

#include <algorithm>
#include <cstddef>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <thread>
#include <vector>

namespace Common {

  struct TSCStamp {
    std::uint64_t ticks_ = 0;
    std::uint32_t aux_ = 0;
  };

  /// True when the build target provides x86 RDTSCP/TSC_AUX.
  inline constexpr bool HAS_HARDWARE_TSC =
#if defined(__x86_64__) || defined(__i386__)
      true;
#else
      false;
#endif

  /**
   * Read a serialized timestamp.
   *
   * LFENCE prevents surrounding loads/instructions from crossing the timestamp
   * boundary. RDTSCP also returns IA32_TSC_AUX, normally programmed by Linux to
   * identify the logical CPU, so a benchmark can reject samples that migrated.
   *
   * The non-x86 fallback exists so development builds remain portable. It is
   * CLOCK_MONOTONIC time in nanoseconds, not TSC ticks, and must not be reported
   * as an x86 TSC benchmark.
   */
  [[nodiscard]] inline TSCStamp readTSC() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    std::uint32_t lo = 0;
    std::uint32_t hi = 0;
    std::uint32_t aux = 0;

    __asm__ __volatile__(
        "lfence\n\t"
        "rdtscp\n\t"
        "lfence\n\t"
        : "=a"(lo), "=d"(hi), "=c"(aux)
        :
        : "memory");

    return {(static_cast<std::uint64_t>(hi) << 32U) | lo, aux};
#else
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return {static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(now).count()),
            0};
#endif
  }

  /// Retained for existing benchmarks and START_MEASURE/END_MEASURE call sites.
  [[nodiscard]] inline std::uint64_t rdtsc() noexcept {
    return readTSC().ticks_;
  }

  [[nodiscard]] inline std::uint64_t monotonicRawNanos() noexcept {
#if defined(__linux__)
    timespec now{};
    clock_gettime(CLOCK_MONOTONIC_RAW, &now);
    return static_cast<std::uint64_t>(now.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(now.tv_nsec);
#else
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
#endif
  }

  /**
   * Calibrate TSC frequency against a monotonic clock. Calibration is startup
   * work and must never be called from the measured hot path.
   */
  [[nodiscard]] inline double calibrateTSCHz(
      const std::chrono::milliseconds interval = std::chrono::milliseconds(500),
      const std::size_t rounds = 5) {
    if (!HAS_HARDWARE_TSC) {
      return 1'000'000'000.0; // fallback ticks are already nanoseconds
    }

    std::vector<double> estimates;
    estimates.reserve(rounds);

    for (std::size_t round = 0; round < rounds; ++round) {
      const auto wall_start = monotonicRawNanos();
      const auto tsc_start = readTSC();
      std::this_thread::sleep_for(interval);
      const auto tsc_end = readTSC();
      const auto wall_end = monotonicRawNanos();

      const auto elapsed_ns = wall_end - wall_start;
      if (tsc_start.aux_ == tsc_end.aux_ && elapsed_ns != 0 &&
          tsc_end.ticks_ > tsc_start.ticks_) {
        estimates.push_back(static_cast<double>(tsc_end.ticks_ - tsc_start.ticks_) *
                            1'000'000'000.0 / static_cast<double>(elapsed_ns));
      }
    }

    if (estimates.empty()) {
      return 0.0;
    }

    std::sort(estimates.begin(), estimates.end());
    return estimates[estimates.size() / 2];
  }

  [[nodiscard]] inline double tscTicksToNanos(const std::uint64_t ticks,
                                               const double tsc_hz) noexcept {
    return tsc_hz > 0.0 ? static_cast<double>(ticks) * 1'000'000'000.0 / tsc_hz
                        : 0.0;
  }

  /// Median cost of an empty pair of serialized timestamp reads.
  [[nodiscard]] inline std::uint64_t measureTSCOverhead(
      const std::size_t samples = 100'000) {
    std::vector<std::uint64_t> values;
    values.reserve(samples);

    for (std::size_t i = 0; i < samples; ++i) {
      const auto start = readTSC();
      const auto end = readTSC();
      if (start.aux_ == end.aux_ && end.ticks_ >= start.ticks_) {
        values.push_back(end.ticks_ - start.ticks_);
      }
    }

    if (values.empty()) {
      return 0;
    }

    const auto middle = values.begin() + static_cast<std::ptrdiff_t>(values.size() / 2);
    std::nth_element(values.begin(), middle, values.end());
    return *middle;
  }

} // namespace Common

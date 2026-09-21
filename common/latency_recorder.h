#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "tsc_clock.h"

namespace Common {

  /**
   * A fixed-capacity, single-writer latency recorder.
   *
   * Its backing storage is allocated and faulted in before measurement. record()
   * performs no allocation, locking, formatting, or I/O. Raw ticks are retained
   * so conversion and percentile calculation can happen after the run.
   */
  class LatencyRecorder final {
  public:
    explicit LatencyRecorder(const std::size_t capacity)
        : samples_(capacity, 0) {
      // Touch every element before the benchmark to fault backing pages in.
      for (auto& sample : samples_) {
        sample = 0;
      }
    }

    LatencyRecorder() = delete;
    LatencyRecorder(const LatencyRecorder&) = delete;
    LatencyRecorder& operator=(const LatencyRecorder&) = delete;

    [[nodiscard]] TSCStamp start() const noexcept {
      return readTSC();
    }

    bool record(const TSCStamp& start, const TSCStamp& end) noexcept {
      if (start.aux_ != end.aux_) {
        ++migration_count_;
        return false;
      }
      if (end.ticks_ < start.ticks_) {
        ++invalid_count_;
        return false;
      }
      if (size_ == samples_.size()) {
        ++dropped_count_;
        return false;
      }

      samples_[size_++] = end.ticks_ - start.ticks_;
      return true;
    }

    bool stopAndRecord(const TSCStamp& start) noexcept {
      return record(start, readTSC());
    }

    void reset() noexcept {
      size_ = 0;
      migration_count_ = 0;
      invalid_count_ = 0;
      dropped_count_ = 0;
    }

    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] std::size_t capacity() const noexcept { return samples_.size(); }
    [[nodiscard]] std::uint64_t migrationCount() const noexcept { return migration_count_; }
    [[nodiscard]] std::uint64_t invalidCount() const noexcept { return invalid_count_; }
    [[nodiscard]] std::uint64_t droppedCount() const noexcept { return dropped_count_; }
    [[nodiscard]] const std::uint64_t* data() const noexcept { return samples_.data(); }

    bool writeCSV(const std::string& filename,
                  const std::uint64_t overhead_ticks,
                  const double tsc_hz) const {
      std::ofstream output(filename);
      if (!output.is_open()) {
        return false;
      }

      output << "sample,raw_ticks,corrected_ticks,raw_ns,corrected_ns\n";
      for (std::size_t i = 0; i < size_; ++i) {
        const auto raw = samples_[i];
        const auto corrected = raw > overhead_ticks ? raw - overhead_ticks : 0;
        output << i << ',' << raw << ',' << corrected << ','
               << tscTicksToNanos(raw, tsc_hz) << ','
               << tscTicksToNanos(corrected, tsc_hz) << '\n';
      }
      return output.good();
    }

  private:
    std::vector<std::uint64_t> samples_;
    std::size_t size_ = 0;
    std::uint64_t migration_count_ = 0;
    std::uint64_t invalid_count_ = 0;
    std::uint64_t dropped_count_ = 0;
  };

  /**
   * Fixed-capacity recorder for measurements whose endpoints execute on
   * different threads/CPUs. Samples are already CLOCK_MONOTONIC_RAW
   * nanoseconds, so no TSC_AUX equality check or frequency conversion applies.
   */
  class NanosecondLatencyRecorder final {
  public:
    explicit NanosecondLatencyRecorder(const std::size_t capacity)
        : samples_(capacity, 0) {
      for (auto& sample : samples_) {
        sample = 0;
      }
    }

    NanosecondLatencyRecorder() = delete;
    NanosecondLatencyRecorder(const NanosecondLatencyRecorder&) = delete;
    NanosecondLatencyRecorder& operator=(const NanosecondLatencyRecorder&) = delete;

    bool record(const std::uint64_t start_ns,
                const std::uint64_t end_ns) noexcept {
      if (end_ns < start_ns) {
        ++invalid_count_;
        return false;
      }
      if (size_ == samples_.size()) {
        ++dropped_count_;
        return false;
      }
      samples_[size_++] = end_ns - start_ns;
      return true;
    }

    void reset() noexcept {
      size_ = 0;
      invalid_count_ = 0;
      dropped_count_ = 0;
    }

    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] std::uint64_t invalidCount() const noexcept { return invalid_count_; }
    [[nodiscard]] std::uint64_t droppedCount() const noexcept { return dropped_count_; }

    bool writeCSV(const std::string& filename,
                  const std::uint64_t overhead_ns) const {
      std::ofstream output(filename);
      if (!output.is_open()) {
        return false;
      }

      output << "sample,raw_ns,corrected_ns\n";
      for (std::size_t i = 0; i < size_; ++i) {
        const auto raw = samples_[i];
        const auto corrected = raw > overhead_ns ? raw - overhead_ns : 0;
        output << i << ',' << raw << ',' << corrected << '\n';
      }
      return output.good();
    }

  private:
    std::vector<std::uint64_t> samples_;
    std::size_t size_ = 0;
    std::uint64_t invalid_count_ = 0;
    std::uint64_t dropped_count_ = 0;
  };

} // namespace Common

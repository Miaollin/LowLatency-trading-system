#pragma once

#include "tsc_clock.h"

#if defined(LLT_BENCHMARK_MODE)
  // A formal benchmark supplies one outer measurement boundary and records to
  // a preallocated buffer. Disable nested timestamp reads and per-sample logs.
#define START_MEASURE(TAG) do { } while (false)
#define END_MEASURE(TAG, LOGGER) do { } while (false)
#define TTT_MEASURE(TAG, LOGGER) do { } while (false)
#else
  /// Start latency measurement using rdtsc(). Creates a variable called TAG in the local scope.
#define START_MEASURE(TAG) const auto TAG = Common::rdtsc()

  /// End latency measurement using rdtsc(). Expects a variable called TAG to already exist in the local scope.
#define END_MEASURE(TAG, LOGGER)                                                              \
        do {                                                                                  \
          const auto end = Common::rdtsc();                                                   \
          LOGGER.log("% RDTSC "#TAG" %\n", Common::getCurrentTimeStr(&time_str_), (end - TAG)); \
        } while(false)

  /// Log a current timestamp at the time this macro is invoked.
#define TTT_MEASURE(TAG, LOGGER)                                                              \
        do {                                                                                  \
          const auto TAG = Common::getCurrentNanos();                                         \
          LOGGER.log("% TTT "#TAG" %\n", Common::getCurrentTimeStr(&time_str_), TAG);         \
        } while(false)
#endif

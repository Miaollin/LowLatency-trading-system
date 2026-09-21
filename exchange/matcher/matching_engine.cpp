#include "matching_engine.h"

namespace Exchange {
  MatchingEngine::MatchingEngine(ClientRequestLFQueue *client_requests, ClientResponseLFQueue *client_responses,
                                 MEMarketUpdateLFQueue *market_updates, int core_id)
      : incoming_requests_(client_requests), outgoing_ogw_responses_(client_responses), outgoing_md_updates_(market_updates),
#if !defined(LLT_BENCHMARK_MODE) || defined(LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK)
        core_id_(core_id),
#else
        
#endif
        logger_("exchange_matching_engine.log") {
#if defined(LLT_BENCHMARK_MODE) && !defined(LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK)
    (void) core_id;
#endif
    for(size_t i = 0; i < ticker_order_book_.size(); ++i) {
      ticker_order_book_[i] = new MEOrderBook(i, &logger_, this);
    }
  }

  MatchingEngine::~MatchingEngine() {
    stop();

#if !defined(LLT_BENCHMARK_MODE)
    using namespace std::literals::chrono_literals;
    std::this_thread::sleep_for(1s);
#endif

    incoming_requests_ = nullptr;
    outgoing_ogw_responses_ = nullptr;
    outgoing_md_updates_ = nullptr;

    for(auto& order_book : ticker_order_book_) {
      delete order_book;
      order_book = nullptr;
    }
  }

  /// Start and stop the matching engine main thread.
  auto MatchingEngine::start() -> void {
#if defined(LLT_BENCHMARK_MODE) && !defined(LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK)
    FATAL("MatchingEngine::start() is disabled in LLT_BENCHMARK_MODE.");
#else
    run_.store(true, std::memory_order_release);
    thread_ = Common::createAndStartThread(core_id_, "Exchange/MatchingEngine", [this]() { run(); });
    ASSERT(thread_ != nullptr, "Failed to start MatchingEngine thread.");
#endif
  }

  auto MatchingEngine::stop() -> void {
    run_.store(false, std::memory_order_release);
#if !defined(LLT_BENCHMARK_MODE) || defined(LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK)
    if (thread_ != nullptr) {
      if (thread_->joinable()) {
        thread_->join();
      }
      delete thread_;
      thread_ = nullptr;
    }
#endif
  }
}

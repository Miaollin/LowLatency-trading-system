#pragma once

#include <functional>

#if !defined(LLT_BENCHMARK_MODE) || defined(LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK)
#include "common/thread_utils.h"
#endif
#include "common/time_utils.h"
#include "common/lf_queue.h"
#include "common/macros.h"
#include "common/logging.h"
#if defined(LLT_BENCHMARK_MODE)
#include "common/latency_recorder.h"
#endif

#include "exchange/order_server/client_request.h"
#include "exchange/order_server/client_response.h"
#include "exchange/market_data/market_update.h"

#include "market_order_book.h"

#include "feature_engine.h"
#include "position_keeper.h"
#include "order_manager.h"
#include "risk_manager.h"

#include "market_maker.h"
#include "liquidity_taker.h"

namespace Trading {
  class TradeEngine {
  public:
    TradeEngine(Common::ClientId client_id,
                AlgoType algo_type,
                const TradeEngineCfgHashMap &ticker_cfg,
                Exchange::ClientRequestLFQueue *client_requests,
                Exchange::ClientResponseLFQueue *client_responses,
                Exchange::MEMarketUpdateLFQueue *market_updates,
                int core_id = -1);

    ~TradeEngine();

    /// Start and stop the trade engine main thread.
    auto start() -> void {
#if defined(LLT_BENCHMARK_MODE) && !defined(LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK)
      FATAL("TradeEngine::start() is disabled in LLT_BENCHMARK_MODE.");
#else
      run_.store(true, std::memory_order_release);
      thread_ = Common::createAndStartThread(core_id_, "Trading/TradeEngine", [this] { run(); });
      ASSERT(thread_ != nullptr, "Failed to start TradeEngine thread.");
#endif
    }

    auto stop() -> void {
#if !defined(LLT_BENCHMARK_MODE) || defined(LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK)
      if (thread_ == nullptr) {
        run_.store(false, std::memory_order_release);
        return;
      }
      while(incoming_ogw_responses_->size() || incoming_md_updates_->size()) {
        logger_.log("%:% %() % Sleeping till all updates are consumed ogw-size:% md-size:%\n", __FILE__, __LINE__, __FUNCTION__,
                    Common::getCurrentTimeStr(&time_str_), incoming_ogw_responses_->size(), incoming_md_updates_->size());

        using namespace std::literals::chrono_literals;
        std::this_thread::sleep_for(10ms);
      }

      logger_.log("%:% %() % POSITIONS\n%\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_),
                  position_keeper_.toString());

      run_.store(false, std::memory_order_release);
      if (thread_->joinable()) {
        thread_->join();
      }
      delete thread_;
      thread_ = nullptr;
#else
      run_.store(false, std::memory_order_release);
#endif
    }

    /// Main loop for this thread - processes incoming client responses and market data updates which in turn may generate client requests.
    auto run() noexcept -> void;

    /// Process one already-decoded market update synchronously.
    auto processMarketUpdate(const Exchange::MEMarketUpdate *market_update) noexcept -> void;

#if defined(LLT_BENCHMARK_MODE)
    /// Measure from immediately before processing an update to the first client
    /// request queue commit caused by that update. Returns false when the update
    /// did not produce a client request.
    auto processMarketUpdateForBenchmark(const Exchange::MEMarketUpdate *market_update,
                                         Common::LatencyRecorder *recorder) noexcept -> bool;
#endif

#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
    auto setMarketDequeueObserver(std::function<void(std::uint64_t)> observer) -> void {
      market_dequeue_observer_ = std::move(observer);
    }

    auto setOrderPublishObserver(std::function<void(std::uint64_t)> observer) -> void {
      order_publish_observer_ = std::move(observer);
    }
#endif

    /// Write a client request to the lock free queue for the order server to consume and send to the exchange.
    auto sendClientRequest(const Exchange::MEClientRequest *client_request) noexcept -> void;

    /// Process changes to the order book - updates the position keeper, feature engine and informs the trading algorithm about the update.
    auto onOrderBookUpdate(TickerId ticker_id, Price price, Side side, MarketOrderBook *book) noexcept -> void;

    /// Process trade events - updates the  feature engine and informs the trading algorithm about the trade event.
    auto onTradeUpdate(const Exchange::MEMarketUpdate *market_update, MarketOrderBook *book) noexcept -> void;

    /// Process client responses - updates the position keeper and informs the trading algorithm about the response.
    auto onOrderUpdate(const Exchange::MEClientResponse *client_response) noexcept -> void;

    /// Function wrappers to dispatch order book updates, trade events and client responses to the trading algorithm.
    std::function<void(TickerId ticker_id, Price price, Side side, MarketOrderBook *book)> algoOnOrderBookUpdate_;
    std::function<void(const Exchange::MEMarketUpdate *market_update, MarketOrderBook *book)> algoOnTradeUpdate_;
    std::function<void(const Exchange::MEClientResponse *client_response)> algoOnOrderUpdate_;

    auto initLastEventTime() {
      last_event_time_ = Common::getCurrentNanos();
    }

    auto silentSeconds() {
      return (Common::getCurrentNanos() - last_event_time_) / NANOS_TO_SECS;
    }

    auto clientId() const {
      return client_id_;
    }

    /// Deleted default, copy & move constructors and assignment-operators.
    TradeEngine() = delete;

    TradeEngine(const TradeEngine &) = delete;

    TradeEngine(const TradeEngine &&) = delete;

    TradeEngine &operator=(const TradeEngine &) = delete;

    TradeEngine &operator=(const TradeEngine &&) = delete;

  private:
    /// This trade engine's ClientId.
    const ClientId client_id_;

    /// Hash map container from TickerId -> MarketOrderBook.
    MarketOrderBookHashMap ticker_order_book_;

    /// Lock free queues.
    /// One to publish outgoing client requests to be consumed by the order gateway and sent to the exchange.
    /// Second to consume incoming client responses from, written to by the order gateway based on data received from the exchange.
    /// Third to consume incoming market data updates from, written to by the market data consumer based on data received from the exchange.
    Exchange::ClientRequestLFQueue *outgoing_ogw_requests_ = nullptr;
    Exchange::ClientResponseLFQueue *incoming_ogw_responses_ = nullptr;
    Exchange::MEMarketUpdateLFQueue *incoming_md_updates_ = nullptr;

    Nanos last_event_time_ = 0;
    std::atomic<bool> run_{false};
#if !defined(LLT_BENCHMARK_MODE) || defined(LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK)
    const int core_id_ = -1;
    std::thread *thread_ = nullptr;
#endif

    std::string time_str_;
    Logger logger_;

    /// Feature engine for the trading algorithms.
    FeatureEngine feature_engine_;

    /// Position keeper to track position, pnl and volume.
    PositionKeeper position_keeper_;

    /// Risk manager to track and perform pre-trade risk checks.
    RiskManager risk_manager_;

    /// Order manager to simplify the task of managing orders for the trading algorithms.
    OrderManager order_manager_;

    /// Market making or liquidity taking algorithm instance - only one of these is created in a single trade engine instance.
    MarketMaker *mm_algo_ = nullptr;
    LiquidityTaker *taker_algo_ = nullptr;

#if defined(LLT_BENCHMARK_MODE)
    Common::LatencyRecorder *ttt_recorder_ = nullptr;
    Common::TSCStamp ttt_start_;
    bool ttt_measurement_active_ = false;
    bool ttt_endpoint_reached_ = false;
#endif

#if defined(LLT_TICK_TO_KERNEL_SEND_STAGE_DIAGNOSTIC)
    std::function<void(std::uint64_t)> market_dequeue_observer_ = nullptr;
    std::function<void(std::uint64_t)> order_publish_observer_ = nullptr;
#endif

    /// Default methods to initialize the function wrappers.
    auto defaultAlgoOnOrderBookUpdate(TickerId ticker_id, Price price, Side side, MarketOrderBook *) noexcept -> void {
      logger_.log("%:% %() % ticker:% price:% side:%\n", __FILE__, __LINE__, __FUNCTION__,
                  Common::getCurrentTimeStr(&time_str_), ticker_id, Common::priceToString(price).c_str(),
                  Common::sideToString(side).c_str());
    }

    auto defaultAlgoOnTradeUpdate(const Exchange::MEMarketUpdate *market_update, MarketOrderBook *) noexcept -> void {
      logger_.log("%:% %() % %\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_),
                  market_update->toString().c_str());
    }

    auto defaultAlgoOnOrderUpdate(const Exchange::MEClientResponse *client_response) noexcept -> void {
      logger_.log("%:% %() % %\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_),
                  client_response->toString().c_str());
    }
  };
}

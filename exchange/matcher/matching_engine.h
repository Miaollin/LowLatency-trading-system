#pragma once

#if !defined(LLT_BENCHMARK_MODE) || defined(LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK)
#include "common/thread_utils.h"
#endif
#include <atomic>
#include "common/lf_queue.h"
#include "common/macros.h"

#include "order_server/client_request.h"
#include "order_server/client_response.h"
#include "market_data/market_update.h"

#include "me_order_book.h"

namespace Exchange {
  class MatchingEngine final {
  public:
    MatchingEngine(ClientRequestLFQueue *client_requests,
                   ClientResponseLFQueue *client_responses,
                   MEMarketUpdateLFQueue *market_updates,
                   int core_id = -1);

    ~MatchingEngine();

    /// Start and stop the matching engine main thread.
    auto start() -> void;

    auto stop() -> void;

    /// Called to process a client request read from the lock free queue sent by the order server.
    auto processClientRequest(const MEClientRequest *client_request) noexcept {
      auto order_book = ticker_order_book_[client_request->ticker_id_];
      switch (client_request->type_) {
        case ClientRequestType::NEW: {
          START_MEASURE(Exchange_MEOrderBook_add);
          order_book->add(client_request->client_id_, client_request->order_id_, client_request->ticker_id_,
                           client_request->side_, client_request->price_, client_request->qty_);
          END_MEASURE(Exchange_MEOrderBook_add, logger_);
        }
          break;

        case ClientRequestType::CANCEL: {
          START_MEASURE(Exchange_MEOrderBook_cancel);
          order_book->cancel(client_request->client_id_, client_request->order_id_, client_request->ticker_id_);
          END_MEASURE(Exchange_MEOrderBook_cancel, logger_);
        }
          break;

        default: {
          FATAL("Received invalid client-request-type:" + clientRequestTypeToString(client_request->type_));
        }
          break;
      }
    }

    /// Write client responses to the lock free queue for the order server to consume.
    auto sendClientResponse(const MEClientResponse *client_response) noexcept {
#if !defined(LLT_BENCHMARK_MODE)
      logger_.log("%:% %() % Sending %\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_), client_response->toString());
#endif
      auto next_write = outgoing_ogw_responses_->getNextToWriteTo();
      *next_write = std::move(*client_response);
      outgoing_ogw_responses_->updateWriteIndex();
      TTT_MEASURE(T4t_MatchingEngine_LFQueue_write, logger_);
    }

    /// Write market data update to the lock free queue for the market data publisher to consume.
    auto sendMarketUpdate(const MEMarketUpdate *market_update) noexcept {
#if !defined(LLT_BENCHMARK_MODE)
      logger_.log("%:% %() % Sending %\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_), market_update->toString());
#endif
      auto next_write = outgoing_md_updates_->getNextToWriteTo();
      *next_write = *market_update;
      outgoing_md_updates_->updateWriteIndex();
      TTT_MEASURE(T4_MatchingEngine_LFQueue_write, logger_);
    }

    /// Main loop for this thread - processes incoming client requests which in turn generates client responses and market updates.
    auto run() noexcept {
#if !defined(LLT_BENCHMARK_MODE)
      logger_.log("%:% %() %\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_));
#endif
      while (run_.load(std::memory_order_relaxed)) {
        const auto me_client_request = incoming_requests_->getNextToRead();
        if (LIKELY(me_client_request)) {
          TTT_MEASURE(T3_MatchingEngine_LFQueue_read, logger_);

#if !defined(LLT_BENCHMARK_MODE)
          logger_.log("%:% %() % Processing %\n", __FILE__, __LINE__, __FUNCTION__, Common::getCurrentTimeStr(&time_str_),
                      me_client_request->toString());
#endif
          START_MEASURE(Exchange_MatchingEngine_processClientRequest);
          processClientRequest(me_client_request);
          END_MEASURE(Exchange_MatchingEngine_processClientRequest, logger_);
          incoming_requests_->updateReadIndex();
        }
      }
    }

    /// Deleted default, copy & move constructors and assignment-operators.
    MatchingEngine() = delete;

    MatchingEngine(const MatchingEngine &) = delete;

    MatchingEngine(const MatchingEngine &&) = delete;

    MatchingEngine &operator=(const MatchingEngine &) = delete;

    MatchingEngine &operator=(const MatchingEngine &&) = delete;

  private:
    /// Hash map container from TickerId -> MEOrderBook.
    OrderBookHashMap ticker_order_book_;

    /// Lock free queues.
    /// One to consume incoming client requests sent by the order server.
    /// Second to publish outgoing client responses to be consumed by the order server.
    /// Third to publish outgoing market updates to be consumed by the market data publisher.
    ClientRequestLFQueue *incoming_requests_ = nullptr;
    ClientResponseLFQueue *outgoing_ogw_responses_ = nullptr;
    MEMarketUpdateLFQueue *outgoing_md_updates_ = nullptr;

    std::atomic<bool> run_{false};
#if !defined(LLT_BENCHMARK_MODE) || defined(LLT_ENABLE_COMPONENT_THREADS_IN_BENCHMARK)
    const int core_id_ = -1;
    std::thread *thread_ = nullptr;
#endif

    std::string time_str_;
    Logger logger_;
  };
}

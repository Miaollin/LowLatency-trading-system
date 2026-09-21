#pragma once

#include <functional>
#include <map>
#include <atomic>

#include "common/thread_utils.h"
#include "common/lf_queue.h"
#include "common/macros.h"
#include "common/mcast_socket.h"

#include "exchange/market_data/market_update.h"

namespace Trading {
  class MarketDataConsumer {
  public:
    MarketDataConsumer(Common::ClientId client_id, Exchange::MEMarketUpdateLFQueue *market_updates, const std::string &iface,
                       const std::string &snapshot_ip, int snapshot_port,
                       const std::string &incremental_ip, int incremental_port,
                       int core_id = -1);

    ~MarketDataConsumer() {
      stop();
    }

    /// Start and stop the market data consumer main thread.
    auto start() {
      run_.store(true, std::memory_order_release);
      thread_ = Common::createAndStartThread(core_id_, "Trading/MarketDataConsumer", [this]() { run(); });
      ASSERT(thread_ != nullptr, "Failed to start MarketData thread.");
    }

    auto stop() -> void {
      run_.store(false, std::memory_order_release);
      if (thread_ != nullptr) {
        if (thread_->joinable()) {
          thread_->join();
        }
        delete thread_;
        thread_ = nullptr;
      }
    }

#if defined(LLT_TICK_TO_KERNEL_SEND_BENCHMARK)
    /// Called before a correctly sequenced incremental update is made visible
    /// to the TradeEngine. The timestamp was captured immediately after the
    /// UDP recv() that supplied this update returned.
    auto setIncrementalReceiveObserver(
        std::function<void(const Exchange::MDPMarketUpdate &, std::uint64_t)> observer) -> void {
      incremental_receive_observer_ = std::move(observer);
    }

    /// Called after the update is committed to the TradeEngine LFQueue. This is
    /// used only to make benchmark setup/drain checks race-free.
    auto setIncrementalPublishObserver(
        std::function<void(std::size_t)> observer) -> void {
      incremental_publish_observer_ = std::move(observer);
    }
#endif

    /// Deleted default, copy & move constructors and assignment-operators.
    MarketDataConsumer() = delete;

    MarketDataConsumer(const MarketDataConsumer &) = delete;

    MarketDataConsumer(const MarketDataConsumer &&) = delete;

    MarketDataConsumer &operator=(const MarketDataConsumer &) = delete;

    MarketDataConsumer &operator=(const MarketDataConsumer &&) = delete;

  private:
    /// Track the next expected sequence number on the incremental market data stream, used to detect gaps / drops.
    size_t next_exp_inc_seq_num_ = 1;

    /// Lock free queue on which decoded market data updates are pushed to, to be consumed by the trade engine.
    Exchange::MEMarketUpdateLFQueue *incoming_md_updates_ = nullptr;

    std::atomic<bool> run_{false};
    const int core_id_ = -1;
    std::thread *thread_ = nullptr;

    std::string time_str_;
    Logger logger_;

    /// Multicast subscriber sockets for the incremental and market data streams.
    Common::McastSocket incremental_mcast_socket_, snapshot_mcast_socket_;

    /// Tracks if we are currently in the process of recovering / synchronizing with the snapshot market data stream either because we just started up or we dropped a packet.
    bool in_recovery_ = false;

    /// Information for the snapshot multicast stream.
    const std::string iface_, snapshot_ip_;
    const int snapshot_port_;

    /// Containers to queue up market data updates from the snapshot and incremental channels, queued up in order of increasing sequence numbers.
    typedef std::map<size_t, Exchange::MEMarketUpdate> QueuedMarketUpdates;
    QueuedMarketUpdates snapshot_queued_msgs_, incremental_queued_msgs_;

#if defined(LLT_TICK_TO_KERNEL_SEND_BENCHMARK)
    std::function<void(const Exchange::MDPMarketUpdate &, std::uint64_t)>
        incremental_receive_observer_ = nullptr;
    std::function<void(std::size_t)> incremental_publish_observer_ = nullptr;
#endif

  private:
    /// Main loop for this thread - reads and processes messages from the multicast sockets - the heavy lifting is in the recvCallback() and checkSnapshotSync() methods.
    auto run() noexcept -> void;

    /// Process a market data update, the consumer needs to use the socket parameter to figure out whether this came from the snapshot or the incremental stream.
    auto recvCallback(McastSocket *socket) noexcept -> void;

    /// Queue up a message in the *_queued_msgs_ containers, first parameter specifies if this update came from the snapshot or the incremental streams.
    auto queueMessage(bool is_snapshot, const Exchange::MDPMarketUpdate *request);

    /// Start the process of snapshot synchronization by subscribing to the snapshot multicast stream.
    auto startSnapshotSync() -> void;

    /// Check if a recovery / synchronization is possible from the queued up market data updates from the snapshot and incremental market data streams.
    auto checkSnapshotSync() -> void;
  };
}

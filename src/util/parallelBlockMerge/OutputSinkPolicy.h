// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_OUTPUTSINKPOLICY_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_OUTPUTSINKPOLICY_H

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Iterators.h"
#include "util/NoCopyNoMove.h"

// The Boost.Asio based `AsioInOrderBlockSink` at the bottom of this file
// requires coroutines and is therefore not available in the C++17 backports
// mode.
#ifndef QLEVER_CPP_17
#include <absl/container/node_hash_map.h>

#include <atomic>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#endif

// The output policy of the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`): the `BlockSink` concept, and
// the `InOrderBlockSink` that turns the concurrently produced blocks back into
// a single sequential range.
namespace ad_utility::parallelBlockMerge {

// The requirements of the `BlockSink` concept below, see there for the
// documentation.
template <typename T, typename Block>
CPP_requires(BlockSink_, requires(T& sink, size_t numChunks, size_t chunkIndex,
                                  Block block, std::exception_ptr exception)(
                             sink.setNumChunks(numChunks),
                             sink(chunkIndex, std::move(block)),
                             sink.finishChunk(chunkIndex),
                             sink.pushException(std::move(exception))));

// The output policy of the parallel merge. The merge first announces the total
// number of chunks via `setNumChunks`, and then, for every chunk, calls
// `operator()` once per finished output block of that chunk and finally
// `finishChunk` exactly once. If a worker encounters an exception, it forwards
// it via `pushException`.
//
// `operator()`, `finishChunk`, and `pushException` are called concurrently from
// all worker threads and therefore all have to be thread-safe. `setNumChunks`
// is called exactly once, before any of the other functions.
//
// IMPORTANT: `finishChunk` and `pushException` must never throw and therefore
// have to be `noexcept`. The core calls them on the paths that clean up after a
// failed chunk, where an exception would leave the consumer waiting forever for
// a chunk that is never finished. The concept cannot express this requirement
// (a `noexcept` clause is not available in the C++17 emulation of the
// concepts), so the core enforces it with a `static_assert` instead, see
// `ParallelMergeState`. In contrast, `operator()` *may* throw; the core catches
// such an exception and forwards it via `pushException`.
template <typename T, typename Block>
CPP_concept BlockSink = CPP_requires_ref(BlockSink_, T, Block);

// A `BlockSink` that turns the concurrently produced blocks back into a single
// sequential range in which the blocks of chunk `0` come first, then those of
// chunk `1`, and so on. Within a chunk, the blocks appear in the order in which
// they were pushed. It buffers at most `maxBufferedBlocksPerChunk` blocks per
// chunk; a producer that exceeds this limit is blocked until the consumer has
// caught up.
//
// DEADLOCK-FREEDOM: The consumer always drains the lowest chunk that has not
// yet been fully consumed, so a producer of a *higher* chunk may well block on
// a full buffer. This is intended back-pressure and it never deadlocks, because
// the core dispatches the chunks in strictly increasing order of their index
// and keeps at most `MergeScheduler::maxParallelism()` of them in flight. Hence
// the lowest not-yet-finished chunk always owns a worker thread of its own and
// therefore always makes progress; once it is finished, the consumer moves on
// to the next chunk and thereby unblocks its producer, and so on.
//
// NOTE: The class is neither copyable nor movable, because the producers and
// the consumer refer to it by reference.
template <typename Block>
class InOrderBlockSink : public ad_utility::NoCopyNoMove {
 private:
  // The state of a single chunk.
  struct PerChunk {
    std::queue<Block> blocks_{};
    bool finished_ = false;
  };

  size_t maxBufferedBlocksPerChunk_;
  std::mutex mutex_;
  std::condition_variable consumerCanProceed_;
  std::condition_variable producerCanProceed_;
  std::vector<PerChunk> chunks_;
  size_t numChunks_ = 0;
  // NOTE: This flag is required in addition to `numChunks_`, because the
  // consumer may already start to pull blocks before `setNumChunks` was called,
  // and because a merge with zero chunks is legal.
  bool numChunksIsSet_ = false;
  size_t nextChunkToRead_ = 0;
  bool aborted_ = false;
  std::exception_ptr exception_;

 public:
  using value_type = Block;

  // Construct from the maximal number of blocks that are buffered per chunk.
  // The value has to be at least one.
  explicit InOrderBlockSink(size_t maxBufferedBlocksPerChunk = 2)
      : maxBufferedBlocksPerChunk_{maxBufferedBlocksPerChunk} {
    AD_CONTRACT_CHECK(maxBufferedBlocksPerChunk > 0);
  }

  // Abort, so that no producer is left blocked when this sink goes away.
  ~InOrderBlockSink() { abort(); }

  // Announce the total number of chunks. Call this exactly once, and before any
  // call to `operator()` or `finishChunk`.
  void setNumChunks(size_t numChunks) {
    std::unique_lock lock{mutex_};
    AD_CONTRACT_CHECK(!numChunksIsSet_);
    numChunks_ = numChunks;
    numChunksIsSet_ = true;
    chunks_ = std::vector<PerChunk>(numChunks);
    lock.unlock();
    consumerCanProceed_.notify_all();
  }

  // Push a finished output `block` of the chunk with the given `chunkIndex`.
  // Block while the buffer of that chunk is full, unless the sink was aborted
  // or an exception was pushed, in which case the `block` is silently dropped.
  // Thread-safe.
  void operator()(size_t chunkIndex, Block block) {
    std::unique_lock lock{mutex_};
    AD_CONTRACT_CHECK(numChunksIsSet_);
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    auto& chunk = chunks_[chunkIndex];
    producerCanProceed_.wait(lock, [this, &chunk] {
      return chunk.blocks_.size() < maxBufferedBlocksPerChunk_ || aborted_ ||
             exception_ != nullptr;
    });
    if (aborted_ || exception_ != nullptr) {
      return;
    }
    chunk.blocks_.push(std::move(block));
    lock.unlock();
    consumerCanProceed_.notify_all();
  }

  // Announce that no further blocks will be pushed for the chunk with the given
  // `chunkIndex`. Call this exactly once per chunk. Thread-safe.
  //
  // NOTE: This must never throw, because the core calls it also while it cleans
  // up after a failed chunk, where an exception would leave the consumer
  // waiting for a chunk that is never finished. See the `BlockSink` concept
  // above.
  void finishChunk(size_t chunkIndex) noexcept {
    ad_utility::terminateIfThrows(
        [this, chunkIndex] {
          std::unique_lock lock{mutex_};
          AD_CONTRACT_CHECK(numChunksIsSet_);
          AD_CONTRACT_CHECK(chunkIndex < numChunks_);
          chunks_[chunkIndex].finished_ = true;
          lock.unlock();
          consumerCanProceed_.notify_all();
        },
        "Locking or unlocking a mutex in `InOrderBlockSink::finishChunk` "
        "failed.");
  }

  // Forward an `exception` to the consumer, which will rethrow it. Only the
  // first pushed exception is stored, all later ones are ignored. Pushing an
  // exception also unblocks all producers. Thread-safe.
  void pushException(std::exception_ptr exception) noexcept {
    ad_utility::terminateIfThrows(
        [this, &exception] {
          std::unique_lock lock{mutex_};
          if (exception_ == nullptr) {
            exception_ = std::move(exception);
          }
          lock.unlock();
          consumerCanProceed_.notify_all();
          producerCanProceed_.notify_all();
        },
        "Locking or unlocking a mutex in `InOrderBlockSink::pushException` "
        "failed.");
  }

  // Abort the sink from the consuming side. All blocked producers are unblocked
  // and all further blocks are dropped, and the consumer stops yielding blocks.
  // This is called by the destructor, so that a consumer that stops iterating
  // early (or that exits via an exception) never leaves a producer behind.
  // Thread-safe.
  void abort() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          std::unique_lock lock{mutex_};
          aborted_ = true;
          lock.unlock();
          consumerCanProceed_.notify_all();
          producerCanProceed_.notify_all();
        },
        "Locking or unlocking a mutex in `InOrderBlockSink::abort` failed.");
  }

  // Return a lazy range that yields all blocks of chunk `0`, then all blocks of
  // chunk `1`, and so on. Iterating over the range blocks until the next block
  // is available. If an exception was pushed, it is rethrown from the range.
  // Call this at most once, and only from a single (consuming) thread.
  ad_utility::InputRangeTypeErased<Block> blocks() {
    struct BlockRange : public ad_utility::InputRangeFromGet<Block> {
      InOrderBlockSink* sink_;
      explicit BlockRange(InOrderBlockSink* sink) : sink_{sink} {}
      std::optional<Block> get() override { return sink_->popNextBlock(); }
    };
    return ad_utility::InputRangeTypeErased<Block>{
        std::make_unique<BlockRange>(this)};
  }

 private:
  // Return the next block in the global order, or `std::nullopt` if all chunks
  // are exhausted or the sink was aborted. Rethrow a pushed exception. Block
  // until one of these conditions holds.
  std::optional<Block> popNextBlock() {
    std::unique_lock lock{mutex_};
    while (true) {
      if (exception_ != nullptr) {
        auto exception = exception_;
        lock.unlock();
        std::rethrow_exception(exception);
      }
      if (aborted_) {
        return std::nullopt;
      }
      if (numChunksIsSet_ && nextChunkToRead_ >= numChunks_) {
        return std::nullopt;
      }
      if (numChunksIsSet_) {
        auto& chunk = chunks_[nextChunkToRead_];
        if (!chunk.blocks_.empty()) {
          Block block = std::move(chunk.blocks_.front());
          chunk.blocks_.pop();
          lock.unlock();
          producerCanProceed_.notify_all();
          return block;
        }
        if (chunk.finished_) {
          ++nextChunkToRead_;
          continue;
        }
      }
      consumerCanProceed_.wait(lock);
    }
  }
};

#ifndef QLEVER_CPP_17
// ___________________________________________________________________________
// The Boost.Asio based alternative to `InOrderBlockSink`.
// ___________________________________________________________________________

namespace net = boost::asio;

// The Boost.Asio based counterpart of `InOrderBlockSink` above. It also turns
// the concurrently produced blocks back into a single sequential range in which
// the blocks of chunk `0` come first, then those of chunk `1`, and so on, but
// it never blocks a thread: a producer that has to wait for the consumer to
// catch up, as well as a consumer that has to wait for the next block, suspend
// their coroutine and thereby release their thread.
//
// Each chunk that is currently active owns a single channel with a capacity of
// `maxBufferedBlocksPerChunk`, via which its producer sends the finished output
// blocks to the consumer. Suspending in `async_send` on a full channel *is* the
// back-pressure. The end of a chunk is signalled by an explicit `std::nullopt`
// sentinel, upon which the consumer erases the state of that chunk, so that the
// memory consumption is proportional to the number of chunks that are in flight
// and not to the total number of chunks.
//
// The `mutex_` guards the ordinary shared state only (the map of channels, the
// index of the chunk that is currently read, and the exception). It is *never*
// held while a channel operation is performed, and in particular never across a
// suspension point. The channels are held by `shared_ptr`, so that a producer
// or the consumer may keep one alive across a suspension even though the
// consumer erases its map entry as soon as the chunk is done.
//
// DEADLOCK-FREEDOM: The consumer always drains the lowest chunk that has not
// yet been fully consumed, so the producer of a *higher* chunk may well fill
// its channel and suspend. In contrast to `InOrderBlockSink` this is not a
// problem even if there is only a single thread, because such a producer
// suspends its coroutine instead of blocking its thread. In particular the
// number of chunks that are in flight does not have to be bounded by the
// available parallelism, see `AsioParallelMergeState`.
//
// TODO<joka921> The teardown of this sink (`abort()` and `pushException()`) is
// built on `cancel()` and `close()`, which is the minimal thing that works but
// which is not airtight. The following cases are known to be fishy; all of them
// are benign for the way the parallel merge uses this class, but they should be
// revisited before this sink is used more widely.
//
// 1. THE MAIN HOLE: an operation that is initiated just after the teardown
//    sweep has passed it. `push`, `finishChunk`, and `nextBlock` check
//    `stopRequested_` while they hold the mutex, but they initiate their
//    channel operation only after they have released it. An `abort()` in that
//    window neither sees the operation (it is not suspended yet) nor stops it.
//    It is only harmless because the sweep additionally `close()`s every
//    channel, so the operation fails immediately instead of suspending -- but a
//    channel that is created in that very window (by a `finishChunk` of a chunk
//    that never pushed a block) is created *before* the sweep records it and
//    closed *after*, so the send can still suspend forever. A producer that
//    hangs this way also keeps the `AsioParallelMergeState` alive forever,
//    because every chunk coroutine holds a `shared_ptr` to it. Closing this
//    hole properly needs either per-operation cancellation slots (whose
//    cancellation state is sticky and therefore also catches an operation that
//    has not started yet), or an inversion of the throttling in which a waiter
//    is woken by a *buffered value* rather than by an edge-triggered
//    `cancel()`.
// 2. `cancel()` MUST BE CALLED BEFORE `close()`, NEVER AFTER. `cancel()` tells
// a
//    suspended sender from a suspended receiver by the internal send state of
//    the channel, which `close()` overwrites. Cancelling an already closed
//    channel therefore `static_cast`s a suspended send operation to a receive
//    operation, which is undefined behavior. This is why `requestStop()` runs
//    exactly once per channel (`stopSweepDone_`) and why a channel that is
//    created after the sweep is closed immediately and never cancelled.
// 3. `close()` alone would not do. It does not wake a sender that is already
//    suspended on a full channel, it only makes *later* sends fail. And
//    `cancel()` alone would not do either, because it is edge-triggered: a send
//    that is initiated after the cancel simply suspends again. Only the
//    combination covers both, and only in that order (see 2).
// 4. `nextBlock()` interprets *any* error of `async_receive` as "the merge was
//    stopped" and asserts `stopRequested_`. If a caller ever attaches a
//    cancellation slot to one of these operations, or cancels the surrounding
//    coroutine, that assertion fires instead of the cancellation being handled.
// 5. `push()` drops its block whenever the send reports an error, without
//    distinguishing "was not delivered" from "was delivered". That is correct
//    here because a channel is only ever cancelled or closed while the merge is
//    being torn down, but it would silently lose data if `cancel()` were used
//    for anything else.
// 6. The `node_hash_map` is redundant by now. Its stable addresses were needed
//    when the channels were stored by value; with `shared_ptr` values a
//    `flat_hash_map` would do.
//
// NOTE: The class is neither copyable nor movable, because the producers and
// the consumer refer to it by reference.
template <typename Block>
class AsioInOrderBlockSink : public ad_utility::NoCopyNoMove {
 public:
  using value_type = Block;
  // The value that travels through a channel. A `std::nullopt` is the
  // end-of-chunk sentinel. NOTE: The `std::optional` is also required because
  // `Block` need not be default-constructible, but the completion signature of
  // a channel has to be.
  using OptionalBlock = std::optional<Block>;
  // The channel that transports the finished output blocks of a single chunk
  // from its producer to the consumer.
  using BlockChannel = net::experimental::concurrent_channel<void(
      boost::system::error_code, OptionalBlock)>;
  // The channels are shared, because both the producer of a chunk and the
  // consumer may hold on to one across a suspension, while the consumer erases
  // the map entry of a chunk as soon as that chunk is done.
  using SharedBlockChannel = std::shared_ptr<BlockChannel>;

 private:
  net::any_io_executor executor_;
  size_t maxBufferedBlocksPerChunk_;
  size_t numChunks_;
  // Guard the members below, and only those. NOTE: No channel operation is ever
  // performed while this mutex is held.
  std::mutex mutex_;
  absl::node_hash_map<size_t, SharedBlockChannel> chunks_;
  size_t nextChunkToRead_ = 0;
  std::exception_ptr exception_;
  // Whether the teardown sweep of `requestStop()` has already run. It must run
  // exactly once per channel, see the TODO above.
  bool stopSweepDone_ = false;
  // Set as soon as the merge is stopped, either because the consumer has
  // abandoned it or because a producer has pushed an exception. NOTE: This is
  // an atomic (although it is also guarded by `mutex_`), such that a producer
  // can cheaply poll it between two output blocks.
  std::atomic<bool> stopRequested_{false};

 public:
  // Construct from the `executor` on which the channels are created, the total
  // number of chunks, and the maximal number of blocks that are buffered per
  // chunk (which has to be at least one).
  AsioInOrderBlockSink(net::any_io_executor executor, size_t numChunks,
                       size_t maxBufferedBlocksPerChunk = 2)
      : executor_{std::move(executor)},
        maxBufferedBlocksPerChunk_{maxBufferedBlocksPerChunk},
        numChunks_{numChunks} {
    AD_CONTRACT_CHECK(maxBufferedBlocksPerChunk > 0);
  }

  // Return `true` if the merge was stopped, either by `abort()` or by
  // `pushException()`. A producer should poll this between two output blocks,
  // so that it does not do any superfluous work.
  bool stopRequested() const noexcept { return stopRequested_.load(); }

  // Push a finished output `block` of the chunk with the given `chunkIndex`.
  // Suspend while the channel of that chunk is full. Return `false` if the
  // merge was stopped, in which case the `block` is silently dropped and the
  // producer should stop producing. Thread-safe.
  net::awaitable<bool> push(size_t chunkIndex, Block block) {
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    auto channel = channelIfRunning(chunkIndex);
    if (channel == nullptr) {
      co_return false;
    }
    auto [errorCode] = co_await channel->async_send(
        boost::system::error_code{}, OptionalBlock{std::move(block)},
        net::as_tuple(net::use_awaitable));
    co_return !errorCode && !stopRequested_.load();
  }

  // Announce that no further blocks will be pushed for the chunk with the given
  // `chunkIndex`, by sending the end-of-chunk sentinel. Call this exactly once
  // per chunk, and on every path, because the consumer would otherwise wait for
  // that chunk forever. Suspend while the channel of the chunk is full.
  // Thread-safe.
  net::awaitable<void> finishChunk(size_t chunkIndex) {
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    // NOTE: The channel is created here if the chunk did not push a single
    // block, because the consumer still has to see the sentinel.
    auto channel = getOrCreateChannel(chunkIndex);
    // NOTE: The error is deliberately ignored. It means that the merge was
    // stopped, in which case there is no consumer left that could care.
    co_await channel->async_send(boost::system::error_code{},
                                 OptionalBlock{std::nullopt},
                                 net::as_tuple(net::use_awaitable));
  }

  // Forward an `exception` to the consumer, which will rethrow it. Only the
  // first pushed exception is stored, all later ones are ignored. Pushing an
  // exception also stops the merge. Thread-safe.
  void pushException(std::exception_ptr exception) noexcept {
    ad_utility::terminateIfThrows(
        [this, &exception] {
          {
            std::lock_guard lock{mutex_};
            if (exception_ == nullptr) {
              exception_ = std::move(exception);
            }
          }
          requestStop();
        },
        "Pushing an exception to an `AsioInOrderBlockSink` failed.");
  }

  // Stop the merge from the consuming side. All suspended producers are woken
  // up and all further blocks are dropped, and the consumer stops yielding
  // blocks. Call this when the consumer stops iterating early (or exits via an
  // exception), so that no producer is left suspended forever. Thread-safe.
  void abort() noexcept {
    ad_utility::terminateIfThrows([this] { requestStop(); },
                                  "Aborting an `AsioInOrderBlockSink` failed.");
  }

  // Return the next block in the global order, or `std::nullopt` if all chunks
  // are exhausted or the merge was stopped. Rethrow a pushed exception. Suspend
  // until one of these conditions holds.
  //
  // NOTE: Call this from a single consuming coroutine only, and never
  // concurrently with itself.
  net::awaitable<std::optional<Block>> nextBlock() {
    while (true) {
      SharedBlockChannel channel;
      size_t chunkIndex = 0;
      {
        std::unique_lock lock{mutex_};
        if (exception_ != nullptr) {
          auto exception = exception_;
          lock.unlock();
          std::rethrow_exception(exception);
        }
        if (stopRequested_.load() || nextChunkToRead_ >= numChunks_) {
          co_return std::nullopt;
        }
        chunkIndex = nextChunkToRead_;
        channel = getOrCreateChannelLocked(chunkIndex);
      }
      auto [errorCode, block] =
          co_await channel->async_receive(net::as_tuple(net::use_awaitable));
      if (errorCode) {
        // The channel was cancelled or closed, which only happens while the
        // merge is torn down. Continue, such that the next iteration either
        // rethrows the pushed exception or reports the end of the range.
        AD_CORRECTNESS_CHECK(stopRequested_.load());
        continue;
      }
      if (block.has_value()) {
        co_return std::move(block);
      }
      // The end-of-chunk sentinel, so move on to the next chunk. NOTE: Erasing
      // the entry is safe even if the producer of that chunk still holds the
      // channel, because the channels are shared.
      std::lock_guard lock{mutex_};
      chunks_.erase(chunkIndex);
      ++nextChunkToRead_;
    }
  }

 private:
  // Return the channel of the chunk with the given `chunkIndex`, creating it if
  // it does not exist yet. The `mutex_` has to be held.
  SharedBlockChannel getOrCreateChannelLocked(size_t chunkIndex) {
    auto& channel = chunks_[chunkIndex];
    if (channel == nullptr) {
      channel =
          std::make_shared<BlockChannel>(executor_, maxBufferedBlocksPerChunk_);
      if (stopSweepDone_) {
        // The teardown sweep has already run, so this channel would never be
        // closed by it. Close it right away, such that nobody can suspend on
        // it. NOTE: It must not be cancelled afterwards, see the TODO above.
        channel->close();
      }
    }
    return channel;
  }

  // The same as `getOrCreateChannelLocked`, but acquire the `mutex_`.
  SharedBlockChannel getOrCreateChannel(size_t chunkIndex) {
    std::lock_guard lock{mutex_};
    return getOrCreateChannelLocked(chunkIndex);
  }

  // Return the channel of the chunk with the given `chunkIndex`, or `nullptr`
  // if the merge was stopped.
  SharedBlockChannel channelIfRunning(size_t chunkIndex) {
    std::lock_guard lock{mutex_};
    if (stopRequested_.load()) {
      return nullptr;
    }
    return getOrCreateChannelLocked(chunkIndex);
  }

  // Stop the merge and wake up everybody who is currently suspended. The sweep
  // over the channels runs exactly once, and it first cancels (which wakes the
  // suspended operations) and then closes (which makes all later operations
  // fail immediately instead of suspending). Both the order and the "exactly
  // once" are mandatory, see the TODO at the top of this class.
  void requestStop() {
    std::vector<SharedBlockChannel> channels;
    {
      std::lock_guard lock{mutex_};
      stopRequested_.store(true);
      if (stopSweepDone_) {
        return;
      }
      stopSweepDone_ = true;
      channels.reserve(chunks_.size());
      for (const auto& chunk : chunks_) {
        channels.push_back(chunk.second);
      }
    }
    // NOTE: This deliberately happens outside of the `mutex_`.
    for (const auto& channel : channels) {
      channel->cancel();
      channel->close();
    }
  }
};
#endif  // QLEVER_CPP_17

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_OUTPUTSINKPOLICY_H

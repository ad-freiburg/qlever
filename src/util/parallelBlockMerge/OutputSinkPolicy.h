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
// The state of a single chunk consists of two channels, and only the chunks
// that are currently active have such a state at all (see `chunks_`):
//
// 1. The `BlockChannel` transports the finished output blocks from the producer
//    of the chunk to the consumer. The end of a chunk is signalled by an
//    explicit `std::nullopt` sentinel and *not* by closing the channel, see
//    the note on `close()` below.
// 2. The `CreditChannel` transports the permissions to push a block in the
//    opposite direction. A producer needs one credit per block, and the
//    consumer returns one credit for every block that it has taken out of the
//    `BlockChannel`. This is where the back-pressure happens: the
//    `maxBufferedBlocksPerChunk` credits of a chunk are the only ones that
//    ever exist for it.
//
// IMPORTANT: This class deliberately never calls `close()` or `cancel()` on any
// of its channels. `close()` does *not* wake up a producer that is currently
// suspended in `async_send`, and calling `cancel()` after `close()` is
// undefined behavior, because `cancel()` tells a suspended sender from a
// suspended receiver by the send state of the channel, which `close()` has
// overwritten by then. Instead, the throttling is inverted as described above,
// such that a waiter is always woken up by a plain `try_send` into the very
// channel that it receives from. If such a `try_send` fails, then that channel
// is full, which in turn means that the waiter is *not* suspended and will
// observe `stopRequested_` on its own. Waking up a waiter is therefore
// race-free, and no channel is ever closed or cancelled.
//
// DEADLOCK-FREEDOM: The consumer always drains the lowest chunk that has not
// yet been fully consumed, so the producer of a *higher* chunk may well run out
// of credits. In contrast to `InOrderBlockSink` this is not a problem even if
// there is only a single thread, because such a producer suspends its coroutine
// instead of blocking its thread. In particular the number of chunks that are
// in flight does not have to be bounded by the available parallelism, see
// `AsioParallelMergeState`.
//
// NOTE: The class is neither copyable nor movable, because the producers and
// the consumer refer to it by reference.
template <typename Block>
class AsioInOrderBlockSink : public ad_utility::NoCopyNoMove {
 public:
  using value_type = Block;
  // The value that travels through a `BlockChannel`. A `std::nullopt` is the
  // end-of-chunk sentinel. NOTE: The `std::optional` is also required because
  // `Block` need not be default-constructible, but the completion signature of
  // a channel has to be.
  using OptionalBlock = std::optional<Block>;
  // The channel that transports the finished output blocks of a single chunk
  // from its producer to the consumer, see the class comment.
  using BlockChannel = net::experimental::concurrent_channel<void(
      boost::system::error_code, OptionalBlock)>;
  // The channel that transports the credits of a single chunk from the consumer
  // to its producer, see the class comment.
  using CreditChannel =
      net::experimental::concurrent_channel<void(boost::system::error_code)>;

 private:
  // The state of a chunk that is currently active, that is, of a chunk that has
  // a producer or that the consumer is currently reading from.
  struct ActiveChunk {
    BlockChannel blocks_;
    CreditChannel credits_;

    // Create the two channels and hand out the initial credits. NOTE: Both
    // channels have one slot more than there are credits. For the `blocks_`
    // this slot is reserved for the end-of-chunk sentinel, and for the
    // `credits_` it is reserved for the additional credit that wakes up a
    // suspended producer when the merge is stopped.
    ActiveChunk(const net::any_io_executor& executor, size_t maxBufferedBlocks)
        : blocks_{executor, maxBufferedBlocks + 1},
          credits_{executor, maxBufferedBlocks + 1} {
      for (size_t i = 0; i < maxBufferedBlocks; ++i) {
        bool creditWasSent = credits_.try_send(boost::system::error_code{});
        AD_CORRECTNESS_CHECK(creditWasSent);
      }
    }
  };

  net::any_io_executor executor_;
  size_t maxBufferedBlocksPerChunk_;
  size_t numChunks_;
  // Guard all of the following members. NOTE: This mutex is never held across a
  // suspension point, so it is only ever contended for the very short time that
  // a lookup in `chunks_` or a `try_send` takes.
  std::mutex mutex_;
  // The state of the currently active chunks. Entries are created on demand
  // (by the producer of a chunk, or by the consumer if it happens to arrive
  // first) and erased as soon as the consumer has seen the end-of-chunk
  // sentinel, so the memory consumption is proportional to the number of chunks
  // that are in flight and not to the total number of chunks.
  //
  // NOTE: This is a `node_hash_map` and not a `flat_hash_map`, because
  // references to the values have to stay valid when other entries are inserted
  // or erased.
  absl::node_hash_map<size_t, ActiveChunk> chunks_;
  size_t nextChunkToRead_ = 0;
  std::exception_ptr exception_;
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
  // `pushException`. A producer should poll this between two output blocks, so
  // that it does not do any superfluous work.
  bool stopRequested() const noexcept { return stopRequested_.load(); }

  // Push a finished output `block` of the chunk with the given `chunkIndex`.
  // Suspend while the chunk has no credit left, that is, while its buffer is
  // full. Return `false` if the merge was stopped, in which case the `block` is
  // silently dropped and the producer should stop producing. Thread-safe.
  net::awaitable<bool> push(size_t chunkIndex, Block block) {
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    CreditChannel* credits = nullptr;
    {
      std::lock_guard lock{mutex_};
      if (stopRequested_.load()) {
        co_return false;
      }
      credits = &getOrCreateChunk(chunkIndex).credits_;
    }
    // NOTE: It is safe to use the `credits` channel without holding the mutex,
    // because only the consumer erases a chunk, and only after it has seen the
    // end-of-chunk sentinel, which this producer has not sent yet.
    auto [errorCode] =
        co_await credits->async_receive(net::as_tuple(net::use_awaitable));
    if (errorCode || stopRequested_.load()) {
      co_return false;
    }
    std::lock_guard lock{mutex_};
    if (stopRequested_.load()) {
      co_return false;
    }
    // The credit that we just received is the permission to occupy one slot of
    // the block channel, so this `try_send` cannot fail.
    bool blockWasSent = chunks_.at(chunkIndex)
                            .blocks_.try_send(boost::system::error_code{},
                                              OptionalBlock{std::move(block)});
    AD_CORRECTNESS_CHECK(blockWasSent);
    co_return true;
  }

  // Announce that no further blocks will be pushed for the chunk with the given
  // `chunkIndex`. Call this exactly once per chunk. This never suspends and
  // never throws, because the core also calls it while it cleans up after a
  // failed chunk. Thread-safe.
  void finishChunk(size_t chunkIndex) noexcept {
    ad_utility::terminateIfThrows(
        [this, chunkIndex] {
          AD_CONTRACT_CHECK(chunkIndex < numChunks_);
          std::lock_guard lock{mutex_};
          // NOTE: The channel is created here if the chunk has not pushed a
          // single block, because the consumer has to see the sentinel.
          bool sentinelWasSent =
              getOrCreateChunk(chunkIndex)
                  .blocks_.try_send(boost::system::error_code{},
                                    OptionalBlock{std::nullopt});
          // The slot for the sentinel is reserved (see `ActiveChunk`), so this
          // can only fail if the wake-up sentinel of `requestStop` has taken
          // that slot, in which case the consumer is gone anyway.
          AD_CORRECTNESS_CHECK(sentinelWasSent || stopRequested_.load());
        },
        "Finishing a chunk in `AsioInOrderBlockSink` failed.");
  }

  // Forward an `exception` to the consumer, which will rethrow it. Only the
  // first pushed exception is stored, all later ones are ignored. Pushing an
  // exception also stops the merge. Thread-safe.
  void pushException(std::exception_ptr exception) noexcept {
    ad_utility::terminateIfThrows(
        [this, &exception] {
          std::lock_guard lock{mutex_};
          if (exception_ == nullptr) {
            exception_ = std::move(exception);
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
    ad_utility::terminateIfThrows(
        [this] {
          std::lock_guard lock{mutex_};
          requestStop();
        },
        "Aborting an `AsioInOrderBlockSink` failed.");
  }

  // Return the next block in the global order, or `std::nullopt` if all chunks
  // are exhausted or the merge was stopped. Rethrow a pushed exception.
  // Suspend until one of these conditions holds.
  //
  // NOTE: Call this from a single consuming coroutine only, and never
  // concurrently with itself.
  net::awaitable<std::optional<Block>> nextBlock() {
    while (true) {
      BlockChannel* blocks = nullptr;
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
        blocks = &getOrCreateChunk(nextChunkToRead_).blocks_;
      }
      // NOTE: It is safe to use the `blocks` channel without holding the mutex,
      // because only this consumer erases a chunk.
      auto [errorCode, block] =
          co_await blocks->async_receive(net::as_tuple(net::use_awaitable));
      if (errorCode) {
        // This can only happen if the channel is destroyed while we are
        // suspended, which in turn cannot happen before this coroutine is
        // resumed. We nevertheless handle it, instead of relying on that
        // argument.
        co_return std::nullopt;
      }
      if (block.has_value()) {
        returnCredit(nextChunkToRead_);
        co_return std::move(block);
      }
      // The end-of-chunk sentinel, so move on to the next chunk.
      std::lock_guard lock{mutex_};
      if (!stopRequested_.load()) {
        // NOTE: The producer of this chunk is done with its channels, and if
        // the merge was stopped, then the sentinel may well be the wake-up
        // sentinel of `requestStop` while the producer is still running, so the
        // chunk must not be erased in that case.
        chunks_.erase(nextChunkToRead_);
      }
      ++nextChunkToRead_;
    }
  }

 private:
  // Return the state of the chunk with the given `chunkIndex`, creating it if
  // it does not exist yet. The `mutex_` has to be held.
  ActiveChunk& getOrCreateChunk(size_t chunkIndex) {
    return chunks_
        .try_emplace(chunkIndex, executor_, maxBufferedBlocksPerChunk_)
        .first->second;
  }

  // Give the credit for a block that was just consumed back to the producer of
  // the chunk with the given `chunkIndex`.
  void returnCredit(size_t chunkIndex) {
    std::lock_guard lock{mutex_};
    auto iterator = chunks_.find(chunkIndex);
    AD_CORRECTNESS_CHECK(iterator != chunks_.end());
    bool creditWasSent =
        iterator->second.credits_.try_send(boost::system::error_code{});
    // At most `maxBufferedBlocksPerChunk_` credits exist per chunk and the
    // channel has one slot more, so this can only fail if the additional
    // wake-up credit of `requestStop` has taken that slot.
    AD_CORRECTNESS_CHECK(creditWasSent || stopRequested_.load());
  }

  // Stop the merge and wake up everybody who might currently be suspended. The
  // `mutex_` has to be held.
  //
  // NOTE: Every `try_send` here is best-effort by design: if it fails, then the
  // corresponding channel is full, which means that its receiver is not
  // suspended and will observe `stopRequested_` on its own. See the class
  // comment for why this makes the wake-up race-free.
  void requestStop() {
    stopRequested_.store(true);
    // Wake up the producers that wait for a credit.
    for (auto& chunk : chunks_) {
      chunk.second.credits_.try_send(boost::system::error_code{});
    }
    // Wake up the consumer, which can only be suspended on the chunk that it
    // currently reads. NOTE: This has to happen after the loop above, because
    // it may insert into `chunks_`.
    if (nextChunkToRead_ < numChunks_) {
      getOrCreateChunk(nextChunkToRead_)
          .blocks_.try_send(boost::system::error_code{},
                            OptionalBlock{std::nullopt});
    }
  }
};
#endif  // QLEVER_CPP_17

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_OUTPUTSINKPOLICY_H

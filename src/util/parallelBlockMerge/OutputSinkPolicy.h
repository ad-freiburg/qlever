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
class InOrderBlockSink {
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

  // The sink is referred to by reference from several threads, so it must not
  // be copied or moved.
  InOrderBlockSink(const InOrderBlockSink&) = delete;
  InOrderBlockSink& operator=(const InOrderBlockSink&) = delete;
  InOrderBlockSink(InOrderBlockSink&&) = delete;
  InOrderBlockSink& operator=(InOrderBlockSink&&) = delete;

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
  void finishChunk(size_t chunkIndex) {
    std::unique_lock lock{mutex_};
    AD_CONTRACT_CHECK(numChunksIsSet_);
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    chunks_[chunkIndex].finished_ = true;
    lock.unlock();
    consumerCanProceed_.notify_all();
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

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_OUTPUTSINKPOLICY_H

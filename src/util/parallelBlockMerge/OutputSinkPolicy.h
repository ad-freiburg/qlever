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
#include "util/Forward.h"
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
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/strand.hpp>
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
// instead of occupying their thread.
//
// Each chunk that is currently active owns a single channel with a capacity of
// `maxBufferedBlocksPerChunk`, via which its producer sends the finished output
// blocks to the consumer. Suspending in `async_send` on a full channel *is* the
// back-pressure. The end of a chunk is signalled by an explicit `std::nullopt`
// sentinel, upon which the consumer erases the state of that chunk, so that the
// memory consumption is proportional to the number of chunks that are in flight
// and not to the total number of chunks.
//
// INTERFACE: All the asynchronous operations of this class (their names all
// start with `async`) are ordinary Boost.Asio operations that take a completion
// token, so a caller may await them, attach a callback, obtain a `std::future`,
// or detach them, whatever fits. They may all be initiated from any thread and
// any executor, and their completion handler runs on the executor that is
// associated with the completion token. The completion signature is
// `void(std::exception_ptr, T)` (or `void(std::exception_ptr)` for `T == void`)
// with `T` as documented at the respective operation, so a token such as
// `net::use_awaitable` rethrows on the executor of the caller.
//
// STRAND CONFINEMENT: All the mutable state of this class (the map of channels,
// the index of the chunk that is currently read, and the exception) as well as
// *every* channel operation is confined to a single `strand_`, so that no mutex
// of our own is required and the channels can be plain (non-concurrent)
// channels whose internal mutex is a null mutex. Every operation therefore
// consists of a hop onto the strand, the actual work, and a hop back to the
// executor of the caller; `runOnStrand` is the single place where this happens.
// The only member that is ever read off the strand is the atomic
// `stopRequested_`, which exists so that a producer can cheaply poll "should I
// keep merging?" between two output blocks; it is *written* on the strand only.
//
// IMPORTANT: Only this short bookkeeping runs on the strand. Merging a chunk in
// contrast is ordinary blocking CPU work that may even do I/O, and it has to
// keep running on the general executor, because everything that runs on a
// strand is serialized. Scheduling the merging itself on the strand would
// silently destroy all the parallelism, see `AsioParallelMergeState::runChunk`.
//
// The strand is also what makes the teardown airtight, and it is the reason why
// `cancel()` alone suffices and `close()` is never called: the check of
// `stopRequested_` and the *initiation* of the channel operation happen in a
// single strand-serialized step, with no other strand handler in between.
// `requestStop` sets the flag and then cancels every live channel, which wakes
// everybody who is currently suspended; an operation that starts later runs on
// the strand *after* that, sees the flag, and never touches a channel at all,
// so it cannot suspend. In particular no channel is ever created after the
// stop, so the teardown cannot miss one. Without the strand neither primitive
// would suffice: `cancel()` is edge-triggered and does not stop an operation
// that is initiated afterwards, `close()` does not wake an operation that is
// already suspended, and `cancel()` after `close()` is outright undefined
// behavior, because `cancel()` tells a suspended send from a suspended receive
// by the internal send state that `close()` overwrites.
//
// DEADLOCK-FREEDOM: The consumer always drains the lowest chunk that has not
// yet been fully consumed, so the producer of a *higher* chunk may well fill
// its channel and suspend. In contrast to `InOrderBlockSink` this is not a
// problem even if there is only a single thread, because such a producer
// suspends instead of blocking its thread. In particular the number of chunks
// that are in flight does not have to be bounded by the available parallelism,
// see `AsioParallelMergeState`.
//
// TODO<joka921> The following properties of this sink are still worth
// revisiting before it is used more widely than by the parallel merge. All of
// them are benign for that use case.
//
// 1. `asyncGetNextBlock` interprets *any* error of `async_receive` as "the
//    merge was stopped" and asserts `stopRequested_`. If a caller ever attaches
//    a cancellation slot to one of these operations, or cancels the surrounding
//    coroutine, that assertion fires instead of the cancellation being handled.
// 2. `asyncPush` drops its block whenever the send reports an error, without
//    distinguishing "was not delivered" from "was delivered". That is correct
//    here because a channel is only ever cancelled while the merge is being
//    torn down, but it would silently lose data if `cancel()` were used for
//    anything else.
// 3. Every operation costs one coroutine frame plus two executor hops. That is
//    negligible next to an output block of 100k elements (or 16 MB), but it
//    makes this sink a poor fit for small payloads; such a user would have to
//    batch, or run on the strand to begin with.
// 4. All those hops allocate (a coroutine frame, or the handler that
//    `AsioParallelMergeState::abort` posts), so the teardown itself can fail
//    once memory is exhausted. The failure is then swallowed and the consumer
//    simply sees the end of the range.
// 5. Once the merge was stopped, nothing erases the entries of `chunks_`
//    anymore, so the channels of the chunks that were still in flight, and the
//    blocks that they still buffer, live until this sink is destroyed.
// 6. The `node_hash_map` is redundant by now. Its stable addresses were needed
//    when the channels were stored by value; with `shared_ptr` values a
//    `flat_hash_map` would do.
// 7. `AsioParallelMergeRange::get()`, the synchronous adapter of this
//    machinery, blocks its calling thread on a `future`, so the executor has to
//    be run by *other* threads. See the note there.
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
  // The strand to which all the state of this sink and all the operations on
  // its channels are confined, see the STRAND CONFINEMENT note above.
  using Strand = net::strand<net::any_io_executor>;
  // The channel that transports the finished output blocks of a single chunk
  // from its producer to the consumer. NOTE: A plain (non-concurrent) channel
  // suffices, because every operation on it is performed on `strand_`.
  using BlockChannel = net::experimental::channel<void(
      boost::system::error_code, OptionalBlock)>;
  // The channels are shared, because both the producer of a chunk and the
  // consumer may hold on to one across a suspension, while the consumer erases
  // the map entry of a chunk as soon as that chunk is done.
  using SharedBlockChannel = std::shared_ptr<BlockChannel>;
  // The completion token that the asynchronous operations below use if the
  // caller does not specify one.
  using DefaultToken = net::use_awaitable_t<>;

 private:
  Strand strand_;
  size_t maxBufferedBlocksPerChunk_;
  size_t numChunks_;
  // The following three members are only ever touched on `strand_`.
  absl::node_hash_map<size_t, SharedBlockChannel> chunks_;
  size_t nextChunkToRead_ = 0;
  std::exception_ptr exception_;
  // Set as soon as the merge is stopped, either because the consumer has
  // abandoned it or because a producer has pushed an exception. NOTE: This is
  // the only member that may be read off the strand (it is written on the
  // strand only), such that a producer can cheaply poll it between two output
  // blocks.
  std::atomic<bool> stopRequested_{false};

 public:
  // Construct from the `executor` from which the strand of this sink is
  // derived, the total number of chunks, and the maximal number of blocks that
  // are buffered per chunk (which has to be at least one).
  AsioInOrderBlockSink(net::any_io_executor executor, size_t numChunks,
                       size_t maxBufferedBlocksPerChunk = 2)
      : strand_{net::make_strand(std::move(executor))},
        maxBufferedBlocksPerChunk_{maxBufferedBlocksPerChunk},
        numChunks_{numChunks} {
    AD_CONTRACT_CHECK(maxBufferedBlocksPerChunk > 0);
  }

  // Return `true` if the merge was stopped, either by `asyncAbort()` or by
  // `asyncPushException()`. A producer should poll this between two output
  // blocks, so that it does not do any superfluous work. Callable from
  // anywhere, and the only operation of this class that is synchronous.
  bool stopRequested() const noexcept { return stopRequested_.load(); }

  // Push a finished output `block` of the chunk with the given `chunkIndex`.
  // Suspend while the channel of that chunk is full. Complete with `false` if
  // the merge was stopped, in which case the `block` is silently dropped and
  // the producer should stop producing.
  template <typename CompletionToken = DefaultToken>
  auto asyncPush(size_t chunkIndex, Block block,
                 CompletionToken&& completionToken = {}) {
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    return runOnStrand(
        [this, chunkIndex,
         block = std::move(block)]() mutable -> net::awaitable<bool> {
          co_return co_await sendToChunk(chunkIndex,
                                         OptionalBlock{std::move(block)});
        },
        AD_FWD(completionToken));
  }

  // Announce that no further blocks will be pushed for the chunk with the given
  // `chunkIndex`, by sending the end-of-chunk sentinel. Call this exactly once
  // per chunk, and on every path, because the consumer would otherwise wait for
  // that chunk forever. Suspend while the channel of the chunk is full.
  // Complete with `false` if the merge was stopped, in which case there is no
  // consumer left that could care about the sentinel.
  template <typename CompletionToken = DefaultToken>
  auto asyncFinishChunk(size_t chunkIndex,
                        CompletionToken&& completionToken = {}) {
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    return runOnStrand(
        [this, chunkIndex]() -> net::awaitable<bool> {
          co_return co_await sendToChunk(chunkIndex,
                                         OptionalBlock{std::nullopt});
        },
        AD_FWD(completionToken));
  }

  // Forward an `exception` to the consumer, which will rethrow it. Only the
  // first pushed exception is stored, all later ones are ignored. Pushing an
  // exception also stops the merge. Complete with nothing.
  template <typename CompletionToken = DefaultToken>
  auto asyncPushException(std::exception_ptr exception,
                          CompletionToken&& completionToken = {}) {
    return runOnStrand(
        [this,
         exception = std::move(exception)]() mutable -> net::awaitable<void> {
          AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
          if (exception_ == nullptr) {
            exception_ = std::move(exception);
          }
          requestStop();
          co_return;
        },
        AD_FWD(completionToken));
  }

  // Stop the merge from the consuming side. All suspended producers are woken
  // up and all further blocks are dropped, and the consumer stops yielding
  // blocks. Call this when the consumer stops iterating early (or exits via an
  // exception), so that no producer is left suspended forever. Complete with
  // nothing.
  template <typename CompletionToken = DefaultToken>
  auto asyncAbort(CompletionToken&& completionToken = {}) {
    return runOnStrand(
        [this]() -> net::awaitable<void> {
          requestStop();
          co_return;
        },
        AD_FWD(completionToken));
  }

  // Complete with the next block in the global order, or with `std::nullopt` if
  // all chunks are exhausted or the merge was stopped. Rethrow a pushed
  // exception. Suspend until one of these conditions holds.
  //
  // NOTE: Run this from a single consumer only, and never concurrently with
  // itself.
  template <typename CompletionToken = DefaultToken>
  auto asyncGetNextBlock(CompletionToken&& completionToken = {}) {
    return runOnStrand(
        [this]() -> net::awaitable<std::optional<Block>> {
          AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
          while (true) {
            auto channel = getCurrentOutputChannel();
            if (!channel.has_value()) {
              co_return std::nullopt;
            }
            auto [errorCode, block] = co_await channel.value()->async_receive(
                net::as_tuple(net::use_awaitable));
            if (errorCode) {
              // The channel was cancelled, which only happens while the merge
              // is torn down. Continue, such that the next iteration either
              // rethrows the pushed exception or reports the end of the range.
              AD_CORRECTNESS_CHECK(stopRequested_.load());
              continue;
            }
            if (block.has_value()) {
              co_return std::move(block);
            }
            // The end-of-chunk sentinel, so move on to the next chunk. NOTE:
            // Erasing the entry is safe even if the producer of that chunk
            // still holds the channel, because the channels are shared.
            chunks_.erase(nextChunkToRead_);
            ++nextChunkToRead_;
          }
        },
        AD_FWD(completionToken));
  }

 private:
  // Run the coroutine that the `awaitableFactory` creates on `strand_` and
  // complete via the `completionToken`. This is the single place where the hop
  // onto the strand and the association of the completion handler with the
  // executor of the caller happen: `co_spawn` initiates the operation, forwards
  // the token, and invokes the resulting completion handler on the executor
  // that is associated with that token (or on `strand_` if it has none).
  //
  // NOTE: The work on the strand has to be a separate coroutine that is
  // `co_spawn`ed, because an `awaitable` is bound to a single executor for its
  // whole lifetime and can hence not be moved onto the strand.
  template <typename AwaitableFactory, typename CompletionToken>
  auto runOnStrand(AwaitableFactory awaitableFactory,
                   CompletionToken&& completionToken) {
    return net::co_spawn(strand_, std::move(awaitableFactory),
                         AD_FWD(completionToken));
  }

  // The common implementation of `asyncPush` and `asyncFinishChunk`: send the
  // `optionalBlock` (an output block, or `std::nullopt` as the end-of-chunk
  // sentinel) into the channel of the chunk with the given `chunkIndex`. Return
  // `false` if the merge was stopped, in which case nothing was sent.
  //
  // PRECONDITION: This runs on `strand_`.
  net::awaitable<bool> sendToChunk(size_t chunkIndex,
                                   OptionalBlock optionalBlock) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (stopRequested_.load()) {
      // A block is silently dropped, and there is no consumer left that could
      // care about a sentinel. NOTE: Returning here (instead of creating a
      // channel and sending into it) is what makes the teardown airtight, see
      // the class comment above.
      co_return false;
    }
    // NOTE: For the sentinel the channel is created here if the chunk did not
    // push a single block, because the consumer still has to see that sentinel.
    auto channel = getOrCreateChannel(chunkIndex);
    auto [errorCode] = co_await channel->async_send(
        boost::system::error_code{}, std::move(optionalBlock),
        net::as_tuple(net::use_awaitable));
    co_return !errorCode && !stopRequested_.load();
  }

  // The actual teardown of `asyncAbort()`, for callers that are already on the
  // strand and that must not (or cannot) suspend.
  //
  // PRECONDITION: This runs on `strand_`.
  void requestStop() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
          stopRequested_.store(true);
          // Wake up everybody who is currently suspended. NOTE: There is no
          // need to also `close()` the channels, and no need to run this sweep
          // only once: an operation that is initiated after this point runs on
          // the strand afterwards, sees the flag, and never touches a channel.
          // See the class comment above.
          for (const auto& chunk : chunks_) {
            chunk.second->cancel();
          }
        },
        "Stopping an `AsioInOrderBlockSink` failed.");
  }

  // Return the channel of the chunk that the consumer currently reads from,
  // creating it if it does not exist yet, or `std::nullopt` if there is nothing
  // left to read because all chunks are exhausted or the merge was stopped.
  // Rethrow a pushed exception.
  //
  // NOTE: The exception propagates out of the coroutine that runs on the strand
  // and is rethrown by the completion of `asyncGetNextBlock`, so the consumer
  // sees it on its own executor.
  //
  // PRECONDITION: This runs on `strand_`.
  std::optional<SharedBlockChannel> getCurrentOutputChannel() {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
    if (stopRequested_.load() || nextChunkToRead_ >= numChunks_) {
      return std::nullopt;
    }
    return getOrCreateChannel(nextChunkToRead_);
  }

  // Return the channel of the chunk with the given `chunkIndex`, creating it if
  // it does not exist yet.
  //
  // PRECONDITION: This runs on `strand_`, and the merge was not stopped yet
  // (see the class comment: no channel must ever be created after the stop).
  SharedBlockChannel getOrCreateChannel(size_t chunkIndex) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    AD_CORRECTNESS_CHECK(!stopRequested_.load());
    auto& channel = chunks_[chunkIndex];
    if (channel == nullptr) {
      // NOTE: The channel lives on the strand as well, so that the operations
      // on it are dispatched through the strand no matter which executor the
      // initiating coroutine reports.
      channel =
          std::make_shared<BlockChannel>(strand_, maxBufferedBlocksPerChunk_);
    }
    return channel;
  }
};
#endif  // QLEVER_CPP_17

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_OUTPUTSINKPOLICY_H

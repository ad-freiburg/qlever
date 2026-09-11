// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_INORDERBLOCKSINK_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_INORDERBLOCKSINK_H

// The output side of the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`): the `InOrderBlockSink` that
// turns the concurrently produced blocks back into a single sequential range.
// Its operations are implemented as coroutines, so this whole header is only
// available in C++20 mode and empty when `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17`
// is set, see `util/parallelBlockMerge/ParallelMergeState.h`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <atomic>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <concepts>
#include <cstddef>
#include <exception>
#include <optional>
#include <utility>

#include "util/AsioHelpers.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/BlockStorage.h"

namespace ad_utility::parallelBlockMerge {

// Turn the concurrently produced output blocks of the merge back into a single
// sequential range: the blocks of chunk `0` come first, then those of chunk
// `1`, and so on, and within a chunk in the order in which they were pushed. A
// producer that waits for the consumer to catch up, and a consumer that waits
// for the next block, both suspend instead of blocking their thread.
//
// The blocks do not live in this class but in a `Storage` that models the
// `BlockStorageConcept` (see `util/parallelBlockMerge/BlockStorage.h`) and that
// holds one FIFO queue per chunk. This class only adds the global order on top
// of those queues: it drains the queue of the lowest chunk that is not done
// yet, and moves on to the next chunk when it sees that chunk's end-of-chunk
// sentinel. The storage also decides what happens to a producer that has
// finished a block while the consumer has not caught up: a storage that keeps
// the blocks in memory bounds its buffer, so its producers suspend and that
// back-pressure is what bounds the memory consumption of the merge; a storage
// that spills to disk lets a producer run ahead instead. It is owned by value
// and created by the factory that the constructor takes, so that it can be
// configured at the single place where a sink is created.
//
// INTERFACE: All the asynchronous operations of this class (their names start
// with `async`) are ordinary Boost.Asio operations that take a completion
// token, so a caller may await them, attach a callback, obtain a `std::future`,
// or detach them. They may be initiated from any thread and any executor, and
// their completion handler runs on the executor that is associated with the
// token. The completion signature is `void(std::exception_ptr, T)` (or
// `void(std::exception_ptr)` for `T == void`) with `T` as documented at the
// respective operation, so a token such as `net::use_awaitable` rethrows on the
// executor of the caller.
//
// STRAND CONFINEMENT: All the mutable state of this class and *every* operation
// of the storage are confined to a single `strand_`, so that neither needs a
// mutex. Every operation therefore consists of a hop onto the strand, the
// actual work (a coroutine, see `spawnOnStrand`), and a hop back to the
// executor of the caller. The only member that is ever read off the strand is
// the atomic `stopRequested_`, which a producer polls between two output
// blocks; it is *written* on the strand only.
//
// IMPORTANT: The hop back is always a `net::post`, so a completion handler of
// this class never runs while the strand is held. That matters because
// everything on a strand is serialized, and a producer merges a whole output
// block in its completion handler, which is blocking CPU work that may even do
// I/O. A storage that does blocking work of its own has to offload it likewise.
//
// The strand is also what makes the teardown airtight and is the reason why
// cancelling the storage alone suffices: the check of `stopRequested_` and the
// *initiation* of a storage operation happen in a single strand-serialized
// step. `requestStop` sets the flag and cancels the storage, which wakes
// everybody who is suspended; an operation that starts later sees the flag and
// never touches the storage, so it cannot suspend and no queue is created after
// the stop. Cancellation alone would not do, because it is edge-triggered and
// does not stop an operation that is initiated afterwards, which is why the
// `BlockStorageConcept` forbids initiating one.
//
// DEADLOCK-FREEDOM: The consumer always drains the lowest chunk that is not
// done yet, so the producer of a *higher* chunk may well fill its queue and
// suspend. That is fine even with a single thread, because it suspends instead
// of blocking; the number of chunks that are in flight therefore need not be
// bounded by the available parallelism, see `ParallelMergeState`.
//
// NOTE: The following properties of this sink are worth revisiting should this
// class be used by anything else than the parallel merge. All of them are
// benign for that use case.
//
// 1. `asyncGetNextBlock` reads a cancelled `getBlock` as "the merge was
//    stopped" and asserts `stopRequested_`, so a caller that attaches a
//    cancellation slot or cancels the surrounding coroutine would hit that
//    assertion instead of having its cancellation handled.
// 2. `asyncPush` drops its block whenever the storage reports that it was not
//    stored, without distinguishing "not delivered" from "delivered". That is
//    correct here because a storage is only ever cancelled during the teardown,
//    but it would silently lose data if the cancellation were used for anything
//    else.
// 3. Every operation costs two executor hops plus a coroutine frame. That is
//    negligible next to an output block of 100k elements (or 16 MB), but it
//    makes this sink a poor fit for small payloads, which would have to be
//    batched.
// 4. All those hops allocate, so the teardown itself can fail once memory is
//    exhausted; the failure is then swallowed and the consumer simply sees the
//    end of the range.
//
// NOTE: The class is neither copyable nor movable, because the producers and
// the consumer refer to it by reference.
template <typename Block, BlockStorageConcept<Block> Storage>
class InOrderBlockSink : public ad_utility::NoCopyNoMove {
 public:
  using value_type = Block;
  // The value that is handed to the storage. A `std::nullopt` is the
  // end-of-chunk sentinel.
  using OptionalBlock = parallelBlockMerge::OptionalBlock<Block>;
  // The strand to which all the state of this sink and all the operations on
  // its storage are confined, see the STRAND CONFINEMENT note above.
  using Strand = parallelBlockMerge::Strand;

 private:
  // The executor from which `strand_` is derived. It is the fallback for
  // completion handlers that have no associated executor of their own, see
  // `spawnOnStrand`.
  net::any_io_executor executor_;
  Strand strand_;
  Storage storage_;
  size_t numChunks_;
  // The following two members are only ever touched on `strand_`.
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
  // derived, the total number of chunks, and a factory that creates the storage
  // of the blocks. The factory is called exactly once, with that strand, and
  // the storage that it returns has to confine itself to exactly that strand,
  // see the CONTRACT of the `BlockStorageConcept`.
  template <typename StorageFactory>
  requires std::invocable<StorageFactory, const Strand&>
  InOrderBlockSink(net::any_io_executor executor, size_t numChunks,
                   StorageFactory storageFactory)
      : executor_{std::move(executor)},
        strand_{net::make_strand(executor_)},
        storage_{std::move(storageFactory)(strand_)},
        numChunks_{numChunks} {}

  // Return `true` if the merge was stopped, either by `asyncStop()` or by
  // `asyncPushException()`. A producer should poll this between two output
  // blocks, so that it does not do any superfluous work. Callable from
  // anywhere, and the only operation of this class that is synchronous.
  bool stopRequested() const noexcept { return stopRequested_.load(); }

  // Push a finished output `block` of the chunk with the given `chunkIndex`.
  // Suspend while the storage of that chunk has no room. Complete with `false`
  // if the merge was stopped, in which case the `block` is silently dropped and
  // the producer should stop producing.
  //
  // PRECONDITION: At most one `asyncPush` or `asyncFinishChunk` of a given
  // chunk may be in flight at a time, because two concurrent ones could
  // complete in either order and the order within the chunk would be lost. Each
  // chunk of the merge has a single producer that pushes its blocks one after
  // the other, so this holds there.
  template <typename CompletionToken>
  auto asyncPush(size_t chunkIndex, Block block,
                 CompletionToken&& completionToken) {
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    return spawnOnStrand(
        sendToChunk(chunkIndex, OptionalBlock{std::move(block)}),
        AD_FWD(completionToken));
  }

  // Announce that no further blocks will be pushed for the chunk with the given
  // `chunkIndex`, by sending the end-of-chunk sentinel. Call this exactly once
  // per chunk, and on every path, because the consumer would otherwise wait for
  // that chunk forever. Suspend while the storage of the chunk has no room.
  // Complete with `false` if the merge was stopped, in which case there is no
  // consumer left that could care about the sentinel. The PRECONDITION of
  // `asyncPush` applies here as well.
  template <typename CompletionToken>
  auto asyncFinishChunk(size_t chunkIndex, CompletionToken&& completionToken) {
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    return spawnOnStrand(sendToChunk(chunkIndex, OptionalBlock{std::nullopt}),
                         AD_FWD(completionToken));
  }

  // Forward an `exception` to the consumer, which will rethrow it. Only the
  // first pushed exception is stored, all later ones are ignored. Pushing an
  // exception also stops the merge. Complete with nothing.
  template <typename CompletionToken>
  auto asyncPushException(std::exception_ptr exception,
                          CompletionToken&& completionToken) {
    return ad_utility::runFunctionOnExecutor(
        strand_,
        [this, exception = std::move(exception)]() mutable {
          // NOTE: Only the *first* exception stops the merge, and that is
          // enough: `requestStop` has then already woken everybody who was
          // suspended, and no queue is ever created afterwards (see the class
          // comment above), so a later exception has nothing left to wake and
          // its sweep over the storage would be redundant.
          if (exception_ == nullptr) {
            exception_ = std::move(exception);
            requestStop();
          }
        },
        AD_FWD(completionToken));
  }

  // Stop the merge from the consuming side. All suspended producers are woken
  // up and all further blocks are dropped, and the consumer stops yielding
  // blocks. Call this when the consumer stops iterating early (or exits via an
  // exception), so that no producer is left suspended forever. Complete with
  // nothing.
  template <typename CompletionToken>
  auto asyncStop(CompletionToken&& completionToken) {
    return ad_utility::runFunctionOnExecutor(
        strand_, [this]() { requestStop(); }, AD_FWD(completionToken));
  }

  // Complete with the next block in the global order, or with `std::nullopt` if
  // all chunks are exhausted or the merge was stopped. Rethrow a pushed
  // exception. Suspend until one of these conditions holds.
  //
  // IMPORTANT: Run this from a single consumer only, and never concurrently
  // with itself. The strand does not make this requirement go away, because it
  // serializes the individual steps of an operation and not two whole
  // operations: two concurrent calls would both read from the queue of the same
  // chunk, so the global order would be lost, and the one that gets the
  // end-of-chunk sentinel would erase (and thereby destroy) the very queue that
  // the other one is still waiting on.
  template <typename CompletionToken>
  auto asyncGetNextBlock(CompletionToken&& completionToken) {
    return spawnOnStrand(receiveNextBlock(), AD_FWD(completionToken));
  }

 private:
  // Run the coroutine `awaitable` on `strand_` and complete the
  // `completionToken` of the operation that it implements with its result. This
  // is the single place where the coroutine-based operations of this class hop
  // onto the strand and back, and the completion signature is therefore
  // `void(std::exception_ptr, T)`: an exception that leaves the coroutine is
  // handed to the completion instead of terminating the program.
  template <typename T, typename CompletionToken>
  auto spawnOnStrand(net::awaitable<T> awaitable,
                     CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken, void(std::exception_ptr, T)>(
        [this, awaitable = std::move(awaitable)](auto handler) mutable {
          auto executor = net::get_associated_executor(handler, executor_);
          net::co_spawn(
              strand_, std::move(awaitable),
              [executor, handler = std::move(handler)](
                  std::exception_ptr exception, T result) mutable {
                // IMPORTANT: This deliberately uses `net::post` and not
                // `net::dispatch`, so that a completion handler never runs
                // while the strand is held, see the IMPORTANT note in the class
                // comment above. `net::dispatch` would invoke the handler
                // *inline* whenever the calling thread already belongs to the
                // target executor, which is exactly the case for a producer
                // whose handler is bound to the thread pool that also runs this
                // strand.
                net::post(executor, [handler = std::move(handler),
                                     exception = std::move(exception),
                                     result = std::move(result)]() mutable {
                  std::move(handler)(std::move(exception), std::move(result));
                });
              });
        },
        completionToken);
  }

  // The body of `asyncPush` and `asyncFinishChunk`: store the `optionalBlock`
  // (an output block, or `std::nullopt` as the end-of-chunk sentinel) as the
  // next value of the chunk with the given `chunkIndex`. Return `false` if the
  // merge was stopped, in which case nothing was stored.
  //
  // PRECONDITION: This runs on `strand_`, see `spawnOnStrand`.
  net::awaitable<bool> sendToChunk(size_t chunkIndex,
                                   OptionalBlock optionalBlock) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (stopRequested_.load()) {
      // A block is silently dropped, and there is no consumer left that could
      // care about a sentinel. NOTE: Returning here (instead of touching the
      // storage) is what makes the teardown airtight, see the class comment
      // above.
      co_return false;
    }
    bool wasStored = co_await storage_.storeBlock(
        chunkIndex, std::move(optionalBlock), net::use_awaitable);
    co_return wasStored && !stopRequested_.load();
  }

  // The body of `asyncGetNextBlock`: return the next block, or `std::nullopt`
  // if there is nothing left to read, or rethrow the pushed exception.
  //
  // PRECONDITION: This runs on `strand_`, see `spawnOnStrand`.
  net::awaitable<OptionalBlock> receiveNextBlock() {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    while (true) {
      if (exception_ != nullptr) {
        // NOTE: The consumer rethrows this on its own executor, because the
        // completion signature starts with an `std::exception_ptr`.
        std::rethrow_exception(exception_);
      }
      if (stopRequested_.load() || nextChunkToRead_ >= numChunks_) {
        co_return OptionalBlock{std::nullopt};
      }
      GetResult<Block> result =
          co_await storage_.getBlock(nextChunkToRead_, net::use_awaitable);
      if (result.wasCancelled()) {
        // The storage was cancelled, which only happens while the merge is torn
        // down. Continue, such that the next round either rethrows the pushed
        // exception or reports the end of the range.
        AD_CORRECTNESS_CHECK(stopRequested_.load());
        continue;
      }
      if (result.hasValue()) {
        co_return OptionalBlock{std::move(result).get()};
      }
      // The end-of-chunk sentinel, so move on to the next chunk. NOTE: The
      // storage drops that chunk on its own, see `BlockStorageConcept`.
      AD_CORRECTNESS_CHECK(result.isEndOfChunk());
      ++nextChunkToRead_;
    }
  }

  // The actual teardown of `asyncStop()`, for callers that are already on the
  // strand and that must not (or cannot) suspend.
  //
  // PRECONDITION: This runs on `strand_`.
  void requestStop() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
          stopRequested_.store(true);
          storage_.cancelAll();
        },
        "Stopping an `InOrderBlockSink` failed.");
  }
};

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_INORDERBLOCKSINK_H

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

#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"
#include "util/NoCopyNoMove.h"
#include "util/RunFunctionOnExecutor.h"
#include "util/parallelBlockMerge/BlockStorage.h"

namespace ad_utility::parallelBlockMerge {

// Turn the concurrently produced output blocks of the merge back into a single
// sequential range in which the blocks of chunk `0` come first, then those of
// chunk `1`, and so on. Within a chunk, the blocks appear in the order in which
// they were pushed. The sink never blocks a thread: a producer that has to wait
// for the consumer to catch up, as well as a consumer that has to wait for the
// next block, suspend instead of occupying their thread.
//
// The blocks themselves do not live in this class but in a `Storage` that
// models the `BlockStorageConcept` (see
// `util/parallelBlockMerge/BlockStorage.h`), which holds one FIFO queue per
// chunk and is at the same time the buffer, the FIFO order, and the rendezvous
// between a producer and the consumer. This class only adds the global order on
// top of those queues: it drains the queue of the lowest chunk that has not
// been fully consumed yet, and moves on to the next chunk when it sees that
// chunk's end-of-chunk sentinel (an empty `OptionalBlock`), upon which it also
// erases the state of that chunk from the storage, so that the memory
// consumption is proportional to the number of chunks that are in flight and
// not to the total number of chunks.
//
// Which storage is used decides what happens to a producer that has finished a
// block while the consumer has not caught up yet: a storage that keeps the
// blocks in memory bounds the number of blocks that it buffers per chunk, so
// its producers suspend and that back-pressure is what bounds the memory
// consumption of the merge; a storage that spills to disk lets a producer run
// ahead instead. The storage is owned by value and is created by the factory
// that the constructor takes, so that it can be configured (in particular with
// the size of its buffer) at the single place where a sink is created.
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
// STRAND CONFINEMENT: All the mutable state of this class (the index of the
// chunk that is currently read and the exception) as well as *every* operation
// of the storage is confined to a single `strand_`, so that no mutex of our own
// is required and the storage needs no synchronization of its own. Every
// operation therefore consists of a hop onto the strand, the actual work, and a
// hop back to the executor of the caller. The work itself is a coroutine that
// is spawned onto the strand (see `spawnOnStrand`). The only member that is
// ever read off the strand is the atomic `stopRequested_`, which exists so that
// a producer can cheaply poll "should I keep merging?" between two output
// blocks; it is *written* on the strand only.
//
// IMPORTANT: Only this short bookkeeping ever runs on the strand, and the hop
// back is always a `net::post` (see `spawnOnStrand` and
// `ad_utility::runFunctionOnExecutor`), so a completion handler of this class
// never runs while the strand is held. That guarantee matters, because
// everything that runs on a strand is serialized. The producer of a chunk for
// example merges a whole output block in its completion handler, which is
// ordinary blocking CPU work that may even do I/O, and running that on the
// strand would serialize the entire merge. A storage that does blocking work of
// its own (such as compressing a block and writing it to disk) therefore has to
// offload that work to another executor as well.
//
// The strand is also what makes the teardown airtight, and it is the reason why
// cancelling the storage alone suffices: the check of `stopRequested_` and the
// *initiation* of the storage operation happen in a single strand-serialized
// step, with no other strand handler in between. `requestStop` sets the flag
// and then cancels the storage, which wakes everybody who is currently
// suspended; an operation that starts later runs on the strand *after* that,
// sees the flag, and never touches the storage at all, so it cannot suspend. In
// particular no queue is ever created after the stop, so the teardown cannot
// miss one. Without the strand this would not work: cancellation is
// edge-triggered and does not stop an operation that is initiated afterwards,
// which is why the `BlockStorageConcept` requires that no operation is
// initiated after `cancelAll`.
//
// DEADLOCK-FREEDOM: The consumer always drains the lowest chunk that has not
// yet been fully consumed, so the producer of a *higher* chunk may well fill
// its queue and suspend. That is not a problem even if there is only a single
// thread, because such a producer suspends instead of blocking its thread. In
// particular the number of chunks that are in flight does not have to be
// bounded by the available parallelism, see `ParallelMergeState`.
//
// TODO<joka921> The following properties of this sink are still worth
// revisiting before it is used more widely than by the parallel merge. All of
// them are benign for that use case.
//
// 1. `asyncGetNextBlock` interprets a cancelled `getBlock` as "the merge was
//    stopped" and asserts `stopRequested_`. If a caller ever attaches a
//    cancellation slot to one of these operations, or cancels the surrounding
//    coroutine, that assertion fires instead of the cancellation being handled.
// 2. `asyncPush` drops its block whenever the storage reports that it was not
//    stored, without distinguishing "was not delivered" from "was delivered".
//    That is correct here because a storage is only ever cancelled while the
//    merge is being torn down, but it would silently lose data if the
//    cancellation were used for anything else.
// 3. Every operation costs two executor hops plus a coroutine frame. That is
//    negligible next to an output block of 100k elements (or 16 MB), but it
//    makes this sink a poor fit for small payloads; such a user would have to
//    batch, or run on the strand to begin with.
// 4. All those hops allocate (the coroutine frame, the handler that is posted
//    back to the caller, and the one that `ParallelMergeState::stop` posts), so
//    the teardown itself can fail once memory is exhausted. The failure is then
//    swallowed and the consumer simply sees the end of the range.
// 5. Once the merge was stopped, nothing calls `eraseChunk` anymore, so the
//    queues of the chunks that were still in flight, and the blocks that they
//    still buffer, live until the storage is destroyed.
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
      : strand_{net::make_strand(std::move(executor))},
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
          auto executor = net::get_associated_executor(handler, strand_);
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
    // NOTE: The storage completes on `strand_` (see the CONTRACT of the
    // `BlockStorageConcept`), so this coroutine is resumed there as well.
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
      if (!result.has_value()) {
        // The storage was cancelled, which only happens while the merge is torn
        // down. Continue, such that the next round either rethrows the pushed
        // exception or reports the end of the range.
        AD_CORRECTNESS_CHECK(stopRequested_.load());
        continue;
      }
      if (result.value().has_value()) {
        co_return std::move(result).value();
      }
      // The end-of-chunk sentinel, so move on to the next chunk.
      storage_.eraseChunk(nextChunkToRead_);
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

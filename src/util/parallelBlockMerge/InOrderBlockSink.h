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

#include <absl/functional/any_invocable.h>

#include <atomic>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <cstddef>
#include <exception>
#include <memory>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"
#include "util/NoCopyNoMove.h"
#include "util/RunFunctionOnExecutor.h"
#include "util/parallelBlockMerge/BlockStorage.h"

// The output side of the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`): the `InOrderBlockSink` that
// turns the concurrently produced blocks back into a single sequential range.
namespace ad_utility::parallelBlockMerge {

namespace net = boost::asio;

// Turn the concurrently produced output blocks of the merge back into a single
// sequential range in which the blocks of chunk `0` come first, then those of
// chunk `1`, and so on. Within a chunk, the blocks appear in the order in which
// they were pushed. The sink never blocks a thread: a producer that has to wait
// for the consumer to catch up, as well as a consumer that has to wait for the
// next block, suspend instead of occupying their thread.
//
// The blocks themselves do not live in this class but in a `BlockStorage` (see
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
// block while the consumer has not caught up yet: with the default
// `InMemoryBlockStorage` the producer suspends, and that back-pressure is what
// bounds the memory consumption of the merge; with a storage that spills to
// disk the producer runs ahead instead. Pass a `StorageFactory` to the
// corresponding constructor to choose (see for example
// `engine/idTable/CompressedIdTableBlockStorage.h`).
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
// NOTE: The completion token is deliberately *not* defaulted, because
// `net::use_awaitable` (the only sensible default) requires coroutines, which
// are not available in the C++17 backports mode.
//
// STRAND CONFINEMENT: All the mutable state of this class (the index of the
// chunk that is currently read and the exception) as well as *every* operation
// of the storage is confined to a single `strand_`, so that no mutex of our own
// is required and the storage needs no synchronization of its own. Every
// operation therefore consists of a hop onto the strand (a `net::post`), the
// actual work, and a hop back to the executor of the caller. None of these
// operations is a coroutine; they are plain `async_initiate` operations built
// from handlers. The only member that is ever read off the strand is the atomic
// `stopRequested_`, which exists so that a producer can cheaply poll "should I
// keep merging?" between two output blocks; it is *written* on the strand only.
//
// IMPORTANT: Only this short bookkeeping ever runs on the strand, and the hop
// back is always a `net::post` (see `completeOn` and
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
// which is why `BlockStorage` requires that no operation is initiated after
// `cancelAll`.
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
// 3. Every operation costs two executor hops and the handler allocations that
//    come with them. That is negligible next to an output block of 100k
//    elements (or 16 MB), but it makes this sink a poor fit for small payloads;
//    such a user would have to batch, or run on the strand to begin with.
// 4. All those hops allocate (the handler that is posted onto the strand, the
//    type-erased handler that is passed to the storage, and the one that
//    `ParallelMergeState::abort` posts), so the teardown itself can fail once
//    memory is exhausted. The failure is then swallowed and the consumer simply
//    sees the end of the range.
// 5. Once the merge was stopped, nothing calls `BlockStorage::eraseRun`
//    anymore, so the queues of the chunks that were still in flight, and the
//    blocks that they still buffer, live until the storage is destroyed.
// 6. `ParallelMergeRange::get()`, the synchronous adapter of this machinery,
//    blocks its calling thread on a `future`, so the executor has to be run by
//    *other* threads. See the note there.
//
// NOTE: The class is neither copyable nor movable, because the producers and
// the consumer refer to it by reference.
template <typename Block>
class InOrderBlockSink : public ad_utility::NoCopyNoMove {
 public:
  using value_type = Block;
  // The storage that holds the blocks, and the value that is stored in it. A
  // `std::nullopt` is the end-of-chunk sentinel.
  using Storage = BlockStorage<Block>;
  using OptionalBlock = typename Storage::OptionalBlock;
  // The strand to which all the state of this sink and all the operations on
  // its storage are confined, see the STRAND CONFINEMENT note above.
  using Strand = typename Storage::Strand;
  // A factory that creates the storage of a sink, see `BlockStorageFactory`.
  using StorageFactory = BlockStorageFactory<Block>;

 private:
  Strand strand_;
  std::unique_ptr<Storage> storage_;
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
  // derived, the total number of chunks, and the factory for the storage of the
  // blocks.
  InOrderBlockSink(net::any_io_executor executor, size_t numChunks,
                   StorageFactory storageFactory)
      : strand_{net::make_strand(std::move(executor))},
        storage_{std::move(storageFactory)(strand_)},
        numChunks_{numChunks} {
    AD_CONTRACT_CHECK(storage_ != nullptr);
  }

  // Construct a sink that keeps the blocks in memory, buffering at most
  // `maxBufferedBlocksPerChunk` (which has to be at least one) of them per
  // chunk.
  InOrderBlockSink(net::any_io_executor executor, size_t numChunks,
                   size_t maxBufferedBlocksPerChunk = 2)
      : InOrderBlockSink{
            std::move(executor), numChunks,
            makeInMemoryStorageFactory(maxBufferedBlocksPerChunk)} {}

  // The `StorageFactory` that creates an `InMemoryBlockStorage`, which is what
  // the constructor above uses.
  static StorageFactory makeInMemoryStorageFactory(
      size_t maxBufferedBlocksPerChunk) {
    return [maxBufferedBlocksPerChunk](const Strand& strand) {
      return std::make_unique<InMemoryBlockStorage<Block>>(
          strand, maxBufferedBlocksPerChunk);
    };
  }

  // Return `true` if the merge was stopped, either by `asyncAbort()` or by
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
  // chunk of the merge has a single producer that pushes its blocks in a
  // handler chain, so this holds there.
  template <typename CompletionToken>
  auto asyncPush(size_t chunkIndex, Block block,
                 CompletionToken&& completionToken) {
    return sendToChunk(chunkIndex, OptionalBlock{std::move(block)},
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
    return sendToChunk(chunkIndex, OptionalBlock{std::nullopt},
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
  auto asyncAbort(CompletionToken&& completionToken) {
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
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, OptionalBlock)>(
        [this](auto handler) {
          auto executor = net::get_associated_executor(handler, strand_);
          net::post(strand_,
                    [this, handler = std::move(handler), executor]() mutable {
                      receiveNextBlock(std::move(handler), executor);
                    });
        },
        completionToken);
  }

 private:
  // Invoke the `handler` with the given `args` on the `executor`, which is the
  // executor that is associated with the completion token of the operation that
  // the `handler` belongs to. This is where the storage-based operations of
  // this class hop back from `strand_` to the executor of their caller.
  //
  // IMPORTANT: This deliberately uses `net::post` and not `net::dispatch`, so
  // that a completion handler never runs while the strand is held, see the
  // IMPORTANT note in the class comment above. `net::dispatch` would invoke the
  // handler *inline* whenever the calling thread already belongs to the target
  // executor, which is exactly the case for a producer whose handler is bound
  // to the thread pool that also runs this strand.
  //
  // NOTE: The `args` are captured as a single `tuple`, because expanding a
  // parameter pack in the init-capture of a lambda requires C++20.
  template <typename Executor, typename Handler, typename... Args>
  static void completeOn(const Executor& executor, Handler handler,
                         Args&&... args) {
    // Boost.Asio invokes a completion handler exactly once and with moved-in
    // arguments, so every argument here has to be an rvalue. An lvalue would be
    // copied into the `tuple` below instead of being moved.
    static_assert((!std::is_lvalue_reference_v<Args> && ...),
                  "The arguments of a completion handler have to be rvalues");
    net::post(executor, [handler = std::move(handler),
                         args = std::make_tuple(std::move(args)...)]() mutable {
      std::apply(
          [&handler](auto&&... unpacked) {
            std::move(handler)(std::move(unpacked)...);
          },
          std::move(args));
    });
  }

  // The common implementation of `asyncPush` and `asyncFinishChunk`: store the
  // `optionalBlock` (an output block, or `std::nullopt` as the end-of-chunk
  // sentinel) as the next value of the chunk with the given `chunkIndex`.
  // Complete with `false` if the merge was stopped, in which case nothing was
  // stored. The completion signature is `void(std::exception_ptr, bool)`.
  template <typename CompletionToken>
  auto sendToChunk(size_t chunkIndex, OptionalBlock optionalBlock,
                   CompletionToken&& completionToken) {
    AD_CONTRACT_CHECK(chunkIndex < numChunks_);
    return net::async_initiate<CompletionToken, void(std::exception_ptr, bool)>(
        [this, chunkIndex,
         optionalBlock = std::move(optionalBlock)](auto handler) mutable {
          auto executor = net::get_associated_executor(handler, strand_);
          net::post(strand_,
                    [this, chunkIndex, optionalBlock = std::move(optionalBlock),
                     handler = std::move(handler), executor]() mutable {
                      sendToChunkOnStrand(chunkIndex, std::move(optionalBlock),
                                          std::move(handler), executor);
                    });
        },
        completionToken);
  }

  // The body of `sendToChunk`, which completes the `handler` on the `executor`.
  //
  // PRECONDITION: This runs on `strand_`.
  template <typename Handler, typename Executor>
  void sendToChunkOnStrand(size_t chunkIndex, OptionalBlock optionalBlock,
                           Handler handler, const Executor& executor) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (stopRequested_.load()) {
      // A block is silently dropped, and there is no consumer left that could
      // care about a sentinel. NOTE: Returning here (instead of touching the
      // storage) is what makes the teardown airtight, see the class comment
      // above.
      completeOn(executor, std::move(handler), std::exception_ptr{}, false);
      return;
    }
    storage_->storeBlock(
        chunkIndex, std::move(optionalBlock),
        typename Storage::StoreHandler{
            [this, executor, handler = std::move(handler)](
                std::exception_ptr exception, bool wasStored) mutable {
              // NOTE: This runs on `strand_`, see the CONTRACT of
              // `BlockStorage`, which is why `stopRequested_` may be read here.
              completeOn(executor, std::move(handler), std::move(exception),
                         wasStored && !stopRequested_.load());
            }});
  }

  // The body of `asyncGetNextBlock`: complete the `handler` on the `executor`
  // with the next block, or with `std::nullopt` if there is nothing left to
  // read, or with the pushed exception.
  //
  // NOTE: This is the loop of `asyncGetNextBlock` in the coroutine-free world:
  // instead of iterating, the completion handler of `getBlock` calls this
  // function again. That recursion is bounded in the two senses that matter: it
  // always instantiates the same specialization (so the templates terminate),
  // and a storage operation that could complete immediately is still completed
  // via a `post` and never inline (so the stack does not grow).
  //
  // PRECONDITION: This runs on `strand_`.
  template <typename Handler, typename Executor>
  void receiveNextBlock(Handler handler, const Executor& executor) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (exception_ != nullptr) {
      // NOTE: The consumer rethrows this on its own executor, because the
      // completion signature starts with an `std::exception_ptr`.
      completeOn(executor, std::move(handler), std::exception_ptr{exception_},
                 OptionalBlock{std::nullopt});
      return;
    }
    if (stopRequested_.load() || nextChunkToRead_ >= numChunks_) {
      completeOn(executor, std::move(handler), std::exception_ptr{},
                 OptionalBlock{std::nullopt});
      return;
    }
    storage_->getBlock(
        nextChunkToRead_,
        typename Storage::GetHandler{
            [this, executor, handler = std::move(handler)](
                std::exception_ptr exception,
                typename Storage::GetResult result) mutable {
              // NOTE: This runs on `strand_`, see `sendToChunkOnStrand`.
              if (exception != nullptr) {
                completeOn(executor, std::move(handler), std::move(exception),
                           OptionalBlock{std::nullopt});
                return;
              }
              if (!result.has_value()) {
                // The storage was cancelled, which only happens while the merge
                // is torn down. Continue, such that the next round either
                // rethrows the pushed exception or reports the end of the
                // range.
                AD_CORRECTNESS_CHECK(stopRequested_.load());
                receiveNextBlock(std::move(handler), executor);
                return;
              }
              if (result.value().has_value()) {
                completeOn(executor, std::move(handler), std::exception_ptr{},
                           std::move(result).value());
                return;
              }
              // The end-of-chunk sentinel, so move on to the next chunk.
              storage_->eraseRun(nextChunkToRead_);
              ++nextChunkToRead_;
              receiveNextBlock(std::move(handler), executor);
            }});
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
          storage_->cancelAll();
        },
        "Stopping an `InOrderBlockSink` failed.");
  }
};

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_INORDERBLOCKSINK_H

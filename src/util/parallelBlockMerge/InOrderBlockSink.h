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

#include <absl/container/node_hash_map.h>

#include <atomic>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/system/error_code.hpp>
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
// Each chunk that is currently active owns a single channel with a capacity of
// `maxBufferedBlocksPerChunk`, via which its producer sends the finished output
// blocks to the consumer. Suspending in `async_send` on a full channel *is* the
// back-pressure that bounds the memory consumption of the merge. The end of a
// chunk is signalled by an explicit `std::nullopt` sentinel, upon which the
// consumer erases the state of that chunk, so that the memory consumption is
// proportional to the number of chunks that are in flight and not to the total
// number of chunks.
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
// STRAND CONFINEMENT: All the mutable state of this class (the map of channels,
// the index of the chunk that is currently read, and the exception) as well as
// *every* channel operation is confined to a single `strand_`, so that no mutex
// of our own is required and the channels can be plain (non-concurrent)
// channels whose internal mutex is a null mutex. Every operation therefore
// consists of a hop onto the strand (a `net::post`), the actual work, and a hop
// back to the executor of the caller. None of these operations is a coroutine;
// they are plain `async_initiate` operations built from handlers.
// The only member that is ever read off the strand is the atomic
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
// strand would serialize the entire merge.
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
// its channel and suspend. That is not a problem even if there is only a single
// thread, because such a producer suspends instead of blocking its thread. In
// particular the number of chunks that are in flight does not have to be
// bounded by the available parallelism, see `ParallelMergeState`.
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
// 3. Every operation costs two executor hops and the handler allocations that
//    come with them. That is negligible next to an output block of 100k
//    elements (or 16 MB), but it makes this sink a poor fit for small payloads;
//    such a user would have to batch, or run on the strand to begin with.
// 4. All those hops allocate (the handler that is posted onto the strand, and
//    the one that `ParallelMergeState::abort` posts), so the teardown itself
//    can fail once memory is exhausted. The failure is then swallowed and the
//    consumer simply sees the end of the range.
// 5. Once the merge was stopped, nothing erases the entries of `chunks_`
//    anymore, so the channels of the chunks that were still in flight, and the
//    blocks that they still buffer, live until this sink is destroyed.
// 6. The `node_hash_map` is redundant by now. Its stable addresses were needed
//    when the channels were stored by value; with `shared_ptr` values a
//    `flat_hash_map` would do.
// 7. `ParallelMergeRange::get()`, the synchronous adapter of this machinery,
//    blocks its calling thread on a `future`, so the executor has to be run by
//    *other* threads. See the note there.
// 8. The intermediate storage of the blocks is not abstracted away: `chunks_`
//    stores them in one channel per chunk, and that channel is at the same time
//    the buffer, the FIFO order, and the back-pressure. A `BlockStorage`
//    interface (a `unique_ptr` to an abstract base, with a plain in-memory
//    implementation and one that keeps compressed blocks on disk, the latter
//    sharing its code with `CompressedExternalIdTableWriter`) would let a
//    caller spill instead of suspending its producers. NOTE: Such an interface
//    cannot be synchronous, because both storing and retrieving a block have to
//    be able to suspend, so it would have to be an asynchronous operation of
//    its own. It would also have to be templated on `Block`, while the on-disk
//    implementation only exists for `IdTable`.
//
// NOTE: The class is neither copyable nor movable, because the producers and
// the consumer refer to it by reference.
template <typename Block>
class InOrderBlockSink : public ad_utility::NoCopyNoMove {
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
  InOrderBlockSink(net::any_io_executor executor, size_t numChunks,
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
  template <typename CompletionToken>
  auto asyncPush(size_t chunkIndex, Block block,
                 CompletionToken&& completionToken) {
    return sendToChunk(chunkIndex, OptionalBlock{std::move(block)},
                       AD_FWD(completionToken));
  }

  // Announce that no further blocks will be pushed for the chunk with the given
  // `chunkIndex`, by sending the end-of-chunk sentinel. Call this exactly once
  // per chunk, and on every path, because the consumer would otherwise wait for
  // that chunk forever. Suspend while the channel of the chunk is full.
  // Complete with `false` if the merge was stopped, in which case there is no
  // consumer left that could care about the sentinel.
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
          // suspended, and no channel is ever created afterwards (see
          // `getOrCreateChannel`), so a later exception has nothing left to
          // wake and its sweep over the channels would be redundant.
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
  // operations: two concurrent calls would both receive from the channel of the
  // same chunk, so the global order would be lost, and the one that gets the
  // end-of-chunk sentinel would erase (and thereby destroy) the very channel
  // that the other one is still waiting on.
  template <typename CompletionToken>
  auto asyncGetNextBlock(CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, std::optional<Block>)>(
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
  // the `handler` belongs to. This is where the channel-based operations of
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

  // The common implementation of `asyncPush` and `asyncFinishChunk`: send the
  // `optionalBlock` (an output block, or `std::nullopt` as the end-of-chunk
  // sentinel) into the channel of the chunk with the given `chunkIndex`.
  // Complete with `false` if the merge was stopped, in which case nothing was
  // sent. The completion signature is `void(std::exception_ptr, bool)`.
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
      // care about a sentinel. NOTE: Returning here (instead of creating a
      // channel and sending into it) is what makes the teardown airtight, see
      // the class comment above.
      completeOn(executor, std::move(handler), std::exception_ptr{}, false);
      return;
    }
    // NOTE: For the sentinel the channel is created here if the chunk did not
    // push a single block, because the consumer still has to see that sentinel.
    SharedBlockChannel channel;
    try {
      channel = getOrCreateChannel(chunkIndex);
    } catch (...) {
      completeOn(executor, std::move(handler), std::current_exception(), false);
      return;
    }
    channel->async_send(
        boost::system::error_code{}, std::move(optionalBlock),
        [this, handler = std::move(handler),
         executor](boost::system::error_code errorCode) mutable {
          // NOTE: This runs on `strand_`, because the channel
          // was created with `strand_` as its executor and this
          // handler has no executor of its own that would
          // override that.
          completeOn(executor, std::move(handler), std::exception_ptr{},
                     !errorCode && !stopRequested_.load());
        });
  }

  // The body of `asyncGetNextBlock`: complete the `handler` on the `executor`
  // with the next block, or with `std::nullopt` if there is nothing left to
  // read, or with the pushed exception.
  //
  // NOTE: This is the loop of `asyncGetNextBlock` in the coroutine-free world:
  // instead of iterating, the completion handler of `async_receive` calls this
  // function again. That recursion is bounded in the two senses that matter: it
  // always instantiates the same specialization (so the templates terminate),
  // and a channel operation that could complete immediately is still completed
  // via a `post` and never inline (so the stack does not grow).
  //
  // PRECONDITION: This runs on `strand_`.
  template <typename Handler, typename Executor>
  void receiveNextBlock(Handler handler, const Executor& executor) {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    std::optional<SharedBlockChannel> channel;
    try {
      channel = getCurrentOutputChannel();
    } catch (...) {
      completeOn(executor, std::move(handler), std::current_exception(),
                 std::optional<Block>{std::nullopt});
      return;
    }
    if (!channel.has_value()) {
      completeOn(executor, std::move(handler), std::exception_ptr{},
                 std::optional<Block>{std::nullopt});
      return;
    }
    channel.value()->async_receive(
        [this, handler = std::move(handler), executor](
            boost::system::error_code errorCode, OptionalBlock block) mutable {
          // NOTE: This runs on `strand_`, see `sendToChunkOnStrand`.
          if (errorCode) {
            // The channel was cancelled, which only happens while the merge is
            // torn down. Continue, such that the next round either rethrows the
            // pushed exception or reports the end of the range.
            AD_CORRECTNESS_CHECK(stopRequested_.load());
            receiveNextBlock(std::move(handler), executor);
            return;
          }
          if (block.has_value()) {
            completeOn(executor, std::move(handler), std::exception_ptr{},
                       std::move(block));
            return;
          }
          // The end-of-chunk sentinel, so move on to the next chunk. NOTE:
          // Erasing the entry is safe even if the producer of that chunk still
          // holds the channel, because the channels are shared.
          chunks_.erase(nextChunkToRead_);
          ++nextChunkToRead_;
          receiveNextBlock(std::move(handler), executor);
        });
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
        "Stopping an `InOrderBlockSink` failed.");
  }

  // Return the channel of the chunk that the consumer currently reads from,
  // creating it if it does not exist yet, or `std::nullopt` if there is nothing
  // left to read because all chunks are exhausted or the merge was stopped.
  // Rethrow a pushed exception.
  //
  // NOTE: The exception propagates out of the handler that runs on the strand
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

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_INORDERBLOCKSINK_H

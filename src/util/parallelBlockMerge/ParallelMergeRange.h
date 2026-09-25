// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGERANGE_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGERANGE_H

// The synchronous consumer side of a parallel merge that pushes to an
// `InOrderBlockSink`. It adapts a merge that is driven by coroutines, so this
// whole header is only available in C++20 mode and empty when
// `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set, see
// `util/parallelBlockMerge/ParallelMergeState.h`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "util/Exception.h"
#include "util/Iterators.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/InOrderBlockSink.h"

namespace ad_utility::parallelBlockMerge {
namespace detail {

// The read-ahead of a `ParallelMergeRange` (see below): a chain of asynchronous
// `asyncGetNextBlock` operations on the sink that keeps up to
// `numPrefetchedBlocks` output blocks ready, together with the buffer of those
// blocks and the synchronization that the blocking consumer needs. A consumer
// that asks for the next block therefore typically gets one that is already
// there, instead of having to wait for the merge.
//
// CHAIN: At most a single `asyncGetNextBlock` is ever in flight: the next one
// is only initiated when the previous one has completed, or by the consumer
// when it has made room in the buffer while the chain is paused. That is not an
// implementation detail but a hard requirement of
// `InOrderBlockSink::asyncGetNextBlock`, which must never run concurrently with
// itself, see there. The read-ahead is hence bounded by the buffer and not by
// the number of concurrent operations.
//
// NOTE: No thread of its own is involved. The completion handlers have no
// associated executor and therefore run on the executor of the sink (see
// `InOrderBlockSink::spawnOnStrand`), which is the executor of the merge
// itself.
//
// LIFETIME: A completion handler may still be in flight when the consumer is
// already gone, which is why this state lives in a `shared_ptr` that every
// handler holds alive (instead of the handlers referring to the range), and why
// `shutDown()` waits until no operation is in flight any more, see there.
template <typename Block, typename Sink>
class BlockPrefetcher
    : public std::enable_shared_from_this<BlockPrefetcher<Block, Sink>>,
      public ad_utility::NoCopyNoMove {
 private:
  // A tag that makes the constructor unusable from the outside, such that a
  // `BlockPrefetcher` can only be created via `create()` and hence only ever
  // exists inside a `shared_ptr`, see the LIFETIME note above.
  struct PrivateTag {};

  std::shared_ptr<Sink> sink_;
  // The number of blocks that the buffer below may hold, see
  // `MergeOptions::numPrefetchedOutputBlocks`.
  size_t numPrefetchedBlocks_;
  std::mutex mutex_;
  // Notified whenever any of the members below changes, that is when a block
  // becomes available, when the end of the merge or an exception is seen, and
  // when the operation that was in flight has completed.
  std::condition_variable stateChanged_;
  // All the following members are guarded by `mutex_`.
  std::deque<Block> readyBlocks_;
  // The first exception that the merge has pushed. It is deliberately kept (and
  // not moved out), such that every further call to `getNextBlock()` rethrows
  // it, exactly as the sink itself does.
  std::exception_ptr exception_;
  bool endOfMergeWasReached_ = false;
  bool operationIsInFlight_ = false;
  bool wasShutDown_ = false;

 public:
  // The constructor is effectively private, use `create()` instead.
  BlockPrefetcher(PrivateTag, std::shared_ptr<Sink> sink,
                  size_t numPrefetchedBlocks)
      : sink_{std::move(sink)}, numPrefetchedBlocks_{numPrefetchedBlocks} {
    AD_CORRECTNESS_CHECK(sink_ != nullptr);
    AD_CORRECTNESS_CHECK(numPrefetchedBlocks_ > 0);
  }

  // Create a prefetcher that reads up to `numPrefetchedBlocks` blocks ahead
  // from the `sink`, and start its chain right away.
  static std::shared_ptr<BlockPrefetcher> create(std::shared_ptr<Sink> sink,
                                                 size_t numPrefetchedBlocks) {
    auto self = std::make_shared<BlockPrefetcher>(PrivateTag{}, std::move(sink),
                                                  numPrefetchedBlocks);
    // NOTE: The chain can only be started once the `shared_ptr` exists, because
    // the completion handlers hold this object alive via `shared_from_this()`,
    // see the LIFETIME note above.
    self->startOperationIfNeeded(std::unique_lock{self->mutex_});
    return self;
  }

  // Return the next block of the merge, or `std::nullopt` at its end, and block
  // the calling thread until one of the two is available. Rethrow an exception
  // that the merge has pushed, once all the blocks that were already buffered
  // have been returned.
  std::optional<Block> getNextBlock() {
    std::unique_lock lock{mutex_};
    stateChanged_.wait(lock, [this]() {
      return !readyBlocks_.empty() || endOfMergeWasReached_ ||
             exception_ != nullptr || wasShutDown_;
    });
    if (!readyBlocks_.empty()) {
      Block block = std::move(readyBlocks_.front());
      readyBlocks_.pop_front();
      // There is room in the buffer again, so re-arm a chain that is paused.
      startOperationIfNeeded(std::move(lock));
      return std::optional<Block>{std::move(block)};
    }
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
    return std::nullopt;
  }

  // Stop the read-ahead and release everything that it holds. Wait until the
  // operation that is in flight (if any) has completed, such that no completion
  // handler is left that could still touch the sink.
  //
  // PRECONDITION: The merge has already been stopped, because only then does
  // the sink complete a pending `asyncGetNextBlock` promptly (with
  // `std::nullopt`) and this wait terminates. See the destructor of
  // `ParallelMergeRange` for the exact ordering.
  //
  // NOTE: This blocks the calling thread, which therefore must not be one of
  // the threads that run the executor of the merge, because that thread is then
  // no longer available to complete the very operation that this waits for.
  void shutDown() {
    std::unique_lock lock{mutex_};
    // No further operation is started from now on, in particular not by the
    // completion handler of the one that may still be in flight.
    wasShutDown_ = true;
    stateChanged_.wait(lock, [this]() { return !operationIsInFlight_; });
    readyBlocks_.clear();
    sink_.reset();
  }

 private:
  // Initiate the next `asyncGetNextBlock` if the chain is currently paused and
  // there is room in the buffer for another block. The `lock` (which has to be
  // held) is released before the operation is initiated, see below.
  void startOperationIfNeeded(std::unique_lock<std::mutex> lock) {
    AD_CORRECTNESS_CHECK(lock.owns_lock());
    if (operationIsInFlight_ || wasShutDown_ || endOfMergeWasReached_ ||
        exception_ != nullptr || readyBlocks_.size() >= numPrefetchedBlocks_) {
      return;
    }
    operationIsInFlight_ = true;
    // NOTE: The flag above is what keeps the `sink_` alive while the lock is
    // not held: `shutDown()` only resets it once no operation is in flight.
    // Initiating the operation is deliberately done without the lock, because
    // its completion handler may run on another thread while we are still
    // inside the initiating call.
    lock.unlock();
    try {
      sink_->asyncGetNextBlock([self = this->shared_from_this()](
                                   std::exception_ptr exception,
                                   std::optional<Block> block) mutable {
        self->onOperationComplete(std::move(exception), std::move(block));
      });
    } catch (...) {
      // Initiating the operation can only fail if the allocation of its
      // completion handler fails, in which case no handler will ever run. Play
      // that handler ourselves, so that the operation does not stay in flight
      // forever (which would make `shutDown()` wait forever) and the consumer
      // sees the failure.
      onOperationComplete(std::current_exception(), std::nullopt);
    }
  }

  // The completion handler of a single `asyncGetNextBlock`: store what it
  // yielded, wake up a consumer that is waiting for it, and continue the chain.
  void onOperationComplete(std::exception_ptr exception,
                           std::optional<Block> block) {
    std::unique_lock lock{mutex_};
    AD_CORRECTNESS_CHECK(operationIsInFlight_);
    operationIsInFlight_ = false;
    if (exception != nullptr) {
      // NOTE: Only the first exception is kept, just as in the sink itself.
      if (exception_ == nullptr) {
        exception_ = std::move(exception);
      }
    } else if (block.has_value()) {
      readyBlocks_.push_back(std::move(block).value());
    } else {
      endOfMergeWasReached_ = true;
    }
    // NOTE: This also wakes up a `shutDown()` that waits for this very
    // operation. That wait is only over once this function has released the
    // lock, so the decision whether the chain continues has been made by then.
    stateChanged_.notify_all();
    startOperationIfNeeded(std::move(lock));
  }
};

// A synchronous adapter for a parallel merge that pushes its output blocks to
// an `InOrderBlockSink`: a lazy range of blocks whose `get()` blocks the
// calling thread until the next block is available. Use this to plug the merge
// into a consumer that is not itself asynchronous, see
// `parallelBlockMergeToRange`.
//
// The blocks are read ahead in the background: the range keeps up to
// `numPrefetchedBlocks` blocks ready, so that the consumer typically does not
// have to wait for the merge at all, see `BlockPrefetcher` above. The
// read-ahead costs memory (it holds those blocks, plus the one that the
// operation which is in flight is about to deliver), which the caller has to
// account for.
//
// The range releases everything that the merge owns as soon as that merge is
// over, and not only when the range itself is destroyed: reaching the end (and
// likewise an exception that reaches the consumer) runs exactly the same
// teardown as the destructor, see `releaseEverything()`. That is essential and
// not merely tidy, see the comment there.
//
// IMPORTANT: The executor of the merge has to be run by *other* threads (for
// example by a `boost::asio::thread_pool`), because the thread that iterates
// over this range is blocked while it waits for the next block and can
// therefore not run any of the merge's coroutines itself. The same holds for
// the thread that destroys this range, which waits for those coroutines, see
// the destructor.
template <typename State, typename Sink>
class ParallelMergeRange
    : public ad_utility::InputRangeFromGet<typename State::Block>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename State::Block;

 private:
  using Prefetcher = BlockPrefetcher<Block, Sink>;

  // All of the following are dropped by `releaseEverything()`, which is what
  // makes a range that is over hold on to nothing at all. In particular
  // `state_ == nullptr` is exactly the state "everything was released".
  std::shared_ptr<State> state_;
  std::shared_ptr<Sink> sink_;
  // The read-ahead, which is the only thing that ever reads from the `sink_`.
  std::shared_ptr<Prefetcher> prefetcher_;
  // Becomes ready when the merge has released everything that it owns, see
  // `releaseEverything()`.
  std::future<void> mergeIsComplete_;
  // The exception that `get()` has propagated to the consumer, if any. It is
  // kept, such that every further call to `get()` rethrows it, exactly as the
  // sink itself does.
  std::exception_ptr exception_;

 public:
  // Construct from the `state` of a merge that was already started, together
  // with the `sink` that this merge pushes to, see `parallelBlockMergeToSink`.
  // The `numPrefetchedBlocks` (which have to be positive) are the number of
  // blocks that are read ahead, see `MergeOptions::numPrefetchedOutputBlocks`.
  ParallelMergeRange(std::shared_ptr<State> state, std::shared_ptr<Sink> sink,
                     size_t numPrefetchedBlocks)
      : state_{std::move(state)}, sink_{std::move(sink)} {
    AD_CONTRACT_CHECK(state_ != nullptr);
    AD_CONTRACT_CHECK(sink_ != nullptr);
    AD_CONTRACT_CHECK(numPrefetchedBlocks > 0);
    mergeIsComplete_ = state_->asyncWaitForCompletion();
    prefetcher_ = Prefetcher::create(sink_, numPrefetchedBlocks);
  }

  // Release everything that this range holds, see `releaseEverything()`. It is
  // typically already gone, because a range that was consumed to its end (or
  // that has propagated an exception) has released everything right away.
  //
  // NOTE: This blocks the calling thread, which therefore must not be one of
  // the threads that run the executor of the merge, see the IMPORTANT note at
  // the class comment above.
  ~ParallelMergeRange() override { releaseEverything(); }

  // Return the next block of the merge, or `std::nullopt` at its end. Rethrow
  // an exception that the merge has pushed. Release everything that the merge
  // owns as soon as either of the two happens, see `releaseEverything()`.
  //
  // IMPORTANT: This is *synchronous and blocking*: it waits until the next
  // block is available and hence occupies its thread for that whole time (which
  // the read-ahead makes much less likely, but never impossible). It therefore
  // deadlocks if it is called from one of the threads that run the executor of
  // the merge, because that thread is then no longer available to run the
  // coroutines that produce the very block it waits for. In the extreme case of
  // a single-threaded executor the *first* call already deadlocks. See also the
  // IMPORTANT note in the class comment above.
  std::optional<Block> get() override {
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
    if (prefetcher_ == nullptr) {
      // Everything was already released, so this range is simply over.
      return std::nullopt;
    }
    std::optional<Block> block;
    try {
      block = prefetcher_->getNextBlock();
    } catch (...) {
      // The consumer of a range that has thrown may well keep that range alive
      // for a long time, so the merge has to be torn down here as well.
      exception_ = std::current_exception();
      releaseEverything();
      throw;
    }
    if (!block.has_value()) {
      releaseEverything();
    }
    return block;
  }

 private:
  // Stop the merge, such that the coroutines that are still in flight finish
  // instead of waiting for a consumer that is gone, then wait until they are
  // actually done, and drop everything that this range holds. Idempotent, and
  // called both when this range is over (see `get()`) and when it is destroyed.
  //
  // The wait is essential and not merely tidy. The merge owns its input for as
  // long as a single one of its coroutines is still running, and it keeps
  // *reading* from that input until it sees the stop (see the LIFETIME note at
  // `ParallelMergeState`). Consumers in turn legitimately dispose of the
  // resources behind that input as soon as the merge is over: a
  // `CompressedExternalIdTableSorter`, for example, may be `clear()`ed right
  // afterwards, which closes and deletes the very file that the input reads its
  // blocks from.
  //
  // IMPORTANT: Reaching the end of the range has to release the input just like
  // the destructor does, because a consumer that has read a range to its end
  // legitimately keeps that (now exhausted) range alive while it disposes of
  // the input. `CompressedIdTableRunsInput` for example unregisters itself from
  // `CompressedExternalIdTableWriter::registerActiveReader` only in its
  // *destructor*, and `IndexImpl::buildOspWithPatterns` exhausts such a range,
  // keeps it in a local variable, and then calls `clear()` on the sorter, which
  // throws while a reader is still registered.
  //
  // NOTE: The order of the steps below is subtle. The merge is stopped
  // *first*, because only then does the sink complete a pending
  // `asyncGetNextBlock` of the read-ahead promptly (with `std::nullopt`)
  // instead of waiting for blocks that nobody will produce any more. Only then
  // can we wait for the read-ahead to be quiescent, which we have to do before
  // the sink and the state are dropped, and which also releases the blocks that
  // were read ahead. The wait for the merge itself comes last, because the
  // coroutines only release the state once our own references are gone.
  //
  // NOTE: Both waits block the calling thread, which therefore must not be one
  // of the threads that run the executor of the merge, see the IMPORTANT note
  // at the class comment above.
  void releaseEverything() {
    if (state_ == nullptr) {
      // Everything was already released, in particular `mergeIsComplete_` was
      // already waited for.
      return;
    }
    state_->stop();
    prefetcher_->shutDown();
    prefetcher_.reset();
    // Drop our own references first, so that the merge can release its state as
    // soon as its last coroutine is done.
    sink_.reset();
    state_.reset();
    mergeIsComplete_.wait();
  }
};

}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGERANGE_H

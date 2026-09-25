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

#include <boost/asio/use_future.hpp>
#include <exception>
#include <future>
#include <memory>
#include <optional>
#include <utility>

#include "util/Exception.h"
#include "util/Iterators.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/InOrderBlockSink.h"

namespace ad_utility::parallelBlockMerge {
namespace detail {

// A synchronous adapter for a parallel merge that pushes its output blocks to
// an `InOrderBlockSink`: a lazy range of blocks whose `get()` blocks the
// calling thread until the next block is available. Use this to plug the merge
// into a consumer that is not itself asynchronous, see
// `parallelBlockMergeToRange`.
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
  // All of the following are dropped by `releaseEverything()`, which is what
  // makes a range that is over hold on to nothing at all. In particular
  // `state_ == nullptr` is exactly the state "everything was released".
  std::shared_ptr<State> state_;
  std::shared_ptr<Sink> sink_;
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
  ParallelMergeRange(std::shared_ptr<State> state, std::shared_ptr<Sink> sink)
      : state_{std::move(state)}, sink_{std::move(sink)} {
    AD_CONTRACT_CHECK(state_ != nullptr);
    AD_CONTRACT_CHECK(sink_ != nullptr);
    mergeIsComplete_ = state_->asyncWaitForCompletion();
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
  // IMPORTANT: This is *synchronous and blocking*: it waits on a `future` until
  // the next block is available and hence occupies its thread for that whole
  // time. It therefore deadlocks if it is called from one of the threads that
  // run the executor of the merge, because that thread is then no longer
  // available to run the coroutines that produce the very block it waits for.
  // In the extreme case of a single-threaded executor the *first* call already
  // deadlocks. See also the IMPORTANT note in the class comment above.
  std::optional<Block> get() override {
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
    if (sink_ == nullptr) {
      // Everything was already released, so this range is simply over.
      return std::nullopt;
    }
    std::optional<Block> block;
    try {
      block = sink_->asyncGetNextBlock(net::use_future).get();
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
  // NOTE: The wait for the merge comes last, because the coroutines only
  // release the state once our own references are gone.
  //
  // NOTE: The wait blocks the calling thread, which therefore must not be one
  // of the threads that run the executor of the merge, see the IMPORTANT note
  // at the class comment above.
  void releaseEverything() {
    if (state_ == nullptr) {
      // Everything was already released, in particular `mergeIsComplete_` was
      // already waited for.
      return;
    }
    state_->stop();
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

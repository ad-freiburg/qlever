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

#include <boost/asio/any_io_executor.hpp>
#include <cstddef>
#include <memory>
#include <optional>
#include <utility>

#include "util/Exception.h"
#include "util/Iterators.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/BlockPrefetcher.h"
#include "util/parallelBlockMerge/InOrderBlockSink.h"

namespace ad_utility::parallelBlockMerge {
namespace detail {

// A synchronous adapter for a parallel merge that pushes its output blocks to
// an `InOrderBlockSink`: a lazy range of blocks whose `get()` blocks the
// calling thread until the next block is available. Use this to plug the merge
// into a consumer that is not itself asynchronous, see
// `parallelBlockMergeToRange`.
//
// The blocks are read ahead in the background: the range keeps up to
// `numPrefetchedBlocks` blocks ready, so that the consumer typically does not
// have to wait for the merge at all, see `BlockPrefetcher`. The read-ahead
// costs memory (it holds those blocks, plus the one that it is about to hand
// over), which the caller has to account for.
//
// IMPORTANT: The executor of the merge has to be run by *other* threads (for
// example by a `boost::asio::thread_pool`), because the thread that iterates
// over this range is blocked while it waits for the next block and can
// therefore not run any of the merge's coroutines itself. The same holds for
// the thread that destroys this range, which waits for the read-ahead, see the
// destructor.
template <typename State, typename Sink>
class ParallelMergeRange
    : public ad_utility::InputRangeFromGet<typename State::Block>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename State::Block;

 private:
  using Prefetcher = BlockPrefetcher<Block, Sink>;

  std::shared_ptr<State> state_;
  std::shared_ptr<Sink> sink_;
  // The read-ahead, which is the only thing that ever reads from the `sink_`.
  std::unique_ptr<Prefetcher> prefetcher_;

 public:
  // Construct from the `executor` of a merge that was already started, its
  // `state`, and the `sink` that this merge pushes to, see
  // `parallelBlockMergeToSink`. The `numPrefetchedBlocks` (which have to be
  // positive) are the number of blocks that are read ahead (on the `executor`),
  // see `MergeOptions::numPrefetchedOutputBlocks`.
  ParallelMergeRange(net::any_io_executor executor,
                     std::shared_ptr<State> state, std::shared_ptr<Sink> sink,
                     size_t numPrefetchedBlocks)
      : state_{std::move(state)}, sink_{std::move(sink)} {
    AD_CONTRACT_CHECK(state_ != nullptr);
    AD_CONTRACT_CHECK(sink_ != nullptr);
    AD_CONTRACT_CHECK(numPrefetchedBlocks > 0);
    prefetcher_ = std::make_unique<Prefetcher>(std::move(executor), sink_,
                                               numPrefetchedBlocks);
  }

  // Stop the merge, such that the coroutines that are still in flight finish
  // instead of waiting for a consumer that is gone, and then shut down the
  // read-ahead. The order matters: only a stopped merge makes the sink complete
  // a pending `asyncGetNextBlock` of the read-ahead promptly (with
  // `std::nullopt`) instead of waiting for blocks that nobody will produce any
  // more, and only then does the wait in `BlockPrefetcher::shutDown()`
  // terminate.
  //
  // NOTE: This blocks the calling thread, which therefore must not be one of
  // the threads that run the executor of the merge, see the IMPORTANT note at
  // the class comment above.
  ~ParallelMergeRange() override {
    state_->stop();
    prefetcher_->shutDown();
  }

  // Return the next block of the merge, or `std::nullopt` at its end.
  //
  // IMPORTANT: This is *synchronous and blocking*: it waits until the next
  // block is available and hence occupies its thread for that whole time (which
  // the read-ahead makes much less likely, but never impossible). It therefore
  // deadlocks if it is called from one of the threads that run the executor of
  // the merge, because that thread is then no longer available to run the
  // coroutines that produce the very block it waits for. In the extreme case of
  // a single-threaded executor the *first* call already deadlocks. See also the
  // IMPORTANT note in the class comment above.
  std::optional<Block> get() override { return prefetcher_->getNextBlock(); }
};

}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGERANGE_H

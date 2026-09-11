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
// IMPORTANT: The executor of the merge has to be run by *other* threads (for
// example by a `boost::asio::thread_pool`), because the thread that iterates
// over this range is blocked while it waits for the next block and can
// therefore not run any of the merge's coroutines itself.
template <typename State>
class ParallelMergeRange
    : public ad_utility::InputRangeFromGet<typename State::Block>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename State::Block;
  using Sink = InOrderBlockSink<Block>;

 private:
  std::shared_ptr<State> state_;
  std::shared_ptr<Sink> sink_;

 public:
  // Construct from the `state` of a merge that was already started, together
  // with the `sink` that this merge pushes to, see `parallelBlockMergeToSink`.
  ParallelMergeRange(std::shared_ptr<State> state, std::shared_ptr<Sink> sink)
      : state_{std::move(state)}, sink_{std::move(sink)} {
    AD_CONTRACT_CHECK(state_ != nullptr);
    AD_CONTRACT_CHECK(sink_ != nullptr);
  }

  // Stop the merge, such that the coroutines that are still in flight finish
  // instead of waiting for a consumer that is gone.
  ~ParallelMergeRange() override { state_->stop(); }

  // Return the next block of the merge, or `std::nullopt` at its end.
  //
  // IMPORTANT: This is *synchronous and blocking*: it waits on a `future` until
  // the next block is available and hence occupies its thread for that whole
  // time. It therefore deadlocks if it is called from one of the threads that
  // run the executor of the merge, because that thread is then no longer
  // available to run the coroutines that produce the very block it waits for.
  // In the extreme case of a single-threaded executor the *first* call already
  // deadlocks. See also the IMPORTANT note in the class comment above.
  std::optional<Block> get() override {
    return sink_->asyncGetNextBlock(net::use_future).get();
  }
};

}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGERANGE_H

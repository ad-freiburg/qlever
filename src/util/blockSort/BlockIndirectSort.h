// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Derived from Boost.Sort, file
// `boost/sort/block_indirect_sort/block_indirect_sort.hpp`:
// Copyright (c) 2016 Francisco Jose Tapia (fjtapia@gmail.com)
// Distributed under the Boost Software License, Version 1.0. (See the
// accompanying file `LICENSE_1_0.txt` or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_BLOCKINDIRECTSORT_H
#define QLEVER_SRC_UTIL_BLOCKSORT_BLOCKINDIRECTSORT_H

#include <algorithm>
#include <bit>
#include <boost/sort/pdqsort/pdqsort.hpp>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <utility>

#include "backports/algorithm.h"
#include "backports/asio.h"
#include "backports/concepts.h"
#include "util/Exception.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include "util/blockSort/MergeBlocks.h"
#include "util/blockSort/MoveBlocks.h"
#include "util/blockSort/ParallelQuicksort.h"
#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"
#endif

// `boost::sort::block_indirect_sort` (by Francisco Jose Tapia), ported to
// Boost.Asio coroutines so that it runs on an existing executor instead of
// spawning its own threads.
//
// The input is divided into blocks, which are sorted in parallel
// (`ParallelQuicksort.h`). The sorted parts are then merged pairwise by
// permuting an index of the blocks; elements only move between neighbouring
// blocks that overlap (`MergeBlocks.h`). Finally the blocks are moved to where
// the index says (`MoveBlocks.h`). The extra memory is one block per thread.
//
// Differences to Boost: a task waiting for its children suspends
// (`TaskGroup.h`) instead of spinning on a shared work stack, and the
// `thread_local` scratch buffer is replaced by `ScratchBuffers` (`SortState.h`)
// because a coroutine may change threads.
namespace ad_utility::blockSort {

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
namespace detail {

namespace net = boost::asio;

// Below this many threads, only the parallel quicksort is used. This is
// Boost's `BOOST_NTHREAD_BORDER`.
constexpr uint32_t minNumThreadsForBlocks = 6;

// Sort the blocks `[posIndexBegin, posIndexEnd)`: split them in half
// `numRecursionsLeft` more times, sort each part with `parallelQuicksort`, then
// merge the halves on the way back up.
template <typename State>
net::awaitable<void> sortRecursively(State& state, size_t posIndexBegin,
                                     size_t posIndexEnd,
                                     uint32_t numRecursionsLeft) {
  // `sortOnExecutor` limits the number of threads such that every part has at
  // least `BLOCKS_PER_TASK` blocks, see `mergeTail` for why this matters.
  AD_CORRECTNESS_CHECK(posIndexEnd - posIndexBegin >= BLOCKS_PER_TASK);
  if (numRecursionsLeft == 0) {
    // No block has been moved yet, so physical and logical positions agree.
    co_await parallelQuicksort(state, state.getBlockBegin(posIndexBegin),
                               state.getBlockBegin(posIndexEnd));
    co_return;
  }
  size_t posIndexMid = std::midpoint(posIndexBegin, posIndexEnd);
  co_await state.runConcurrently(
      sortRecursively(state, posIndexBegin, posIndexMid, numRecursionsLeft - 1),
      sortRecursively(state, posIndexMid, posIndexEnd, numRecursionsLeft - 1));
  co_await mergeSortedHalves(state, posIndexBegin, posIndexMid, posIndexEnd);
}

// The root task of a sort, Boost's `start_function`.
template <typename State>
net::awaitable<void> startSort(State& state, uint32_t numThreads) {
  if (numThreads < minNumThreadsForBlocks) {
    co_await parallelQuicksort(state, state.globalRange_.first,
                               state.globalRange_.last);
    co_return;
  }
  // Split into the largest power of two of parts that is smaller than
  // `numThreads` (e.g. 4 parts for 6 to 8 threads), like Boost. Each part is
  // sorted by a parallel quicksort, so the other threads are used too.
  auto numRecursions =
      static_cast<uint32_t>(std::bit_width(numThreads - 1)) - 1;
  co_await sortRecursively(state, 0, state.numBlocks_, numRecursions);
  co_await moveBlocks(state);
}

// The default tuning parameters for elements of type `Value`.
template <typename Value>
[[nodiscard]] constexpr SortParams defaultSortParams() {
  return {blockSizeFor<Value>(), defaultMaxElementsPerTask<Value>()};
}

// Sort `[begin, end)` with the given tuning parameters, see
// `blockIndirectSort` below. Should run on `exec`: the tasks of the sort resume
// their parents on the executor of the parent, so running elsewhere is correct,
// but costs a hop between the executors for every join.
template <typename Iterator, typename Compare>
net::awaitable<void> sortOnExecutor(Iterator begin, Iterator end, Compare comp,
                                    uint32_t numThreads,
                                    ql::any_io_executor exec,
                                    SortParams params) {
  AD_CORRECTNESS_CHECK(params.blockSize > 0);
  AD_CORRECTNESS_CHECK(params.maxElementsPerTask >= 16);
  // Cheap special cases: already sorted (including empty), or sorted in
  // reverse.
  if (ql::ranges::is_sorted(begin, end, comp)) {
    co_return;
  }
  if (isDescending(begin, end, comp)) {
    ql::ranges::reverse(begin, end);
    co_return;
  }

  // At most one thread per group of blocks.
  size_t numElements = static_cast<size_t>(end - begin);
  numThreads = std::min(
      numThreads, static_cast<uint32_t>(
                      numElements / (params.blockSize * BLOCKS_PER_TASK) + 1));

  // Sort small inputs (or without threads) in a single task.
  if (numElements < params.maxElementsPerTask || numThreads < 2) {
    boost::sort::pdqsort(begin, end, comp);
    co_return;
  }

  BlockSortState<Iterator, Compare> state{begin, end, std::move(comp), params,
                                          exec};
  co_await startSort(state, numThreads);
}

// Blocking version of `sortOnExecutor`, see the note at `blockIndirectSort`.
// Rethrows the first exception of any task.
template <typename Iterator, typename Compare>
void runSort(Iterator begin, Iterator end, Compare comp, uint32_t numThreads,
             ql::any_io_executor exec, SortParams params) {
  net::co_spawn(
      exec,
      sortOnExecutor(begin, end, std::move(comp), numThreads, exec, params),
      net::use_future)
      .get();
}

}  // namespace detail
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// Sort `range` by `comp` (a strict weak ordering) on `exec`, which must not be
// empty. The sort is not stable. Small inputs and `numThreads < 2` are sorted
// by a single task, and below `minNumThreadsForBlocks` threads only the
// parallel quicksort is used.
//
// `numThreads` is a hint for how many tasks of the sort run at the same time.
// For the best performance, `exec` should be run by at least that many threads.
// It determines the number of parts of the sort.
//
// IMPORTANT: The calling thread blocks until the sort is done, so `exec` has to
// be run by other threads (e.g. a `boost::asio::thread_pool` that this thread
// is not part of), and must not be a strand. From a coroutine, prefer
// `blockIndirectSortAsync` below, which doesn't block.
//
// The iterators may hand out proxy references, like those of `IdTable`. The
// elements are sorted in place, so sorting a temporary container (e.g. a
// `std::vector` rvalue) has no visible effect.
//
// If an exception is thrown (e.g. by `comp`), it is rethrown once all tasks
// have finished, and the contents of `range` are unspecified.
//
// In C++17 mode (no coroutines), this always sorts single-threaded in the
// calling thread; `numThreads` and `exec` are then not used.
CPP_template(typename Range, typename Compare)(
    requires ql::ranges::random_access_range<
        Range>) void blockIndirectSort(Range&& range, Compare comp,
                                       [[maybe_unused]] uint32_t numThreads,
                                       ql::any_io_executor exec) {
  AD_CONTRACT_CHECK(static_cast<bool>(exec));
  auto begin = ql::ranges::begin(range);
  auto end = begin + ql::ranges::distance(range);
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  boost::sort::pdqsort(begin, end, comp);
#else
  using Value = ql::ranges::range_value_t<Range>;
  detail::runSort(begin, end, std::move(comp), numThreads, std::move(exec),
                  detail::defaultSortParams<Value>());
#endif
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// Like `blockIndirectSort`, but the calling coroutine suspends until the sort
// is done instead of blocking its thread, so it may also run on `exec` (e.g. on
// a `boost::asio::thread_pool` with a single thread). Afterwards, it resumes on
// its own executor.
//
// NOTE: `range` is only accessed once the result is awaited, so it has to stay
// alive until then. Temporaries in `co_await blockIndirectSortAsync(...)` do.
CPP_template(typename Range,
             typename Compare)(requires ql::ranges::random_access_range<Range>)
    boost::asio::awaitable<void> blockIndirectSortAsync(
        Range&& range, Compare comp, uint32_t numThreads,
        ql::any_io_executor exec) {
  AD_CONTRACT_CHECK(static_cast<bool>(exec));
  auto begin = ql::ranges::begin(range);
  auto end = begin + ql::ranges::distance(range);
  using Value = ql::ranges::range_value_t<Range>;
  // The sort runs on `exec`, see `sortOnExecutor`, and the `co_spawn` brings
  // us back to our own executor.
  co_await boost::asio::co_spawn(
      exec,
      detail::sortOnExecutor(begin, end, std::move(comp), numThreads, exec,
                             detail::defaultSortParams<Value>()),
      boost::asio::use_awaitable);
}
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

}  // namespace ad_utility::blockSort

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_BLOCKINDIRECTSORT_H

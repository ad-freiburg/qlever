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
#include <iterator>
#include <numeric>
#include <utility>

#include "backports/algorithm.h"
#include "backports/asio.h"
#include "backports/concepts.h"
#include "util/Exception.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include "util/blockSort/MergeBlocks.h"
#include "util/blockSort/MoveBlocks.h"
#include "util/blockSort/ParallelSort.h"
#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"
#endif

// `boost::sort::block_indirect_sort` (by Francisco Jose Tapia), ported to
// Boost.Asio coroutines so that it runs on an existing executor instead of
// spawning its own threads.
//
// The input is divided into blocks, which are sorted in parallel
// (`ParallelSort.h`). The sorted parts are then merged pairwise by permuting an
// index of the blocks; elements only move between neighbouring blocks that
// overlap (`MergeBlocks.h`). Finally the blocks are moved to where the index
// says (`MoveBlocks.h`). The extra memory is one block per thread.
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

// Sort the blocks `[posIndex1, posIndex2)`: split in half `levelThread` more
// times, sort the halves with `parallelSort` at level zero, then merge them.
template <typename State>
net::awaitable<void> splitRange(State& state, size_t posIndex1,
                                size_t posIndex2, uint32_t levelThread) {
  size_t numBlocks = posIndex2 - posIndex1;
  // No block has been moved yet, so physical and logical positions agree.
  auto first = state.getBlockBegin(posIndex1);
  auto last = state.getRange(posIndex2 - 1).last;
  if (numBlocks < groupSize) {
    boost::sort::pdqsort(first, last, state.cmp_);
    co_return;
  }
  size_t posIndexMid = std::midpoint(posIndex1, posIndex2);

  TaskGroup group = state.makeTaskGroup();
  if (levelThread != 0) {
    group.spawn(splitRange(state, posIndexMid, posIndex2, levelThread - 1));
    if (!state.hasError()) {
      co_await group.runInline(
          splitRange(state, posIndex1, posIndexMid, levelThread - 1));
    }
  } else {
    auto mid = state.getBlockBegin(posIndexMid);
    group.spawn(parallelSort(state, mid, last));
    if (!state.hasError()) {
      co_await group.runInline(parallelSort(state, first, mid));
    }
  }
  co_await group.join();
  if (state.hasError()) {
    co_return;
  }
  co_await mergeBlocks(state, posIndex1, posIndexMid, posIndex2);
}

// The root task of a sort, Boost's `start_function`.
template <typename State>
net::awaitable<void> startSort(State& state, uint32_t numThreads) {
  if (numThreads < minNumThreadsForBlocks) {
    co_await parallelSort(state, state.globalRange_.first,
                          state.globalRange_.last);
    co_return;
  }
  // Split until there is one part per thread.
  auto levelThread = static_cast<uint32_t>(std::bit_width(numThreads - 1)) - 1;
  co_await splitRange(state, 0, state.numBlocks_, levelThread - 1);
  if (state.hasError()) {
    co_return;
  }
  co_await moveBlocks(state);
}

// The number of elements per block (Boost's `block_size`): bigger elements get
// smaller blocks, so that a block stays in cache.
template <typename Value>
constexpr uint32_t blockSizeFor() {
  constexpr size_t numBytes = sizeof(Value);
  constexpr uint32_t sizes[] = {4096, 4096, 4096, 4096, 2048,
                                1024, 768,  512,  256,  128};
  return sizes[numBytes > 256 ? 9 : std::bit_width(numBytes - 1)];
}

// Sort `[first, last)`, see `blockIndirectSort` below.
template <typename Iterator, typename Compare>
void runSort(Iterator first, Iterator last, Compare comp, uint32_t nthread,
             ql::any_io_executor exec) {
  using Value = typename std::iterator_traits<Iterator>::value_type;
  constexpr uint32_t blockSize = blockSizeFor<Value>();
  AD_CONTRACT_CHECK(last >= first);
  size_t numElements = static_cast<size_t>(last - first);
  if (numElements == 0) {
    return;
  }

  // Cheap special cases: already sorted, or sorted in reverse.
  if (ql::ranges::is_sorted(first, last, comp)) {
    return;
  }
  if (isDescending(first, last, comp)) {
    ql::ranges::reverse(first, last);
    return;
  }

  // At most one thread per group of blocks.
  nthread = std::min(nthread, static_cast<uint32_t>(
                                  numElements / (blockSize * groupSize) + 1));

  // Sort small inputs (or without threads) in the calling thread.
  if (numElements < maxElementsPerTask<Value>() || nthread < 2) {
    boost::sort::pdqsort(first, last, comp);
    return;
  }

  SortState<blockSize, Iterator, Compare> state{first, last, std::move(comp),
                                                nthread, exec};
  // Blocks the calling thread, see the note at `blockIndirectSort`.
  net::co_spawn(exec, startSort(state, nthread), net::use_future).get();
  state.errors_.rethrowIfError();
}

}  // namespace detail
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// Sort `range` by `comp` (a strict weak ordering) on up to `nthread` threads
// of `exec`, which must not be empty. The sort is not stable. Small inputs and
// `nthread < 2` are sorted in the calling thread, and below
// `minNumThreadsForBlocks` threads only the parallel quicksort is used.
//
// `nthread` should be the number of threads that run `exec`: it determines the
// number of parts and of scratch buffers (one block each).
//
// IMPORTANT: The calling thread blocks until the sort is done, so `exec` has to
// be run by other threads (e.g. a `boost::asio::thread_pool` that this thread
// is not part of), and must not be a strand.
//
// The iterators may hand out proxy references, like those of `IdTable`. The
// `range` must be borrowed (e.g. a `ql::span` or a
// `ql::ranges::subrange`), so that the caller's elements are sorted, not a
// copy.
//
// If an exception is thrown (e.g. by `comp`), it is rethrown once all tasks
// have finished, and the contents of `range` are unspecified.
//
// In C++17 mode (no coroutines) this always sorts in the calling thread.
CPP_template(typename Range, typename Compare)(
    requires ql::ranges::random_access_range<Range> CPP_and
        ql::ranges::borrowed_range<
            Range>) void blockIndirectSort(Range range, Compare comp,
                                           uint32_t nthread,
                                           ql::any_io_executor exec) {
  AD_CONTRACT_CHECK(static_cast<bool>(exec));
  auto first = ql::ranges::begin(range);
  auto last = first + ql::ranges::distance(range);
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  static_cast<void>(nthread);
  boost::sort::pdqsort(first, last, comp);
#else
  detail::runSort(first, last, std::move(comp), nthread, std::move(exec));
#endif
}

}  // namespace ad_utility::blockSort

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_BLOCKINDIRECTSORT_H

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_BLOCKINDIRECTSORT_H
#define QLEVER_SRC_UTIL_BLOCKSORT_BLOCKINDIRECTSORT_H

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <string>
#include <type_traits>
#include <utility>

#include "backports/algorithm.h"
#include "backports/asio.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/blockSort/SortPrimitives.h"

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
static constexpr uint32_t minNumThreadsForBlocks = 6;

// Sort the blocks `[posIndex1, posIndex2)`: split in half `levelThread` more
// times, sort the halves with `parallelSort` at level zero, then merge them.
template <typename State>
net::awaitable<void> splitRange(State& state, size_t posIndex1,
                                size_t posIndex2, uint32_t levelThread) {
  size_t numBlocks = posIndex2 - posIndex1;
  // No block has been moved yet, so physical and logical positions agree.
  auto first = state.getBlockBegin(posIndex1);
  auto last = state.getRange(posIndex2 - 1).last;
  if (numBlocks < State::groupSize_) {
    sortSequentially(first, last, state.cmp_);
    co_return;
  }
  size_t posIndexMid = posIndex1 + (numBlocks >> 1);

  TaskGroup group = state.makeTaskGroup();
  if (levelThread != 0) {
    group.spawn(splitRange(state, posIndexMid, posIndex2, levelThread - 1));
    if (!state.hasError()) {
      co_await group.runInline(
          splitRange(state, posIndex1, posIndexMid, levelThread - 1));
    }
  } else {
    auto mid = first + (numBlocks >> 1) * State::blockSize_;
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

// Sort `[first, last)` with the given block and group size.
template <uint32_t BlockSize, uint32_t GroupSize, typename Iterator,
          typename Compare>
void runSort(Iterator first, Iterator last, Compare comp, uint32_t nthread,
             ql::any_io_executor exec) {
  using Value = typename std::iterator_traits<Iterator>::value_type;
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
  size_t maxNumThreads =
      numElements / (size_t{BlockSize} * size_t{GroupSize}) + 1;
  nthread = static_cast<uint32_t>(
      std::min(static_cast<size_t>(nthread), maxNumThreads));

  // Sort small inputs (or without threads/executor) in the calling thread.
  if (numElements < maxElementsPerTask<Value>() || nthread < 2 ||
      !static_cast<bool>(exec)) {
    sortSequentially(first, last, comp);
    return;
  }

  SortState<BlockSize, GroupSize, Iterator, Compare> state{
      first, last, std::move(comp), nthread, exec};
  // Blocks the calling thread, see the note at `blockIndirectSort`.
  net::co_spawn(exec, startSort(state, nthread), net::use_future).get();
  state.errors_.rethrowIfError();
}

// The number of elements per block for elements of `numBytes` bytes (Boost's
// `block_size`): bigger elements get smaller blocks, so a block stays in cache.
constexpr uint32_t blockSizeForElements(size_t numBytes) {
  constexpr uint32_t sizes[] = {4096, 4096, 4096, 4096, 2048,
                                1024, 768,  512,  256,  128};
  size_t index = numBytes == 0 ? 0
                 : numBytes > 256
                     ? 9
                     : static_cast<size_t>(std::bit_width(numBytes - 1));
  return sizes[index];
}

// The number of blocks that a single task merges or moves.
constexpr uint32_t groupSizeForElements(bool isString) {
  return isString ? 128 : 64;
}

}  // namespace detail
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// Sort `range` by `comp` (a strict weak ordering) on up to `nthread` threads
// of `exec`. The sort is not stable. Pays off from about six threads upwards;
// small inputs, `nthread < 2` or an empty `exec` are sorted in the calling
// thread.
//
// `nthread` should be the number of threads that run `exec`: it determines the
// number of parts and of scratch buffers (one block each).
//
// IMPORTANT: The calling thread blocks until the sort is done, so `exec` has to
// be run by other threads (e.g. a `boost::asio::thread_pool` that this thread
// is not part of), and must not be a strand.
//
// The iterators may hand out proxy references, like those of `IdTable`, see
// `SortPrimitives.h`. The `range` must be borrowed (e.g. a `ql::span` or a
// `ql::ranges::subrange`), so that the caller's elements are sorted, not a
// copy.
//
// In C++17 mode (no coroutines) this always sorts in the calling thread.
CPP_template(typename Range, typename Compare)(
    requires ql::ranges::random_access_range<Range> CPP_and
        ql::ranges::borrowed_range<
            Range>) void blockIndirectSort(Range range, Compare comp,
                                           uint32_t nthread,
                                           ql::any_io_executor exec) {
  auto first = ql::ranges::begin(range);
  auto last = first + ql::ranges::distance(range);
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  static_cast<void>(nthread);
  static_cast<void>(exec);
  detail::sortSequentially(first, last, comp);
#else
  using Value = typename std::iterator_traits<decltype(first)>::value_type;
  constexpr bool isString = std::is_same_v<Value, std::string>;
  // Like Boost, use smaller blocks and bigger groups for strings.
  constexpr uint32_t blockSize =
      isString ? 128 : detail::blockSizeForElements(sizeof(Value));
  constexpr uint32_t groupSize = detail::groupSizeForElements(isString);
  detail::runSort<blockSize, groupSize>(first, last, std::move(comp), nthread,
                                        std::move(exec));
#endif
}

}  // namespace ad_utility::blockSort

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_BLOCKINDIRECTSORT_H

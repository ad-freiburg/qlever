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

// An in-place parallel sort for large inputs and many threads: Francisco Jose
// Tapia's *block indirect sort*, ported from `boost::sort::block_indirect_sort`
// to Boost.Asio coroutines.
//
// WHAT THE ALGORITHM DOES. The elements are divided into blocks of a fixed
// number of elements. The blocks are first sorted in parallel, in the ordinary
// divide-and-conquer way (`ParallelSort.h`). The sorted parts are then merged
// pairwise, but a merge does *not* move the elements around: it only puts the
// blocks into the right order in an index (`MergeBlocks.h`), and moves elements
// only between the few neighbouring blocks that actually overlap. Only at the
// very end are the blocks moved to where the index says they belong
// (`MoveBlocks.h`). That is what makes the algorithm in-place: the only extra
// memory it needs is one block per thread.
//
// WHY COROUTINES. Boost runs `nthread` threads which all execute work items
// from a shared stack, and a task that waits for its children keeps taking work
// items off that stack in the meantime, because otherwise its thread would be
// lost for the duration. With coroutines that is unnecessary: a task that waits
// for its children simply suspends, and the executor gives the thread to the
// next task all by itself. So the concurrent work stack, the `std::async` calls
// and the spinning `exec()` loop of the original are all replaced by
// `co_await group.join()`, see `TaskGroup.h`.
//
// The one thing that does not survive the port is the `static thread_local`
// scratch buffer of Boost's `backbone`: a coroutine is not tied to a thread, so
// the buffer has to belong to whoever is currently using it rather than to a
// thread. `ScratchBuffers` in `SortState.h` is what replaces it.
namespace ad_utility::blockSort {

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
namespace detail {

namespace net = boost::asio;

// Below this number of threads the block indirect part does not pay for itself
// and the plain parallel quicksort is used instead. This is Boost's
// `BOOST_NTHREAD_BORDER`.
static constexpr uint32_t minNumThreadsForBlocks = 6;

// Sort the blocks of the index range `[posIndex1, posIndex2)` by splitting it
// in half `levelThread` more times, and merge the two halves afterwards. At
// level zero the two halves are sorted by `parallelSort`, which is where the
// elements are actually compared.
template <typename State>
net::awaitable<void> splitRange(State& state, size_t posIndex1,
                                size_t posIndex2, uint32_t levelThread) {
  size_t numBlocks = posIndex2 - posIndex1;
  // The blocks have not been moved yet, so their physical position still is
  // their logical position.
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

// The root task of a sort, the port of Boost's `start_function`.
template <typename State>
net::awaitable<void> startSort(State& state, uint32_t numThreads) {
  if (numThreads < minNumThreadsForBlocks) {
    co_await parallelSort(state, state.globalRange_.first,
                          state.globalRange_.last);
    co_return;
  }
  // Split until there is a part for every thread, which is why the depth is
  // the number of bits of `numThreads - 1`.
  auto levelThread = static_cast<uint32_t>(std::bit_width(numThreads - 1)) - 1;
  co_await splitRange(state, 0, state.numBlocks_, levelThread - 1);
  if (state.hasError()) {
    co_return;
  }
  co_await moveBlocks(state);
}

// Sort `[first, last)` with the given block and group size, see
// `blockSizeForElements` below.
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

  // Already sorted, or sorted the wrong way round. Both are linear scans and
  // both happen often enough in practice to be worth checking for.
  if (ql::ranges::is_sorted(first, last, comp)) {
    return;
  }
  if (isDescending(first, last, comp)) {
    ql::ranges::reverse(first, last);
    return;
  }

  // More threads than there are groups of blocks would only split the input
  // into parts that are too small to be worth a task of their own.
  size_t maxNumThreads =
      numElements / (size_t{BlockSize} * size_t{GroupSize}) + 1;
  nthread = static_cast<uint32_t>(
      std::min(static_cast<size_t>(nthread), maxNumThreads));

  // For a small input, a single thread, or no executor to run the tasks on,
  // sorting right here is cheaper than anything the executor could do.
  if (numElements < maxElementsPerTask<Value>() || nthread < 2 ||
      !static_cast<bool>(exec)) {
    sortSequentially(first, last, comp);
    return;
  }

  SortState<BlockSize, GroupSize, Iterator, Compare> state{
      first, last, std::move(comp), nthread, exec};
  // The calling thread blocks here, so the `exec` has to be run by other
  // threads, see the note at `blockIndirectSort` below.
  net::co_spawn(exec, startSort(state, nthread), net::use_future).get();
  state.errors_.rethrowIfError();
}

// The block size for elements of `numBytes` bytes, taken from Boost: the
// bigger the elements, the fewer of them fit into the cache, and a block that
// does not fit into the cache costs more than the extra blocks do.
constexpr uint32_t blockSizeForElements(size_t numBytes) {
  // Indexed by the number of bits of `numBytes - 1`, capped at 256 bytes.
  constexpr uint32_t sizes[] = {4096, 4096, 4096, 4096, 2048,
                                1024, 768,  512,  256,  128};
  size_t index = numBytes == 0 ? 0
                 : numBytes > 256
                     ? 9
                     : static_cast<size_t>(std::bit_width(numBytes - 1));
  return sizes[index];
}

// The number of blocks that a single task merges or moves at once.
constexpr uint32_t groupSizeForElements(bool isString) {
  return isString ? 128 : 64;
}

}  // namespace detail
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// Sort `range` according to `comp`, using up to `nthread` threads of `exec`.
//
// The `comp` has to be a strict weak ordering. The sort is *not* stable.
//
// This algorithm is made for a lot of data and a lot of threads: it pays off
// from roughly six threads upwards, and it scales to the hundreds. For fewer
// threads, for a small input, or for an empty `exec`, it falls back to sorting
// in the calling thread, so it is always safe to call.
//
// `nthread` is the number of threads that are expected to run the `exec`; it
// determines into how many parts the input is split and how many scratch
// buffers (of `blockSizeForElements(sizeof(element))` elements each) are
// allocated. Giving a number that is much larger than the number of threads
// that actually run the `exec` wastes memory, a much smaller one wastes
// parallelism.
//
// IMPORTANT: The calling thread blocks until the sort is done, so the `exec`
// has to be run by *other* threads (for example by a `boost::asio::thread_pool`
// that this thread is not part of). In particular the `exec` must not be a
// strand, and must not be the executor of an `io_context` that only the calling
// thread runs.
//
// NOTE: When `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set there are no
// coroutines, and this function always sorts in the calling thread and ignores
// `nthread` and `exec`.
//
// NOTE: The iterators of the `range` may hand out proxy references instead of
// true `Value&` references (as the iterators of QLever's column-major
// `IdTable` do). Everything that Boost would do to an element through such an
// iterator is routed through `util/blockSort/SortPrimitives.h`, see there.
//
// NOTE: The `range` is taken by value and therefore has to be a *borrowed*
// range — a `ql::span`, a `ql::ranges::subrange`, or any other view of the
// elements. That is deliberate: a container that was taken by value would be
// a copy, and sorting the copy would leave the caller's elements untouched.
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
  // Strings are indirect, so what a block has to hold in the cache is not the
  // strings themselves; Boost uses smaller blocks and bigger groups for them.
  constexpr uint32_t blockSize =
      isString ? 128 : detail::blockSizeForElements(sizeof(Value));
  constexpr uint32_t groupSize = detail::groupSizeForElements(isString);
  detail::runSort<blockSize, groupSize>(first, last, std::move(comp), nthread,
                                        std::move(exec));
#endif
}

}  // namespace ad_utility::blockSort

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_BLOCKINDIRECTSORT_H

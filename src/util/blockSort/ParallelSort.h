// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_PARALLELSORT_H
#define QLEVER_SRC_UTIL_BLOCKSORT_PARALLELSORT_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <algorithm>
#include <bit>
#include <boost/asio/awaitable.hpp>
#include <boost/sort/common/pivot.hpp>
#include <boost/sort/pdqsort/pdqsort.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <utility>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"

// A parallel quicksort, which is the part of the block indirect sort that does
// the actual comparing and moving of elements. It is used on its own for a
// small number of threads, and for the initial sort of the individual parts in
// the full algorithm.
//
// This is a port of `boost::sort::blk_detail::parallel_sort`; see
// `util/blockSort/BlockIndirectSort.h` for how the pieces fit together.
namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// The number of elements that one task sorts on its own. Like Boost we make
// this smaller for bigger elements, because what should stay within the cache
// is the number of *bytes*, not the number of elements.
template <typename Value>
constexpr size_t maxElementsPerTask() {
  auto bitsOfSize = static_cast<uint32_t>(std::bit_width(sizeof(Value))) >> 1;
  return size_t{1} << (18 - std::min(bitsOfSize, uint32_t{5}));
}

// Whether `[first, last)` is sorted in descending order, that is whether
// reversing it would sort it.
template <typename Iterator, typename Compare>
bool isDescending(Iterator first, Iterator last, const Compare& cmp) {
  return ql::ranges::is_sorted(
      first, last,
      [&cmp](const auto& a, const auto& b) -> bool { return cmp(b, a); });
}

// Sort `[first, last)`, splitting it into ever smaller parts until `level`
// reaches zero or a part is small enough to be sorted by a single task. Every
// second half is handed to the `group`, while the first half stays in this
// very task, exactly like Boost's `divide_sort`.
//
// NOTE: The `group` is the one of the `parallelSort` below, which is why the
// recursive calls may spawn into it: a group is only complete once its owner
// has stopped spawning, and the owner is still waiting for this very task.
//
// NOTE: This is deliberately an ordinary (recursive) function and not a
// coroutine. It never waits for anything — it only ever *hands off* the second
// half and then carries straight on with the first one — and a coroutine would
// cost a frame and a trip through `co_spawn` for every one of the many splits.
template <typename State, typename Iterator>
void divideSort(State& state, Iterator first, Iterator last, uint32_t level,
                size_t maxPerTask, TaskGroup& group) {
  using Value = typename State::Value;
  const auto& cmp = state.cmp_;
  if (ql::ranges::is_sorted(first, last, cmp)) {
    return;
  }
  size_t numElements = static_cast<size_t>(last - first);
  if (level == 0 || numElements < maxPerTask) {
    boost::sort::pdqsort(first, last, cmp);
    return;
  }

  // Move the median of nine elements to the front and partition around it.
  bsc::pivot9(first, last, cmp);
  const Value& pivot = *first;
  Iterator cFirst = first + 1;
  Iterator cLast = last - 1;
  while (cmp(*cFirst, pivot)) {
    ++cFirst;
  }
  while (cmp(pivot, *cLast)) {
    --cLast;
  }
  while (cFirst < cLast) {
    std::swap(*(cFirst++), *(cLast--));
    while (cmp(*cFirst, pivot)) {
      ++cFirst;
    }
    while (cmp(pivot, *cLast)) {
      --cLast;
    }
  }
  std::swap(*first, *cLast);

  // The second half may be sorted by any thread, the first one stays here.
  group.spawnFunction([&state, cFirst, last, level, maxPerTask, &group]() {
    divideSort(state, cFirst, last, level - 1, maxPerTask, group);
  });
  if (state.hasError()) {
    return;
  }
  divideSort(state, first, cLast, level - 1, maxPerTask, group);
}

// Sort `[first, last)` in parallel with a quicksort whose halves are
// independent tasks. The port of Boost's `parallel_sort` constructor.
template <typename State, typename Iterator>
net::awaitable<void> parallelSort(State& state, Iterator first, Iterator last) {
  using Value = typename State::Value;
  const auto& cmp = state.cmp_;
  AD_CORRECTNESS_CHECK(last >= first);
  size_t numElements = static_cast<size_t>(last - first);

  // Already sorted, or sorted the wrong way round: both happen often enough in
  // practice that the two linear scans pay for themselves.
  if (ql::ranges::is_sorted(first, last, cmp)) {
    co_return;
  }
  if (isDescending(first, last, cmp)) {
    ql::ranges::reverse(first, last);
    co_return;
  }

  constexpr size_t maxPerTask = maxElementsPerTask<Value>();
  if (numElements < maxPerTask) {
    boost::sort::pdqsort(first, last, cmp);
    co_return;
  }
  // The depth at which the splitting stops. A little more than the depth that
  // would be needed for perfectly even splits, because they never are.
  auto level =
      static_cast<uint32_t>((std::bit_width(numElements / maxPerTask) * 3) / 2);

  TaskGroup group = state.makeTaskGroup();
  // NOTE: `divideSort` does not suspend, so the `catch` may not be turned into
  // a `co_await` of the `group`; that has to happen below, see the LIFETIME
  // note at `TaskGroup`.
  try {
    if (!state.hasError()) {
      divideSort(state, first, last, level, maxPerTask, group);
    }
  } catch (...) {
    state.storeError(std::current_exception());
  }
  co_await group.join();
}

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_PARALLELSORT_H

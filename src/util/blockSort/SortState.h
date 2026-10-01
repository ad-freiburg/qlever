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
// `boost/sort/block_indirect_sort/blk_detail/backbone.hpp`:
// Copyright (c) 2016 Francisco Jose Tapia (fjtapia@gmail.com)
// Distributed under the Boost Software License, Version 1.0. (See the
// accompanying file `LICENSE_1_0.txt` or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H
#define QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <algorithm>
#include <atomic>
#include <bit>
#include <boost/asio/awaitable.hpp>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "backports/asio.h"
#include "util/Exception.h"
#include "util/blockSort/TaskGroup.h"

namespace ad_utility::blockSort::detail {

// The default number of elements that one task sorts on its own (see
// `SortState::maxElementsPerTask_`), like in Boost: bigger elements get smaller
// tasks, so that the work per task stays roughly comparable. It is 2^18 for
// elements of a single byte and halves at 2, 8, 32, 128, and 512 bytes, down to
// 2^13. For example, 4 bytes give 2^17, 8 bytes 2^16, and 64 bytes 2^15.
template <typename Value>
constexpr size_t defaultMaxElementsPerTask() {
  auto bitsOfSize = static_cast<uint32_t>(std::bit_width(sizeof(Value))) / 2;
  return size_t{1} << (18 - std::min(bitsOfSize, uint32_t{5}));
}

// The state shared by the tasks of a single sort, similar to
// `boost::sort::blk_detail::backbone`.
template <typename Compare>
class SortState {
 public:
  // The maximal number of elements that a single task sorts on its own (with
  // `pdqsort`); bigger ranges are split further, see `parallelQuicksort`. The
  // default is `defaultMaxElementsPerTask`.
  size_t maxElementsPerTask_;
  // The comparator. It is called concurrently by all tasks of the sort.
  Compare cmp_;
  // The executor on which the tasks run. The `TaskGroup`s of the sort keep a
  // reference to it.
  ql::any_io_executor executor_;
  // Set as soon as any task of this sort has failed, see `TaskGroup`.
  std::atomic<bool> stopped_{false};

  // A sort by `cmp` on `executor` whose tasks sort at most `maxElementsPerTask`
  // elements on their own. `maxElementsPerTask` must be at least 16, because
  // the pivot selection needs nine distinct samples.
  SortState(Compare cmp, size_t maxElementsPerTask,
            ql::any_io_executor executor)
      : maxElementsPerTask_{maxElementsPerTask},
        cmp_{std::move(cmp)},
        executor_{std::move(executor)} {
    AD_CONTRACT_CHECK(maxElementsPerTask_ >= 16);
  }

  // Run `inlined` and `spawned` concurrently and wait for both, see
  // `TaskGroup::runConcurrently`.
  [[nodiscard]] net::awaitable<void> runConcurrently(
      net::awaitable<void> inlined, net::awaitable<void> spawned) {
    return TaskGroup::runConcurrently(executor_, stopped_, std::move(inlined),
                                      std::move(spawned));
  }
};

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_SORTSTATE_H

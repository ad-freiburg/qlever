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

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <cstddef>
#include <utility>

#include "backports/asio.h"
#include "util/Exception.h"
#include "util/blockSort/TaskGroup.h"

namespace ad_utility::blockSort::detail {

// The state shared by the tasks of a single sort, Boost's `backbone` without
// its work stack.
template <typename Compare>
class SortState {
 public:
  size_t maxElementsPerTask_;
  Compare cmp_;
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

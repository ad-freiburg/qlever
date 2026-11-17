// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_SORTCONFIG_H
#define QLEVER_SRC_INDEX_SORTCONFIG_H

#include <cstdint>

#include "util/CancellationHandle.h"
#include "util/Executor.h"
#include "util/Forward.h"
#include "util/blockSort/BlockIndirectSort.h"

// How the in-memory sorts (see `IdTableUtils::sort`) run in parallel, and the
// handle via which they can be cancelled. For queries, the
// `QueryExecutionContext` creates it from the runtime parameters and the
// query's cancellation handle, see `QueryExecutionContext::makeSortConfig`.
struct SortConfig {
  // The executor on which the tasks of the sort run. It must not be a strand.
  qlever::Executor executor_;
  // A hint for how many tasks of the sort run at the same time. Values below
  // `2` mean that the sort runs in a single task (still on the `executor_`).
  uint32_t numTasksInFlight_ = 1;
  // Checked regularly by the sort, which then throws a `CancellationException`
  // when it has been cancelled. Must not be `nullptr`. Defaults to a fresh
  // handle, which only the owner of this `SortConfig` can cancel.
  ad_utility::SharedCancellationHandle cancellationHandle_ =
      std::make_shared<ad_utility::CancellationHandle<>>();

  // Sort the `range` in place by `comp` via `blockIndirectSort` (not stable),
  // as configured above. The calling thread blocks until the sort is done, so
  // it must not belong to the `executor_`. If the sort throws, the contents of
  // `range` are unspecified.
  template <typename Range, typename Compare>
  void sort(Range&& range, Compare comp) const {
    ad_utility::blockSort::blockIndirectSort(AD_FWD(range), std::move(comp),
                                             numTasksInFlight_, executor_,
                                             cancellationHandle_);
  }
};

#endif  // QLEVER_SRC_INDEX_SORTCONFIG_H

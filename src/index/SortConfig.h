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

#include "util/Executor.h"

// How the in-memory sort of an `IdTable` (see `IdTableUtils::sort`) runs in
// parallel. For queries, the `QueryExecutionContext` creates it from the
// runtime parameters, see `QueryExecutionContext::makeSortConfig`.
struct SortConfig {
  // The executor on which the tasks of the sort run. It must not be a strand.
  qlever::Executor executor_;
  // A hint for how many tasks of the sort run at the same time. Values below
  // `2` mean that the sort runs in a single task (still on the `executor_`).
  uint32_t numTasksInFlight_ = 1;
};

#endif  // QLEVER_SRC_INDEX_SORTCONFIG_H

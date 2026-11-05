// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_EXECUTOR_H
#define QLEVER_SRC_UTIL_EXECUTOR_H

#include "backports/asio.h"

namespace qlever {

// The type of the executors on which queries, index scans, and the writing of
// permutations run their parallel work (see for example
// `QueryExecutionContext::executor`, `util/GlobalExecutor.h`, and
// `util/ConcurrencyLimitedExecutor.h`). All the code that passes such an
// executor around uses this alias, so that a change of the type only has to be
// made here.
using Executor = ql::any_io_executor;

}  // namespace qlever

#endif  // QLEVER_SRC_UTIL_EXECUTOR_H

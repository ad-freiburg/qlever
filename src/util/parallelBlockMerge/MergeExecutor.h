// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEEXECUTOR_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEEXECUTOR_H

#include <algorithm>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/thread_pool.hpp>
#include <cstddef>
#include <thread>

// The default source of parallelism of the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`), for callers that do not
// bring an executor of their own.
namespace ad_utility::parallelBlockMerge {

namespace net = boost::asio;

// Return the number of threads that `defaultMergeExecutor()` below runs, which
// is one per hardware thread.
inline size_t defaultMergeParallelism() {
  return std::max<size_t>(1, std::thread::hardware_concurrency());
}

// Return the executor of the process-wide default thread pool of the parallel
// merge. The pool has `defaultMergeParallelism()` threads, is created lazily on
// the first call, and is shared by all merges that do not specify an executor
// of their own.
//
// NOTE: Sharing a single pool between concurrent merges is safe, because a
// chunk that cannot make progress suspends instead of occupying its thread, so
// the merges cannot starve each other. It is however *not* safe to consume a
// merge from one of the threads of its own executor, see
// `parallelBlockMergeToRange`.
inline net::any_io_executor defaultMergeExecutor() {
  static net::thread_pool pool{defaultMergeParallelism()};
  return pool.get_executor();
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEEXECUTOR_H

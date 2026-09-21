// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_GLOBALEXECUTOR_H
#define QLEVER_SRC_UTIL_GLOBALEXECUTOR_H

#include <boost/asio/any_io_executor.hpp>
#include <cstddef>

namespace ad_utility {

// Set the number of threads of the global thread pool (see `globalExecutor`).
// The `numThreads` have to be greater than zero. This function has to be called
// before the first call to `globalExecutor()`; if the pool already exists, then
// it is *not* recreated, but a warning is logged and the call has no effect.
// Thread-safe.
void setGlobalExecutorNumThreads(size_t numThreads);

// Return the number of threads that the global thread pool has (if it already
// exists) or will have (if it doesn't exist yet), see `globalExecutor`. The
// default is the number of hardware threads of the machine, but at least one.
// Thread-safe.
size_t globalExecutorNumThreads();

// Return the executor of the process-wide global thread pool, which has
// `globalExecutorNumThreads()` threads and which is created lazily on the first
// call to this function.
//
// There is deliberately only a single such pool: its users are the phases of
// the index build (the merge phase of the external sorters and the permutation
// writer), which all want to use the machine's threads and would oversubscribe
// it if each of them had a pool of its own. A single pool also makes the total
// parallelism of the process configurable via a single knob (the `--num-threads
// / -j` option of the index builder, see `setGlobalExecutorNumThreads`).
//
// NOTE: The pool has static lifetime and we never `join()` or `stop()` it, so
// it outlives everything that posts to it, which is exactly what its users
// need: they post tasks that only have to be completed before the process ends.
//
// NOTE: It is safe to share this executor between concurrent users of the
// parallel merge (see `util/parallelBlockMerge/ParallelBlockMerge.h`), because
// a chunk of a merge that cannot make progress suspends instead of occupying
// its thread, so that concurrent merges cannot starve each other. It is however
// *not* safe to *consume* a merge from one of the threads of the executor that
// the merge runs on, see `parallelBlockMerge::parallelBlockMergeToRange`.
boost::asio::any_io_executor globalExecutor();

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GLOBALEXECUTOR_H

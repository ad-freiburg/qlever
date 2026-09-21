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

#include <cstddef>

#include "backports/asio.h"

namespace ad_utility {

// A process-wide thread pool, together with the knobs to configure the number
// of its threads.
//
// TODO<joka921> This design is deliberately hacky for now and should be
// reworked in the future. It is a mutable process-wide singleton with static
// lifetime that is never joined, and whose number of threads can only be set
// before its first use. Once all the phases of the index build (and the query
// engine) agree on how they obtain their parallelism, the pool should become an
// explicitly owned object that is passed to its users, so that its lifetime and
// its configuration are no longer global state.

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
// There is deliberately only a single such pool: its intended users are the
// phases of the index build, all of which want to use the machine's threads and
// would oversubscribe it if each of them had a pool of its own. A single pool
// also makes the total parallelism of the process configurable via a single
// knob (the `--num-threads / -j` option of the index builder, see
// `setGlobalExecutorNumThreads`).
//
// NOTE: The pool is not used in production yet; porting the phases of the index
// build onto it is work in progress.
//
// NOTE: The pool has static lifetime and we never `join()` or `stop()` it, so
// it outlives everything that posts to it, which is exactly what its users
// need: they post tasks that only have to be completed before the process ends.
// A phase that needs to know when its own work is done therefore has to
// establish that itself.
ql::any_io_executor globalExecutor();

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GLOBALEXECUTOR_H

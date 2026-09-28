// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/HpxAsioExecutor.h"

#include <hpx/init.hpp>

#include "util/Exception.h"
#include "util/GlobalExecutor.h"

namespace ad_utility {

// _____________________________________________________________________________
void ensureHpxRuntimeIsRunning() {
  // NOTE: The initialization of a function-local static is thread-safe, so the
  // lambda runs exactly once, no matter how many threads call this function.
  static bool runtimeIsRunning = []() {
    hpx::local::init_params params;
    params.cfg = {
        // A single worker thread is enough, because this runtime only runs the
        // continuations that join the results of the work, see the
        // documentation of this function.
        "hpx.os_threads=1",
        // Do not install handlers for `SIGINT`, `SIGSEGV`, `SIGPIPE` and
        // others, and do not replace the `std::new_handler`, both of which HPX
        // does by default and both of which would change the behavior of the
        // rest of QLever.
        "hpx.handle_signals=0", "hpx.handle_failed_new=0"};
    // The `nullptr` means that the runtime doesn't run an entry point function
    // of its own, and passing no command line arguments means that HPX doesn't
    // see (and doesn't try to interpret) QLever's arguments.
    bool started = hpx::local::start(nullptr, 0, nullptr, params);
    AD_CORRECTNESS_CHECK(started);
    return started;
  }();
  AD_CORRECTNESS_CHECK(runtimeIsRunning);
}

// _____________________________________________________________________________
HpxAsioExecutor globalHpxExecutor() {
  ensureHpxRuntimeIsRunning();
  return HpxAsioExecutor{globalExecutor(), globalExecutorNumThreads()};
}

}  // namespace ad_utility

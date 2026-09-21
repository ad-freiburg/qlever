// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/GlobalExecutor.h"

#include <algorithm>
#include <boost/asio/thread_pool.hpp>
#include <mutex>
#include <thread>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

namespace {

// The configuration of the global thread pool. All its members are protected by
// the `mutex_`.
struct GlobalExecutorConfig {
  std::mutex mutex_;
  // The number of threads that the pool has or will have.
  size_t numThreads_ = std::max<size_t>(1, std::thread::hardware_concurrency());
  // Set as soon as the pool has been created, after which the `numThreads_` can
  // no longer be changed.
  bool poolWasCreated_ = false;
};

// Return the single instance of the `GlobalExecutorConfig`. It is a
// function-local static, such that it is initialized on the first use and in
// particular before any other static initialization can access it.
GlobalExecutorConfig& config() {
  static GlobalExecutorConfig config;
  return config;
}

}  // namespace

// _____________________________________________________________________________
void setGlobalExecutorNumThreads(size_t numThreads) {
  AD_CONTRACT_CHECK(numThreads > 0);
  auto& conf = config();
  std::lock_guard lock{conf.mutex_};
  if (conf.poolWasCreated_) {
    AD_LOG_WARN << "The number of threads of the global thread pool was set to "
                << numThreads
                << " after the pool had already been created with "
                << conf.numThreads_ << " threads; the new setting is ignored"
                << std::endl;
    return;
  }
  conf.numThreads_ = numThreads;
}

// _____________________________________________________________________________
size_t globalExecutorNumThreads() {
  auto& conf = config();
  std::lock_guard lock{conf.mutex_};
  return conf.numThreads_;
}

// _____________________________________________________________________________
boost::asio::any_io_executor globalExecutor() {
  // NOTE: The initialization of a function-local static is thread-safe, so the
  // lambda (and with it the marking of the configuration as final) runs exactly
  // once.
  static boost::asio::thread_pool pool{[]() {
    auto& conf = config();
    std::lock_guard lock{conf.mutex_};
    conf.poolWasCreated_ = true;
    return conf.numThreads_;
  }()};
  return pool.get_executor();
}

}  // namespace ad_utility

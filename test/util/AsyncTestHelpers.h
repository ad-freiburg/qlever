//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASYNCTESTHELPERS_H
#define QLEVER_ASYNCTESTHELPERS_H

#include <gtest/gtest.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <vector>

#include "util/Exception.h"
#include "util/jthread.h"

namespace net = boost::asio;

template <typename Func>
concept TestableCoroutine =
    std::is_invocable_r_v<net::awaitable<void>, Func, net::io_context&>;

template <TestableCoroutine Func>
void runCoroutine(Func innerRun, size_t numThreads) {
  net::io_context ioContext{};

  auto future = net::co_spawn(ioContext, innerRun(ioContext), net::use_future);

  std::vector<ad_utility::JThread> workers{};

  AD_CONTRACT_CHECK(numThreads > 0);
  workers.reserve(numThreads - 1);

  for (size_t i = 0; i < numThreads - 1; i++) {
    workers.emplace_back(
        [&ioContext]() { ioContext.run_for(std::chrono::seconds(10)); });
  }

  ioContext.run_for(std::chrono::seconds(10));
  auto status = future.wait_for(std::chrono::seconds(0));
  AD_CORRECTNESS_CHECK(status != std::future_status::deferred);
  if (status == std::future_status::timeout) {
    FAIL() << "Timeout for awaitable reached!";
  }
}

#define COROUTINE_NAME(test_suite_name, test_name) \
  test_suite_name##_##test_name##_coroutine

#define ASYNC_TEST_N(test_suite_name, test_name, num_threads)              \
  net::awaitable<void> COROUTINE_NAME(test_suite_name,                     \
                                      test_name)(net::io_context&);        \
  TEST(test_suite_name, test_name) {                                       \
    runCoroutine(COROUTINE_NAME(test_suite_name, test_name), num_threads); \
  }                                                                        \
  net::awaitable<void> COROUTINE_NAME(test_suite_name,                     \
                                      test_name)(net::io_context & ioContext)

// Drop-in replacement for gtest's TEST() macro, but for tests that make
// use of boost asio's awaitable coroutines. Note that this prevents you
// from using ASSERT_* macros unless you redefine the return keword with
// co_return so it works nicely with the coroutine.

#define ASYNC_TEST(test_suite_name, test_name) \
  ASYNC_TEST_N(test_suite_name, test_name, 1)

#endif  // QLEVER_ASYNCTESTHELPERS_H

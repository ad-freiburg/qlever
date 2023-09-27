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

#include "util/Exception.h"

namespace net = boost::asio;

template <typename Func>
concept TestableCoroutine =
    std::is_invocable_r_v<net::awaitable<void>, Func, net::io_context&>;

template <TestableCoroutine Func>
void runCoroutine(Func innerRun) {
  net::io_context ioContext{};

  auto future = net::co_spawn(ioContext, innerRun(ioContext), net::use_future);
  ioContext.run_for(std::chrono::seconds(10));
  auto status = future.wait_for(std::chrono::seconds(0));
  AD_CORRECTNESS_CHECK(status != std::future_status::deferred);
  if (status == std::future_status::timeout) {
    FAIL() << "Timeout for awaitable reached!";
  }
}

#define COROUTINE_NAME(test_suite_name, test_name) \
  test_suite_name##_##test_name##_coroutine

// Drop-in replacement for gtest's TEST() macro, but for tests that make
// use of boost asio's awaitable coroutines. Note that this prevents you
// from using ASSERT_* macros unless you redefine the return keword with
// co_return so it works nicely with the coroutine.
#define ASYNC_TEST(test_suite_name, test_name)                      \
  net::awaitable<void> COROUTINE_NAME(test_suite_name,              \
                                      test_name)(net::io_context&); \
  TEST(test_suite_name, test_name) {                                \
    runCoroutine(COROUTINE_NAME(test_suite_name, test_name));       \
  }                                                                 \
  net::awaitable<void> COROUTINE_NAME(test_suite_name,              \
                                      test_name)(net::io_context & ioContext)

#endif  // QLEVER_ASYNCTESTHELPERS_H

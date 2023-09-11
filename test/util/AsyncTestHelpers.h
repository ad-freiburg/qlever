//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASYNCTESTHELPERS_H
#define QLEVER_ASYNCTESTHELPERS_H

#include <gtest/gtest.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>

namespace net = boost::asio;

template <typename Func>
void runCoroutine(Func innerRun) {
  net::io_context ioContext{};
  bool sanityFlag = false;
  auto lambdaWrapper = [&]() -> net::awaitable<void> {
    co_await innerRun(ioContext);
    sanityFlag = true;
  };

  net::co_spawn(ioContext, lambdaWrapper(), net::detached);
  ioContext.run();
  EXPECT_TRUE(sanityFlag);
}

#define COROUTINE_NAME(test_suite_name, test_name) \
  test_suite_name##_##test_name##_coroutine

#define ASYNC_TEST(test_suite_name, test_name)                      \
  net::awaitable<void> COROUTINE_NAME(test_suite_name,              \
                                      test_name)(net::io_context&); \
  TEST(test_suite_name, test_name) {                                \
    runCoroutine(COROUTINE_NAME(test_suite_name, test_name));       \
  }                                                                 \
  net::awaitable<void> COROUTINE_NAME(test_suite_name,              \
                                      test_name)(net::io_context & ioContext)

#endif  // QLEVER_ASYNCTESTHELPERS_H

//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASYNCTESTHELPERS_H
#define QLEVER_ASYNCTESTHELPERS_H

#include <gtest/gtest.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

namespace net = boost::asio;
using namespace boost::asio::experimental::awaitable_operators;

template <typename T>
net::awaitable<T> withTimeout(net::awaitable<T> t) {
  net::deadline_timer timer{co_await net::this_coro::executor,
                            boost::posix_time::seconds(2)};
  auto variant =
      co_await (std::move(t) || timer.async_wait(net::use_awaitable));
  if (variant.index() == 0) {
    if constexpr (std::is_void_v<T>) {
      co_return;
    } else {
      co_return std::get<0>(variant);
    }
  }
  ADD_FAILURE() << "Timeout while waiting for awaitable" << std::endl;
  throw std::runtime_error{"Timeout while waiting for awaitable"};
}

template <typename Func>
void runCoroutine(Func innerRun) {
  net::io_context ioContext{};

  auto future = net::co_spawn(ioContext, innerRun(ioContext), net::use_future);
  ioContext.run();
  future.get();
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

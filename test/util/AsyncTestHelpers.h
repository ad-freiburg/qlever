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

template <typename Func>
concept TestableFunction = std::is_invocable_r_v<void, Func, net::io_context&>;

template <typename Func>
requires(TestableCoroutine<Func> || TestableFunction<Func>)
void runAsyncTest(Func innerRun, size_t numThreads) {
  auto ioContext = std::make_shared<net::io_context>();
  auto future = [&]() {
    if constexpr (TestableCoroutine<Func>) {
      return net::co_spawn(*ioContext, innerRun(*ioContext), net::use_future);
    } else {
      return net::post(*ioContext, std::packaged_task<void()>{
                                       [&] { innerRun(*ioContext); }});
    }
  }();

  std::vector<ad_utility::JThread> workers{};

  AD_CONTRACT_CHECK(numThreads > 0);
  workers.reserve(numThreads);

  for (size_t i = 0; i < numThreads; i++) {
    workers.emplace_back(
        [ioContext]() { ioContext->run_for(std::chrono::seconds(10)); });
  }

  // Wait for at most 10 seconds (400 * 50 ms) for the test to finish and then
  // stop it with a failure. Check every 50ms if it has already finished s.t. we
  // don't waste time.
  for (size_t i = 0; i < 400; ++i) {
    auto status = future.wait_for(std::chrono::milliseconds(50));
    AD_CORRECTNESS_CHECK(status != std::future_status::deferred);
    if (status == std::future_status::ready) {
      break;
    }
  }
  auto status = future.wait_for(std::chrono::milliseconds(0));
  if (status == std::future_status::timeout) {
    for (auto& thread : workers) {
      thread.detach();
    }
    FAIL() << "Timeout for awaitable reached!";
  } else {
    // Propagate exceptions
    future.get();
  }
}

#define TEST_IMPL_NAME(test_suite_name, test_name) \
  test_suite_name##_##test_name##_impl

#define ASYNC_TEST_N(test_suite_name, test_name, num_threads)              \
  net::awaitable<void> TEST_IMPL_NAME(                                     \
      test_suite_name, test_name)([[maybe_unused]] net::io_context&);      \
  TEST(test_suite_name, test_name) {                                       \
    runAsyncTest(TEST_IMPL_NAME(test_suite_name, test_name), num_threads); \
  }                                                                        \
  net::awaitable<void> TEST_IMPL_NAME(test_suite_name, test_name)(         \
      [[maybe_unused]] net::io_context & ioContext)

#define ASIO_TEST_N(test_suite_name, test_name, num_threads)               \
  void TEST_IMPL_NAME(test_suite_name,                                     \
                      test_name)([[maybe_unused]] net::io_context&);       \
  TEST(test_suite_name, test_name) {                                       \
    runAsyncTest(TEST_IMPL_NAME(test_suite_name, test_name), num_threads); \
  }                                                                        \
  void TEST_IMPL_NAME(test_suite_name,                                     \
                      test_name)([[maybe_unused]] net::io_context & ioContext)

// Drop-in replacement for gtest's TEST() macro, but for tests that make
// use of boost asio's awaitable coroutines. Note that this prevents you
// from using ASSERT_* macros unless you redefine the return keword with
// co_return so it works nicely with the coroutine.
#define ASYNC_TEST(test_suite_name, test_name) \
  ASYNC_TEST_N(test_suite_name, test_name, 1)

#define ASIO_TEST(test_suite_name, test_name) \
  ASIO_TEST_N(test_suite_name, test_name, 1)

#endif  // QLEVER_ASYNCTESTHELPERS_H

//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <boost/asio/awaitable.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "util/Exception.h"

namespace ad_utility {

namespace net = boost::asio;

/// Helper function that ensures that co_await resumes on the executor
/// this coroutine was co_spawned on.
/// IMPORTANT: If the coroutine is cancelled, no guarantees are given. Make
/// sure to keep that in mind when handling cancellation errors!
template <typename T>
inline net::awaitable<T> resumeOnOriginalExecutor(net::awaitable<T> awaitable) {
  std::exception_ptr exceptionPtr;
  try {
    T result = co_await std::move(awaitable);
    co_await net::post(net::use_awaitable);
    co_return result;
  } catch (...) {
    exceptionPtr = std::current_exception();
  }
  auto cancellationState = co_await net::this_coro::cancellation_state;
  if (cancellationState.cancelled() == net::cancellation_type::none) {
    // use_awaitable always resumes the coroutine on the executor the coroutine
    // was co_spawned on
    co_await net::post(net::use_awaitable);
  }
  AD_CORRECTNESS_CHECK(exceptionPtr);
  std::rethrow_exception(exceptionPtr);
}

// _____________________________________________________________________________

/// Helper function that ensures that co_await resumes on the executor
/// this coroutine was co_spawned on. Overload for void.
inline net::awaitable<void> resumeOnOriginalExecutor(
    net::awaitable<void> awaitable) {
  std::exception_ptr exceptionPtr;
  try {
    co_await std::move(awaitable);
  } catch (...) {
    exceptionPtr = std::current_exception();
  }
  if ((co_await net::this_coro::cancellation_state).cancelled() ==
      net::cancellation_type::none) {
    // use_awaitable always resumes the coroutine on the executor the coroutine
    // was co_spawned on
    co_await net::post(net::use_awaitable);
  }
  if (exceptionPtr) {
    std::rethrow_exception(exceptionPtr);
  }
}
}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H

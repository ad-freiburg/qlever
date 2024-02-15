//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <absl/cleanup/cleanup.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

namespace net = boost::asio;

template <typename Strand, typename CompletionToken, std::invocable Function>
auto runFunctionOnStrand(Strand strand, Function function,
                         CompletionToken& token) {
  LOG(INFO) << "Run on strandd" << std::endl;
  auto awaitable =
      [](auto function) -> net::awaitable<std::invoke_result_t<Function>> {
    co_return std::invoke(function);
  };
  auto innerAwaitable =
      [](auto awaitable,
         auto strand) -> net::awaitable<std::invoke_result_t<Function>> {
    co_return (co_await net::co_spawn(strand, std::move(awaitable),
                                      net::use_awaitable));
  };
  return net::co_spawn(
      strand, innerAwaitable(awaitable(std::move(function)), strand), token);
}

template <typename Strand, typename T>
net::awaitable<T> runAwaitableOnStrandAwaitable(Strand strand,
                                                net::awaitable<T> awaitable) {
  auto state = co_await net::this_coro::cancellation_state;
  auto slot = state.slot();
  net::cancellation_signal signal;
  if (slot.is_connected()) {
    slot.assign([&signal, &strand](auto type) {
      net::dispatch(strand, [&signal, type]() { signal.emit(type); });
    });
  }
  absl::Cleanup c{[&slot]() { slot.clear(); }};
  auto token = net::bind_executor(
      strand, net::bind_cancellation_slot(signal.slot(), net::use_awaitable));
  co_await net::post(token);
  if constexpr (std::is_void_v<T>) {
    co_await std::move(awaitable);
    slot.clear();
    co_await net::post(net::use_awaitable);
    co_return;
  } else {
    auto res = co_await std::move(awaitable);
    slot.clear();
    co_await net::post(net::use_awaitable);
    co_return res;
  }
}
#if false
template <typename Strand, typename T>
net::awaitable<T> runAwaitableOnStrandAwaitable(Strand strand,
                                                net::awaitable<T> awaitable) {
  auto inner = [](auto strand, auto f) -> net::awaitable<T> {
    co_await net::post(net::use_awaitable);
    // auto state = co_await net::this_coro::cancellation_state;
    // absl::Cleanup c{[&](){state.slot().clear(); LOG(INFO) << "After clear" <<
    // std::endl;} }; LOG(INFO) << "before spawn" << std::endl;
    co_return (
        co_await net::co_spawn(strand, std::move(f), net::use_awaitable));
  };
  if constexpr (std::is_void_v<T>) {
    // co_await net::co_spawn(strand, std::move(awaitable), net::deferred);
    // LOG(INFO) << "before spawn" << std::endl;
    co_await net::co_spawn(strand, inner(strand, std::move(awaitable)),
                           net::use_awaitable);
    // auto state = co_await net::this_coro::cancellation_state;
    // state.slot().clear();
    // LOG(INFO) << "After clear" << std::endl;
    co_await net::post(net::use_awaitable);
  } else {
    /*
    decltype(auto) res = co_await net::co_spawn(strand, std::move(awaitable),
                                                net::use_awaitable);
                                                */
    // LOG(INFO) << "before spawn" << std::endl;
    decltype(auto) res = co_await net::co_spawn(
        strand, inner(strand, std::move(awaitable)), net::use_awaitable);
    // auto state = co_await net::this_coro::cancellation_state;
    // state.slot().clear();
    // LOG(INFO) << "After clear" << std::endl;

    co_await net::post(net::use_awaitable);
    co_return res;
  }
}
#endif

/*
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
 */
}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H

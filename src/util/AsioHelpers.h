//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <boost/asio/awaitable.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <future>

#include "util/Exception.h"

namespace ad_utility {

namespace net = boost::asio;

// Run `f` on the `executor` asynchronously and return an `awaitable` that returns the result of that invocation.
// Note, that
//  1. The returned `awaitable` will resume on the executor on which it is `co_await`ed which is often different from
//     the inner `executor`. In other terms, only `f` is run on the executor, but no other state changes.
//  2. Once started, `f` will always run to completion, even if the outer awaitable is cancelled. In other words if an
//     expression `co_await runOnExecutor(exec, f)` is canceled, it is either cancelled before or after invoking `f`. Make sure to only schedule
//     functions for which this behavior is okay (for example because they only
//     run for a short time or because they have to run to completion anyway and
//     are never canceled. The reason for this limitation lies in the mechanics of Boost::ASIO. It is not possible to change
//     the executor within the same coroutine stack (which would be cancelable all the way through) without causing data
//     races on the cancellation state, about which the thread sanitizer (correctly) complains.
template <typename Executor, std::invocable F>
    requires(!std::is_void_v<std::invoke_result_t<F>>)
net::awaitable<std::invoke_result_t<F>> runOnExecutor(const Executor& exec, F f) {
  using Res = std::invoke_result_t<F>;
  std::variant<std::monostate, Res, std::exception_ptr> res;
  std::atomic_flag flag(false);
  net::dispatch(exec, [&]() {
    try {
      res = std::invoke(std::move(f));
    } catch (...) {
      res.template emplace<std::exception_ptr>(std::current_exception());
    }
    flag.test_and_set();
  });
  flag.wait(false);
  if (std::holds_alternative<std::exception_ptr>(res)) {
    std::rethrow_exception(std::get<std::exception_ptr>(res));
  }
  co_return std::move(std::get<Res>(res));
}

template <typename Executor, std::invocable F>
requires(std::is_void_v<std::invoke_result_t<F>>)
net::awaitable<void> runOnExecutor(const Executor& exec, F f) {
  std::optional<std::exception_ptr> ex;
  std::atomic_flag flag(false);
  net::dispatch(exec, [&]() {
    try {
      std::invoke(std::move(f));
    } catch (...) {
      ex = std::current_exception();
    }
    flag.test_and_set();
  });
  flag.wait(false);
  if (ex) {
    std::rethrow_exception(ex.value());
  }
  co_return;
}

/*
// Overload of `runOnExecutor` for functions that return `void`. For a detailed documentation see above.
template <typename Executor, std::invocable F>
requires(std::is_void_v<std::invoke_result_t<F>>)
net::awaitable<void> runOnExecutor(const Executor& exec,
                                                      F f) {
  auto awaitable = [](F f) -> net::awaitable<void> {
    std::invoke(f);
    co_return;
  };
  co_await net::co_spawn(exec, awaitable(std::move(f)), net::use_awaitable);
}
     */

}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H

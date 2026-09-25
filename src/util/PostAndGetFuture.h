// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_POSTANDGETFUTURE_H
#define QLEVER_SRC_UTIL_POSTANDGETFUTURE_H

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <functional>
#include <future>
#include <type_traits>
#include <utility>

namespace ad_utility {

// Post the `function` to the `executor` and return a `std::future` for its
// result. The future becomes ready as soon as the executor has run the
// function, and rethrows the exception that the function has thrown (if any) on
// `get()`. This is the drop-in replacement for `std::async(std::launch::async,
// function)` that runs on a given thread pool instead of on a thread of its
// own.
//
// NOTE: The `executor` (more precisely: the execution context behind it) has to
// outlive the returned future, exactly as with any other `boost::asio::post`.
//
// NOTE: The future becomes ready as soon as the `function` has *returned*. The
// function object itself, and thus everything that it owns, is destroyed only
// shortly afterwards (in contrast to a `JThread`, where `join()` also waits for
// that destruction). A caller that relies on a resource of the `function` being
// released has to make the `function` release it explicitly before it returns,
// see `ad_utility::streams::runStreamAsync` for an example.
//
// NOTE: This is deliberately not `ad_utility::runFunctionOnExecutor(executor,
// function, net::use_future)` from `util/AsioHelpers.h`, which is the same
// thing for a result that is default-constructible (or `void`). The callers
// here hand back blocks (a `SortBlockBuffer`, an `IdTableStatic`) that are
// not, because they own an allocator. Prefer `runFunctionOnExecutor` wherever
// that restriction does not bite.
template <typename Function>
std::future<std::invoke_result_t<Function&>> postAndGetFuture(
    const boost::asio::any_io_executor& executor, Function function) {
  using Result = std::invoke_result_t<Function&>;
  std::packaged_task<Result()> task{std::move(function)};
  auto future = task.get_future();
  // NOTE: The `task` is deliberately wrapped in a lambda instead of being
  // posted directly. Boost::Asio treats a `std::packaged_task` as a completion
  // *token* and then calls `get_future()` on it itself, which would throw
  // `std::future_error: Future already retrieved` here.
  boost::asio::post(executor,
                    [task = std::move(task)]() mutable { std::invoke(task); });
  return future;
}

// Return a `std::future<void>` that is already fulfilled. This is the
// replacement for `std::async(std::launch::deferred, []{})`, which is used
// where a valid but trivial future is required.
inline std::future<void> makeReadyFuture() {
  std::promise<void> promise;
  promise.set_value();
  return promise.get_future();
}

// Return a `std::future` that is already fulfilled with the given `value`. Use
// this where a valid future is required, but the value that it transports is
// already known.
template <typename T>
std::future<std::decay_t<T>> makeReadyFuture(T&& value) {
  std::promise<std::decay_t<T>> promise;
  promise.set_value(std::forward<T>(value));
  return promise.get_future();
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_POSTANDGETFUTURE_H

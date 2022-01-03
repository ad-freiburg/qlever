//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ASYNCWAITFORFUTURE_H
#define QLEVER_ASYNCWAITFORFUTURE_H
// Inspired by https://gist.github.com/inetic/dc9081baf45ec4b60037

#include <future>

#include "../HttpServer/beast.h"

namespace ad_utility::asio_helpers {

// An async operation that can be used inside boost::asio.
// The `function` (which takes no arguments) is started on a new detached
// thread (unrelated to the executor used by the `completionToken`).
// As soon as the `function` is finished, the completion handler that is
// obtained from the `completionToken` is invoked with the result of the
// `function`. The handler obtained from the `completionToken` must be callable
// with (std::exception_ptr, returnValueOfFunction).
template <typename CompletionToken, typename Function>
auto async_on_external_thread(Function function,
                              CompletionToken&& completionToken) {
  namespace asio = boost::asio;
  namespace system = boost::system;

  using Value = std::invoke_result_t<Function>;
  using Signature = void(std::exception_ptr, Value);

  auto initiatingFunction = [function =
                                 std::move(function)](auto&& handler) mutable {
    auto onRemoteThread =
        [function = std::move(function),
         handler = std::forward<decltype(handler)>(handler)]() mutable {
          try {
            // If `function()` throws no exception, we have no exception_ptr
            // and the return value as the second argument.
            handler(nullptr, function());
          } catch (...) {
            // If `function()` throws, we propagate the exception to the
            // handler, and have no return value.
            // TODO<C++23/26> When we get networking+executors, we hopefully
            // have `setError` method which doesn't need a dummy return value.
            Value* ptr = nullptr;
            handler(std::current_exception(), std::move(*ptr));
          }
        };

    std::thread thread(std::move(onRemoteThread));
    thread.detach();
  };

  // This helper function automagically obtains a completionHandler from the
  // `completionToken` and returns the correct `boost::asio::async_result`.
  return boost::asio::async_initiate<CompletionToken, Signature>(
      std::move(initiatingFunction), completionToken);
}
}  // namespace ad_utility::asio_helpers

#endif  // QLEVER_ASYNCWAITFORFUTURE_H

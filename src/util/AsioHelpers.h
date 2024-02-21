//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Authors: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//            Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <absl/cleanup/cleanup.h>

#include <boost/asio.hpp>
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

// Run the `function` on the `executor` (e.g. a strand for synchronization or
// the executor of a thread pool). As soon as the function has completed, the
// completion handler which is obtained from the `completionToken` is posted to
// its associated executor (which might be different from the `executor`).
template <typename Executor, typename CompletionToken, std::invocable Function>
auto runFunctionOnExecutor(Executor executor, Function function,
                           CompletionToken& completionToken) {
  using Value = std::invoke_result_t<Function>;
  static constexpr bool isVoid = std::is_void_v<Value>;

  // Obtain the call signature for the handler.
  [[maybe_unused]] auto getSignature = []() {
    if constexpr (isVoid) {
      return std::type_identity<void(std::exception_ptr)>{};
    } else {
      return std::type_identity<void(std::exception_ptr, Value)>{};
    }
  };
  using Signature = typename decltype(getSignature())::type;

  auto initiatingFunction = [function = std::move(function),
                             executor](auto&& handler) mutable {
    auto onExecutor = [executor, function = std::move(function),
                       handler = AD_FWD(handler)]() mutable {
      // Get the executor of the completion handler, to which we will post the
      // handler.
      auto handlerExec = net::get_associated_executor(handler, executor);
      // Call the completion handler with the given arguments, but post this
      // calling to the `handlerExec`.
      auto callHandler = [handlerExec, &handler](auto... args) mutable {
        auto doCall = [handler = std::move(handler),
                       ... args = std::move(args)]() mutable {
          handler(std::move(args)...);
        };
        net::post(net::bind_executor(handlerExec, std::move(doCall)));
      };
      try {
        // If `function()` throws no exception, we have no
        // exception_ptr and the return value as the second argument.
        if constexpr (isVoid) {
          function();
          callHandler(nullptr);
        } else {
          callHandler(nullptr, function());
        }
      } catch (...) {
        // If `function()` throws, we propagate the exception to the
        // handler, and have no return value. Unfortunately we still need to
        // pass a reference to the handler.
        if constexpr (isVoid) {
          callHandler(std::current_exception());
        } else {
          auto doCall = [handler = std::move(handler),
                         ex = std::current_exception()]() mutable {
            Value* ptr = nullptr;
            handler(std::move(ex), std::move(*ptr));
          };
          net::post(net::bind_executor(handlerExec, std::move(doCall)));
        }
      }
    };

    net::post(executor, std::move(onExecutor));
  };
  return net::async_initiate<CompletionToken, Signature>(
      std::move(initiatingFunction), completionToken);
}
}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H

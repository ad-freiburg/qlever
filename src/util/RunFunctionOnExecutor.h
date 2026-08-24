// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2023 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_RUNFUNCTIONONEXECUTOR_H
#define QLEVER_SRC_UTIL_RUNFUNCTIONONEXECUTOR_H

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/post.hpp>
#include <exception>
#include <tuple>
#include <type_traits>
#include <utility>

#include "backports/concepts.h"
#include "util/Forward.h"

// `runFunctionOnExecutor`, which turns an ordinary synchronous function into a
// Boost.Asio operation that runs on a given executor. It lives in a header of
// its own (and not in `util/AsioHelpers.h`, which also declares it) because it
// is deliberately free of coroutines and therefore usable in the C++17
// backports mode.
namespace ad_utility {

namespace net = boost::asio;

// Internal implementation for `runFunctionOnExecutor`.
namespace detail {
// Call  `function()`. If this call doesn't throw, call `handler(nullptr,
// resultOfTheCall)`, Else call `handler(current_exeption(), *nullptr)`. The
// function is called directly, but the invocation of the handler is posted to
// the associated executor of the `handler`, or to the `executor` if no such
// associated executor exists.
CPP_template(typename Executor, typename Function,
             typename Handler)(requires ql::concepts::invocable<
                               Function>) struct CallFunctionAndPassToHandler {
  Executor executor_;
  Function function_;
  Handler handler_;
  using Value = std::invoke_result_t<Function>;
  static constexpr bool isVoid = std::is_void_v<Value>;
  void operator()() {
    // Get the executor of the completion handler, to which we will post the
    // handler.
    auto handlerExec = net::get_associated_executor(handler_, executor_);
    // Call the completion handler with the given arguments, but post this
    // calling to the `handlerExec`.
    //
    // NOTE: The arguments are captured as a single `tuple`, because expanding a
    // parameter pack in the init-capture of a lambda requires C++20.
    auto callHandler = [&handlerExec,
                        &handler = handler_](auto... args) mutable {
      auto doCall = [handler = std::move(handler),
                     args = std::make_tuple(std::move(args)...)]() mutable {
        std::apply(
            [&handler](auto&&... unpacked) { handler(std::move(unpacked)...); },
            std::move(args));
      };
      net::post(net::bind_executor(handlerExec, std::move(doCall)));
    };
    try {
      // If `function_()` throws no exception, we have no
      // exception_ptr and the return value as the second argument.
      if constexpr (isVoid) {
        function_();
        callHandler(std::exception_ptr{});
      } else {
        callHandler(std::exception_ptr{}, function_());
      }
    } catch (...) {
      // If `function_()` throws, we propagate the exception to the
      // handler, and have no return value.
      if constexpr (isVoid) {
        callHandler(std::current_exception());
      } else {
        // Unfortunately we need to pass a `Value{}` even in the case of an
        // error. This is due to a limitation in Boost::Asio.
        callHandler(std::current_exception(), Value{});
      }
    }
  }
};
// Explicit deduction guides, we need objects and not references as the template
// parameters.
CPP_template(typename Executor, typename Function, typename Handler)(
    requires ql::concepts::invocable<std::decay_t<Function>>)
    CallFunctionAndPassToHandler(Executor&&, Function&&, Handler&&)
        -> CallFunctionAndPassToHandler<std::decay_t<Executor>,
                                        std::decay_t<Function>,
                                        std::decay_t<Handler>>;
}  // namespace detail

// Run the `function` on the `executor` (e.g. a strand for synchronization or
// the executor of a thread pool). As soon as the function has completed, the
// completion handler which is obtained from the `completionToken` is posted to
// its associated executor (which might be different from the `executor`).
// Note: If no executor is associated with the `completionToken`, then the
// handler will also be run on the `executor` that is passed to this function as
// there is no other way of running it.
CPP_template(typename Executor, typename CompletionToken, typename Function)(
    requires ql::concepts::invocable<Function> CPP_and(
        std::is_default_constructible_v<std::invoke_result_t<Function>> ||
        std::is_void_v<std::invoke_result_t<
            Function>>)) auto runFunctionOnExecutor(Executor executor,
                                                    Function function,
                                                    CompletionToken&&
                                                        completionToken) {
  using Value = std::invoke_result_t<Function>;
  static constexpr bool isVoid = std::is_void_v<Value>;

  // Obtain the call signature for the handler.
  // Note: The `DummyRes` is needed, because both branches (even the one that is
  // statically not taken) must be valid types.
  using DummyRes = std::conditional_t<isVoid, int, Value>;
  using Signature = std::conditional_t<isVoid, void(std::exception_ptr),
                                       void(std::exception_ptr, DummyRes)>;

  auto initiatingFunction = [function = std::move(function),
                             executor](auto&& handler) mutable {
    auto onExecutor = detail::CallFunctionAndPassToHandler{
        executor, std::move(function), AD_FWD(handler)};
    net::post(executor, std::move(onExecutor));
  };
  return net::async_initiate<CompletionToken, Signature>(
      std::move(initiatingFunction), completionToken);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RUNFUNCTIONONEXECUTOR_H

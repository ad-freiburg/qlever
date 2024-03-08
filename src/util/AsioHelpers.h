//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Authors: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//            Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <boost/asio/experimental/awaitable_operators.hpp>

#include "global/Constants.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/http/beast.h"

namespace ad_utility {

namespace net = boost::asio;

// Internal implementation for `runFunctionOnExecutor`.
namespace detail {
// Call  `function()`. If this call doesn't throw, call `handler(nullptr,
// resultOfTheCall)`, Else call `handler(current_exeption(), *nullptr)`. The
// function is called directly, but the invocation of the handler is posted to
// the associated executor of the `handler`, or to the `executor` if no such
// associated executor exists.
template <typename Executor, std::invocable Function, typename Handler>
struct CallFunctionAndPassToHandler {
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
    auto callHandler = [&handlerExec,
                        &handler = handler_](auto... args) mutable {
      auto doCall = [handler = std::move(handler),
                     ... args = std::move(args)]() mutable {
        handler(std::move(args)...);
      };
      net::post(net::bind_executor(handlerExec, std::move(doCall)));
    };
    try {
      // If `function_()` throws no exception, we have no
      // exception_ptr and the return value as the second argument.
      if constexpr (isVoid) {
        function_();
        callHandler(nullptr);
      } else {
        callHandler(nullptr, function_());
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
template <typename Executor, typename Function, typename Handler>
requires std::invocable<std::decay_t<Function>>
CallFunctionAndPassToHandler(Executor&&, Function&&, Handler&&)
    -> CallFunctionAndPassToHandler<
        std::decay_t<Executor>, std::decay_t<Function>, std::decay_t<Handler>>;
}  // namespace detail

// Run the `function` on the `executor` (e.g. a strand for synchronization or
// the executor of a thread pool). As soon as the function has completed, the
// completion handler which is obtained from the `completionToken` is posted to
// its associated executor (which might be different from the `executor`).
// Note: If no executor is associated with the `completionToken`, then the
// handler will also be run on the `executor` that is passed to this function as
// there is no other way of running it.
template <typename Executor, typename CompletionToken, std::invocable Function>
requires std::is_default_constructible_v<std::invoke_result_t<Function>> ||
         std::is_void_v<std::invoke_result_t<Function>>
auto runFunctionOnExecutor(Executor executor, Function function,
                           CompletionToken& completionToken) {
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

/// Helper function, that checks a cancellation handle regularly while waiting
/// on an awaitable object that doesn't do this on its own. It's always better
/// to integrate cancellation checks right into the awaitable itself, but
/// sometimes this doesn't work because it's part of a library or boost asio
/// itself. Once `timerRunning` resolves to `false` (or the awaitable is
/// finished, whatever happens first), the cancellation checks are stopped.
/// This needs to be called on a strand or in a single-threaded environment,
/// otherwise this may lead to race conditions, due to issues with boost asio.
template <typename T>
inline net::awaitable<T> interruptible(
    net::awaitable<T> awaitable, ad_utility::SharedCancellationHandle handle,
    std::shared_ptr<std::atomic_flag> timerRunning =
        std::make_shared<std::atomic_flag>(true),
    ad_utility::source_location loc = ad_utility::source_location::current()) {
  using namespace net::experimental::awaitable_operators;

  auto timerLoop = [](std::shared_ptr<std::atomic_flag> timerRunning,
                      ad_utility::SharedCancellationHandle handle,
                      ad_utility::source_location loc) -> net::awaitable<void> {
    constexpr auto timeout = DESIRED_CANCELLATION_CHECK_INTERVAL / 2;
    absl::Cleanup cleanup{[&timerRunning]() { timerRunning->clear(); }};
    net::steady_timer timer{co_await net::this_coro::executor};
    while (timerRunning->test()) {
      handle->throwIfCancelled(loc);
      timer.expires_after(timeout);
      auto [ec] = co_await timer.async_wait(net::as_tuple(net::deferred));
      if (ec) {
        AD_CORRECTNESS_CHECK(ec == net::error::operation_aborted);
        break;
      }
    }
  };
  auto wrapper = [](net::awaitable<T> awaitable,
                    std::shared_ptr<std::atomic_flag> timerRunning) mutable
      -> net::awaitable<T> {
    absl::Cleanup cleanup{
        [timerRunning = std::move(timerRunning)]() { timerRunning->clear(); }};
    co_return co_await std::move(awaitable);
  };

  auto timerClone = timerRunning;
  try {
    co_return co_await (
        timerLoop(std::move(timerClone), std::move(handle), std::move(loc)) &&
        wrapper(std::move(awaitable), std::move(timerRunning)));
  } catch (const net::multiple_exceptions& e) {
    // Ignore other exceptions
    std::rethrow_exception(e.first_exception());
  }
}

/// Helper function to wait for an awaitable that is supposed to be run on an io
/// context.
template <typename T>
inline T runAndWaitForAwaitable(net::awaitable<T> awaitable,
                                net::io_context& ioContext) {
  auto future = net::co_spawn(net::make_strand(ioContext), std::move(awaitable),
                              net::use_future);

  while (true) {
    auto futureStatus = future.wait_for(std::chrono::milliseconds{0});
    if (futureStatus == std::future_status::ready) {
      break;
    }
    AD_CORRECTNESS_CHECK(futureStatus != std::future_status::deferred);
    ioContext.poll_one();
  }
  return future.get();
}
}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H

//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Authors: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//            Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <absl/cleanup/cleanup.h>

#include <boost/asio/experimental/awaitable_operators.hpp>

#include "backports/atomic_flag.h"
#include "global/Constants.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/RunFunctionOnExecutor.h"
#include "util/http/beast.h"

namespace ad_utility {

namespace net = boost::asio;

// Like `net::co_spawn(executor, awaitable, completionToken)`, but the
// completion handler is never invoked inline. Instead, once the coroutine has
// completed, the handler is posted to its associated executor (default:
// `executor`).
//
// The completion signature is `void(std::exception_ptr)` if `T` is `void`, and
// `void(std::exception_ptr, T)` otherwise. If the coroutine threw, the
// `exception_ptr` holds the exception, and the `T` is default-constructed.
//
// This matters if the caller holds a resource (e.g. a strand or a lock) that
// the handler must not run under: a plain `co_spawn` (or `net::dispatch`)
// runs the handler inline when the current thread already belongs to the
// handler's executor.
template <typename T, typename Executor, typename CompletionToken>
auto coSpawnWithPostedCompletion(Executor executor, net::awaitable<T> awaitable,
                                 CompletionToken&& completionToken) {
  static constexpr bool isVoid = std::is_void_v<T>;
  using DummyRes = std::conditional_t<isVoid, int, T>;
  using Signature = std::conditional_t<isVoid, void(std::exception_ptr),
                                       void(std::exception_ptr, DummyRes)>;
  auto initiatingFunction = [executor, awaitable = std::move(awaitable)](
                                auto handler) mutable {
    auto handlerExecutor = net::get_associated_executor(handler, executor);
    // NOTE: The pack `result` lets the same lambda serve both completion
    // signatures of `co_spawn`: it is empty if `T` is `void`, and holds
    // exactly one element (the result of the coroutine) otherwise.
    net::co_spawn(
        executor, std::move(awaitable),
        [handlerExecutor, handler = std::move(handler)](
            std::exception_ptr exception, auto... result) mutable {
          net::post(
              handlerExecutor,
              [handler = std::move(handler), exception = std::move(exception),
               ... result = std::move(result)]() mutable {
                std::move(handler)(std::move(exception), std::move(result)...);
              });
        });
  };
  return net::async_initiate<CompletionToken, Signature>(
      std::move(initiatingFunction), completionToken);
}

/// Helper function, that checks a cancellation handle regularly while waiting
/// on an awaitable object that doesn't do this on its own. It's always better
/// to integrate cancellation checks right into the awaitable itself, but
/// sometimes this doesn't work because it's part of a library or boost asio
/// itself. Once the value for `cancelCallback` is set and called by the caller
/// of this function (or the awaitable is finished, whatever happens first), the
/// cancellation checks are stopped. This needs to be called on a strand or in a
/// single-threaded environment, otherwise this will lead to race conditions,
/// due to issues with boost asio.
template <typename T>
inline net::awaitable<T> interruptible(
    net::awaitable<T> awaitable, ad_utility::SharedCancellationHandle handle,
    std::promise<std::function<void()>> cancelCallback,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  using namespace net::experimental::awaitable_operators;
  auto timer =
      std::make_shared<net::steady_timer>(co_await net::this_coro::executor);
  auto running = std::make_shared<ql::atomic_flag>(true);
  auto cancelTimer = [timer, running]() mutable {
    auto strand = timer->get_executor();
    running->clear();
    net::dispatch(strand, [timer = std::move(timer)]() { timer->cancel(); });
  };
  // Provide callback to outer world in order to cancel the timer preemptively.
  cancelCallback.set_value(cancelTimer);

  auto timerLoop = [](std::shared_ptr<net::steady_timer> timer,
                      std::shared_ptr<ql::atomic_flag> running,
                      ad_utility::SharedCancellationHandle handle,
                      ad_utility::source_location loc) -> net::awaitable<void> {
    constexpr auto timeout = DESIRED_CANCELLATION_CHECK_INTERVAL / 2;
    while (running->test()) {
      handle->throwIfCancelled(loc);
      timer->expires_after(timeout);
      // Workaround for a GCC 15/16 bug: the hidden object of a by-value
      // structured binding in a coroutine is not always destroyed at the end
      // of its scope, for example when it is initialized by a `co_await` or
      // lives across one (gcc.gnu.org bug 124584).
      auto waitResult =
          co_await timer->async_wait(net::as_tuple(net::deferred));
      auto& [ec] = waitResult;
      if (ec) {
        AD_CORRECTNESS_CHECK(ec == net::error::operation_aborted);
        break;
      }
    }
  };
  auto wrapper = [](net::awaitable<T> awaitable,
                    auto cancelTimer) mutable -> net::awaitable<T> {
    absl::Cleanup cleanup{std::move(cancelTimer)};
    co_return co_await std::move(awaitable);
  };

  try {
    co_return co_await (timerLoop(std::move(timer), std::move(running),
                                  std::move(handle), std::move(loc)) &&
                        wrapper(std::move(awaitable), std::move(cancelTimer)));
  } catch (const net::multiple_exceptions& e) {
    // Ignore other exceptions
    std::rethrow_exception(e.first_exception());
  }
}

/// Overload without an explicit promise object passed for convenience.
template <typename T>
inline net::awaitable<T> interruptible(
    net::awaitable<T> awaitable, ad_utility::SharedCancellationHandle handle,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  return interruptible(std::move(awaitable), std::move(handle),
                       std::promise<std::function<void()>>{}, std::move(loc));
}

/// Helper function to wait for an awaitable that is supposed to be run on an io
/// context.
template <typename T>
inline T runAndWaitForAwaitable(net::awaitable<T> awaitable,
                                net::io_context& ioContext) {
  auto future = net::co_spawn(net::make_strand(ioContext), std::move(awaitable),
                              net::use_future);

  std::future_status futureStatus;
  do {
    bool computedSomething = ioContext.poll_one();
    // 5ms is an arbitrarily chosen interval to not overload the CPU.
    std::chrono::milliseconds timeout{computedSomething ? 0 : 5};
    futureStatus = future.wait_for(timeout);
    AD_CORRECTNESS_CHECK(futureStatus != std::future_status::deferred);
  } while (futureStatus != std::future_status::ready);
  return future.get();
}
}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H

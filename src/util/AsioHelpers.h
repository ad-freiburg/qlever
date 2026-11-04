//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Authors: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//            Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <absl/cleanup/cleanup.h>

#include <atomic>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <optional>
#include <stdexcept>
#include <variant>

#include "backports/atomic_flag.h"
#include "global/Constants.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/http/beast.h"

namespace ad_utility {

namespace net = boost::asio;

// Internal implementation for `runFunctionOnExecutor`.
namespace detail {
// Call `function()`. If this call doesn't throw, call the handler with
// `std::exception_ptr{}` and, unless `Value` is `void`, the result of the call.
// Else call it with the `std::exception_ptr` of the thrown exception and,
// unless `Value` is `void`, a default-constructed `Value` (see the NOTE at
// `runFunctionOnExecutor` below for why that value has to be passed at all).
// The function is called directly, but the invocation of the handler is posted
// to the associated executor of the `handler`, or to the `executor` if no such
// associated executor exists.
CPP_template(typename Executor, typename Function, typename Handler)(
    requires ql::concepts::invocable<Function>)
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
    std::exception_ptr exception;
    try {
      // If `function_()` throws no exception, we have no
      // exception_ptr and the return value as the second argument.
      if constexpr (isVoid) {
        function_();
        callHandler(std::exception_ptr{});
      } else {
        callHandler(std::exception_ptr{}, function_());
      }
      return;
    } catch (...) {
      exception = std::current_exception();
    }
    // If `function_()` throws, we propagate the exception to the handler, and
    // have no return value. The handler is posted only after the `catch` block
    // has been left, so that this thread holds no reference to the exception
    // anymore when the handler runs.
    //
    // NOTE: Otherwise, the handler could run on another thread, read the
    // exception and release its reference while this thread still holds the
    // reference of the `catch` block, so that this thread would free the
    // exception when leaving the block.
    //
    // That is correct, but the reference count is maintained inside
    // `libstdc++`, which is not instrumented, so ThreadSanitizer reports the
    // freeing as a data race (see the entry for `exception_ptr::_M_release` in
    // `misc/tsan-suppressions.txt`, which does not match this case).
    if constexpr (isVoid) {
      callHandler(std::move(exception));
    } else {
      // Unfortunately we need to pass a `Value{}` even in the case of an
      // error. This is due to a limitation in Boost::Asio.
      callHandler(std::move(exception), Value{});
    }
  }
};
// Explicit deduction guides, we need objects and not references as the template
// parameters.
CPP_template(typename Executor, typename Function, typename Handler)(
            requires ql::concepts::invocable<std::decay_t<Function>>)
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
//
// NOTE: The return type of the `function` has to be default-constructible (or
// `void`), see the constraint below. The reason is that Boost.Asio supports
// only a single completion signature per operation in the version that we
// currently require, so the handler has to be called with a `Value` even on the
// error path, where there is no result to report.
CPP_template(typename Executor, typename CompletionToken, typename Function)(
    requires ql::concepts::invocable<Function> &&
    (std::is_default_constructible_v<std::invoke_result_t<Function>> ||
     std::is_void_v<std::invoke_result_t<Function>>))
auto runFunctionOnExecutor(Executor executor, Function function,
                           CompletionToken&& completionToken) {
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

// Thrown by `runOnThreadPoolWithQueueDeadline` below when no thread of the pool
// has picked the function up before the deadline.
class ThreadPoolQueueDeadlineError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// Run `function` on the `threadPool` and return its result, like
// `ad_utility::runFunctionOnExecutor` with `net::use_awaitable`, but give up
// when no thread of the pool has picked the function up within `maxQueueWait`.
// Then throw a `ThreadPoolQueueDeadlineError` with the `errorMessage` right
// away, and the pool skips the function when it eventually gets to it (so the
// thread is free again at once and no work is done for a caller that has
// already given up). Once the function has been picked up, wait for it without
// a deadline.
//
// NOTE: The coroutine has to run on a strand (or a single-threaded executor),
// like `ad_utility::interruptible`, because the pool thread cancels the
// `timer` via `net::dispatch` on the executor of this coroutine.
template <typename Function, typename T = std::invoke_result_t<Function>>
net::awaitable<T> runOnThreadPoolWithQueueDeadline(
    net::static_thread_pool& threadPool, Function function,
    std::chrono::milliseconds maxQueueWait, std::string errorMessage) {
  // The state shared between this coroutine and the task on the pool. The task
  // moves it from `PENDING` to `STARTED` when it picks the function up and to
  // `DONE` when the function has returned. This coroutine moves it from
  // `PENDING` to `ABANDONED` when the deadline has passed first (or when it is
  // cancelled while waiting, see below).
  enum class State { PENDING, STARTED, DONE, ABANDONED };
  static constexpr bool isVoid = std::is_void_v<T>;
  using Result = std::conditional_t<isVoid, std::monostate, T>;
  struct Shared {
    std::atomic<State> state_{State::PENDING};
    std::optional<Result> result_;
    std::exception_ptr exception_;
  };
  auto shared = std::make_shared<Shared>();

  // The `timer` doubles as the event that wakes this coroutine up when the task
  // is done. It is owned by this coroutine only, so that the task never touches
  // an executor that may be gone when the coroutine has already returned.
  struct Timer {
    net::any_io_executor executor_;
    net::steady_timer timer_;
    explicit Timer(net::any_io_executor executor)
        : executor_{executor}, timer_{executor} {}
  };
  auto timer = std::make_shared<Timer>(co_await net::this_coro::executor);
  net::post(threadPool, [shared, weakTimer = std::weak_ptr{timer},
                         function = std::move(function)]() mutable {
    auto expected = State::PENDING;
    if (!shared->state_.compare_exchange_strong(expected, State::STARTED)) {
      AD_CORRECTNESS_CHECK(expected == State::ABANDONED);
      return;
    }
    try {
      if constexpr (isVoid) {
        std::invoke(std::move(function));
        shared->result_.emplace();
      } else {
        shared->result_.emplace(std::invoke(std::move(function)));
      }
    } catch (...) {
      shared->exception_ = std::current_exception();
    }
    shared->state_.store(State::DONE);
    if (auto timer = weakTimer.lock()) {
      // The executor is copied before the `timer` is moved into the handler
      // (the order in which the arguments are evaluated is unspecified).
      auto executor = timer->executor_;
      net::dispatch(executor,
                    [timer = std::move(timer)]() { timer->timer_.cancel(); });
    }
  });

  // Wait until the deadline has passed or the task is done, whichever comes
  // first. If the task has not even started by then, abandon it.
  timer->timer_.expires_after(maxQueueWait);
  auto [deadlineError] =
      co_await timer->timer_.async_wait(net::as_tuple(net::use_awaitable));
  if (!deadlineError) {
    auto expected = State::PENDING;
    if (shared->state_.compare_exchange_strong(expected, State::ABANDONED)) {
      throw ThreadPoolQueueDeadlineError{std::move(errorMessage)};
    }
  }

  // Wait until the task is done. A wait ends early only when the task is done
  // or this coroutine has been cancelled (see `ad_utility::interruptible`,
  // whose own exception then supersedes the one thrown here). In the latter
  // case, abandon the task if it has not started yet, so that it does not
  // occupy a thread later on.
  while (shared->state_.load() != State::DONE) {
    auto cancellationState = co_await net::this_coro::cancellation_state;
    if (cancellationState.cancelled() != net::cancellation_type::none) {
      auto expected = State::PENDING;
      shared->state_.compare_exchange_strong(expected, State::ABANDONED);
      throw ad_utility::CancellationException{
          ad_utility::CancellationState::MANUAL,
          "Cancelled while waiting for a free thread of the query thread pool"};
    }
    timer->timer_.expires_at(std::chrono::steady_clock::time_point::max());
    co_await timer->timer_.async_wait(net::as_tuple(net::use_awaitable));
  }
  if (shared->exception_) {
    std::rethrow_exception(shared->exception_);
  }
  if constexpr (!isVoid) {
    co_return std::move(shared->result_.value());
  }
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

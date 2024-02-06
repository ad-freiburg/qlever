//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <absl/functional/any_invocable.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <future>

#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

namespace net = boost::asio;

// Run `f` on the `executor` asynchronously and return an `awaitable` that
// returns the result of that invocation. Note, that
//  1. The returned `awaitable` will resume on the executor on which it is
//  `co_await`ed which is often different from
//     the inner `executor`. In other terms, only `f` is run on the executor,
//     but no other state changes.
//  2. Once started, `f` will always run to completion, even if the outer
//  awaitable is cancelled. In other words if an
//     expression `co_await runOnExecutor(exec, f)` is canceled, it is either
//     cancelled before or after invoking `f`. Make sure to only schedule
//     functions for which this behavior is okay (for example because they only
//     run for a short time or because they have to run to completion anyway and
//     are never canceled. The reason for this limitation lies in the mechanics
//     of Boost::ASIO. It is not possible to change the executor within the same
//     coroutine stack (which would be cancelable all the way through) without
//     causing data races on the cancellation state, about which the thread
//     sanitizer (correctly) complains.
template <typename Executor, std::invocable F>
requires(!std::is_void_v<std::invoke_result_t<F>>)
net::awaitable<std::invoke_result_t<F>> runOnExecutor(const Executor& exec,
                                                      F f) {
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

struct AsyncMutex {
 private:
  net::any_io_executor executor_;
  std::mutex mutex_;
  std::vector<absl::AnyInvocable<void()>> waiters{};
  bool occupied_{false};

 public:
  [[nodiscard]] struct LockGuard {
   private:
    AsyncMutex* mutex_;
    bool active_ = true;

   public:
    [[nodiscard]] explicit LockGuard(AsyncMutex* mutex) : mutex_{mutex} {}
    ~LockGuard() {
      if (active_) {
        mutex_->unlock();
      }
    }
    [[nodiscard]] LockGuard(LockGuard&& l)
        : mutex_{l.mutex_}, active_(std::exchange(l.active_, false)) {}
    [[nodiscard]] LockGuard& operator=(LockGuard&& l) {
      mutex_ = l.mutex_;
      active_ = std::exchange(l.active_, false);
      return *this;
    }
  };

 private:
  friend class AsyncSignal;
  template <typename CompletionHandler>
  void asyncLockImpl(CompletionHandler&& handler) {
    auto executor = net::get_associated_executor(handler, executor_);
    std::unique_lock lock{mutex_};
    if (!occupied_) {
      occupied_ = true;
      lock.unlock();
      // TODDO<joka921> Several unit tests made by Robing fail when this is
      // changed to `dispatch`, we need to discuss, whether this behavior is
      // important.
      net::post(net::bind_executor(
          executor, [handler = std::move(handler)]() mutable { handler(); }));
    } else {
      auto resume = [executor, handler = AD_FWD(handler)]() mutable {
        net::post(net::bind_executor(
            executor, [handler = std::move(handler)]() mutable { handler(); }));
      };
      waiters.push_back(std::move(resume));
    }
  }
  template <typename CompletionHandler>
  void asyncLockGuardImpl(CompletionHandler&& handler) {
    auto executor = net::get_associated_executor(handler, executor_);
    std::unique_lock lock{mutex_};
    if (!occupied_) {
      occupied_ = true;
      lock.unlock();
      net::post(net::bind_executor(
          executor, [handler = std::move(handler), self = this]() mutable {
            handler(LockGuard{self});
          }));
    } else {
      auto resume = [executor, handler = AD_FWD(handler),
                     self = this]() mutable {
        net::post(net::bind_executor(
            executor, [handler = std::move(handler), self]() mutable {
              handler(LockGuard{self});
            }));
      };
      waiters.push_back(std::move(resume));
    }
  }

 public:
  AsyncMutex(net::any_io_executor executor) : executor_(std::move(executor)) {}
  template <typename CompletionToken>
  auto asyncLock(CompletionToken token) -> decltype(auto) {
    auto impl = [self = this](auto&& handler) {
      self->asyncLockImpl(AD_FWD(handler));
    };
    return net::async_initiate<CompletionToken, void()>(std::move(impl), token);
  }

  template <typename CompletionToken>
  auto asyncLockGuard(CompletionToken token) -> decltype(auto) {
    auto impl = [self = this](auto&& handler) {
      self->asyncLockGuardImpl(AD_FWD(handler));
    };
    return net::async_initiate<CompletionToken, void(LockGuard)>(
        std::move(impl), token);
  }

  void unlock() {
    std::unique_lock l{mutex_};
    if (waiters.empty()) {
      occupied_ = false;
      return;
    } else {
      // TODO<joka921> This is rather expensive, use something that supports
      // FiFO better. But we currently need FIFO to make some unit tests pass.
      auto f = std::move(waiters.front());
      waiters.erase(waiters.begin());
      l.unlock();
      std::invoke(f);
    }
  }
};

class AsyncSignal {
 private:
  std::vector<absl::AnyInvocable<void()>> waiters{};

 private:
  template <typename CompletionHandler>
  void asyncWaitImpl(CompletionHandler&& handler, AsyncMutex& mutex) {
    auto resume = [handler = AD_FWD(handler), &mutex]() mutable {
      mutex.asyncLockImpl(AD_FWD(handler));
    };
    waiters.push_back(std::move(resume));
    mutex.unlock();
  }

 public:
  explicit AsyncSignal() = default;

  template <typename CompletionToken>
  auto asyncWait(AsyncMutex& mutex, CompletionToken token) -> decltype(auto) {
    auto impl = [self = this, &mutex](auto&& handler) {
      auto slot = net::get_associated_cancellation_slot(handler);
      slot.emplace([](){LOG(INFO) << "Cancelled an async wait" << std::endl;});
      self->asyncWaitImpl(AD_FWD(handler), mutex);
    };
    return net::async_initiate<CompletionToken, void()>(std::move(impl), token);
  }

  void notifyAll() {
    std::ranges::for_each(waiters, [](auto& f) { std::invoke(std::move(f)); });
    waiters.clear();
  }
  ~AsyncSignal() {
    notifyAll();
  }
};

}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H

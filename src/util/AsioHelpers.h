//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ASIOHELPERS_H
#define QLEVER_ASIOHELPERS_H

#include <absl/functional/any_invocable.h>

#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <concepts>
#include <future>
#include <list>
#include <ranges>

#include "util/Exception.h"
#include "util/GenericBaseClasses.h"
#include "util/Log.h"
#include "util/ResetWhenMoved.h"
#include "util/Synchronized.h"
#include "util/TransparentFunctors.h"

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
template <typename Executor, std::invocable F,
          typename Res = std::invoke_result_t<F>>
requires(!std::is_void_v<Res>)
net::awaitable<std::invoke_result_t<F>> runOnExecutor(Executor exec, F f) {
  auto run = [](auto f) -> net::awaitable<std::optional<Res>> {
    co_return std::invoke(f);
  };

  co_return std::move(
      (co_await net::co_spawn(exec, run(std::move(f)), net::use_awaitable))
          .value());
}

template <typename Executor, std::invocable F>
requires(std::is_void_v<std::invoke_result_t<F>>)
net::awaitable<void> runOnExecutor(Executor exec, F f) {
  auto run = [](auto f) -> net::awaitable<void> {
    std::invoke(f);
    co_return;
  };
  co_await net::co_spawn(exec, run(std::move(f)), net::use_awaitable);
}

struct AsyncMutex {
 private:
  net::any_io_executor executor_;
  std::mutex mutex_;
  std::list<absl::AnyInvocable<void()>> waiters_{};
  bool occupied_{false};

  friend class AsyncConditionVariable;

 public:
  [[nodiscard]] struct LockGuard : public NoCopyDefaultMove {
   private:
    AsyncMutex* mutex_;
    ad_utility::ResetWhenMoved<bool, false> active_ = true;

   public:
    [[nodiscard]] explicit LockGuard(AsyncMutex* mutex) : mutex_{mutex} {}
    ~LockGuard() {
      if (active_) {
        mutex_->unlock();
      }
    }
    LockGuard(LockGuard&&) = default;
    LockGuard& operator=(LockGuard&&) = default;
  };

 private:
  template <bool returnLockGuard>
  void asyncLockImpl(auto&& handler) {
    auto slot = net::get_associated_cancellation_slot(handler);
    auto log = [](auto) {
      LOG(INFO) << "Cancelled an async lock" << std::endl;
    };
    if (slot.is_connected()) {
      slot.template emplace<decltype(log)>(log);
    }
    auto executor = net::get_associated_executor(handler, executor_);
    std::unique_lock lock{mutex_};
    auto resumeAction = net::bind_executor(
        executor, [handler = std::move(handler), self = this]() mutable {
          if constexpr (returnLockGuard) {
            handler(LockGuard{self});
          } else {
            (void)self;
            handler();
          }
        });
    if (!occupied_) {
      occupied_ = true;
      lock.unlock();
      // TODDO<joka921> Several unit tests made by Robing fail when this is
      // changed to `dispatch`, we need to discuss, whether this behavior is
      // important.
      net::post(std::move(resumeAction));
    } else {
      auto resume = [resumeAction = std::move(resumeAction)]() mutable {
        net::post(std::move(resumeAction));
      };
      waiters_.push_back(std::move(resume));
    }
  }

 public:
  explicit AsyncMutex(net::any_io_executor executor)
      : executor_(std::move(executor)) {}

  template <typename CompletionToken>
  auto asyncLock(CompletionToken token) -> decltype(auto) {
    auto impl = [self = this](auto&& handler) {
      self->asyncLockImpl<false>(AD_FWD(handler));
    };
    return net::async_initiate<CompletionToken, void()>(std::move(impl), token);
  }

  template <typename CompletionToken>
  auto asyncLockGuard(CompletionToken token) -> decltype(auto) {
    auto impl = [self = this](auto&& handler) {
      self->asyncLockImpl<true>(AD_FWD(handler));
    };
    return net::async_initiate<CompletionToken, void(LockGuard)>(
        std::move(impl), token);
  }

  void unlock() {
    std::unique_lock l{mutex_};
    AD_CORRECTNESS_CHECK(occupied_);
    if (waiters_.empty()) {
      occupied_ = false;
      return;
    } else {
      auto f = std::move(waiters_.front());
      waiters_.pop_front();
      std::invoke(f);
      l.unlock();
    }
  }
};

class AsyncConditionVariable {
 public:
  struct State {
    static_assert(std::constructible_from<size_t>);
    std::list<absl::AnyInvocable<void()>> waiters_{};
    static_assert(std::constructible_from<
                  std::vector<std::pair<size_t, absl::AnyInvocable<void()>>>>);
    State() = default;
  };
  ad_utility::Synchronized<State> state_{State{}};

 private:
  void cancel(const auto& it) {
    auto x = state_.wlock();
    auto el = std::move(*it);
    // The order of these two calls seems to be important ALTHOUGH we are
    // holding on to the lock.
    // TODO<joka921> Figure this out.
    x->waiters_.erase(it);
    std::invoke(el);
  }

  template <typename CompletionHandler>
  void asyncWaitImpl(CompletionHandler&& handler, AsyncMutex& mutex) {
    auto slot = net::get_associated_cancellation_slot(handler);
    auto resume = [handler = AD_FWD(handler), &mutex]() mutable {
      mutex.asyncLockImpl<false>(AD_FWD(handler));
    };
    state_.withWriteLock([self = this, &resume, &slot](State& s) mutable {
      s.waiters_.emplace_back(std::move(resume));
      // TODO<joka921> Don't ignore the cancellationType.
      auto it = --s.waiters_.end();
      auto doCancel = [self, it](auto) { self->cancel(it); };
      if (slot.is_connected()) {
        slot.template emplace<decltype(doCancel)>(doCancel);
      }
    });
    mutex.unlock();
  }

 public:
  // AsyncConditionVariable() = default;
  AsyncConditionVariable() {
    LOG(INFO) << "Async condition variable at " << this << std::endl;
  }

  template <typename CompletionToken>
  auto asyncWait(AsyncMutex& mutex, CompletionToken token) -> decltype(auto) {
    auto impl = [self = this, &mutex](auto&& handler) {
      self->asyncWaitImpl(AD_FWD(handler), mutex);
    };
    return net::async_initiate<CompletionToken, void()>(std::move(impl), token);
  }

  void notifyAll() {
    state_.withWriteLock([](State& s) {
      std::ranges::for_each(s.waiters_,
                            [](auto& f) { std::invoke(std::move(f)); });
      s.waiters_.clear();
    });
  }
  ~AsyncConditionVariable() {
    LOG(INFO) << "Destroing async condition variable at " << this << std::endl;
    notifyAll();
  }
};

}  // namespace ad_utility

#endif  // QLEVER_ASIOHELPERS_H

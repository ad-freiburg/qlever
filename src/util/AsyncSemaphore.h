// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ASYNCSEMAPHORE_H
#define QLEVER_SRC_UTIL_ASYNCSEMAPHORE_H

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>
#include <cstddef>
#include <memory>
#include <utility>

#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"

namespace ad_utility {

namespace net = boost::asio;

// An asynchronous counting semaphore: a fixed number of permits, which can be
// taken out and returned, and where taking out a permit while none is free
// suspends the caller instead of blocking its thread.
//
// The permits are handed out as RAII handles (`AsyncSemaphore::Permit`), so
// that a permit cannot be leaked; the handle returns it in its destructor.
//
// USAGE: Either take out a permit explicitly via `asyncAcquire` and keep the
// resulting handle alive for as long as the permit is needed, or use the free
// function `asyncWithPermit` below, which scopes a permit to a single
// asynchronous operation.
//
// THREAD SAFETY: The permits are held by a `concurrent_channel`, which
// synchronizes itself internally, so every operation of this class may be
// initiated from any thread and any executor without further synchronization
// and without a hop onto a strand.
//
// NOTE: The state is held by a `shared_ptr`, so that a `Permit` (which returns
// itself in its destructor) may safely outlive the `AsyncSemaphore` object it
// was obtained from. A copy of an `AsyncSemaphore` therefore refers to the
// *same* set of permits; copy it only to share it, never to obtain a second
// semaphore.
class AsyncSemaphore {
 public:
  // The channel that holds the permits: it buffers one (empty) element per
  // permit that is currently free. NOTE: This is a *concurrent* channel, so all
  // of its operations may be used from any thread, see the THREAD SAFETY note
  // above.
  using PermitChannel =
      net::experimental::concurrent_channel<void(boost::system::error_code)>;

 private:
  // The shared state, see the NOTE in the class comment above.
  struct Impl {
    // The executor of the channel, which is also the fallback executor for a
    // completion handler that has none of its own.
    net::any_io_executor executor_;
    PermitChannel permits_;

    // NOTE: The declaration order of the two members matters, because the
    // channel is constructed from the `executor_`.
    Impl(net::any_io_executor executor, size_t numPermits)
        : executor_{std::move(executor)}, permits_{executor_, numPermits} {}
  };
  std::shared_ptr<Impl> impl_;

 public:
  // A single permit of an `AsyncSemaphore`, which is returned to that semaphore
  // when this handle is destroyed. A default-constructed (or moved-from) handle
  // is empty and holds no permit.
  //
  // NOTE: This is move-only, and it keeps the state of its semaphore alive, so
  // it may safely outlive the `AsyncSemaphore` object it was obtained from.
  class Permit {
   private:
    std::shared_ptr<Impl> impl_;

   public:
    // Construct an empty handle that holds no permit. This is the same state
    // that `release()` and a moved-from handle leave behind, and it lets a
    // caller declare a `Permit` before it owns one and fill it in later.
    Permit() = default;

    // Construct from the state of the semaphore that the permit belongs to. A
    // `nullptr` yields an empty handle, see `isValid`.
    explicit Permit(std::shared_ptr<Impl> impl) : impl_{std::move(impl)} {}

    // NOTE: Moving a `shared_ptr` already empties the source, so the defaulted
    // move constructor does the right thing. The move *assignment* in contrast
    // has to be written by hand, because it must return the permit that it
    // overwrites instead of just dropping it.
    Permit(Permit&&) noexcept = default;
    Permit& operator=(Permit&& other) noexcept {
      if (this != &other) {
        release();
        impl_ = std::move(other.impl_);
      }
      return *this;
    }
    Permit(const Permit&) = delete;
    Permit& operator=(const Permit&) = delete;

    ~Permit() { release(); }

    // Return `true` if this handle actually holds a permit.
    bool isValid() const noexcept { return impl_ != nullptr; }

    // Return the permit to its semaphore and make this handle empty. This is
    // also done by the destructor; call it explicitly to return the permit
    // early. Calling this on an empty handle does nothing.
    void release() noexcept {
      releasePermit(std::move(impl_));
      impl_ = nullptr;
    }
  };

  // Construct a semaphore with `numPermits` permits, all of which are initially
  // free. The channel of this semaphore runs on the `executor`, which somebody
  // else has to run.
  AsyncSemaphore(net::any_io_executor executor, size_t numPermits)
      : impl_{std::make_shared<Impl>(std::move(executor), numPermits)} {
    AD_CONTRACT_CHECK(numPermits > 0);
    // Hand out the initial permits. NOTE: This happens in the constructor,
    // which is always synchronous and which nothing else can run concurrently
    // with.
    for (size_t i = 0; i < numPermits; ++i) {
      bool permitWasSent =
          impl_->permits_.try_send(boost::system::error_code{});
      AD_CORRECTNESS_CHECK(permitWasSent);
    }
  }

  // Return the executor on which a completion handler of this semaphore runs if
  // it has no executor of its own. This is the executor that the semaphore was
  // constructed with.
  const net::any_io_executor& defaultHandlerExecutor() const noexcept {
    return impl_->executor_;
  }

  // Take out a permit, suspending until one is free. The completion signature
  // is `void(boost::system::error_code, Permit)`. On success the `error_code`
  // is falsy and the `Permit` is valid; if the wait was interrupted by
  // `cancel()` the `error_code` is `operation_aborted` and the `Permit` is
  // empty.
  //
  // NOTE: This may be initiated from any thread and any executor. The
  // completion handler runs on the executor that is associated with the
  // `completionToken`, or on `defaultHandlerExecutor()` if the token has none.
  template <typename CompletionToken>
  auto asyncAcquire(CompletionToken&& completionToken) {
    using Signature = void(boost::system::error_code, Permit);
    auto initiate = [impl = impl_](auto&& handler) mutable {
      // The executor on which the completion handler has to run, see the NOTE
      // above.
      auto handlerExecutor =
          net::get_associated_executor(handler, impl->executor_);
      // IMPORTANT: The channel has to be bound to a local reference *before*
      // the completion handler below is created, because that handler moves out
      // of `impl` and the order in which the arguments of a call are evaluated
      // is unspecified. The reference stays valid, because the handler keeps
      // the `Impl` itself alive.
      auto& permits = impl->permits_;
      permits.async_receive(
          [impl = std::move(impl), handler = AD_FWD(handler), handlerExecutor](
              const boost::system::error_code& errorCode) mutable {
            Permit permit{errorCode ? nullptr : std::move(impl)};
            net::post(net::bind_executor(
                handlerExecutor, [handler = std::move(handler), errorCode,
                                  permit = std::move(permit)]() mutable {
                  std::move(handler)(errorCode, std::move(permit));
                }));
          });
    };
    return net::async_initiate<CompletionToken, Signature>(std::move(initiate),
                                                           completionToken);
  }

  // Wake up everybody who currently waits for a permit; those waiters complete
  // with `operation_aborted` and an empty `Permit`. Callable from anywhere and
  // any number of times.
  //
  // NOTE: This is *not* sticky. The permits that are currently free (and those
  // that are returned later) are not discarded, so an `asyncAcquire` that is
  // initiated after this call may well succeed again. A user that has to stop
  // for good therefore needs a stop flag of its own and has to check it in the
  // completion handler of `asyncAcquire`.
  void cancel() noexcept {
    // NOTE: The channel is concurrent, so it can be cancelled right here from
    // any thread, without a hop onto any executor.
    ad_utility::terminateIfThrows([this] { impl_->permits_.cancel(); },
                                  "Cancelling an `AsyncSemaphore` failed.");
  }

 private:
  // Return the permit that is held by the `impl` (if any). The channel is
  // concurrent, so this sends the permit right away and never waits, which
  // makes it callable from anywhere, in particular from a destructor.
  static void releasePermit(std::shared_ptr<Impl> impl) noexcept {
    if (impl == nullptr) {
      return;
    }
    ad_utility::terminateIfThrows(
        [&impl] {
          bool permitWasSent =
              impl->permits_.try_send(boost::system::error_code{});
          // At most `numPermits` permits exist and the channel has exactly that
          // capacity, so there is always a free slot. NOTE: The channel is
          // never closed, and `cancel()` does not discard the buffered permits
          // either, so this cannot fail while a user of this semaphore is being
          // torn down either.
          AD_CORRECTNESS_CHECK(permitWasSent);
        },
        "Returning a permit of an `AsyncSemaphore` failed.");
  }
};

// Take out a permit of the `semaphore`, then run the asynchronous operation
// `work`, and return the permit as soon as that operation is complete. The
// completion signature is `void(boost::system::error_code)`: the `error_code`
// is falsy if the `work` was run, and `operation_aborted` if the wait for a
// permit was interrupted by `AsyncSemaphore::cancel()`, in which case the
// `work` is *not* run at all.
//
// `work` has to be an asynchronous operation in the Boost.Asio sense, i.e. a
// callable that takes a completion token and completes with the signature
// `void()`. It is initiated on the executor that is associated with the
// `completionToken` (or on `semaphore.defaultHandlerExecutor()` if the token
// has none), so a `work` that does actual computation has to schedule that
// computation onto an executor of its own.
//
// NOTE: Use this whenever a permit is scoped to exactly one asynchronous
// operation. A caller that has to continue as soon as the permit was *acquired*
// (and not when the work is done) has to use `AsyncSemaphore::asyncAcquire`
// directly and keep the `Permit` alive itself.
template <typename Work, typename CompletionToken>
auto asyncWithPermit(AsyncSemaphore semaphore, Work work,
                     CompletionToken&& completionToken) {
  using Signature = void(boost::system::error_code);
  auto initiate = [semaphore = std::move(semaphore),
                   work = std::move(work)](auto&& handler) mutable {
    auto handlerExecutor = net::get_associated_executor(
        handler, semaphore.defaultHandlerExecutor());
    semaphore.asyncAcquire(net::bind_executor(
        handlerExecutor, [work = std::move(work), handler = AD_FWD(handler)](
                             const boost::system::error_code& errorCode,
                             AsyncSemaphore::Permit permit) mutable {
          if (errorCode) {
            std::move(handler)(errorCode);
            return;
          }
          // NOTE: The `permit` is moved into the completion handler of the
          // `work` and is hence returned to the semaphore as soon as that
          // handler has run.
          std::move(work)([handler = std::move(handler),
                           permit = std::move(permit)]() mutable {
            permit.release();
            std::move(handler)(boost::system::error_code{});
          });
        }));
  };
  return net::async_initiate<CompletionToken, Signature>(std::move(initiate),
                                                         completionToken);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ASYNCSEMAPHORE_H

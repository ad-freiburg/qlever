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
#include <boost/asio/dispatch.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
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
// STRAND CONFINEMENT: All the state is confined to a strand that this class
// creates from the executor it is constructed with, so that a plain
// (non-concurrent) channel suffices and no mutex is required. Every operation
// therefore consists of a hop onto that strand, the actual work, and a hop back
// to the executor that is associated with the completion token of the caller.
// In particular all the operations may be initiated from any thread and any
// executor.
//
// NOTE: The state is held by a `shared_ptr`, so that the handlers which are
// posted onto the strand (in particular the one that returns a permit) keep it
// alive. A copy of an `AsyncSemaphore` therefore refers to the *same* set of
// permits; copy it only to share it, never to obtain a second semaphore.
class AsyncSemaphore {
 public:
  // The strand to which all the state of this semaphore is confined.
  using Strand = net::strand<net::any_io_executor>;
  // The channel that holds the permits: it buffers one (empty) element per
  // permit that is currently free. NOTE: A plain (non-concurrent) channel
  // suffices, because every operation on it is performed on the strand.
  using PermitChannel =
      net::experimental::channel<void(boost::system::error_code)>;

 private:
  // The shared state, see the NOTE in the class comment above.
  struct Impl {
    Strand strand_;
    PermitChannel permits_;

    Impl(net::any_io_executor executor, size_t numPermits)
        : strand_{net::make_strand(std::move(executor))},
          permits_{strand_, numPermits} {}
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
    // Construct an empty handle that holds no permit.
    Permit() = default;

    // Construct from the state of the semaphore that the permit belongs to. A
    // `nullptr` yields an empty handle, see `isValid`.
    explicit Permit(std::shared_ptr<Impl> impl) : impl_{std::move(impl)} {}

    Permit(Permit&& other) noexcept : impl_{std::move(other.impl_)} {
      other.impl_ = nullptr;
    }
    Permit& operator=(Permit&& other) noexcept {
      if (this != &other) {
        release();
        impl_ = std::move(other.impl_);
        other.impl_ = nullptr;
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
  // free. The strand of this semaphore is derived from the `executor`, which
  // somebody else has to run.
  AsyncSemaphore(net::any_io_executor executor, size_t numPermits)
      : impl_{std::make_shared<Impl>(std::move(executor), numPermits)} {
    AD_CONTRACT_CHECK(numPermits > 0);
    // Hand out the initial permits. NOTE: This happens in the constructor,
    // which is always synchronous and which nothing else can run concurrently
    // with, so no hop onto the strand is required here.
    for (size_t i = 0; i < numPermits; ++i) {
      bool permitWasSent =
          impl_->permits_.try_send(boost::system::error_code{});
      AD_CORRECTNESS_CHECK(permitWasSent);
    }
  }

  // Return the strand to which the state of this semaphore is confined, see the
  // STRAND CONFINEMENT note above. It is also the executor on which a
  // completion handler runs that has no executor of its own.
  const Strand& strand() const noexcept { return impl_->strand_; }

  // Take out a permit, suspending until one is free. The completion signature
  // is `void(boost::system::error_code, Permit)`. On success the `error_code`
  // is falsy and the `Permit` is valid; if the wait was interrupted by
  // `cancel()` the `error_code` is `operation_aborted` and the `Permit` is
  // empty.
  //
  // NOTE: This may be initiated from any thread and any executor. The
  // completion handler runs on the executor that is associated with the
  // `completionToken`, or on `strand()` if the token has none.
  template <typename CompletionToken>
  auto asyncAcquire(CompletionToken&& completionToken) {
    using Signature = void(boost::system::error_code, Permit);
    auto initiate = [impl = impl_](auto&& handler) mutable {
      // The executor on which the completion handler has to run, see the NOTE
      // above.
      auto handlerExecutor =
          net::get_associated_executor(handler, impl->strand_);
      // The channel may only be touched on the strand, so hop there first. Note
      // that the actual `async_receive` is what may suspend, and that its own
      // completion handler is again bound to the strand, because Boost.Asio
      // would otherwise run it on the channel's executor without dispatching it
      // through the strand.
      auto onStrand = [impl, handler = AD_FWD(handler),
                       handlerExecutor]() mutable {
        auto& permits = impl->permits_;
        permits.async_receive(net::bind_executor(
            impl->strand_,
            [impl = std::move(impl), handler = std::move(handler),
             handlerExecutor](
                const boost::system::error_code& errorCode) mutable {
              Permit permit{errorCode ? nullptr : std::move(impl)};
              net::post(net::bind_executor(
                  handlerExecutor, [handler = std::move(handler), errorCode,
                                    permit = std::move(permit)]() mutable {
                    std::move(handler)(errorCode, std::move(permit));
                  }));
            }));
      };
      net::dispatch(impl->strand_, std::move(onStrand));
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
    ad_utility::terminateIfThrows(
        [this] {
          net::post(impl_->strand_,
                    [impl = impl_] { impl->permits_.cancel(); });
        },
        "Cancelling an `AsyncSemaphore` failed.");
  }

 private:
  // Return the permit that is held by the `impl` (if any), by posting the
  // corresponding send onto its strand. This never waits for that send to
  // actually happen, so it may be called from anywhere, in particular from a
  // destructor.
  static void releasePermit(std::shared_ptr<Impl> impl) noexcept {
    if (impl == nullptr) {
      return;
    }
    ad_utility::terminateIfThrows(
        [&impl] {
          auto& strand = impl->strand_;
          net::post(strand, [impl = std::move(impl)] {
            bool permitWasSent =
                impl->permits_.try_send(boost::system::error_code{});
            // At most `numPermits` permits exist and the channel has exactly
            // that capacity, so there is always a free slot. NOTE: The channel
            // is never closed, and `cancel()` does not discard the buffered
            // permits either, so this cannot fail while a user of this
            // semaphore is being torn down either.
            AD_CORRECTNESS_CHECK(permitWasSent);
          });
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
// `completionToken` (or on `semaphore.strand()` if the token has none), so a
// `work` that does actual computation has to schedule that computation onto an
// executor of its own.
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
    auto handlerExecutor =
        net::get_associated_executor(handler, semaphore.strand());
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

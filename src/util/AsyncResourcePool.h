// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ASYNCRESOURCEPOOL_H
#define QLEVER_SRC_UTIL_ASYNCRESOURCEPOOL_H

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>
#include <cstddef>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"

namespace ad_utility {

namespace net = boost::asio;

// An asynchronous pool of a fixed number of resources of type `ResourceType`,
// which can be taken out and returned, and where taking out a resource while
// none is free suspends the caller instead of blocking its thread.
//
// The resources are handed out as RAII handles (`AsyncResourcePool::Handle`),
// so that a resource cannot be leaked; the handle returns it in its destructor.
// A handle gives access to its resource via `Handle::get()`, so that the
// resource may be used (and modified) for as long as the handle is held.
//
// The `ResourceType` may be
//   * `void`, in which case the resources are empty and the pool degenerates
//     into a plain asynchronous counting semaphore (there is no `Handle::get`),
//   * a `const T`, in which case a mutable `T` is stored internally, but
//     `Handle::get()` only hands out a `const T&`, or
//   * any other movable type.
//
// USAGE: Either take out a resource explicitly via `asyncAcquire` and keep the
// resulting handle alive for as long as the resource is needed, or use the free
// function `asyncWithResource` below, which scopes a resource to a single
// asynchronous operation.
//
// THREAD SAFETY: The resources are held by a `concurrent_channel`, which
// synchronizes itself internally, so every operation of this class may be
// initiated from any thread and any executor without further synchronization
// and without a hop onto a strand.
//
// NOTE: The state is held by a `shared_ptr`, so that a `Handle` (which returns
// its resource in its destructor) may safely outlive the `AsyncResourcePool`
// object it was obtained from. A copy of an `AsyncResourcePool` therefore
// refers to the *same* set of resources; copy it only to share it, never to
// obtain a second pool.
template <typename ResourceType>
class AsyncResourcePool {
 public:
  // `true` if this pool actually manages resources, and `false` for the
  // degenerate case `AsyncResourcePool<void>`, see the class comment above.
  static constexpr bool hasResources = !std::is_void_v<ResourceType>;

 private:
  // The type that is stored internally for a pool of `T`. The `const` is
  // stripped (only `Handle::get` reinstates it, see the class comment above),
  // and a `void` pool stores an empty `std::monostate`, so that there is
  // exactly one implementation for all cases.
  //
  // NOTE: This is an alias *template* (and not a plain alias), because the
  // constraints of the constructors below have to depend on a template
  // parameter of their own for the C++17 backport of `CPP_template_2`.
  template <typename T>
  using StoredTypeOf = std::conditional_t<std::is_void_v<T>, std::monostate,
                                          std::remove_const_t<T>>;
  using StoredType = StoredTypeOf<ResourceType>;

  // The type from which a single resource is constructed when the pool is set
  // up from a range. The elements of an rvalue range are moved out of it, the
  // elements of an lvalue range are copied.
  template <typename Range>
  using RangeElement = std::conditional_t<
      std::is_rvalue_reference_v<Range&&>,
      std::remove_reference_t<ql::ranges::range_reference_t<Range>>&&,
      ql::ranges::range_reference_t<Range>>;

  // The channel that holds the resources: it buffers one element per resource
  // that is currently free. The element is a `std::optional`, because a
  // cancelled `async_receive` completes with a value-initialized element, and
  // `StoredType` itself need not be default-constructible.
  //
  // NOTE: This is a *concurrent* channel, so all of its operations may be used
  // from any thread, see the THREAD SAFETY note above.
  using ResourceChannel = net::experimental::concurrent_channel<void(
      boost::system::error_code, std::optional<StoredType>)>;

  // The shared state, see the NOTE in the class comment above.
  struct Impl {
    // The executor of the channel, which is also the fallback executor for a
    // completion handler that has none of its own.
    net::any_io_executor executor_;
    ResourceChannel resources_;

    // NOTE: The declaration order of the two members matters, because the
    // channel is constructed from the `executor_`.
    Impl(net::any_io_executor executor, size_t numResources)
        : executor_{std::move(executor)}, resources_{executor_, numResources} {}
  };
  std::shared_ptr<Impl> impl_;

  // A tag for the private constructor that all the public constructors
  // delegate to, see below.
  struct FromResourcesTag {};

 public:
  // A single resource of an `AsyncResourcePool`, which is returned to that pool
  // when this handle is destroyed. A default-constructed (or moved-from) handle
  // is empty and holds no resource.
  //
  // NOTE: This is move-only, and it keeps the state of its pool alive, so it
  // may safely outlive the `AsyncResourcePool` object it was obtained from.
  class Handle {
   private:
    std::shared_ptr<Impl> impl_;
    // The resource that this handle currently holds. It has a value exactly if
    // `impl_ != nullptr`, see `isValid`.
    std::optional<StoredType> resource_;

   public:
    // Construct an empty handle that holds no resource. This is the same state
    // that `release()` and a moved-from handle leave behind, and it lets a
    // caller declare a `Handle` before it owns one and fill it in later.
    Handle() = default;

    // Construct from the state of the pool that the resource belongs to, and
    // from the resource itself. A `nullptr` yields an empty handle, in which
    // case the `resource` has to be empty as well, see `isValid`.
    Handle(std::shared_ptr<Impl> impl, std::optional<StoredType> resource)
        : impl_{std::move(impl)}, resource_{std::move(resource)} {
      AD_CORRECTNESS_CHECK((impl_ != nullptr) == resource_.has_value());
    }

    // NOTE: Both of these have to be written by hand. Moving a `std::optional`
    // leaves the source engaged (with a moved-from value inside), so the source
    // has to be emptied explicitly to restore the invariant of `resource_`. The
    // move *assignment* additionally has to return the resource that it
    // overwrites instead of just dropping it.
    Handle(Handle&& other) noexcept
        : impl_{std::move(other.impl_)}, resource_{std::move(other.resource_)} {
      other.resource_.reset();
    }
    Handle& operator=(Handle&& other) noexcept {
      if (this != &other) {
        release();
        impl_ = std::move(other.impl_);
        resource_ = std::move(other.resource_);
        other.resource_.reset();
      }
      return *this;
    }
    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;

    ~Handle() { release(); }

    // Return `true` if this handle actually holds a resource.
    bool isValid() const noexcept { return impl_ != nullptr; }

    // Return a reference to the resource that this handle holds. The reference
    // is only valid for as long as this handle holds the resource, so in
    // particular it is invalidated by `release()` and by moving the handle.
    //
    // NOTE: This does not exist for an `AsyncResourcePool<void>`, and for an
    // `AsyncResourcePool<const T>` it hands out a `const T&`.
    CPP_template_2(typename T = ResourceType)(
        requires(!std::is_void_v<T>)) std::add_lvalue_reference_t<T> get() {
      AD_CONTRACT_CHECK(isValid());
      return resource_.value();
    }

    // Return the resource to its pool and make this handle empty. This is also
    // done by the destructor; call it explicitly to return the resource early.
    // Calling this on an empty handle does nothing.
    void release() noexcept {
      releaseResource(std::move(impl_), std::move(resource_));
      impl_ = nullptr;
      resource_.reset();
    }
  };

  // Construct a pool of `numResources` value-initialized resources. The channel
  // of this pool runs on the `executor`, which somebody else has to run.
  //
  // NOTE: This is the only constructor of an `AsyncResourcePool<void>`, where
  // `numResources` is simply the number of permits of the semaphore.
  CPP_template_2(typename T = ResourceType)(
      requires ql::concepts::default_initializable<StoredTypeOf<T>>)
      AsyncResourcePool(net::any_io_executor executor, size_t numResources)
      : AsyncResourcePool{FromResourcesTag{}, std::move(executor),
                          std::vector<StoredType>(numResources)} {}

  // Construct a pool that manages exactly the elements of the `range`, one
  // resource per element. The elements are moved out of the `range` if it is an
  // rvalue range.
  CPP_template_2(typename Range)(
      requires ql::ranges::input_range<Range> CPP_and_2 std::is_constructible_v<
          StoredTypeOf<ResourceType>, RangeElement<Range>>)
      AsyncResourcePool(net::any_io_executor executor, Range&& range)
      : AsyncResourcePool{FromResourcesTag{}, std::move(executor),
                          toVector(AD_FWD(range))} {}

  // Construct a pool of `numResources` resources, each of which is a copy of
  // the `prototype`.
  CPP_template_2(typename T = ResourceType)(
      requires(!std::is_void_v<T>)
          CPP_and_2 ql::concepts::copy_constructible<StoredTypeOf<T>>)
      AsyncResourcePool(net::any_io_executor executor, size_t numResources,
                        const StoredTypeOf<T>& prototype)
      : AsyncResourcePool{FromResourcesTag{}, std::move(executor),
                          std::vector<StoredType>(numResources, prototype)} {}

  // Return the executor on which a completion handler of this pool runs if it
  // has no executor of its own. This is the executor that the pool was
  // constructed with.
  const net::any_io_executor& defaultHandlerExecutor() const noexcept {
    return impl_->executor_;
  }

  // Take out a resource, suspending until one is free. The completion signature
  // is `void(boost::system::error_code, Handle)`. On success the `error_code`
  // is falsy and the `Handle` is valid; if the wait was interrupted by
  // `cancel()` the `error_code` is `operation_aborted` and the `Handle` is
  // empty.
  //
  // NOTE: This may be initiated from any thread and any executor. The
  // completion handler runs on the executor that is associated with the
  // `completionToken`, or on `defaultHandlerExecutor()` if the token has none.
  template <typename CompletionToken>
  auto asyncAcquire(CompletionToken&& completionToken) {
    using Signature = void(boost::system::error_code, Handle);
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
      auto& resources = impl->resources_;
      resources.async_receive(
          [impl = std::move(impl), handler = AD_FWD(handler), handlerExecutor](
              const boost::system::error_code& errorCode,
              std::optional<StoredType> resource) mutable {
            Handle handle;
            if (!errorCode) {
              AD_CORRECTNESS_CHECK(resource.has_value());
              handle = Handle{std::move(impl), std::move(resource)};
            }
            net::post(net::bind_executor(
                handlerExecutor, [handler = std::move(handler), errorCode,
                                  handle = std::move(handle)]() mutable {
                  std::move(handler)(errorCode, std::move(handle));
                }));
          });
    };
    return net::async_initiate<CompletionToken, Signature>(std::move(initiate),
                                                           completionToken);
  }

  // Wake up everybody who currently waits for a resource; those waiters
  // complete with `operation_aborted` and an empty `Handle`. Callable from
  // anywhere and any number of times.
  //
  // NOTE: This is *not* sticky. The resources that are currently free (and
  // those that are returned later) are not discarded, so an `asyncAcquire` that
  // is initiated after this call may well succeed again. A user that has to
  // stop for good therefore needs a stop flag of its own and has to check it in
  // the completion handler of `asyncAcquire`.
  void cancel() noexcept {
    // NOTE: The channel is concurrent, so it can be cancelled right here from
    // any thread, without a hop onto any executor.
    ad_utility::terminateIfThrows([this] { impl_->resources_.cancel(); },
                                  "Cancelling an `AsyncResourcePool` failed.");
  }

 private:
  // The constructor that all the public constructors above delegate to: it
  // takes the resources that this pool manages, one per element.
  AsyncResourcePool(FromResourcesTag, net::any_io_executor executor,
                    std::vector<StoredType> resources)
      : impl_{std::make_shared<Impl>(std::move(executor), resources.size())} {
    size_t numResources = resources.size();
    AD_CONTRACT_CHECK(numResources > 0);
    // Hand out the initial resources. NOTE: This happens in the constructor,
    // which is always synchronous and which nothing else can run concurrently
    // with.
    for (auto& resource : resources) {
      bool resourceWasSent = impl_->resources_.try_send(
          boost::system::error_code{},
          std::optional<StoredType>{std::move(resource)});
      AD_CORRECTNESS_CHECK(resourceWasSent);
    }
  }

  // Collect the elements of the `range` into a vector of the stored resources,
  // moving them out of the `range` if it is an rvalue range, see
  // `RangeElement`.
  template <typename Range>
  static std::vector<StoredType> toVector(Range&& range) {
    std::vector<StoredType> result;
    for (auto&& element : range) {
      if constexpr (std::is_rvalue_reference_v<Range&&>) {
        result.emplace_back(std::move(element));
      } else {
        result.emplace_back(element);
      }
    }
    return result;
  }

  // Return the `resource` that is held by the `impl` (if any). The channel is
  // concurrent, so this sends the resource right away and never waits, which
  // makes it callable from anywhere, in particular from a destructor.
  static void releaseResource(std::shared_ptr<Impl> impl,
                              std::optional<StoredType> resource) noexcept {
    if (impl == nullptr) {
      return;
    }
    ad_utility::terminateIfThrows(
        [&impl, &resource] {
          AD_CORRECTNESS_CHECK(resource.has_value());
          bool resourceWasSent = impl->resources_.try_send(
              boost::system::error_code{}, std::move(resource));
          // At most `numResources` resources exist and the channel has exactly
          // that capacity, so there is always a free slot. NOTE: The channel is
          // never closed, and `cancel()` does not discard the buffered
          // resources either, so this cannot fail while a user of this pool is
          // being torn down either.
          AD_CORRECTNESS_CHECK(resourceWasSent);
        },
        "Returning a resource of an `AsyncResourcePool` failed.");
  }
};

// Take out a resource of the `pool`, then run the asynchronous operation
// `work` on it, and return the resource as soon as that operation is complete.
// The completion signature is `void(boost::system::error_code)`: the
// `error_code` is falsy if the `work` was run, and `operation_aborted` if the
// wait for a resource was interrupted by `AsyncResourcePool::cancel()`, in
// which case the `work` is *not* run at all.
//
// `work` has to be an asynchronous operation in the Boost.Asio sense, i.e. a
// callable that completes with the signature `void()`. It is called as
// `work(resource, completionToken)`, where `resource` is a reference to the
// acquired resource that stays valid until the `work` is complete; for an
// `AsyncResourcePool<void>` there is no resource and it is called as
// `work(completionToken)`. The `work` is initiated on the executor that is
// associated with the `completionToken` (or on `pool.defaultHandlerExecutor()`
// if the token has none), so a `work` that does actual computation has to
// schedule that computation onto an executor of its own.
//
// NOTE: Use this whenever a resource is scoped to exactly one asynchronous
// operation. A caller that has to continue as soon as the resource was
// *acquired* (and not when the work is done) has to use
// `AsyncResourcePool::asyncAcquire` directly and keep the `Handle` alive
// itself.
template <typename ResourceType, typename Work, typename CompletionToken>
auto asyncWithResource(AsyncResourcePool<ResourceType> pool, Work work,
                       CompletionToken&& completionToken) {
  using Pool = AsyncResourcePool<ResourceType>;
  using Signature = void(boost::system::error_code);
  auto initiate = [pool = std::move(pool),
                   work = std::move(work)](auto&& handler) mutable {
    auto handlerExecutor =
        net::get_associated_executor(handler, pool.defaultHandlerExecutor());
    pool.asyncAcquire(net::bind_executor(
        handlerExecutor, [work = std::move(work), handler = AD_FWD(handler)](
                             const boost::system::error_code& errorCode,
                             typename Pool::Handle handle) mutable {
          if (errorCode) {
            std::move(handler)(errorCode);
            return;
          }
          // NOTE: The `handle` is moved into the completion handler of the
          // `work` and the resource is hence returned to the pool as soon as
          // that handler has run.
          if constexpr (Pool::hasResources) {
            // NOTE: The handle has to live at a stable address, because the
            // reference to its resource is handed to the `work`, while the
            // handle itself is moved into the completion handler of that
            // `work`.
            auto handlePtr =
                std::make_unique<typename Pool::Handle>(std::move(handle));
            auto& resource = handlePtr->get();
            std::move(work)(resource, [handlePtr = std::move(handlePtr),
                                       handler = std::move(handler)]() mutable {
              handlePtr->release();
              std::move(handler)(boost::system::error_code{});
            });
          } else {
            std::move(work)([handle = std::move(handle),
                             handler = std::move(handler)]() mutable {
              handle.release();
              std::move(handler)(boost::system::error_code{});
            });
          }
        }));
  };
  return net::async_initiate<CompletionToken, Signature>(std::move(initiate),
                                                         completionToken);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ASYNCRESOURCEPOOL_H

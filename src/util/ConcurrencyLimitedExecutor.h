// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_CONCURRENCYLIMITEDEXECUTOR_H
#define QLEVER_SRC_UTIL_CONCURRENCYLIMITEDEXECUTOR_H

#include <cstddef>
#include <utility>

#include "backports/asio.h"
#include "util/Exception.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/execution.hpp>
#include <boost/asio/prefer.hpp>
#include <boost/asio/query.hpp>
#include <boost/asio/require.hpp>
#include <boost/system/error_code.hpp>
#include <concepts>
#include <functional>
#include <type_traits>

#include "util/AsyncResourcePool.h"
#include "util/Forward.h"
#endif

namespace ad_utility {

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
namespace detail {
// The properties that `ql::any_io_executor` supports for the respective
// operation.
template <typename Property>
concept QueryableProperty =
    boost::asio::can_query_v<const ql::any_io_executor&, Property>;
template <typename Property>
concept RequirableProperty =
    boost::asio::can_require_v<const ql::any_io_executor&, Property>;
template <typename Property>
concept PreferableProperty =
    boost::asio::can_prefer_v<const ql::any_io_executor&, Property>;

// The requirements of Boost.Asio on a function object that is passed to an
// executor (see `boost::asio::execution::is_executor_of`). In particular, it is
// invoked as an lvalue.
template <typename Function>
concept ExecutableFunction =
    std::invocable<std::decay_t<Function>&> &&
    std::constructible_from<std::decay_t<Function>, Function> &&
    std::move_constructible<std::decay_t<Function>>;
}  // namespace detail

// An executor that runs its function objects on an underlying executor, but
// never more than `maxConcurrency` of them at the same time. The others wait
// (in FIFO order) for a running one to complete. Copies share the same limit.
//
// NOTE: Function objects are never run inline. If one throws, the exception
// propagates out of the underlying executor, but its slot is still released.
//
// This needs `AsyncResourcePool` and thus Boost >= 1.80, so it doesn't exist in
// the reduced C++17 build, see `makeConcurrencyLimitedExecutor` below.
class ConcurrencyLimitedExecutor {
  using Semaphore = AsyncResourcePool<void>;

  ql::any_io_executor executor_;
  // `mutable`, because `execute` has to be `const`.
  mutable Semaphore semaphore_;

  ConcurrencyLimitedExecutor(ql::any_io_executor executor, Semaphore semaphore)
      : executor_{std::move(executor)}, semaphore_{std::move(semaphore)} {}

  static ql::any_io_executor checkNotEmpty(ql::any_io_executor executor) {
    AD_CONTRACT_CHECK(static_cast<bool>(executor));
    return executor;
  }

 public:
  // The `executor` must not be empty and `maxConcurrency` has to be at least
  // one.
  ConcurrencyLimitedExecutor(ql::any_io_executor executor,
                             size_t maxConcurrency)
      : executor_{checkNotEmpty(std::move(executor))},
        semaphore_{executor_, maxConcurrency} {}

  bool operator==(const ConcurrencyLimitedExecutor&) const = default;

  // All properties are forwarded to the underlying executor.
  template <detail::QueryableProperty Property>
  decltype(auto) query(const Property& property) const {
    return boost::asio::query(executor_, property);
  }

  template <detail::RequirableProperty Property>
  ConcurrencyLimitedExecutor require(const Property& property) const {
    return {boost::asio::require(executor_, property), semaphore_};
  }

  template <detail::PreferableProperty Property>
  ConcurrencyLimitedExecutor prefer(const Property& property) const {
    return {boost::asio::prefer(executor_, property), semaphore_};
  }

  // Run `function` on the underlying executor as soon as a slot is free.
  template <detail::ExecutableFunction Function>
  void execute(Function&& function) const {
    semaphore_.asyncAcquire(boost::asio::bind_executor(
        executor_, [function = AD_FWD(function)](
                       boost::system::error_code errorCode,
                       [[maybe_unused]] Semaphore::Handle permit) mutable {
          // The semaphore is never cancelled.
          AD_CORRECTNESS_CHECK(!errorCode);
          // The `permit` is released after this returns (or throws).
          std::invoke(function);
        }));
  }
};
#endif

// Return an executor that runs at most `maxConcurrency` function objects on
// `executor` at the same time. In the reduced C++17 build the `executor` is
// returned unchanged, as the workloads there are controlled.
inline ql::any_io_executor makeConcurrencyLimitedExecutor(
    ql::any_io_executor executor, size_t maxConcurrency) {
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  return ConcurrencyLimitedExecutor{std::move(executor), maxConcurrency};
#else
  AD_CONTRACT_CHECK(static_cast<bool>(executor));
  AD_CONTRACT_CHECK(maxConcurrency > 0);
  return executor;
#endif
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONCURRENCYLIMITEDEXECUTOR_H

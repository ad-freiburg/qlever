// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_HPXASIOEXECUTOR_H
#define QLEVER_SRC_UTIL_HPXASIOEXECUTOR_H

#ifndef QLEVER_USE_HPX
#error \
    "`HpxAsioExecutor.h` requires the `USE_HPX` CMake option, include it only from code that is guarded by `#ifdef QLEVER_USE_HPX`"
#endif

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <cstddef>
#include <exception>
#include <hpx/algorithm.hpp>
#include <hpx/execution.hpp>
#include <hpx/execution_base/execution.hpp>
#include <hpx/execution_base/traits/is_executor.hpp>
#include <hpx/functional/deferred_call.hpp>
#include <hpx/modules/futures.hpp>
#include <hpx/timing/steady_clock.hpp>
#include <memory>
#include <type_traits>
#include <utility>

#include "util/Exception.h"

// One of HPX's headers includes the system header `<sys/param.h>`, which
// defines the function-like macros `MIN` and `MAX`. Those break any header that
// is included afterwards and uses those names for something else (the
// ANTLR-generated SPARQL parser for example declares functions `MIN()` and
// `MAX()`), so we undefine them right here.
//
// This is also the reason why this header is the only place in QLever that
// includes HPX's headers (it deliberately includes more of them than it needs
// itself, in particular the parallel algorithms): everything else that uses
// HPX includes this header instead and is then macro-safe.
#undef MIN
#undef MAX

// This module makes HPX's parallel algorithms (for example `hpx::sort`) run
// their work on a `boost::asio` executor, typically on QLever's global thread
// pool (see `util/GlobalExecutor.h`). It consists of two parts:
//
// 1. The `HpxAsioExecutor` below, which models HPX's executor concept and
//    forwards HPX's execution customization points to an
//    `boost::asio::any_io_executor`. An algorithm that is invoked with an
//    execution policy that uses this executor (for example
//    `hpx::execution::par.on(executor)`) runs *all* the work that it schedules
//    on the `asio` executor, and none of it on a thread of HPX's own runtime.
//
// 2. `ensureHpxRuntimeIsRunning()`, which starts a minimal HPX runtime. This is
//    needed because the parallel algorithms do not only schedule work, they
//    also *combine* the results of that work via `hpx::future` and
//    `hpx::dataflow`, and that machinery needs a running HPX runtime to attach
//    the continuations to (see the documentation of the function for details).
//
// So HPX's own runtime does exist next to the `asio` thread pool; it is however
// configured with a single worker thread, which only runs the (tiny)
// continuations that join the results of the work, never the work itself.
namespace ad_utility {

namespace detail {

// A task that is posted to the `asio` executor by `HpxAsioExecutor`. It runs
// the nullary `function_` and transports its result (or the exception it has
// thrown) into the `promise_`, the future of which is what
// `HpxAsioExecutor`'s `async_execute` returns to HPX.
template <typename Function>
struct HpxAsioTask {
  using Result = std::invoke_result_t<Function&>;

  Function function_;
  hpx::promise<Result> promise_;

  explicit HpxAsioTask(Function function) : function_{std::move(function)} {}

  // Run the function and fulfill the promise. Must be called exactly once.
  void run() {
    try {
      if constexpr (std::is_void_v<Result>) {
        function_();
        promise_.set_value();
      } else {
        promise_.set_value(function_());
      }
    } catch (...) {
      promise_.set_exception(std::current_exception());
    }
  }
};

}  // namespace detail

// An HPX executor that runs all the work that HPX schedules on it on a
// `boost::asio` executor, see the comment at the top of this file. The number
// of threads that the `asio` executor has cannot be queried from the executor
// itself, so it has to be passed in separately; HPX uses it to decide into how
// many chunks it splits the work.
//
// NOTE: An algorithm that is run via this executor blocks the thread it is
// called from until the algorithm is complete (unless it is run with one of
// HPX's `...(task)` policies). It must therefore not be called from a thread of
// the very `asio` executor that it runs its work on, because the work would
// then be queued behind the blocked thread; if all the threads of the executor
// did that at the same time, they would deadlock.
class HpxAsioExecutor {
 public:
  // The work that is scheduled via this executor may run concurrently and in
  // any order (HPX uses this tag to select the parallel implementations of its
  // algorithms).
  using execution_category = hpx::execution::parallel_execution_tag;
  // The default strategy by which HPX splits the work into chunks. It is only
  // a default, an algorithm can always be invoked with different parameters.
  using executor_parameters_type =
      hpx::execution::experimental::static_chunk_size;

 private:
  boost::asio::any_io_executor executor_;
  size_t numThreads_;

 public:
  // Construct from the `executor` to run the work on, and the number of
  // threads that this executor has.
  HpxAsioExecutor(boost::asio::any_io_executor executor, size_t numThreads)
      : executor_{std::move(executor)}, numThreads_{numThreads} {
    AD_CONTRACT_CHECK(numThreads_ > 0);
  }

  // Two executors are equal if they schedule their work on the same `asio`
  // executor. This is part of HPX's executor concept.
  bool operator==(const HpxAsioExecutor& rhs) const noexcept {
    return executor_ == rhs.executor_;
  }
  bool operator!=(const HpxAsioExecutor& rhs) const noexcept {
    return !(*this == rhs);
  }

  // The customization point for "fire and forget" work (HPX's
  // `NeverBlockingOneWayExecutor` concept).
  template <typename Function, typename... Args>
  friend void tag_invoke(hpx::parallel::execution::post_t,
                         const HpxAsioExecutor& executor, Function&& function,
                         Args&&... args) {
    // NOTE: The copy of the `asio` executor has to be made *before* the
    // arguments are moved into the task below, see the comment in
    // `async_execute` for the explanation.
    auto asioExecutor = executor.executor_;
    boost::asio::post(asioExecutor,
                      [call = hpx::util::deferred_call(
                           std::forward<Function>(function),
                           std::forward<Args>(args)...)]() mutable { call(); });
  }

  // The customization point for work the result of which is awaited via an
  // `hpx::future` (HPX's `TwoWayExecutor` concept). This is the one that HPX's
  // parallel algorithms use, and hence the one that does the actual work of
  // this class. Note that HPX synthesizes the remaining customization points
  // (`sync_execute`, `then_execute`, `bulk_async_execute`, ...) from this one
  // and from `post` above.
  template <typename Function, typename... Args>
  friend auto tag_invoke(hpx::parallel::execution::async_execute_t,
                         const HpxAsioExecutor& executor, Function&& function,
                         Args&&... args) {
    // NOTE: The copy of the `asio` executor has to be made *before* the
    // arguments are moved into the task below. The reason is that HPX's
    // algorithms call this function as `async_execute(policy.executor(), f,
    // std::move(policy), ...)`, so `executor` may well be a reference *into*
    // one of the `args`; moving that argument then leaves `executor` as a
    // moved-from (and therefore empty) `any_io_executor`, and posting to it
    // throws `boost::asio::bad_executor`.
    auto asioExecutor = executor.executor_;
    auto call = hpx::util::deferred_call(std::forward<Function>(function),
                                         std::forward<Args>(args)...);
    auto task =
        std::make_shared<detail::HpxAsioTask<decltype(call)>>(std::move(call));
    auto future = task->promise_.get_future();
    boost::asio::post(asioExecutor,
                      [task = std::move(task)]() { task->run(); });
    return future;
  }

  // Report the number of threads that the underlying `asio` executor has. HPX
  // uses this to determine the number of chunks that it splits the work of an
  // algorithm into.
  template <typename Parameters>
  friend constexpr size_t tag_invoke(
      hpx::execution::experimental::processing_units_count_t, Parameters&&,
      const HpxAsioExecutor& executor,
      const hpx::chrono::steady_duration& = hpx::chrono::null_duration,
      size_t = 0) {
    return executor.numThreads_;
  }
};

// Start the HPX runtime, unless it is already running. Thread-safe and
// idempotent.
//
// The HPX runtime is needed even though all the actual work runs on the `asio`
// executor: HPX's parallel algorithms combine the results of the work they
// schedule via `hpx::future` and `hpx::dataflow`, and both the unwrapping of a
// nested `hpx::future` and the continuation of a `dataflow` are scheduled as
// HPX threads, which only exist while an HPX runtime is up. Calling for
// example `hpx::sort` without a running runtime therefore aborts the process
// (with `terminate called without an active exception`).
//
// The runtime that is started here is as small as possible: it is the *local*
// (single process, no networking) runtime with a single worker thread, it does
// not run an entry point function of its own, and it is told not to install
// signal handlers (which it does by default, for `SIGINT`, `SIGSEGV`,
// `SIGPIPE` and others) and not to replace the `std::new_handler`, so that it
// does not change the behavior of the rest of QLever.
//
// NOTE: The runtime is deliberately never stopped again; it is leaked at the
// end of the process, exactly like the thread pool of `globalExecutor()`.
// Stopping it would require a synchronization point at which no HPX future is
// alive anymore, which QLever has no way of establishing, and HPX supports
// this usage: its non-blocking start explicitly releases the ownership of the
// runtime object.
void ensureHpxRuntimeIsRunning();

// Return an `HpxAsioExecutor` for the global thread pool (see
// `util/GlobalExecutor.h`), and make sure that the HPX runtime is running.
// This is the executor that QLever's uses of HPX algorithms should use.
HpxAsioExecutor globalHpxExecutor();

}  // namespace ad_utility

namespace hpx::execution::experimental {

// Tell HPX which of its executor concepts `ad_utility::HpxAsioExecutor`
// models. These traits are not deduced from the `tag_invoke` overloads above,
// they have to be declared explicitly.
template <>
struct is_one_way_executor<ad_utility::HpxAsioExecutor> : std::true_type {};

template <>
struct is_never_blocking_one_way_executor<ad_utility::HpxAsioExecutor>
    : std::true_type {};

template <>
struct is_two_way_executor<ad_utility::HpxAsioExecutor> : std::true_type {};

}  // namespace hpx::execution::experimental

#endif  // QLEVER_SRC_UTIL_HPXASIOEXECUTOR_H

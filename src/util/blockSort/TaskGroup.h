// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_TASKGROUP_H
#define QLEVER_SRC_UTIL_BLOCKSORT_TASKGROUP_H

// The fork/join primitive of the block indirect sort. C++20 only.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <atomic>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <utility>

#include "backports/asio.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"

namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// Collects the first exception of any task of a sort; the caller of the sort
// rethrows it at the end. Tasks don't propagate exceptions to their parent
// (which would unwind while its other children still run), and tasks check
// `hasError()` to stop early. Unlike the `bool error` of Boost, which only
// covers `std::bad_alloc`, this also reports exceptions of the comparator.
class ErrorSink {
 private:
  // Lock-free fast path for `hasError()`.
  std::atomic<bool> hasError_{false};
  std::mutex mutex_;
  std::exception_ptr error_;

 public:
  bool hasError() const noexcept {
    return hasError_.load(std::memory_order_acquire);
  }

  // Store `error` unless an exception has already been stored.
  void store(std::exception_ptr error) noexcept {
    if (error == nullptr) {
      return;
    }
    std::lock_guard<std::mutex> lock{mutex_};
    if (error_ == nullptr) {
      error_ = std::move(error);
      hasError_.store(true, std::memory_order_release);
    }
  }

  // Only to be called once the whole sort has finished.
  void rethrowIfError() {
    if (error_ != nullptr) {
      std::rethrow_exception(error_);
    }
  }
};

// A move-only type-erased `void()` completion handler. This is
// `boost::asio::any_completion_handler<void()>`, which needs Boost >= 1.82.
class AnyNullaryHandler {
 private:
  struct Base {
    virtual void invoke() = 0;
    virtual ~Base() = default;
  };
  template <typename Handler>
  struct Holder : Base {
    Handler handler_;
    explicit Holder(Handler handler) : handler_{std::move(handler)} {}
    void invoke() override { std::move(handler_)(); }
  };
  std::unique_ptr<Base> handler_;

 public:
  AnyNullaryHandler() = default;
  template <typename Handler>
  explicit AnyNullaryHandler(Handler handler)
      : handler_{std::make_unique<Holder<Handler>>(std::move(handler))} {}

  // Invoke and destroy the handler. It is moved out first, because running it
  // may destroy the `TaskGroup` that owns this object.
  void operator()() {
    auto handler = std::move(handler_);
    handler->invoke();
  }
};

// The children of a parent coroutine, which the parent awaits with
// `co_await group.join()`. Replaces Boost's `counter` + `backbone::exec`: the
// parent suspends instead of spinning. Exceptions of children go to the
// `ErrorSink`.
//
// LIFETIME: The children may refer to the parent's frame, so `join()` must be
// awaited on every path, including exceptional ones. As `co_await` is not
// allowed in a `catch` block, a parent catches, stores the exception in the
// `ErrorSink`, and awaits `join()` afterwards.
class TaskGroup {
 private:
  const ql::any_io_executor& executor_;
  ErrorSink& errors_;
  // The unfinished children, plus one for the parent until `join()` has stored
  // its handler.
  std::atomic<size_t> numPending_{1};
  // Resumes the parent. Only used by whoever brings `numPending_` to zero.
  AnyNullaryHandler resumeParent_;

  // Completes once `numPending_` has dropped to zero.
  template <typename CompletionToken>
  auto initiateJoin(CompletionToken&& token) {
    return net::async_initiate<CompletionToken, void()>(
        [this](auto handler) {
          // Store the handler before giving up the parent's count.
          resumeParent_ = AnyNullaryHandler{std::move(handler)};
          if (numPending_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            // All children are done. Post, because we are still inside the
            // initiation.
            net::post(executor_, std::move(resumeParent_));
          }
        },
        token);
  }

 public:
  TaskGroup(const ql::any_io_executor& executor, ErrorSink& errors)
      : executor_{executor}, errors_{errors} {}

  // The children hold a pointer to this object.
  TaskGroup(const TaskGroup&) = delete;
  TaskGroup& operator=(const TaskGroup&) = delete;

  // Children that are still running would access a dangling frame, see
  // LIFETIME above, so terminate.
  ~TaskGroup() {
    ad_utility::terminateIfThrows(
        [this] {
          AD_CORRECTNESS_CHECK(numPending_.load(std::memory_order_acquire) ==
                               0);
        },
        "A `TaskGroup` of the block indirect sort was destroyed before all of "
        "its children had finished");
  }

  // Spawn `child` on the executor. Prefer `spawnFunction` for children that
  // don't suspend.
  //
  // NOTE: `co_spawn` starts the coroutine via `dispatch`, i.e. inline on a
  // thread of the executor, which would run all children serially. The `post`
  // queues it instead, so that other threads can pick it up.
  void spawn(net::awaitable<void> child) {
    numPending_.fetch_add(1, std::memory_order_relaxed);
    try {
      net::post(executor_, [this, child = std::move(child)]() mutable {
        try {
          net::co_spawn(executor_, std::move(child),
                        [this](std::exception_ptr error) {
                          errors_.store(std::move(error));
                          childIsDone();
                        });
        } catch (...) {
          errors_.store(std::current_exception());
          childIsDone();
        }
      });
    } catch (...) {
      // The child was never started.
      errors_.store(std::current_exception());
      childIsDone();
    }
  }

  // Like `spawn`, but for a plain function, which needs no coroutine frame.
  template <typename Function>
  void spawnFunction(Function function) {
    numPending_.fetch_add(1, std::memory_order_relaxed);
    try {
      net::post(executor_, [this, function = std::move(function)]() mutable {
        try {
          function();
        } catch (...) {
          errors_.store(std::current_exception());
        }
        childIsDone();
      });
    } catch (...) {
      errors_.store(std::current_exception());
      childIsDone();
    }
  }

  // Run `child` directly in the parent (like Boost does with the first half of
  // every split), which saves a trip through the executor and keeps the cache
  // warm. It is not counted as a child; it only lives here because its
  // exception has to go to the `ErrorSink`, too.
  net::awaitable<void> runInline(net::awaitable<void> child) {
    try {
      co_await std::move(child);
    } catch (...) {
      errors_.store(std::current_exception());
    }
  }

  // `runInline` for a plain function.
  template <typename Function>
  void runInlineFunction(Function function) {
    try {
      function();
    } catch (...) {
      errors_.store(std::current_exception());
    }
  }

  // Suspend the parent until all spawned children have finished.
  net::awaitable<void> join() {
    // Fast path: only the parent's own count is left, so all children are done.
    if (numPending_.load(std::memory_order_acquire) == 1) {
      numPending_.store(0, std::memory_order_relaxed);
      co_return;
    }
    co_await initiateJoin(net::use_awaitable);
  }

 private:
  // Resume the parent if this was the last child. May destroy `*this`, so no
  // member may be touched after moving out the handler.
  void childIsDone() {
    if (numPending_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      auto resumeParent = std::move(resumeParent_);
      resumeParent();
    }
  }
};

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_TASKGROUP_H

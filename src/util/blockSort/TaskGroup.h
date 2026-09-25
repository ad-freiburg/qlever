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

// The fork/join primitive of the block indirect sort (see
// `util/blockSort/BlockIndirectSort.h`). It is expressed with Boost.Asio
// coroutines and hence only available in C++20 mode.
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

// The single place where the exceptions of a whole sort are collected. The
// tasks of the sort never propagate an exception to the task that spawned them
// (that task would otherwise have to unwind while its other children are still
// running, see `TaskGroup`); instead they store it here, and the tasks that
// have not started yet bail out early because `hasError()` has become `true`.
// The thread that started the sort finally calls `rethrowIfError()`.
//
// This is the equivalent of the `bool error` of Boost's `backbone`, which only
// ever reports a `std::bad_alloc`. We keep the exception itself, so that an
// exception from a comparator is reported as well instead of deadlocking the
// sort.
class ErrorSink {
 private:
  // Read on almost every task boundary, hence a separate atomic instead of
  // locking `mutex_` just to find out that there is nothing to report.
  std::atomic<bool> hasError_{false};
  std::mutex mutex_;
  std::exception_ptr error_;

 public:
  // Whether any task has failed so far. A task that is about to start
  // substantial work checks this and returns early if it is `true`.
  bool hasError() const noexcept {
    return hasError_.load(std::memory_order_acquire);
  }

  // Store `error` as the outcome of the sort, unless an earlier exception has
  // already been stored. Never throws, so that it can be called from the
  // `catch` block of a task that must not fail.
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

  // Rethrow the first exception that any task has stored, if there is one.
  // Only to be called once the whole sort has finished.
  void rethrowIfError() {
    if (error_ != nullptr) {
      std::rethrow_exception(error_);
    }
  }
};

// A minimal move-only type erasure for the completion handler that
// `TaskGroup::join()` has to store until the last child is done.
//
// NOTE: This is exactly `boost::asio::any_completion_handler<void()>`, which we
// cannot use because it was only added in Boost 1.82, while QLever still
// supports Boost 1.81.
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

  // Invoke and destroy the stored handler. The handler is moved out *before* it
  // runs, because running it may destroy the `TaskGroup` that owns this object,
  // see `TaskGroup::childIsDone`.
  void operator()() {
    auto handler = std::move(handler_);
    handler->invoke();
  }
};

// The children that a single parent coroutine has spawned, and which it waits
// for with `co_await group.join()` before it returns. This is what replaces the
// `atomic_t counter` plus `backbone::exec(counter)` of Boost's implementation:
// a parent that waits for its children suspends instead of spinning on the
// counter and executing other work items in the meantime — that part is the job
// of the executor, which hands the thread to the next task all by itself.
//
// A child never propagates an exception to its parent, it stores it in the
// `ErrorSink` instead (see there).
//
// LIFETIME: A `TaskGroup` lives on the coroutine frame of its parent, and the
// children may refer to that frame. `join()` must therefore be awaited before
// the parent returns, on *every* path including the exceptional ones. Because
// `co_await` may not appear inside an exception handler, a parent whose
// spawning loop can throw catches the exception, stores it in the `ErrorSink`,
// and awaits `join()` afterwards.
class TaskGroup {
 private:
  // Both outlive this object, they belong to the state of the whole sort.
  const ql::any_io_executor& executor_;
  ErrorSink& errors_;
  // The number of children that have not finished yet, plus one for the parent
  // itself for as long as `join()` has not been called. That extra count is
  // what keeps a child from completing the group before the parent has stopped
  // spawning and has stored its handler.
  std::atomic<size_t> numPending_{1};
  // Resumes the parent. Only ever touched by the thread that brings
  // `numPending_` down to zero, which is why it needs no lock of its own.
  AnyNullaryHandler resumeParent_;

  // The asynchronous operation behind `join()`, which completes once
  // `numPending_` has dropped to zero.
  template <typename CompletionToken>
  auto initiateJoin(CompletionToken&& token) {
    return net::async_initiate<CompletionToken, void()>(
        [this](auto handler) {
          // The handler has to be in place before the parent's own count is
          // given up, otherwise the last child would find a group that is
          // complete but has nobody to resume.
          resumeParent_ = AnyNullaryHandler{std::move(handler)};
          if (numPending_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            // All children finished while we were storing the handler. Post
            // instead of resuming inline, because we are still inside the
            // initiation of the operation that the parent is awaiting.
            net::post(executor_, std::move(resumeParent_));
          }
        },
        token);
  }

 public:
  TaskGroup(const ql::any_io_executor& executor, ErrorSink& errors)
      : executor_{executor}, errors_{errors} {}

  // Not copyable and not movable, the children hold a pointer to this object.
  TaskGroup(const TaskGroup&) = delete;
  TaskGroup& operator=(const TaskGroup&) = delete;

  // NOTE: Destroying a group whose children are still running would leave them
  // with a dangling parent frame, see the LIFETIME note above. There is no way
  // to recover from that at this point, so the check terminates rather than
  // letting the children write into a frame that is gone.
  ~TaskGroup() {
    ad_utility::terminateIfThrows(
        [this] {
          AD_CORRECTNESS_CHECK(numPending_.load(std::memory_order_acquire) ==
                               0);
        },
        "A `TaskGroup` of the block indirect sort was destroyed before all of "
        "its children had finished");
  }

  // Spawn `child` as a coroutine on the executor. For a child that does not
  // suspend at all, prefer `spawnFunction` below, which is cheaper.
  //
  // IMPORTANT: The `co_spawn` is wrapped in a `post`, and the `post` is what
  // makes this concurrent at all. `co_spawn` starts the coroutine with a
  // `dispatch`, which runs it *inline* when the spawning thread is already
  // running the executor — the whole point of a `dispatch`. Spawning the
  // children of a task directly would therefore run them one after the other
  // in the very thread that spawned them, turning the algorithm back into a
  // depth-first serial one. A `post` uses `relationship.fork` and hence always
  // puts the task into the shared queue of the executor, where a thread that
  // has nothing to do can pick it up.
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
      // The child was never started, so this call is what accounts for it.
      errors_.store(std::current_exception());
      childIsDone();
    }
  }

  // Run `function` on the executor. This is the cheaper `spawn` for the leaves
  // of the algorithm, which never suspend and hence need no coroutine frame.
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

  // Run `child` in the parent coroutine itself instead of handing it to the
  // executor. This is what Boost's implementation does with the first half of
  // every split ("insert the second half in the stack of works, and let the
  // actual thread execute the first part"): it saves a round trip through the
  // executor and keeps the data that the parent has just touched in the cache
  // of this very thread.
  //
  // NOTE: This does *not* count towards the children of the group, it is
  // simply part of the parent. It only lives here because an exception of the
  // `child` has to end up in the `ErrorSink` just like that of a spawned one.
  net::awaitable<void> runInline(net::awaitable<void> child) {
    try {
      co_await std::move(child);
    } catch (...) {
      errors_.store(std::current_exception());
    }
  }

  // `runInline` for a child that is an ordinary function, see `spawnFunction`.
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
    // Fast path: all children are already done, so nobody is left who could
    // resume us and there is no reason to suspend at all. Note that a load of
    // `1` is conclusive: the only remaining count is the parent's own, which
    // means that every child has already run its `childIsDone()` to the end.
    if (numPending_.load(std::memory_order_acquire) == 1) {
      numPending_.store(0, std::memory_order_relaxed);
      co_return;
    }
    co_await initiateJoin(net::use_awaitable);
  }

 private:
  // Account for a child that has finished, and resume the parent if this was
  // the last one. NOTE: This may destroy `*this`, hence nothing must touch a
  // member after the handler has been moved out.
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

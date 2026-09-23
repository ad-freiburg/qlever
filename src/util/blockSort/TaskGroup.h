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

#include <absl/functional/any_invocable.h>

#include <atomic>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstddef>
#include <exception>
#include <mutex>
#include <utility>

#include "backports/asio.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/NoCopyNoMove.h"

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
  // Whether an exception has been stored.
  [[nodiscard]] bool hasError() const noexcept {
    return hasError_.load(std::memory_order_acquire);
  }

  // Store `error`, which must not be null, unless an exception has already been
  // stored.
  void store(std::exception_ptr error) {
    AD_CONTRACT_CHECK(error != nullptr);
    std::lock_guard lock{mutex_};
    if (error_ == nullptr) {
      error_ = std::move(error);
      hasError_.store(true, std::memory_order_release);
    }
  }

  // Rethrow the stored exception, if any. Only to be called once the whole sort
  // has finished.
  void rethrowIfError() {
    if (error_ != nullptr) {
      std::rethrow_exception(error_);
    }
  }
};

// The children of a parent coroutine, which the parent awaits with
// `co_await group.join()`. Replaces Boost's `counter` + `backbone::exec`: the
// parent suspends instead of spinning. Exceptions of children go to the
// `ErrorSink`, and children (spawned or inline) that start after an error are
// skipped.
//
// LIFETIME: The children may refer to the parent's frame, so `join()` must be
// awaited on every path on which a child was spawned. A parent therefore does
// everything that may throw before it spawns its first child.
//
// NOTE: A failure to allocate the bookkeeping of a child (the queued handler or
// the frame of a coroutine) terminates the program, see `postChild`.
//
// Not movable, because the children hold a pointer to this object.
class TaskGroup : public ad_utility::NoCopyNoMove {
 private:
  const ql::any_io_executor& executor_;
  ErrorSink& errors_;
  // The unfinished children, plus one for the parent until `join()` has stored
  // its handler.
  std::atomic<size_t> numPending_{1};
  // Resumes the parent. Only used by whoever brings `numPending_` to zero.
  absl::AnyInvocable<void() &&> resumeParent_;

  // Completes once `numPending_` has dropped to zero.
  template <typename CompletionToken>
  auto initiateJoin(CompletionToken&& token) {
    return net::async_initiate<CompletionToken, void()>(
        [this](auto handler) {
          // Store the handler before giving up the parent's count.
          resumeParent_ = std::move(handler);
          if (numPending_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            // All children are done. Post, because we are still inside the
            // initiation.
            net::post(executor_, std::move(resumeParent_));
          }
        },
        token);
  }

 public:
  // Create a group whose children run on `executor` and report to `errors`.
  TaskGroup(const ql::any_io_executor& executor, ErrorSink& errors)
      : executor_{executor}, errors_{errors} {}

  // Children that are still running would access a dangling frame, see
  // LIFETIME above, so terminate. A count of one means that no child was
  // spawned, but `join()` wasn't awaited because an exception left the parent;
  // that exception propagates.
  ~TaskGroup() {
    ad_utility::terminateIfThrows(
        [this] {
          AD_CORRECTNESS_CHECK(numPending_.load(std::memory_order_acquire) <=
                               1);
        },
        "A `TaskGroup` of the block indirect sort was destroyed before all of "
        "its children had finished");
  }

  // Spawn `child` on the executor. Prefer `spawnFunction` for children that
  // don't suspend.
  //
  // NOTE: `co_spawn` starts the coroutine via `dispatch`, i.e. inline on a
  // thread of the executor, which would run all children serially. The `post`
  // in `postChild` queues it instead, so that other threads can pick it up.
  void spawn(net::awaitable<void> child) {
    postChild([this, child = std::move(child)]() mutable {
      net::co_spawn(executor_, std::move(child),
                    [this](std::exception_ptr error) {
                      if (error != nullptr) {
                        errors_.store(std::move(error));
                      }
                      childIsDone();
                    });
    });
  }

  // Like `spawn`, but for a plain function, which needs no coroutine frame.
  void spawnFunction(absl::AnyInvocable<void() &&> function) {
    postChild([this, function = std::move(function)]() mutable {
      try {
        std::move(function)();
      } catch (...) {
        errors_.store(std::current_exception());
      }
      childIsDone();
    });
  }

  // Run `inlined` in the parent and `spawned` concurrently on the executor, and
  // wait for both (and all other children).
  net::awaitable<void> runConcurrently(net::awaitable<void> inlined,
                                       net::awaitable<void> spawned) {
    spawn(std::move(spawned));
    co_await runInline(std::move(inlined));
    co_await join();
  }

  // Suspend the parent until all spawned children have finished. Must be
  // awaited exactly once.
  net::awaitable<void> join() { return initiateJoin(net::use_awaitable); }

 private:
  // Run `child` directly in the parent (like Boost does with the first half of
  // every split), which saves a trip through the executor and keeps the cache
  // warm. It is skipped after an error, and its exception goes to the
  // `ErrorSink`, too.
  net::awaitable<void> runInline(net::awaitable<void> child) {
    if (errors_.hasError()) {
      co_return;
    }
    try {
      co_await std::move(child);
    } catch (...) {
      errors_.store(std::current_exception());
    }
  }

  // Queue `task` on the executor as a new child. `task` has to call
  // `childIsDone()` exactly once; after an error it is skipped instead.
  //
  // NOTE: This is `noexcept`, because a child that could neither be queued nor
  // started can't be accounted for without leaving the group inconsistent.
  void postChild(absl::AnyInvocable<void() &&> task) noexcept {
    numPending_.fetch_add(1, std::memory_order_relaxed);
    net::post(executor_, [this, task = std::move(task)]() mutable noexcept {
      if (errors_.hasError()) {
        childIsDone();
        return;
      }
      std::move(task)();
    });
  }

  // Resume the parent if this was the last child. Resuming may destroy
  // `*this`, so the handler is moved out into a temporary first.
  void childIsDone() {
    if (numPending_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      std::exchange(resumeParent_, nullptr)();
    }
  }
};

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_TASKGROUP_H

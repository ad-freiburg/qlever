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
#include "util/Synchronized.h"

namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// The children that a coroutine spawns onto an executor, with structured
// exception handling: a group only exists inside `withChildren` or
// `runConcurrently`, which always wait for all of its children, also if the
// parent throws after spawning some of them, and then rethrow the first
// exception of the parent or of a child. Exceptions hence propagate through the
// awaiting coroutines like through ordinary function calls.
//
// Replaces Boost's `counter` + `backbone::exec`: a parent that waits for its
// children suspends instead of spinning.
//
// An exception also sets the `stopped` flag that all groups of a sort share, so
// that children that haven't started yet anywhere in the sort are skipped.
//
// NOTE: A failure to allocate the bookkeeping of a child (the queued handler or
// the frame of a coroutine) terminates the program, see `postChild`.
//
// Not movable, because the children hold a pointer to this object.
class TaskGroup : public ad_utility::NoCopyNoMove {
 private:
  const ql::any_io_executor& executor_;
  std::atomic<bool>& stopped_;
  // The first exception of the parent or of a child.
  ad_utility::Synchronized<std::exception_ptr, std::mutex> firstError_;
  // The unfinished children, plus one for the parent until `join()` has stored
  // its handler.
  std::atomic<size_t> numPending_{1};
  // Resumes the parent. Only used by whoever brings `numPending_` to zero.
  absl::AnyInvocable<void() &&> resumeParent_;

 public:
  // Run `body`, which may spawn children into the group that it is given, on
  // the calling coroutine. Then wait for all children, also if `body` throws,
  // and rethrow the first exception of `body` or of a child.
  //
  // NOTE: `body` is owned by the returned awaitable, so that it stays alive no
  // matter when the awaitable is awaited.
  static net::awaitable<void> withChildren(
      const ql::any_io_executor& executor, std::atomic<bool>& stopped,
      absl::AnyInvocable<void(TaskGroup&)> body) {
    TaskGroup group{executor, stopped};
    try {
      body(group);
    } catch (...) {
      group.fail(std::current_exception());
    }
    co_await group.join();
  }

  // Run `inlined` on the calling coroutine (like Boost does with the first half
  // of every split, which saves a trip through the executor and keeps the cache
  // warm) and `spawned` concurrently on the executor. Then wait for both and
  // rethrow the first exception.
  static net::awaitable<void> runConcurrently(
      const ql::any_io_executor& executor, std::atomic<bool>& stopped,
      net::awaitable<void> inlined, net::awaitable<void> spawned) {
    TaskGroup group{executor, stopped};
    group.spawn(std::move(spawned));
    try {
      co_await std::move(inlined);
    } catch (...) {
      group.fail(std::current_exception());
    }
    co_await group.join();
  }

  // A group is only destroyed after `join()`, so a child that is still running
  // is a bug that would let the child access a dangling group. Terminate
  // instead.
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
  // in `postChild` queues it instead, so that other threads can pick it up.
  void spawn(net::awaitable<void> child) {
    postChild([this, child = std::move(child)]() mutable {
      net::co_spawn(executor_, std::move(child),
                    [this](std::exception_ptr error) {
                      if (error != nullptr) {
                        fail(std::move(error));
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
        fail(std::current_exception());
      }
      childIsDone();
    });
  }

 private:
  // Only `withChildren` and `runConcurrently` create groups, see above.
  TaskGroup(const ql::any_io_executor& executor, std::atomic<bool>& stopped)
      : executor_{executor}, stopped_{stopped} {}

  // Record `error` as the outcome of this group, unless an earlier exception
  // already is, and stop the whole sort.
  void fail(std::exception_ptr error) {
    firstError_.withWriteLock([&error](std::exception_ptr& firstError) {
      if (firstError == nullptr) {
        firstError = std::move(error);
      }
    });
    stopped_.store(true, std::memory_order_release);
  }

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

  // Suspend the parent until all children have finished, then rethrow the
  // first exception.
  net::awaitable<void> join() {
    co_await initiateJoin(net::use_awaitable);
    if (auto firstError = std::exchange(*firstError_.wlock(), nullptr);
        firstError != nullptr) {
      std::rethrow_exception(std::move(firstError));
    }
  }

  // Queue `task` on the executor as a new child. `task` has to call
  // `childIsDone()` exactly once; if the sort is stopped, it is skipped
  // instead.
  //
  // NOTE: This is `noexcept`, because a child that could neither be queued nor
  // started can't be accounted for without leaving the group inconsistent.
  void postChild(absl::AnyInvocable<void() &&> task) noexcept {
    numPending_.fetch_add(1, std::memory_order_relaxed);
    net::post(executor_, [this, task = std::move(task)]() mutable noexcept {
      if (stopped_.load(std::memory_order_acquire)) {
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

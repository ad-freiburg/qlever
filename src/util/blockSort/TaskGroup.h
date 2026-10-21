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
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstddef>
#include <exception>
#include <functional>
#include <mutex>
#include <utility>

#include "backports/asio.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/NoCopyNoMove.h"
#include "util/Synchronized.h"

namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// Run several tasks (coroutines or plain functions) concurrently on an
// executor and wait asynchronously until all of them have finished, similar to
// `boost::asio::experimental::make_parallel_group` or
// `std::execution::when_all`. A group only exists inside `withChildren` or
// `runConcurrently`: they run the code of the parent, which may spawn children
// into the group, and then suspend the parent (without blocking a thread) until
// all children have finished, also if the parent threw after spawning some of
// them.
//
// Exceptions: an exception of the parent or of a child doesn't cancel the other
// tasks of the group. Once all of them have finished, the first exception that
// was recorded is rethrown to the caller of `withChildren` or
// `runConcurrently` (if several tasks fail concurrently, which one is
// unspecified), and all others are dropped. Exceptions hence propagate through
// the awaiting coroutines like through ordinary function calls.
//
// Stopping: all groups of a sort share the `stopped` flag, which the first
// exception anywhere in the sort sets (it is never reset). From then on,
// children that haven't started yet are skipped in all groups and count as
// finished, while running children and the code of the parents run to
// completion. A group whose children were only skipped finishes without an
// exception; the exception reaches the caller of the sort through the group of
// the failed task and the groups that await it.
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
  // The number of unfinished children, plus one for the parent until `join()`
  // has stored its handler.
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
  // don't suspend. A running child may also spawn further children into its
  // own group, because it counts as pending until it is done.
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
          // Store the handler before giving up the parent's count. It resumes
          // the parent via the associated executor of the handler (the
          // executor of the parent's coroutine): inline if the calling thread
          // already runs on it, e.g. if the last child ran on the same thread
          // pool, otherwise via a hop to that executor.
          resumeParent_ = [handler = std::move(handler),
                           executor = &executor_]() mutable {
            auto handlerExecutor =
                net::get_associated_executor(handler, *executor);
            net::dispatch(handlerExecutor, std::move(handler));
          };
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
    // Relaxed, like the increment of the reference count of a
    // `std::shared_ptr`: the caller already holds a count, so this can't race
    // with the decrement to zero, and the `post` orders the child after this.
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
      std::invoke(std::exchange(resumeParent_, nullptr));
    }
  }
};

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_TASKGROUP_H

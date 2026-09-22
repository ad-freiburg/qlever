// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_TASKQUEUEONEXECUTOR_H
#define QLEVER_SRC_UTIL_TASKQUEUEONEXECUTOR_H

#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <mutex>
#include <string>
#include <type_traits>
#include <utility>

#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"

namespace ad_utility {

// A queue of tasks that are run on an *external* `boost::asio` executor, which
// is passed in by the caller. Tasks are enqueued via calls to `push()`. The
// destructor, or its manual equivalent `finish()`, block until all tasks have
// run to completion.
//
// NOTE: The interface of this class is blocking, not asynchronous; it is only
// the *execution* of the tasks that happens asynchronously on the executor.
// Every member function blocks the calling thread: `push` and `submit` while
// the maximal number of tasks is already in flight, and `finish`,
// `waitUntilAllTasksAreDone`, `waitUntilFinished`, and the destructor until
// the tasks they wait for have completed. None of them returns an awaitable.
//
// This class mimics the observable behavior of `ad_utility::TaskQueue<false>`
// (see `util/TaskQueue.h`), but with the following important differences:
//
// 1. This class owns no threads. The number of threads (and thus the actual
//    parallelism) is a property of the executor and not of this queue. The
//    queue only bounds how much work it keeps *in flight* (see `push`).
// 2. `finish()`, `waitUntilFinished()`, and the destructor block the calling
//    thread. They must therefore NOT be called from a thread that runs the
//    executor, because then the tasks that are waited for might never get a
//    thread to run on (the same holds for a `push` that has to wait for a free
//    slot).
// 3. The execution context behind the executor has to outlive this queue,
//    because the destructor waits for tasks that run on that context.
// 4. Whether the tasks run concurrently, and in which order, is a property of
//    the executor. With `maxNumTasksInFlight == 1`, a task is only posted to
//    the executor after the previous task has completed, so the tasks then run
//    strictly sequentially, and, if they are all pushed by the same thread, in
//    the order in which they were pushed. With a larger bound, several tasks
//    may be in flight at the same time; they then run concurrently and in an
//    arbitrary order. Callers that need a larger bound, but still want the
//    tasks to be run sequentially and in order, have to pass a
//    `boost::asio::strand` as the executor.
//
// NOTE: An exception that is thrown by a task terminates the process, unless
// the task was passed to `submit`, in which case the exception is stored in
// the returned future. For the details see `push` and `submit` below.
class TaskQueueOnExecutor {
 public:
  using Task = absl::AnyInvocable<void()>;

 private:
  boost::asio::any_io_executor executor_;
  size_t maxNumTasksInFlight_;
  // The message that is logged if a task throws or cannot be scheduled (it
  // contains the name of this queue). It is precomputed, because it is needed
  // for every single task.
  std::string errorMessage_;

  // The mutex that protects all of the following members, together with the
  // condition variable that is notified whenever one of them changes.
  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;
  // The number of tasks that have been pushed, but not yet completed (they are
  // either waiting to be run by the executor, or currently running).
  size_t numTasksInFlight_ = 0;
  bool startedFinishing_ = false;
  bool finishedFinishing_ = false;

 public:
  // Construct from the `executor` on which the tasks will be run, the maximal
  // number of tasks that are kept in flight, and a `name` that is only used in
  // error messages. The `executor` must not be empty and
  // `maxNumTasksInFlight` has to be at least one.
  //
  // NOTE: To understand the practicality of the bound, it helps to look at the
  // two extremes. If `maxNumTasksInFlight` is very small, the "pusher" is
  // blocked whenever it is faster than the executor. If it is too large and
  // the "pusher" is faster for many tasks, then the pending tasks will pile up
  // and might not fit into memory. The queue works optimally when on the
  // average the executor is at least as fast as the "pusher", but the pusher
  // is faster sometimes (which the queue can then accommodate).
  TaskQueueOnExecutor(boost::asio::any_io_executor executor,
                      size_t maxNumTasksInFlight, std::string name = "")
      : executor_{std::move(executor)},
        maxNumTasksInFlight_{maxNumTasksInFlight},
        errorMessage_{
            absl::StrCat("In the TaskQueueOnExecutor \"", name, "\".")} {
    AD_CONTRACT_CHECK(static_cast<bool>(executor_));
    AD_CONTRACT_CHECK(maxNumTasksInFlight_ > 0);
  }

  // Add a task to the queue for execution. Block until fewer than
  // `maxNumTasksInFlight()` tasks are in flight. May be called concurrently
  // from several threads, but not after a call to `finish()`.
  //
  // NOTE: If the execution of the task throws, then `std::terminate` will be
  // called (this matches the behavior of `TaskQueue`, where an exception
  // escapes the worker thread). Use `submit` below if the exception should
  // instead be propagated to the pushing thread. The *scheduling* of the task
  // also terminates if it throws, see below.
  void push(Task task) {
    std::unique_lock lock{mutex_};
    AD_CONTRACT_CHECK(!startedFinishing_);
    cv_.wait(lock,
             [this]() { return numTasksInFlight_ < maxNumTasksInFlight_; });
    ++numTasksInFlight_;
    lock.unlock();
    // NOTE: The only way in which `boost::asio::post` can fail is that the
    // executor runs out of resources while scheduling the task (in practice,
    // one of the allocations for the queued operation throws `std::bad_alloc`;
    // the `boost::asio::bad_executor` for an empty executor is excluded by the
    // contract check in the constructor). Such a failure is not recoverable,
    // so we terminate instead of leaving the queue in a state where a slot is
    // occupied by a task that will never be run.
    ad_utility::terminateIfThrows(
        [this, &task]() {
          boost::asio::post(executor_, [this,
                                        task = std::move(task)]() mutable {
            ad_utility::terminateIfThrows([&task]() { task(); }, errorMessage_);
            taskIsDone();
          });
        },
        errorMessage_);
  }

  // Submit a callable and return a `std::future` for its result. The returned
  // future resolves (or throws) once the task completes.
  //
  // NOTE: In contrast to `push` above, an exception that is thrown by `func`
  // does NOT terminate the process, but is stored in the returned future and
  // is rethrown by its `get()`. The exception is therefore silently swallowed
  // if the future is discarded.
  template <typename Func>
  auto submit(Func&& func)
      -> std::future<std::invoke_result_t<std::decay_t<Func>>> {
    using R = std::invoke_result_t<std::decay_t<Func>>;
    std::packaged_task<R()> task{AD_FWD(func)};
    auto future = task.get_future();
    push(std::move(task));
    return future;
  }

  // Block until all tasks that have been pushed have been completed. After a
  // call to `finish()`, no more calls to `push` are allowed. Calling `finish()`
  // several times is allowed; all calls but the first one simply wait until the
  // first call has completed (and thus return immediately if it already has).
  void finish() {
    std::unique_lock lock{mutex_};
    if (startedFinishing_) {
      cv_.wait(lock, [this]() { return finishedFinishing_; });
      return;
    }
    startedFinishing_ = true;
    cv_.wait(lock, [this]() { return numTasksInFlight_ == 0; });
    finishedFinishing_ = true;
    cv_.notify_all();
  }

  // Block until all tasks that have been pushed so far have been completed. In
  // contrast to `finish()`, further tasks may be pushed afterwards, so this
  // function may be called several times to wait for consecutive batches of
  // tasks. It may only be called by the thread that pushes the tasks, because
  // a task that another thread pushes concurrently is not necessarily waited
  // for.
  //
  // NOTE: Just like `finish()`, this function must not be called from a thread
  // that runs the executor, see the class comment above.
  void waitUntilAllTasksAreDone() {
    std::unique_lock lock{mutex_};
    cv_.wait(lock, [this]() { return numTasksInFlight_ == 0; });
  }

  // Block the current thread until a call to `finish()` on this queue has been
  // completed. In contrast to `finish()`, this function doesn't itself initiate
  // the finishing.
  void waitUntilFinished() const {
    std::unique_lock lock{mutex_};
    cv_.wait(lock, [this]() { return finishedFinishing_; });
  }

  // Return the maximal number of tasks that this queue keeps in flight (see
  // `push` above).
  size_t maxNumTasksInFlight() const { return maxNumTasksInFlight_; }

  // The destructor waits for all pushed tasks to complete, see `finish()`.
  ~TaskQueueOnExecutor() {
    ad_utility::terminateIfThrows([this]() { finish(); },
                                  "In the destructor of TaskQueueOnExecutor.");
  }

 private:
  // Account for a task that is no longer in flight, and notify all threads that
  // are waiting for a free slot or for the queue to be finished.
  void taskIsDone() {
    std::lock_guard lock{mutex_};
    AD_CORRECTNESS_CHECK(numTasksInFlight_ > 0);
    --numTasksInFlight_;
    cv_.notify_all();
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_TASKQUEUEONEXECUTOR_H

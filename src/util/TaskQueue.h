//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_TASKQUEUE_H
#define QLEVER_TASKQUEUE_H

#include <absl/cleanup/cleanup.h>

#include <functional>
#include <optional>
#include <queue>
#include <string>
#include <thread>

#include "util/Exception.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"
#include "util/jthread.h"

namespace ad_utility {
/**
 * A Queue of tasks, which are executed by a thread pool. Tasks can be enqued
 * via calls to push(). The destructor, or its manual equivalent finish(),
 * block until all tasks have run to completion.
 * @tparam TrackTimes if set to true, the time that is spent waiting for push()
 * to return, and the time that threads from the thread pool are idle is
 * measured and can be retrieved for statistics.
 */
template <bool TrackTimes = false>
class TaskQueue {
 private:
  using Task = std::function<void()>;
  using Timer = ad_utility::Timer;
  using AtomicMs = std::atomic<std::chrono::milliseconds::rep>;
  using Queue = ad_utility::data_structures::ThreadSafeQueue<Task>;

  std::atomic_flag startedFinishing_ = false;
  std::atomic_flag finishedFinishing_ = false;
  size_t queueMaxSize_ = 1;
  Queue queuedTasks_{queueMaxSize_};
  std::vector<ad_utility::JThread> threads_;
  std::string name_;
  // Keep track of the time spent waiting in the push/pop operation
  AtomicMs pushTime_ = 0, popTime_ = 0;

 public:
  /// Construct from the maximum size of the queue, and the number of worker
  /// threads. If there are more than `maxQueueSize` tasks in the queue, then
  /// calls to `push` block, until a task was finished. Tasks that are currently
  /// being computed by one of the threads do NOT count towards the
  /// `maxQueueSize`. MaxQueueSize has to be at least one, numThreads may be 0,
  /// then all the tasks have to be retrieved manually via calls to
  /// `popManually`.
  ///
  /// NOTE: To understand the practicality of this task queue, it helps to look
  /// at the two extremes. If `maxQueueSize` is zero, the "pusher" is blocked
  /// when its faster than the workers. If 'maxQueueSize' is too large and the
  /// "pusher" is faster for many tasks, the queue will grow too large to fit
  /// into memory. The task queue will work optimally, when on the average the
  /// workers are at least as fast as the "pusher", but the pusher is faster
  /// sometimes (which the queue can then accommodate).
  TaskQueue(size_t maxQueueSize, size_t numThreads, std::string name = "")
      : queueMaxSize_{maxQueueSize}, name_{std::move(name)} {
    AD_CONTRACT_CHECK(queueMaxSize_ > 0);
    threads_.reserve(numThreads);
    for (size_t i = 0; i < numThreads; ++i) {
      threads_.emplace_back(&TaskQueue::function_for_thread, this);
    }
  }

  /// Add a task to the queue for Execution. Blocks until there is at least
  /// one free spot in the queue.
  /// Note: If the execution of the task throws, `std::terminate` will be
  /// called.
  bool push(Task t) {
    // the actual logic
    auto action = [&, this] { return queuedTasks_.push(std::move(t)); };

    // If TrackTimes==true, measure the time and add it to pushTime_,
    // else only perform the pushing.
    return executeAndUpdateTimer(action, pushTime_);
  }

  // Blocks until all tasks have been computed. After a call to finish, no more
  // calls to push are allowed.
  void finish() {
    if (startedFinishing_.test_and_set()) {
      // There was a previous call to `finish()` , so we don't need to do
      // anything.
      return;
    }
    finishImpl();
  }

  void resetTimers() requires TrackTimes {
    pushTime_ = 0;
    popTime_ = 0;
  }

  // Execute the callable f of type F. If TrackTimes==true, add the passed time
  // to `duration`. This is designed to work for functions with no return
  // value(void) as well as for functions with a return value. See the
  // applications above.
  template <typename F>
  decltype(auto) executeAndUpdateTimer(F&& f, AtomicMs& duration) {
    if constexpr (TrackTimes) {
      ad_utility::Timer t{ad_utility::Timer::Started};
      auto cleanup =
          absl::Cleanup{[&duration, &t] { duration += t.msecs().count(); }};
      return f();
    } else {
      return f();
    }
  }

  // __________________________________________________________________________
  std::string getTimeStatistics() const requires TrackTimes {
    return "Time spent waiting in queue " + name_ + ": " +
           std::to_string(pushTime_) + "ms (push), " +
           std::to_string(popTime_) + "ms (pop)";
  }

  ~TaskQueue() {
    if (startedFinishing_.test_and_set()) {
      // Someone has already called `finish`, we have to wait for the finishing
      // to complete, otherwise there is a data race on the `threads_`.
      finishedFinishing_.wait(false);
    } else {
      // We are responsible for finishing, but we already have set the
      // `startedFinishing_` to true, so we can run the `impl` directly.
      ad_utility::terminateIfThrows([this]() { finishImpl(); },
                                    "In the destructor of TaskQueue.");
    }
  }

 private:
  // _________________________________________________________________________
  void function_for_thread() {
    while (auto task = queuedTasks_.pop()) {
      task.value()();
    }
  }

  // The implementation of `finish`. Must only be called if this is the thread
  // that set `startedFinishing_` from false to true.
  void finishImpl() {
    queuedTasks_.finish();
    ql::ranges::for_each(threads_, [](auto& thread) {
      // If `finish` was called from inside the queue, the calling thread cannot
      // join itself.
      AD_CORRECTNESS_CHECK(thread.joinable());
      if (thread.get_id() != std::this_thread::get_id()) {
        thread.join();
      }
    });
    finishedFinishing_.test_and_set();
    finishedFinishing_.notify_all();
  }
};
}  // namespace ad_utility

#endif  // QLEVER_TASKQUEUE_H

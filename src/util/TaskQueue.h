//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_TASKQUEUE_H
#define QLEVER_TASKQUEUE_H

#include <functional>
#include <optional>
#include <queue>
#include <string>
#include <thread>

#include "./Exception.h"
#include "./Timer.h"
#include "./jthread.h"

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

  std::vector<ad_utility::JThread> _threads;
  std::queue<Task> _queuedTasks;
  size_t _queueMaxSize = 1;
  // CV to notify that a new task has been added to the queue
  std::condition_variable _newTaskWasPushed;
  // CV to notify that a task was finished by a thread.
  std::condition_variable _workerHasFinishedTask;
  std::mutex _queueMutex;
  std::atomic<bool> _shutdownQueue = false;
  std::string _name;
  // Keep track of the time spent waiting in the push/pop operation
  std::atomic<size_t> _pushTime = 0, _popTime = 0;

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
  /// sometimes (which the queue can then accomodate).
  TaskQueue(size_t maxQueueSize, size_t numThreads, std::string name = "")
      : _queueMaxSize{maxQueueSize}, _name{std::move(name)} {
    AD_CONTRACT_CHECK(_queueMaxSize > 0);
    _threads.reserve(numThreads);
    for (size_t i = 0; i < numThreads; ++i) {
      _threads.emplace_back(&TaskQueue::function_for_thread, this);
    }
  }

  /// Add a task to the queue for Execution. Blocks until there is at least
  /// one free spot in the queue.
  void push(Task t) {
    // the actual logic
    auto action = [&, this] {
      std::unique_lock l{_queueMutex};
      _workerHasFinishedTask.wait(
          l, [&] { return _queuedTasks.size() < _queueMaxSize; });
      _queuedTasks.push(std::move(t));
      _newTaskWasPushed.notify_one();
    };

    // If TrackTimes==true, measure the time and add it to _pushTime,
    // else only perform the pushing.
    executeAndUpdateTimer(action, _pushTime);
  }

  // Blocks until all tasks have been computed. After a call to finish, no more
  // calls to push are allowed.
  void finish() {
    std::unique_lock l{_queueMutex};

    // empty queue and _shutdownQueue set is the way of signalling the
    // destruction to the threads;
    _shutdownQueue = true;
    // Wait not only until the queue is empty, but also until the tasks are
    // actually performed and the threads have joined.
    l.unlock();
    _newTaskWasPushed.notify_all();
    for (auto& thread : _threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

  std::optional<Task> popManually() {
    auto action = [&, this]() -> std::optional<Task> {
      std::unique_lock l{_queueMutex};
      _newTaskWasPushed.wait(
          l, [&] { return !_queuedTasks.empty() || _shutdownQueue; });
      if (_shutdownQueue && _queuedTasks.empty()) {
        return std::nullopt;
      }
      auto task = std::move(_queuedTasks.front());
      _queuedTasks.pop();
      _workerHasFinishedTask.notify_one();
      return task;
    };
    return executeAndUpdateTimer(action, _popTime);
  }

  void resetTimers() requires TrackTimes {
    _pushTime = 0;
    _popTime = 0;
  }

  // Execute the callable f of type F. If TrackTimes==true, add the passed time
  // to `duration`. This is designed to work for functions with no return
  // value(void) as well as for functions with a return value. See the
  // applications above.
  template <typename F>
  decltype(auto) executeAndUpdateTimer(F&& f, std::atomic<size_t>& duration) {
    if constexpr (TrackTimes) {
      struct T {
        ad_utility::Timer _t{ad_utility::Timer::Started};
        std::atomic<size_t>& _target;
        T(std::atomic<size_t>& target) : _target(target) {}
        ~T() { _target += _t.msecs(); }
      };
      T timeHandler{duration};
      return f();
    } else {
      return f();
    }
  }

  // __________________________________________________________________________
  std::string getTimeStatistics() const requires TrackTimes {
    return "Time spent waiting in queue " + _name + ": " +
           std::to_string(_pushTime) + "ms (push), " +
           std::to_string(_popTime) + "ms (pop)";
  }

  ~TaskQueue() { finish(); }

 private:
  // _________________________________________________________________________
  void function_for_thread() {
    while (true) {
      auto optionalTask = popManually();
      if (!optionalTask) {
        return;
      }
      // perform the task without actually holding the lock.
      (*optionalTask)();
    }
  }
};
}  // namespace ad_utility

#endif  // QLEVER_TASKQUEUE_H

// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)
//          Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_THREADSAFEQUEUE_H
#define QLEVER_THREADSAFEQUEUE_H

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

#include "absl/synchronization/mutex.h"

namespace ad_utility::data_structures {

/// A thread safe, multi-consumer, multi-producer queue.
template <typename T>
class ThreadSafeQueue {
  std::exception_ptr pushedException_;
  std::queue<T> _queue;
  std::mutex _mutex;
  std::condition_variable _pushNotification;
  std::condition_variable _popNotification;
  bool _lastElementPushed = false;
  bool _pushDisabled = false;
  size_t _maxSize;

 public:
  explicit ThreadSafeQueue(size_t maxSize) : _maxSize{maxSize} {}

  /// Push an element into the queue. Block until there is free space in the
  /// queue or until disablePush() was called. Return false if disablePush()
  /// was called. In this case the current element element abd akk future
  /// elements are not added to the queue.
  bool push(T value) {
    std::unique_lock lock{_mutex};
    _popNotification.wait(
        lock, [&] { return _queue.size() < _maxSize || _pushDisabled; });
    if (_pushDisabled) {
      return false;
    }
    _queue.push(std::move(value));
    lock.unlock();
    _pushNotification.notify_one();
    return true;
  }

  // The semantics of pushing an exception are as follows: All subsequent calls
  // to `pop` will throw the `exception`, and all subsequent calls to `push`
  // will return `false`.
  void pushException(std::exception_ptr exception) {
    std::unique_lock lock{_mutex};
    pushedException_ = std::move(exception);
    _pushDisabled = true;
    lock.unlock();
    _pushNotification.notify_all();
    _popNotification.notify_all();
  }

  /// Signals all threads waiting for pop() to return that data transmission
  /// has ended and it should stop processing.
  void signalLastElementWasPushed() {
    std::unique_lock lock{_mutex};
    _lastElementPushed = true;
    lock.unlock();
    _pushNotification.notify_all();
  }

  /// Wakes up all blocked threads waiting for push, cancelling execution
  void disablePush() {
    std::unique_lock lock{_mutex};
    _pushDisabled = true;
    lock.unlock();
    _popNotification.notify_all();
  }

  /// Always call `disablePush` on destruction. This makes sure that worker
  /// threads that pop from the queue always see std::nullopt, even if the
  /// threads that push to the queue exit via an exception or if the explicit
  /// call to `disablePush` is missing.
  ~ThreadSafeQueue() { disablePush(); }

  /// Blocks until another thread pushes an element via push() which is
  /// hen returned or signalLastElementWasPushed() is called resulting in an
  /// empty optional, whatever happens first
  std::optional<T> pop() {
    std::unique_lock lock{_mutex};
    _pushNotification.wait(lock, [&] {
      return !_queue.empty() || _lastElementPushed || pushedException_;
    });
    if (pushedException_) {
      std::rethrow_exception(pushedException_);
    }
    if (_lastElementPushed && _queue.empty()) {
      return {};
    }
    std::optional<T> value = std::move(_queue.front());
    _queue.pop();
    lock.unlock();
    _popNotification.notify_one();
    return value;
  }
};

// A thread safe queue that is similar (wrt the interface and the behavior) to
// the `ThreadSafeQueue` above, with the following differente: Each element that
// is pushed is associated with a unique index `n`. A call to `push(n,
// someValue)` will block until other threads have pushed all indices in the
// range [0, ..., n - 1]. This can be used to enforce the ordering of values
// that are asynchronously created by multiple threads. Note that great care has
// to be taken that all the indices will be pushed eventually by some thread,
// and that for each thread individually the indices are increasing, otherwise
// the queue will lead to a deadlock.
template <typename T>
class OrderedThreadSafeQueue {
 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  ThreadSafeQueue<T> queue_;
  size_t nextIndex_ = 0;
  bool pushWasDisabled_ = false;

 public:
  // Construct from the maximal queue size (see `ThreadSafeQueue` for details).
  explicit OrderedThreadSafeQueue(size_t maxSize) : queue_{maxSize} {}

  // Push the `value` to the queue that is associated with the `index`. This
  // call blocks, until `push` has been called for all indices in `[0, ...,
  // index - 1]` or until `disablePush` was called. The remaining behavior is
  // equal to `ThreadSafeQueue::push`.
  bool push(size_t index, T value) {
    std::unique_lock lock{mutex_};
    cv_.wait(lock, [this, index]() {
      return index == nextIndex_ || pushWasDisabled_;
    });
    if (pushWasDisabled_) {
      return false;
    }
    ++nextIndex_;
    bool result = queue_.push(std::move(value));
    lock.unlock();
    cv_.notify_all();
    return result;
  }

  // See `ThreadSafeQueue` for details.
  void pushException(std::exception_ptr exception) {
    std::unique_lock l{mutex_};
    queue_.pushException(std::move(exception));
    pushWasDisabled_ = true;
    l.unlock();
    cv_.notify_all();
  }

  // See `ThreadSafeQueue` for details.
  void signalLastElementWasPushed() { queue_.signalLastElementWasPushed(); }

  // See `ThreadSafeQueue` for details.
  void disablePush() {
    queue_.disablePush();
    std::unique_lock lock{mutex_};
    pushWasDisabled_ = true;
    lock.unlock();
    cv_.notify_all();
  }

  // See `ThreadSafeQueue` for details.
  ~OrderedThreadSafeQueue() { disablePush(); }

  // See `ThreadSafeQueue` for details. All the returned values will be in
  // ascending consecutive order wrt the index with which they were pushed.
  std::optional<T> pop() { return queue_.pop(); }
};

}  // namespace ad_utility::data_structures

#endif  // QLEVER_THREADSAFEQUEUE_H

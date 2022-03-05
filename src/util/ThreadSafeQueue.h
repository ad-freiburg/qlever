// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_THREADSAFEQUEUE_H
#define QLEVER_THREADSAFEQUEUE_H

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

namespace ad_utility::data_structures {

template <typename T>
class ThreadSafeQueue {
  std::queue<T> _queue;
  std::mutex _mutex;
  std::condition_variable _pushNotification;
  std::condition_variable _popNotification;
  bool _lastElementPushed = false;
  bool _disablePush = false;
  size_t _maxSize;

 public:
  explicit ThreadSafeQueue(size_t maxSize) : _maxSize{maxSize} {}

  /// Pushes an element into the queue and blocks if the queue's capacity
  /// is currently exhausted until some space is freed via pop() or abort()
  /// is called to cancel execution
  bool push(T value) {
    std::unique_lock lock{_mutex};
    _popNotification.wait(
        lock, [&] { return _queue.size() < _maxSize || _disablePush; });
    if (_disablePush) {
      return true;
    }
    _queue.push(std::move(value));
    lock.unlock();
    _pushNotification.notify_one();
    return false;
  }

  /// Signals all threads waiting for pop() to return that data transmission
  /// has ended and it should stop processing.
  void finish() {
    std::unique_lock lock{_mutex};
    _lastElementPushed = true;
    lock.unlock();
    _pushNotification.notify_all();
  }

  /// Wakes up all blocked threads waiting for push, cancelling execution
  void abort() {
    std::unique_lock lock{_mutex};
    _disablePush = true;
    lock.unlock();
    _popNotification.notify_all();
  }

  /// Blocks until another thread pushes an element via push() which is
  /// hen returned or finish() is called resulting in an empty optional,
  /// whatever happens first
  std::optional<T> pop() {
    std::unique_lock lock{_mutex};
    _pushNotification.wait(
        lock, [&] { return !_queue.empty() || _lastElementPushed; });
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

}  // namespace ad_utility::data_structures

#endif  // QLEVER_THREADSAFEQUEUE_H

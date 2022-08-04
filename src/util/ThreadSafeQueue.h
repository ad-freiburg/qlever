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

/// A thread safe, multi-consumer, multi-producer queue.
template <typename T>
class ThreadSafeQueue {
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

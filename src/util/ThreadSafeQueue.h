// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_THREADSAFEQUEUE_H
#define QLEVER_THREADSAFEQUEUE_H

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
  bool _finished = false;
  bool _aborted = false;
  size_t _maxSize;

 public:
  explicit ThreadSafeQueue(size_t maxSize) : _maxSize{maxSize} {}

  bool push(T value) {
    std::unique_lock lock{_mutex};
    _popNotification.wait(lock,
                          [&] { return _queue.size() < _maxSize || _aborted; });
    if (_aborted) {
      return true;
    }
    _queue.push(std::move(value));
    lock.unlock();
    _pushNotification.notify_one();
    return false;
  }

  void finish() {
    std::unique_lock lock{_mutex};
    _finished = true;
    lock.unlock();
    _pushNotification.notify_all();
  }

  void abort() {
    std::unique_lock lock{_mutex};
    _aborted = true;
    lock.unlock();
    _popNotification.notify_all();
  }

  std::optional<T> pop() {
    std::unique_lock lock{_mutex};
    _pushNotification.wait(lock, [&] { return !_queue.empty() || _finished; });
    if (_finished && _queue.empty()) {
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

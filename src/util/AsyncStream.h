// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <string_view>
#include <thread>

#include "./StringSupplier.h"

namespace ad_utility::streams {
namespace http = boost::beast::http;

// 100 MiB
constexpr size_t BUFFER_LIMIT = (1u << 20) * 100;

class AsyncStream : public StringSupplier {
  std::unique_ptr<StringSupplier> _supplier;
  std::ostringstream _stream;
  std::string _extraStorage;
  std::atomic_bool _started = false;
  bool _done = false;
  std::atomic_bool _doneRead = false;
  std::mutex _mutex;
  std::condition_variable _conditionVariable;
  bool _ready = false;
  std::exception_ptr _exception = nullptr;
  std::thread _thread;

  void run() {
    try {
      while (_supplier->hasNext()) {
        auto view = _supplier->next();
        std::unique_lock lock{_mutex};
        if (_stream.view().length() >= BUFFER_LIMIT) {
          _conditionVariable.wait(
              lock, [this]() { return _stream.view().empty() || _done; });
          if (_done) {
            return;
          }
        }
        _stream << view;
        _ready = true;
        _done = !_supplier->hasNext();
        lock.unlock();
        _conditionVariable.notify_one();
      }
    } catch (...) {
      std::lock_guard guard{_mutex};
      _exception = std::current_exception();
      _ready = true;
      _done = true;
    }
    _conditionVariable.notify_one();
  }

  void swapStreamStorage() {
    std::ostringstream temp{std::move(_extraStorage)};
    std::swap(temp, _stream);
    _extraStorage = std::move(temp).str();
  }

 public:
  explicit AsyncStream(std::unique_ptr<StringSupplier> supplier)
      : _supplier{std::move(supplier)} {}

  [[nodiscard]] bool hasNext() const override { return !_doneRead.load(); }

  std::string_view next() override {
    if (!_started.exchange(true)) {
      _thread = std::thread{[&]() { run(); }};
    } else {
      _extraStorage.clear();
    }
    std::unique_lock lock{_mutex};
    _conditionVariable.wait(lock, [this]() { return _ready; });
    if (_exception) {
      std::rethrow_exception(_exception);
    }
    swapStreamStorage();
    _ready = false;
    _doneRead = _done;
    lock.unlock();
    _conditionVariable.notify_one();

    return _extraStorage;
  }

  ~AsyncStream() override {
    std::unique_lock lock{_mutex};
    _done = true;
    lock.unlock();
    _conditionVariable.notify_one();
    if (_thread.joinable()) {
      _thread.join();
    }
  }

  void prepareHttpHeaders(
      http::header<false, http::fields>& header) const override {
    _supplier->prepareHttpHeaders(header);
  }
};
}  // namespace ad_utility::streams

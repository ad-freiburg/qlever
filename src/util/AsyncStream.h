// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <string_view>
#include <thread>

#include "./Log.h"
#include "./StringSupplier.h"

namespace ad_utility::streams {
namespace http = boost::beast::http;

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

  void run() {
    try {
      std::cout << "Loop start" << std::endl;
      while (_supplier->hasNext()) {
        // TODO throttle to match client downloads to avoid
        //  buffer build-ups
        auto view = _supplier->next();
        std::unique_lock lock{_mutex};
        _stream << view;
        _ready = true;
        _done = !_supplier->hasNext();
        lock.unlock();
        _conditionVariable.notify_one();
      }
    } catch (...) {
      std::cout << "Loop Exception" << std::endl;
      std::lock_guard guard{_mutex};
      _exception = std::current_exception();
      _ready = true;
      _done = true;
    }
    _conditionVariable.notify_one();
    std::cout << "Loop end" << std::endl;
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
      std::thread{[&]() { run(); }}.detach();
    } else {
      _extraStorage.clear();
    }
    std::cout << "Trying to enter mutex...";
    std::unique_lock lock{_mutex};
    std::cout << " Success!" << std::endl;
    if (!_done) {
      std::cout << "Staring to wait...";
      if (!_conditionVariable.wait_for(lock, std::chrono::seconds{30},
                                       [this]() { return _ready; })) {
        LOG(ERROR) << "TIMEOUT: ";
        LOG(ERROR) << "_ready" << _ready;
        LOG(ERROR) << "_done" << _done;
        LOG(ERROR) << "_doneRead" << _doneRead;
        LOG(ERROR) << "_supplier->hasNext()" << _supplier->hasNext() << std::endl;
      }
      std::cout << " End!" << std::endl;
    }
    std::cout << "Other stuff" << std::endl;
    if (_exception) {
      std::rethrow_exception(_exception);
    }
    swapStreamStorage();
    _ready = false;
    _doneRead = _done;

    std::cout << "INFO: " << std::endl;
    std::cout << "_ready" << _ready;
    std::cout << "_done" << _done;
    std::cout << "_doneRead" << _doneRead;
    std::cout << "_supplier->hasNext()" << _supplier->hasNext() << std::endl;
    return _extraStorage;
  }

  void prepareHttpHeaders(
      http::header<false, http::fields>& header) const override {
    _supplier->prepareHttpHeaders(header);
  }
};
}  // namespace ad_utility::streams

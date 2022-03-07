// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#pragma once

#include <exception>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>

// Coroutines are still experimental in clang libcpp, therefore adapt the
// appropriate namespaces by including the convenience header.
#include "./Coroutines.h"

namespace ad_utility {
template<typename ValueType>
class CoroToStateMachine {

 public:
  struct InitializationDone{};
  struct ValueWasPushed{};
  struct NextValue{};

  struct promise_type {
    enum struct State {Initialization, Running, Done};

    bool _valueWasPushed = false;
    ValueType _nextValueType; // TODO<joka921> Can this be a pointer

    struct ValueWasPushedAwaitable {
      promise_type* _promise_type;
      bool await_ready() {return false;}
      void await_suspend(std::coroutine_handle<>) {}
      bool await_resume() {return _promise_type->_valueWasPushed;}
    };
    struct NextValueAwaitable {
      promise_type* _promise_type;
      bool await_ready() {return true;}
      void await_suspend(std::coroutine_handle<>) {}
      // TODO<joka921> should be a reference
      ValueType& await_resume() {return _promise_type->_nextValueType;}
    };
    ValueWasPushedAwaitable await_transform(ValueWasPushed) {
      return {this};
    }
    NextValueAwaitable await_transform(NextValue) {
      return {this};
    }




    constexpr std::suspend_never initial_suspend() noexcept {return {};}
    constexpr std::suspend_always final_suspend() noexcept {return {};}

    // TODO<joka921> implement
    void unhandled_exception() {}
    void return_void() {}

    CoroToStateMachine get_return_object() {
      return {std::coroutine_handle<promise_type>::from_promise(*this)};
    }

  };

 private:
  using Handle =
  std::coroutine_handle<promise_type>;
  Handle _coro = nullptr;
  bool _isFinished = false;

 public:
  CoroToStateMachine(Handle handle) : _coro{handle} {}

  void push(ValueType value) {
    _coro.promise()._valueWasPushed = true;
    _coro.promise()._nextValueType = std::move(value);
    _coro.resume();
  }

  void finish() {
    if (_isFinished || !_coro) {
      return;
    }
    _isFinished = true;
    _coro.promise()._valueWasPushed = false;
    _coro.resume();
    assert(_coro.done());
    _coro.destroy();
  }

  ~CoroToStateMachine() {
    finish();
  }
  CoroToStateMachine() = default;

  CoroToStateMachine(CoroToStateMachine&& rhs) noexcept : _coro{rhs._coro} {
    rhs._coro = nullptr;
  }

  void swap(CoroToStateMachine& rhs) noexcept {
    std::swap(_coro, rhs._coro);
  }

  CoroToStateMachine& operator=(CoroToStateMachine rhs) {
    swap(rhs);
    return *this;
  }


};
}

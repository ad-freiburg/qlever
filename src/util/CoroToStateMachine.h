// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#pragma once

#include <exception>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>

#include "./Forward.h"

// Coroutines are still experimental in clang libcpp, therefore adapt the
// appropriate namespaces by including the convenience header.
#include "./Coroutines.h"

namespace ad_utility {

// Two empty structs used as tags for the `CoroToStateMachine`
namespace detail {
struct ValueWasPushedTag {};
struct NextValueTag {};
}  // namespace detail

static constexpr detail::ValueWasPushedTag valueWasPushedTag;
static constexpr detail::NextValueTag nextValueTag;
template <typename ValueType, bool isConst = false>
class CoroToStateMachine {
  using value_type = std::remove_reference_t<ValueType>;

 public:
  struct promise_type {
    using reference_type =
        std::conditional_t<isConst, const value_type&, value_type&>;
    using pointer_type =
        std::conditional_t<isConst, const ValueType*, ValueType*>;

    bool _isFinished = false;

    pointer_type _nextValue;
    std::exception_ptr _exception;

    // Don't suspend at the beginning, such that everything before the first
    // `co_await ValueWasPushed{}` is run eagerly and behaves like a
    // "constructor".
    constexpr std::suspend_never initial_suspend() noexcept { return {}; }
    // After the coroutine has finished we have no legal access to the promise
    // type, so we can as well destroy it.
    constexpr std::suspend_always final_suspend() noexcept { return {}; }

    void unhandled_exception() { _exception = std::current_exception(); }
    void return_void() {}

    // Create the actual `CoroToStateMachine` object, which gets a
    // `coroutine_handle` to access the `promise_type` object. Note: We only
    // return a `coroutine_handle` here that will later be implicitly converted
    // to a `CoroToStateMachine` object. This way, the exceptions that occur in
    // the "constructor" part of the coroutine make the construction of the
    // coroutine throw directly.
    std::coroutine_handle<promise_type> get_return_object() {
      return std::coroutine_handle<promise_type>::from_promise(*this);
    }

    // Rethrow an exception that occured in the coroutine.
    void rethrow_if_exception() {
      if (_exception) {
        std::rethrow_exception(_exception);
      }
    }

    // `co_await valueWasPushedTag` suspends the coroutine. As soon as it
    // resumes (either from a call to `push` or `finish`) either the next value
    // is stored in the promise type (`push`), or `isFinished` is true
    // (`finish`);
    struct ValueWasPushedAwaitable {
      promise_type* _promise_type;
      bool await_ready() { return false; }
      void await_suspend(std::coroutine_handle<>) {}
      bool await_resume() { return !_promise_type->_isFinished; }
    };

    // Transform the simple tags into an object that is aware of the
    // `promise_type` that knows, which coroutine it belongs to.
    ValueWasPushedAwaitable await_transform(detail::ValueWasPushedTag) {
      return {this};
    }

    // `co_await nextValueTag` immediately returns the next value from the
    // promise.
    struct NextValueAwaitable {
      promise_type* _promise_type;
      bool await_ready() { return true; }
      void await_suspend(std::coroutine_handle<>) {}
      reference_type await_resume() { return *_promise_type->_nextValue; }
    };
    NextValueAwaitable await_transform(detail::NextValueTag) { return {this}; }
  };

 private:
  using Handle = std::coroutine_handle<promise_type>;
  Handle _coro = nullptr;
  bool _isFinished = false;

 public:
  // Constructor that gets a handle on the actual coroutine frame.
  CoroToStateMachine(Handle handle) : _coro{handle} {
    // Rethrow the exceptions from the constructor part.
    if (_coro.done()) {
      _coro.promise().rethrow_if_exception();
    }
  }

 public:
  // Push the next value to the coroutine loop. Make the `co_await
  // valueWasPushed` return true and store the next value that will be retrieved
  // by `co_await nextValue`.Depending on the `isConst` parameter, `push` may
  // either be called only with non-const (`isConst == false`) or const
  // (`isConst == true`) references.
  void push(value_type& value) requires(!isConst) { pushImpl(AD_FWD(value)); }
  void push(value_type&& value) requires(!isConst) { pushImpl(AD_FWD(value)); }
  void push(const value_type& value) requires(isConst) {
    pushImpl(AD_FWD(value));
  }

 private:
  template <typename T>
  void pushImpl(T&& value) {
    _coro.promise()._nextValue = std::addressof(value);
    _coro.resume();
    if (_coro.done()) {
      finish();
    }
  }

 public:
  // Make `co_await valueWasPushed` return false, run the coroutine to
  // completion and destroy it.
  void finish() {
    if (_isFinished || !_coro) {
      return;
    }
    _isFinished = true;
    _coro.promise()._isFinished = true;
    if (!_coro.done()) {
      _coro.resume();
    }
    assert(_coro.done());
    _coro.promise().rethrow_if_exception();
  }

  // Implicitly call finished if it hasn't been called explicitly before.
  ~CoroToStateMachine() {
    finish();
    if (_coro) {
      _coro.destroy();
    }
  }

  // Uninitialized default constructor, move and swap.
  CoroToStateMachine() = default;
  CoroToStateMachine(CoroToStateMachine&& rhs) noexcept : _coro{rhs._coro} {
    rhs._coro = nullptr;
  }
  void swap(CoroToStateMachine& rhs) noexcept { std::swap(_coro, rhs._coro); }
  CoroToStateMachine& operator=(CoroToStateMachine rhs) {
    swap(rhs);
    return *this;
  }
};
}  // namespace ad_utility

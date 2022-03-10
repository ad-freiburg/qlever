// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#pragma once

#include <exception>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>
#include <future>

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

    value_type _nextValue;
    std::exception_ptr _exception;

    // Don't suspend at the beginning, such that everything before the first
    // `co_await ValueWasPushed{}` is run eagerly and behaves like a
    // "constructor".
    constexpr std::suspend_never initial_suspend() noexcept { return {}; }

    // We need to suspend at the end so that the `promise_type` is not immediately destroyed and we can properly propagate exceptions in the destructor or `finish`
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

    // The following code has these effects:
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

    // Transform the tag into an object that is aware of the
    // `promise_type` and knows which coroutine it belongs to.
    ValueWasPushedAwaitable await_transform(detail::ValueWasPushedTag) {
      return {this};
    }

    // The following code has these effects:
    // `co_await nextValueTag` immediately returns a reference to the most recent value that was
    // passed to `push`.
    struct NextValueAwaitable {
      promise_type* _promise_type;
      bool await_ready() { return true; }
      void await_suspend(std::coroutine_handle<>) {}
      reference_type await_resume() { return _promise_type->_nextValue; }
    };
    NextValueAwaitable await_transform(detail::NextValueTag) { return {this}; }
  };

 private:
  using Handle = std::coroutine_handle<promise_type>;
  Handle _coro = nullptr;
  bool _isFinished = false;

  std::future<void> _pushFuture;
  std::mutex _pushMutex;

 public:
  // Constructor that gets a handle to the coroutine frame.
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
  void push(value_type& value) { pushImpl(AD_FWD(value)); }
  void push(value_type&& value) { pushImpl(AD_FWD(value)); }
  void push(const value_type& value) requires(isConst) {
    pushImpl(AD_FWD(value));
  }

 private:
  template <typename T>
  void pushImpl(T&& value) {
    std::lock_guard l{_pushMutex};
    if (_pushFuture.valid()) {
      _pushFuture.get();
    }
    _coro.promise()._nextValue = AD_FWD(value);
    _coro.promise()._isFinished = false;
    _pushFuture = std::async(std::launch::async, [this] {
      //if (_coro && ! _coro.done()) {
      assert(_coro);
      assert(!_coro.done());
        _coro.resume();
      //}
        assert(!_coro.done());

      /*
      if (coro.done()) {
        // TODO<joka921> let this work also when using the future.
        //finish();
      }
       */
});
  }

 public:
  // The effect of calling this function is that the next
  // `co_await valueWasPushed` returns false such that the coroutine runs to
  // completion.
  void finish() {
    std::lock_guard l{_pushMutex};
    if (_pushFuture.valid()) {
      _pushFuture.get();
    }

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

  // The destructor implicitly calls `finish` if it hasn't been called explicitly before.
  ~CoroToStateMachine() {
    finish();
    if (_coro) {
      _coro.destroy();
    }
  }

  // Default constructor create an object without a coroutine. It can be destroyed or move-assigned from another `CoroToStateMachine`.
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

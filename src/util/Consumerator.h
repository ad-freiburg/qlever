// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#pragma once

#include <coroutine>
#include <exception>
#include <functional>
#include <type_traits>
#include <utility>

#include "util/ExceptionHandling.h"
#include "util/Forward.h"

namespace ad_utility {

// Two empty structs used as tags for the `CoroToStateMachine`
namespace detail {
struct ValueWasPushedTag {};
struct NextValueTag {};
}  // namespace detail

static constexpr detail::ValueWasPushedTag valueWasPushedTag;
static constexpr detail::NextValueTag nextValueTag;
template <typename ReferenceType>
class CoroToStateMachine {
  static_assert(std::is_reference_v<ReferenceType> ||
                    (std::is_trivially_copyable_v<ReferenceType> &&
                     sizeof(ReferenceType) < 32),
                "The `Consumerator` coroutine has to be used a reference type "
                "as its template argument for efficiency reasons (otherwise "
                "unnecessary copies would occur). The only exception of this "
                "rule are small trivially copyable objects wor which the copy "
                "is typically cheaper than taking a reference");

 public:
  struct promise_type {
    bool isFinished_ = false;

    ReferenceType nextValue_;
    std::exception_ptr exception_;

    // Don't suspend at the beginning, such that everything before the first
    // `co_await ValueWasPushed{}` is run eagerly and behaves like a
    // "constructor".
    constexpr std::suspend_never initial_suspend() const noexcept { return {}; }

    // We need to suspend at the end so that the `promise_type` is not
    // immediately destroyed and we can properly propagate exceptions in the
    // destructor or `finish`
    constexpr std::suspend_always final_suspend() const noexcept { return {}; }

    void unhandled_exception() { exception_ = std::current_exception(); }
    void return_void() const noexcept {}

    // Create the actual `CoroToStateMachine` object, which gets a
    // `coroutine_handle` to access the `promise_type` object. Note: We only
    // return a `coroutine_handle` here that will later be implicitly converted
    // to a `CoroToStateMachine` object. This way, the exceptions that occur in
    // the "constructor" part of the coroutine make the construction of the
    // coroutine throw directly.
    // std::coroutine_handle<promise_type> get_return_object() {
    CoroToStateMachine get_return_object() {
      return CoroToStateMachine{
          std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    // Rethrow an exception that occured in the coroutine.
    void rethrow_if_exception() const {
      if (exception_) {
        std::rethrow_exception(exception_);
      }
    }

    // The following code has these effects:
    // `co_await valueWasPushedTag` suspends the coroutine. As soon as it
    // resumes (either from a call to `push` or `finish`) either the next value
    // is stored in the promise type (`push`), or `isFinished` is true
    // (`finish`);
    struct ValueWasPushedAwaitable {
      promise_type& promise_;
      bool await_ready() const { return false; }
      void await_suspend(std::coroutine_handle<>) const {}
      bool await_resume() const { return !promise_.isFinished_; }
    };

    // Transform the tag into an object that is aware of the
    // `promise_type` and knows which coroutine it belongs to.
    ValueWasPushedAwaitable await_transform(detail::ValueWasPushedTag) {
      return {*this};
    }

    // The following code has these effects:
    // `co_await nextValueTag` immediately returns a reference to the most
    // recent value that was passed to `push`.
    struct NextValueAwaitable {
      promise_type& promise_;
      bool await_ready() const { return true; }
      void await_suspend(std::coroutine_handle<>) const {}
      ReferenceType await_resume() const { return promise_.nextValue_; }
    };
    NextValueAwaitable await_transform(detail::NextValueTag) { return {*this}; }
  };

 private:
  using Handle = std::coroutine_handle<promise_type>;
  Handle coro_ = nullptr;
  bool isFinished_ = false;

 public:
  // Constructor that gets a handle to the coroutine frame.
  // Note: The implicit conversion is required by the machinery.
  explicit(false) CoroToStateMachine(Handle handle) : coro_{handle} {
    // Rethrow the exceptions from the constructor part.
    if (coro_.done()) {
      coro_.promise().rethrow_if_exception();
    }
  }

  // Push the next value to the coroutine loop. Make the `co_await
  // valueWasPushed` return true and store the next value that will be retrieved
  // by `co_await nextValue`.Depending on the `isConst` parameter, `push` may
  // either be called only with non-const (`isConst == false`) or const
  // (`isConst == true`) references.
  void push(ReferenceType ref) { pushImpl(AD_FWD(ref)); }

 private:
  template <typename T>
  void pushImpl(T&& value) {
    coro_.promise().nextValue_ = AD_FWD(value);
    coro_.promise().isFinished_ = false;
    AD_EXPENSIVE_CHECK(coro_);
    if (coro_.done()) {
      finish();
    }
    coro_.resume();
  }

 public:
  // The effect of calling this function is that the next
  // `co_await valueWasPushed` returns false such that the coroutine runs to
  // completion.
  void finish() {
    if (!coro_ || coro_.promise().isFinished_) {
      return;
    }
    coro_.promise().isFinished_ = true;
    if (!coro_.done()) {
      coro_.resume();
    }
    AD_EXPENSIVE_CHECK(coro_.done());
    coro_.promise().rethrow_if_exception();
  }

  // The destructor implicitly calls `finish` if it hasn't been called
  // explicitly before.
  ~CoroToStateMachine() {
    ad_utility::terminateIfThrows(
        [this]() {
          finish();
          if (coro_) {
            coro_.destroy();
          }
        },
        "The finish method of a Consumerator, called inside the destructor.");
  }

  // Default constructor, move and swap operations.
  // Note: default-constructed and moved-from objects are invalid and may not be
  // accessed until a valid object has been assigned again (via move assignment
  // or swap).
  CoroToStateMachine() = default;
  CoroToStateMachine(CoroToStateMachine&& rhs) noexcept
      : coro_{std::exchange(rhs.coro_, nullptr)} {}
  void swap(CoroToStateMachine& rhs) noexcept { std::swap(coro_, rhs.coro_); }
  CoroToStateMachine& operator=(CoroToStateMachine rhs) {
    swap(rhs);
    return *this;
  }
};
}  // namespace ad_utility

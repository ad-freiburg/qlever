// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#pragma once

#include <coroutine>
#include <exception>
#include <functional>
#include <type_traits>
#include <utility>

#include "util/Exception.h"
#include "util/Forward.h"
#include "util/OnDestructionDontThrowDuringStackUnwinding.h"

namespace ad_utility {
/*
This implements `Consumers` which are in some sense the opposite of
`generators`. This is best explained by an example:
ConsumerImpl<const std::string&> wordsToFile(const std::string& filename) {
  // Initial portion of the code, executed during the construction phase
  auto file = File{filename, "w"};
  // The inner loop, the structure of the `co_await`s must be always like
  //this, otherwise you get undefined behavior.
  while (co_await valueWasPushedTag) {
    // This loop sees all the values that are `push`ed to the `Consumer`.
    const string& word = co_await nextValueTag;
    file.append(word);
    file.append("\n");
  }
  // Remaining final code, will be executed when `finish` or the destructor is
  // called, whichever happens first.
  file.append("END OF INPUT\n");
}

// This code can be used as follows:
auto consumer = ad_utility::makeConsumer(wordsToFile("words.txt));
// These values appear as the `val` variable in the inner loop
consumer("first line");
consumer("second line");
// break out of the loop
consumer.finish();
 // The file is now closed, and has the contents "first line\nsecond line\nEND OF INPUT\n".

In the following we will describe several aspects of the interfaces.
1. The template argument is the argument type to the `push` function in the
calling code as well as the result type of the `co_await nextValueTag` calls
inside the coroutine loop. It has to either be a reference of any kind (e.g.
string&&, const string&, etc.) Or a small trivially-copyable object type (e.g.
int or std::string_view). The reason for this restriction is to prevent
accidental copies of large objects. If you need copies, either use `const &` and
then copy inside the coroutine, or use `&&` and create the copies for the `push`
function.
2. Exceptions that appear in the first part of the coroutine (before the first
call to `co_await valueWasPushed`) are directly rethrown in the
`makeConsumer()` function. Exceptions that happen inside the loop (i.e.
between two calls to `valueWasPushed` are propagated to the corresponding `push`
call. Exceptions in the last part of the code are propagated to the call to
`finish()` or, if that call is missing or skipped to the destructor. The
destructor only throws if it is not executed during stack unwinding because it
would then crash the program. This scenario is detected and logged, and the
exception is then ignored.
3. For the same reason it is not very useful to store consumers in
containers like `std::vectors` because exceptions in destructors lead to direct
program termination. To make this less probably, Consumers are neither
copy-nor movable. If you really need to store some of them, use `unique_ptr` or
`optional`, but beware e.g. of the `optional::reset()` function which is
unconditionally `noexcept`.
 4. Unfortunately it is necessary to implement the actual coroutine as a
`ConsumerImpl` and then obtain the actual object via `makeConsumer`.
There are currently three reasons, why this is necessary: 4.1 Without it, the
exceptions from the first part of the execution (the "constructor") couldn't be
directly propagated, but only at the first call to push. Consider for example a
Consumer that writes to a file, and the opening of the file fails, and we
want to find out immediately. This can only be achieved with the additional
helper functions because of the internal design of C++20 coroutines and their
initialization. 4.2 G++ currently (versions 11-13) has a bug, that crashes the
compiler when a coroutine has a `noexcept(false)` destructor (see 2.). With the
wrapper function we can work around this bug. 4.3  With the wrapper function we
can make all constructors of the `Consumer` class private. That way we can
rule out several potential bugs (see 3.).
5. A lot of systematic examples can be found in the tests (`ConsumerTest.h`)
 */

// Two empty structs used as tags for the `ConsumerImpl`
namespace detail {
struct ValueWasPushedTag {};
struct NextValueTag {};
}  // namespace detail

static constexpr detail::ValueWasPushedTag valueWasPushedTag;
static constexpr detail::NextValueTag nextValueTag;
template <typename ReferenceType>
class ConsumerImpl {
  static_assert(
      std::is_reference_v<ReferenceType> ||
          (std::is_trivially_copyable_v<ReferenceType> &&
           sizeof(ReferenceType) < 32),
      "The `ConsumerImpl` coroutine has to be used a reference type "
      "as its template argument for efficiency reasons (otherwise "
      "unnecessary copies would occur). The only exception of this "
      "rule are small trivially copyable objects wor which the copy "
      "is typically cheaper than taking a reference");

 public:
  struct promise_type {
    bool isFinished_ = false;
    bool isFirst_ = true;
    using ValueType = std::remove_reference_t<ReferenceType>;

    using PointerType = std::add_pointer_t<ReferenceType>;
    static constexpr bool isLvalueRef =
        std::is_lvalue_reference_v<ReferenceType>;
    using StoredType = std::conditional_t<isLvalueRef, PointerType, ValueType>;

   private:
    StoredType nextValue_;

   public:
    ReferenceType getValue() {
      if constexpr (isLvalueRef) {
        return *nextValue_;
      } else {
        return std::move(nextValue_);
      }
    }
    void setValue(ReferenceType value) {
      if constexpr (isLvalueRef) {
        nextValue_ = std::addressof(value);
      } else {
        nextValue_ = AD_FWD(value);
      }
    }
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

    // Create the actual `ConsumerImpl` object, which gets a
    // `coroutine_handle` to access the `promise_type` object. Note: We only
    // return a `coroutine_handle` here that will later be implicitly converted
    // to a `ConsumerImpl` object. This way, the exceptions that occur in
    // the "constructor" part of the coroutine make the construction of the
    // coroutine throw directly.
    // std::coroutine_handle<promise_type> get_return_object() {
    ConsumerImpl get_return_object() {
      return ConsumerImpl{
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
      bool await_ready() const {
        if (std::exchange(promise_.isFirst_, false)) {
          promise_.rethrow_if_exception();
        }
        return false;
      }
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
      ReferenceType await_resume() const { return promise_.getValue(); }
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
  explicit ConsumerImpl(Handle handle) : coro_{handle} {}

  template <typename T>
  friend class Consumer;

 private:
  // Push the next value to the coroutine loop. Make the `co_await
  // valueWasPushed` return true and store the next value that will be retrieved
  // by `co_await nextValue`.Depending on the `isConst` parameter, `push` may
  // either be called only with non-const (`isConst == false`) or const
  // (`isConst == true`) references.
  void operator()(ReferenceType value) {
    coro_.promise().setValue(AD_FWD(value));
    coro_.promise().isFinished_ = false;
    AD_EXPENSIVE_CHECK(coro_);
    if (coro_.done()) {
      finish();
    } else {
      coro_.resume();
    }
    if (coro_.done()) {
      finish();
    }
  }

  void finishIfException() {
    if (!coro_) {
      return;
    }
    if (!coro_ || !coro_.done() || !coro_.promise().exception_) {
      return;
    }
    auto ptr = std::move(coro_.promise().exception_);
    coro_.promise().isFinished_ = true;
    coro_.destroy();
    coro_ = nullptr;
    std::rethrow_exception(ptr);
  }
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
    auto ptr = std::move(coro_.promise().exception_);
    coro_.destroy();
    coro_ = nullptr;
    if (ptr) {
      std::rethrow_exception(ptr);
    }
  }

 public:
  // The destructor implicitly calls `finish` if it hasn't been called
  // explicitly before.
  ~ConsumerImpl() {
    ad_utility::ignoreExceptionIfThrows([this]() { finish(); });
  }

  // Default constructor, move and swap operations.
  // Note: default-constructed and moved-from objects are invalid and may not be
  // accessed until a valid object has been assigned again (via move assignment
  // or swap).
  ConsumerImpl() = default;
  ConsumerImpl(ConsumerImpl&& rhs) noexcept
      : coro_{std::exchange(rhs.coro_, nullptr)} {}
  void swap(ConsumerImpl& rhs) noexcept { std::swap(coro_, rhs.coro_); }
  ConsumerImpl& operator=(ConsumerImpl rhs) {
    swap(rhs);
    return *this;
  }
};

template <typename T>
struct Consumer {
 private:
  ConsumerImpl<T> consumer_;
  int numExceptionsDuringConstruction_ = std::uncaught_exceptions();

  explicit Consumer(ConsumerImpl<T> consumer)
      : consumer_{std::move(consumer)} {
    consumer_.finishIfException();
  }
  Consumer(Consumer&&) = delete;
  Consumer& operator=(Consumer&&) = delete;

 public:
  void operator()(T t) { consumer_(AD_FWD(t)); }
  void finish() { consumer_.finish(); }
  ~Consumer() noexcept(false) {
    if (numExceptionsDuringConstruction_ == std::uncaught_exceptions()) {
      finish();
    } else {
      ad_utility::ignoreExceptionIfThrows(
          [this]() { finish(); }, "Destructor of `ad_utility::Consumer`");
    }
  }
  template <typename U>
  friend Consumer<U> makeConsumer(ConsumerImpl<U> consumerImpl);
};
template <typename T>
Consumer<T> makeConsumer(ConsumerImpl<T> consumerImpl) {
  return Consumer{std::move(consumerImpl)};
}
}  // namespace ad_utility

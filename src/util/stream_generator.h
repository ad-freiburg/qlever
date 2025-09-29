// Copyright 2021 - 2025 The QLever Authors, in particular:
//
// 2021 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_STREAM_GENERATOR_H
#define QLEVER_SRC_UTIL_STREAM_GENERATOR_H

// This file consists of:
//
// 1. A generator-like type `ad_utility::streams::stream_generator`, in which
// one can `co_yield` `string_view`s, which are then concatenated before being
// yielded to the consumer. This type and functionality is not available when
// `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set.
//
// 2. A class `ad_utility::streams::StringBatcher`, which is a callable type
// that concatenates the `string_view`s with which it is invoked and in turn
// invokes a user-provided callback with the concatenated result.
//
// 3. Several macros that can be used to make a generator-like function either
// using the `stream_generator` or the `StringBatcher`, depending on whether
// `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set or not.

#include <cstring>
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <coroutine>
#include <exception>
#include <sstream>

#include "util/CompilerWarnings.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"
#endif

namespace ad_utility::streams {

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
template <size_t BUFFER_SIZE>
class basic_stream_generator;

namespace detail {
// A Promise for a generator type needs to indicate if a coroutine should
// actually get suspended on co_yield or co_await or if there's a shortcut
// that allows to continue execution without interruption. The standard library
// provides std::suspend_always and std::suspend_never for the most common
// cases, but this class allows to chose between one of the two options
// dynamically using a simple bool.
class suspend_sometimes {
  const bool _suspend;

 public:
  explicit suspend_sometimes(const bool suspend) : _suspend{suspend} {}
  bool await_ready() const noexcept { return !_suspend; }
  constexpr void await_suspend(std::coroutine_handle<>) const noexcept {}
  constexpr void await_resume() const noexcept {}
};

// The promise type that backs the generator type and handles storage and
// suspension-related decisions.
template <size_t BUFFER_SIZE>
class stream_generator_promise {
  std::array<char, BUFFER_SIZE> data_;
  size_t currentIndex_ = 0;
  static_assert(BUFFER_SIZE > 0, "Buffer size must be greater than zero");
  // Temporarily store data that didn't fit into the buffer so far.
  std::string_view overflow_;
  std::exception_ptr exception_;

 public:
  using value_type = std::string_view;
  using reference_type = std::string_view;
  using pointer_type = value_type*;
  stream_generator_promise() = default;

  basic_stream_generator<BUFFER_SIZE> get_return_object() noexcept;

  constexpr std::suspend_always initial_suspend() const noexcept { return {}; }
  constexpr std::suspend_always final_suspend() const noexcept { return {}; }

  // Handles strings passed using co_yield and copies them to the aggregated
  // buffer until that buffer is full. If the buffer is too small to fit the
  // value in its entirety, the coroutine is suspended (and thus the value kept
  // alive) until the buffer has been consumed and has sufficient capacity. In
  // this case the buffer will be filled to maximum capacity, so eventually
  // every bit of `value` is written, then the coroutine will be resumed.
  suspend_sometimes yield_value(std::string_view value) noexcept {
    if (isBufferLargeEnough(value)) {
      std::memcpy(data_.data() + currentIndex_, value.data(), value.size());
      currentIndex_ += value.size();
      overflow_ = {};
      // Only suspend if we reached the maximum capacity exactly.
      return suspend_sometimes{currentIndex_ == BUFFER_SIZE};
    }
    size_t fittingSize = BUFFER_SIZE - currentIndex_;
    std::memcpy(data_.data() + currentIndex_, value.data(), fittingSize);
    currentIndex_ = BUFFER_SIZE;
    overflow_ = value.substr(fittingSize);
    return suspend_sometimes{true};
  }

  // Overload so we can also pass char values, template such that all types that
  // implicitly convert to char are not accepted.
  CPP_template(typename CharT)(requires SimilarTo<CharT, char>)
      suspend_sometimes yield_value(CharT value) {
    std::string_view singleView{&value, 1};
    // This is only safe to do if we can write into the buffer immediately.
    AD_CORRECTNESS_CHECK(isBufferLargeEnough(singleView));
    // Disable false positive warning on GCC.
    DISABLE_OVERREAD_WARNINGS
    return yield_value(singleView);
    GCC_REENABLE_WARNINGS
  }

  // Return true if the overflow has been completely consumed.
  bool doneProcessing() const noexcept {
    return overflow_.empty() && currentIndex_ == 0;
  }

  // Reset buffer and start writing the overflow in it. Return true if the
  // buffer still has capacity after this, false otherwise.
  bool commitOverflow() noexcept {
    currentIndex_ = 0;
    return yield_value(overflow_).await_ready();
  }

  void unhandled_exception() { exception_ = std::current_exception(); }

  constexpr void return_void() const noexcept {}

  reference_type value() const noexcept {
    return std::string_view{data_.data(), currentIndex_};
  }

  // Don't allow any use of 'co_await' inside the generator coroutine.
  template <typename U>
  std::suspend_never await_transform(U&& value) = delete;

  void rethrow_if_exception() {
    if (exception_) {
      std::rethrow_exception(exception_);
    }
  }

 private:
  // Return true if the buffer still has enough capacity remaining to copy
  // `value` in its entirety.
  bool isBufferLargeEnough(std::string_view value) const {
    return currentIndex_ + value.size() <= BUFFER_SIZE;
  }
};

struct stream_generator_sentinel {};

template <size_t BUFFER_SIZE>
class stream_generator_iterator {
  using promise_type = stream_generator_promise<BUFFER_SIZE>;
  using coroutine_handle = std::coroutine_handle<promise_type>;

 public:
  using iterator_category = std::input_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = std::string_view;
  using reference = std::string_view;
  using pointer = value_type*;

  // Iterator needs to be default-constructible to satisfy the Range concept.
  stream_generator_iterator() noexcept : coroutine_{nullptr} {}

  explicit stream_generator_iterator(coroutine_handle coroutine) noexcept
      : coroutine_{coroutine} {}

  friend bool operator==(const stream_generator_iterator& it,
                         stream_generator_sentinel) noexcept {
    // If the coroutine is done processing, but the aggregated string
    // has not been read so far the iterator needs to increment its value again.
    return !it.coroutine_ ||
           (it.coroutine_.done() && it.coroutine_.promise().doneProcessing());
  }

  friend bool operator!=(const stream_generator_iterator& it,
                         stream_generator_sentinel s) noexcept {
    return !(it == s);
  }

  friend bool operator==(stream_generator_sentinel s,
                         const stream_generator_iterator& it) noexcept {
    return (it == s);
  }

  friend bool operator!=(stream_generator_sentinel s,
                         const stream_generator_iterator& it) noexcept {
    return it != s;
  }

  stream_generator_iterator& operator++() {
    // Process overflow first, true means that the buffer still has capacity
    // left.
    if (coroutine_.promise().commitOverflow()) {
      if (!coroutine_.done()) {
        coroutine_.resume();
      }
      if (coroutine_.done()) {
        coroutine_.promise().rethrow_if_exception();
      }
    }

    return *this;
  }

  // Need to provide post-increment operator to implement the 'Range' concept.
  // The void return type disables any invalid uses, since this iterator does
  // not support post-increment
  void operator++(int) { (void)operator++(); }

  reference operator*() noexcept { return coroutine_.promise().value(); }

  pointer operator->() noexcept { return std::addressof(operator*()); }

 private:
  coroutine_handle coroutine_;
};

}  // namespace detail

// The implementation of the generator.
// Use this as the return type of your coroutine.
//
// Example:
// stream_generator example() {
//   co_yield "Hello World";
// }
template <size_t BUFFER_SIZE>
class [[nodiscard]] basic_stream_generator {
 public:
  using promise_type = detail::stream_generator_promise<BUFFER_SIZE>;
  using iterator = detail::stream_generator_iterator<BUFFER_SIZE>;
  using value_type = typename iterator::value_type;

 private:
  std::coroutine_handle<promise_type> coroutine_ = nullptr;

  static basic_stream_generator noOpGenerator() { co_return; }

 public:
  basic_stream_generator() : basic_stream_generator(noOpGenerator()) {}

  basic_stream_generator(basic_stream_generator&& other) noexcept
      : coroutine_{other.coroutine_} {
    other.coroutine_ = nullptr;
  }

  basic_stream_generator(const basic_stream_generator& other) = delete;

  ~basic_stream_generator() {
    if (coroutine_) {
      coroutine_.destroy();
    }
  }

  basic_stream_generator& operator=(basic_stream_generator&& other) noexcept {
    std::swap(coroutine_, other.coroutine_);
    return *this;
  }

  iterator begin() {
    if (coroutine_) {
      coroutine_.resume();
      if (coroutine_.done()) {
        coroutine_.promise().rethrow_if_exception();
      }
    }

    return iterator{coroutine_};
  }

  detail::stream_generator_sentinel end() noexcept {
    return detail::stream_generator_sentinel{};
  }

 private:
  friend class detail::stream_generator_promise<BUFFER_SIZE>;
  explicit basic_stream_generator(
      std::coroutine_handle<promise_type> coroutine) noexcept
      : coroutine_{coroutine} {}
};

namespace detail {
template <size_t BUFFER_SIZE>
inline basic_stream_generator<BUFFER_SIZE>
stream_generator_promise<BUFFER_SIZE>::get_return_object() noexcept {
  using coroutine_handle =
      std::coroutine_handle<stream_generator_promise<BUFFER_SIZE>>;
  return basic_stream_generator{coroutine_handle::from_promise(*this)};
}
}  // namespace detail

// Use 1MiB buffer size by default
using stream_generator = basic_stream_generator<1u << 20>;

#endif

// A class that can be fed `string_view`s (via `operator()`) and concatenates
// them until a certain size (the `BATCH_SIZE`) is reached, at which point it
// invokes a callback with the concatenated result.
//
// NOTE: A `string_view` that is pushed via `operator()` might and often will be
// split up between two callback invocations. The callback for the final batch
// is invoked either in the destructor or via an explicit call to `finish()`.
template <size_t BATCH_SIZE = 1u << 20>
class StringBatcher {
  using CallbackForBatches = std::function<void(std::string_view)>;
  CallbackForBatches callbackForBatches_;
  std::array<char, BATCH_SIZE> currentBatch_;
  size_t currentBatchSize_ = 0;
  static_assert(BATCH_SIZE > 0, "Buffer size must be greater than zero");

 public:
  // Construct by specifying the callback.
  explicit StringBatcher(CallbackForBatches callback)
      : callbackForBatches_(std::move(callback)) {}

  // Add a string to the current batch, invoke the callback if the batch is
  // full.
  void operator()(std::string_view value) {
    auto sizeToCopy = fittingSize(value);
    std::memcpy(currentBatch_.data() + currentBatchSize_, value.data(),
                sizeToCopy);
    currentBatchSize_ += sizeToCopy;
    if (currentBatchSize_ == BATCH_SIZE) {
      commit();
    }
    // If the `value` was only partially stored in the previous batch, call this
    // function again with the unconsumed remainder.
    if (sizeToCopy < value.size()) {
      value.remove_prefix(sizeToCopy);
      (*this)(value);
    }
  }

  // Overload for pushing a single character.
  void operator()(char c) { (*this)(std::string_view{&c, 1}); }

  // Commit the last batch after the last string has been pushed. Is also
  // invoked by the destructor.
  void finish() {
    if (currentBatchSize_ > 0) {
      commit();
    }
  }

  // The destructor also commits the last incomplete batch. If this is not
  // desired, make sure to explicitly call `finish` before destroying the
  // `StringBatcher`.
  ~StringBatcher() { finish(); }

  // Disallow copying or moving, because there are no clear semantics as for how
  // the remaining partial batches should behave.
  StringBatcher(const StringBatcher&) = delete;
  StringBatcher& operator=(const StringBatcher&) = delete;
  StringBatcher(StringBatcher&&) = delete;
  StringBatcher& operator=(StringBatcher&&) = delete;

 private:
  // Return the size of a substring of `value` that can be stored in the current
  // batch without the batch exceeding the `BATCH_SIZE`.
  size_t fittingSize(std::string_view value) const {
    return std::min(value.size(), BATCH_SIZE - currentBatchSize_);
  }

  // Invoke the callback with the `currentBatch_`, and reset the
  // `currentBatch_`.
  void commit() {
    callbackForBatches_(
        std::string_view{currentBatch_.data(), currentBatchSize_});
    currentBatchSize_ = 0;
  }
};
}  // namespace ad_utility::streams

// Define macros to implement a coroutine-like generator that batches strings
// together, when `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is not set (called
// C++20 mode in the following), and when it is set (called C++17 mode in the
// following).
//
// 1. `STREAMABLE_GENERATOR_TYPE` is `stream_generator` (in C++20 mode) or
// `void` (in C++17 mode). In C++20 mode, it defines the coroutine mechanics,
// whereas in C++17 mode, the "yielding" is done via a callback argument.
//
// 2. `STREAMABLE_YIELDER_TYPE` is the type of a mandatory argument with the
// hardcoded name `streamableYielder`, which each function needs to have. It has
// to be the last argument so that it can be defaulted. In C++20 mode, this is
// `int` (a dummy type that is not used), whereas in C++17 mode, it is
// `reference_wrappper<StringBatcher>` (the actual callback that is invoked for
// each "yielded" string).
//
// 3. `STREAMABLE_YIELDER_ARG_DECL` declares the `streamableYielder` argument
// with the proper attributes (which can be defaulted in C++20 mode, where it is
// a dummy).
//
// 4. `STREAMABLE_YIELD(someString)` is either `co_yield someString` (in C++20
// mode) or a call to the `streamableYielder` callback with `someString (in
// C++17 mode).
//
// 5. `STREAMABLE_RETURN` is either `co_return` (in C++20 mode) or `return` (in
// C++17 mode).
//
// To see these macros in action, see the examples in `StringBatcherTest.pp`,
// and their usage in `ExportQueryExecutionTrees.{h,cpp}`.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
using STREAMABLE_GENERATOR_TYPE = ad_utility::streams::stream_generator;
using STREAMABLE_YIELDER_TYPE = int;
#define STREAMABLE_YIELDER_ARG_DECL \
  [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder = {}
#define STREAMABLE_YIELD(...) co_yield __VA_ARGS__
#define STREAMABLE_RETURN co_return

#else

using STREAMABLE_GENERATOR_TYPE = void;
using STREAMABLE_YIELDER_TYPE =
    std::reference_wrapper<ad_utility::streams::StringBatcher<>>;
#define STREAMABLE_YIELDER_ARG_DECL STREAMABLE_YIELDER_TYPE streamableYielder
#define STREAMABLE_YIELD(...) streamableYielder(__VA_ARGS__)
#define STREAMABLE_RETURN return;

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_STREAM_GENERATOR_H

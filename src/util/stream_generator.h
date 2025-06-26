// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_STREAM_GENERATOR_H
#define QLEVER_SRC_UTIL_STREAM_GENERATOR_H

#include <coroutine>
#include <exception>
#include <sstream>

#include "util/Exception.h"

namespace ad_utility::streams {

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
  std::string_view overflowBuffer_;
  std::exception_ptr exception_;

 public:
  using value_type = std::string_view;
  using reference_type = value_type&;
  using pointer_type = value_type*;
  stream_generator_promise() = default;

  basic_stream_generator<BUFFER_SIZE> get_return_object() noexcept;

  constexpr std::suspend_always initial_suspend() const noexcept { return {}; }
  constexpr std::suspend_always final_suspend() const noexcept { return {}; }

  // Handles strings passed using co_yield and copies them to an aggregated
  // buffer.
  suspend_sometimes yield_value(std::string_view value) noexcept {
    if (isBufferLargeEnough(value)) {
      std::memcpy(data_.data() + currentIndex_, value.data(), value.size());
      currentIndex_ += value.size();
      overflowBuffer_ = {};
      // Only suspend if we reached the maximum capacity exactly.
      return suspend_sometimes{currentIndex_ == BUFFER_SIZE};
    }
    size_t fittingSize = BUFFER_SIZE - currentIndex_;
    std::memcpy(data_.data() + currentIndex_, value.data(), fittingSize);
    currentIndex_ = BUFFER_SIZE;
    overflowBuffer_ = value.substr(fittingSize);
    return suspend_sometimes{true};
  }

  suspend_sometimes yield_value(char value) noexcept {
    std::string_view singleView{&value, 1};
    // This is only safe to do if we can write into the buffer immediately.
    AD_CORRECTNESS_CHECK(isBufferLargeEnough(singleView));
    return yield_value(singleView);
  }

  // Prevent accidental implicit conversions to char
  suspend_sometimes yield_value(int value) noexcept = delete;

  bool doneProcessing() const noexcept {
    return overflowBuffer_.empty() && currentIndex_ == 0;
  }

  // Reset buffer and start writing from 0.
  bool commitOverflow() noexcept {
    currentIndex_ = 0;
    return yield_value(overflowBuffer_).await_ready();
  }

  void unhandled_exception() { exception_ = std::current_exception(); }

  constexpr void return_void() const noexcept {}

  value_type value() const noexcept {
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
  // What type should we use for counting elements of a potentially infinite
  // sequence?
  using difference_type = std::ptrdiff_t;
  using value_type = std::string;
  using reference = value_type&;
  using pointer = value_type*;

  // Iterator needs to be default-constructible to satisfy the Range concept.
  stream_generator_iterator() noexcept : coroutine_{nullptr} {}

  explicit stream_generator_iterator(coroutine_handle coroutine) noexcept
      : coroutine_{coroutine}, value_{coroutine_.promise().value()} {}

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
    // Process overflow first
    if (coroutine_.promise().commitOverflow()) {
      if (!coroutine_.done()) {
        coroutine_.resume();
      }
      if (coroutine_.done()) {
        coroutine_.promise().rethrow_if_exception();
      }
    }

    value_ = std::string{coroutine_.promise().value()};

    return *this;
  }

  // Need to provide post-increment operator to implement the 'Range' concept.
  // The void return type disables any invalid uses, since this iterator does
  // not support post-increment
  void operator++(int) { (void)operator++(); }

  reference operator*() noexcept { return value_; }

  pointer operator->() noexcept { return std::addressof(operator*()); }

 private:
  coroutine_handle coroutine_;
  value_type value_;
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
  std::coroutine_handle<promise_type> _coroutine = nullptr;

  static basic_stream_generator noOpGenerator() { co_return; }

 public:
  basic_stream_generator() : basic_stream_generator(noOpGenerator()) {};

  basic_stream_generator(basic_stream_generator&& other) noexcept
      : _coroutine{other._coroutine} {
    other._coroutine = nullptr;
  }

  basic_stream_generator(const basic_stream_generator& other) = delete;

  ~basic_stream_generator() {
    if (_coroutine) {
      _coroutine.destroy();
    }
  }

  basic_stream_generator& operator=(basic_stream_generator&& other) noexcept {
    std::swap(_coroutine, other._coroutine);
    return *this;
  }

  iterator begin() {
    if (_coroutine) {
      _coroutine.resume();
      if (_coroutine.done()) {
        _coroutine.promise().rethrow_if_exception();
      }
    }

    return iterator{_coroutine};
  }

  detail::stream_generator_sentinel end() noexcept {
    return detail::stream_generator_sentinel{};
  }

 private:
  friend class detail::stream_generator_promise<BUFFER_SIZE>;
  explicit basic_stream_generator(
      std::coroutine_handle<promise_type> coroutine) noexcept
      : _coroutine{coroutine} {}
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
}  // namespace ad_utility::streams

#endif  // QLEVER_SRC_UTIL_STREAM_GENERATOR_H

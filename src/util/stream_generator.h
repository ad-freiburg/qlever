// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

// For some include orders the EOF constant is not defined although `<cstdio>`
// was included, so we define it manually.
// TODO<joka921> Find out where this happens.
#ifndef EOF
#define EOF std::char_traits<char>::eof()
#endif

#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <exception>
#include <sstream>

// Coroutines are still experimental in clang libcpp, therefore
// adapt the appropriate namespaces using the convenience header.
#include "./Concepts.h"
#include "./Coroutines.h"
#include "./Exception.h"

namespace ad_utility::streams {

template <size_t MIN_BUFFER_SIZE>
class basic_stream_generator;

namespace detail {
namespace io = boost::iostreams;
/**
 * A Promise for a generator type needs to indicate if a coroutine should
 * actually get suspended on co_yield or co_await or if there's a shortcut
 * that allows to continue execution without interruption. The standard library
 * provides std::suspend_always and std::suspend_never for the most common
 * cases, but this class allows to chose between one of the two options
 * dynamically using a simple bool.
 */
class suspend_sometimes {
  const bool _suspend;

 public:
  explicit suspend_sometimes(const bool suspend) : _suspend{suspend} {}
  bool await_ready() const noexcept { return !_suspend; }
  constexpr void await_suspend(std::coroutine_handle<>) const noexcept {}
  constexpr void await_resume() const noexcept {}
};

/**
 * The promise type that backs the generator type and handles storage and
 * suspension-related decisions.
 */
template <size_t MIN_BUFFER_SIZE>
class stream_generator_promise {
  std::ostringstream _stream;
  std::exception_ptr _exception;
  std::exception_ptr* _externalException = nullptr;

 public:
  using value_type = std::stringbuf;
  using reference_type = value_type&;
  using pointer_type = value_type*;
  stream_generator_promise() = default;

  basic_stream_generator<MIN_BUFFER_SIZE> get_return_object() noexcept;

  constexpr std::suspend_always initial_suspend() const noexcept { return {}; }
  constexpr std::suspend_always final_suspend() const noexcept { return {}; }

  /**
   * Handles values passed using co_yield and stores their respective
   * string representations inside a buffer.
   *
   * @tparam T The Type being passed
   * @param value The value being passed via co_yield
   * @return Whether or not the coroutine should get suspended (currently based
   * on isBufferLargeEnough()), wrapped inside a suspend_sometimes class.
   */
  suspend_sometimes yield_value(
      const ad_utility::Streamable auto& value) noexcept {
    // _stream appends its result to _value
    _stream << value;
    return suspend_sometimes{isBufferLargeEnough()};
  }

  void unhandled_exception() {
    _exception = std::current_exception();
    if (_externalException) {
      *_externalException = _exception;
    }
  }

  constexpr void return_void() const noexcept {}

  reference_type value() noexcept { return *_stream.rdbuf(); }

  // Don't allow any use of 'co_await' inside the generator coroutine.
  template <typename U>
  std::suspend_never await_transform(U&& value) = delete;

  void rethrow_if_exception() {
    if (_exception) {
      std::rethrow_exception(_exception);
    }
  }

  void assignExceptionToThisPointer(std::exception_ptr* ptr) {
    _externalException = ptr;
  }

 private:
  bool isBufferLargeEnough() {
    return _stream.view().length() >= MIN_BUFFER_SIZE;
  }
};

struct stream_generator_sentinel {};

template <size_t MIN_BUFFER_SIZE>
class stream_generator_iterator {
  using promise_type = stream_generator_promise<MIN_BUFFER_SIZE>;
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
  stream_generator_iterator() noexcept : _coroutine(nullptr) {}

  explicit stream_generator_iterator(coroutine_handle coroutine) noexcept
      : _coroutine(coroutine),
        _value{std::move(coroutine.promise().value()).str()} {}

  friend bool operator==(const stream_generator_iterator& it,
                         stream_generator_sentinel) noexcept {
    // If the coroutine is done processing, but the aggregated string
    // has not been read so far the iterator needs to increment its value
    // one last time.
    return !it._coroutine || (it._coroutine.done() && it._value.empty());
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
    _value.clear();
    _coroutine.promise().value().str(std::move(_value));
    // if the coroutine is done but the remaining aggregated
    // buffer has not been cleared yet the iterator needs to be incremented one
    // last time
    if (!_coroutine.done()) {
      _coroutine.resume();
    }
    if (_coroutine.done()) {
      _coroutine.promise().rethrow_if_exception();
    }

    _value = std::move(_coroutine.promise().value()).str();

    return *this;
  }

  // Need to provide post-increment operator to implement the 'Range' concept.
  // The void return type disables any invalid uses, since this iterator does
  // not support post-increment
  void operator++(int) { (void)operator++(); }

  reference operator*() noexcept { return _value; }

  pointer operator->() noexcept { return std::addressof(operator*()); }

 private:
  coroutine_handle _coroutine;
  value_type _value;
};

}  // namespace detail

/**
 * The implementation of the generator.
 * Use this as the return type of your coroutine.
 *
 * Example:
 * @code
 * stream_generator example() {
 *   co_yield "Hello World";
 * }
 */
template <size_t MIN_BUFFER_SIZE>
class [[nodiscard]] basic_stream_generator {
 public:
  using promise_type = detail::stream_generator_promise<MIN_BUFFER_SIZE>;
  using iterator = detail::stream_generator_iterator<MIN_BUFFER_SIZE>;
  using value_type = typename iterator::value_type;

 private:
  std::coroutine_handle<promise_type> _coroutine = nullptr;

  static basic_stream_generator noOpGenerator() { co_return; }

 public:
  basic_stream_generator() : basic_stream_generator(noOpGenerator()){};

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

  // When an exception occurs while iterating, that exception will (in addition
  // to being thrown "normally") be stored in `*ptr`. This can be used to access
  // exceptions that occured inside a `stream_generator`, even after the
  // `stream_generator` was destroyed.
  void assignExceptionToThisPointer(std::exception_ptr* ptr) {
    _coroutine.promise().assignExceptionToThisPointer(ptr);
  }

 private:
  friend class detail::stream_generator_promise<MIN_BUFFER_SIZE>;
  explicit basic_stream_generator(
      std::coroutine_handle<promise_type> coroutine) noexcept
      : _coroutine{coroutine} {}
};

namespace detail {
template <size_t MIN_BUFFER_SIZE>
inline basic_stream_generator<MIN_BUFFER_SIZE>
stream_generator_promise<MIN_BUFFER_SIZE>::get_return_object() noexcept {
  using coroutine_handle =
      std::coroutine_handle<stream_generator_promise<MIN_BUFFER_SIZE>>;
  return basic_stream_generator{coroutine_handle::from_promise(*this)};
}
}  // namespace detail

// Use 1MiB buffer size by default
using stream_generator = basic_stream_generator<1u << 20>;
}  // namespace ad_utility::streams

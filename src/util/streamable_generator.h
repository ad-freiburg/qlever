// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <exception>
#include <sstream>

#include "./ostringstream.h"

// coroutines are still experimental in clang, adapt the appropriate namespaces.
#ifdef __clang__
#include <experimental/coroutine>
namespace qlever_stdOrExp = std::experimental;
#else
#include <coroutine>
namespace qlever_stdOrExp = std;
#endif

namespace ad_utility::stream_generator {

/**
 * A concept to ensure objects can be formatted by std::ostream.
 * @tparam T The Type to be formatted
 */
template <typename T>
concept Streamable = requires(T x, std::ostream& os) {
  os << x;
};

template <size_t MIN_BUFFER_SIZE>
class basic_stream_generator;

namespace detail {

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
  constexpr void await_suspend(
      qlever_stdOrExp::coroutine_handle<>) const noexcept {}
  constexpr void await_resume() const noexcept {}
};

/**
 * The promise type that backs the generator type and handles storage and
 * suspension-related decisions.
 */
template <size_t MIN_BUFFER_SIZE>
class stream_generator_promise {
  ad_utility::streams::ostringstream _value;
  std::exception_ptr _exception;

 public:
  stream_generator_promise() = default;

  basic_stream_generator<MIN_BUFFER_SIZE> get_return_object() noexcept;

  constexpr qlever_stdOrExp::suspend_always initial_suspend() const noexcept {
    return {};
  }
  constexpr qlever_stdOrExp::suspend_always final_suspend() const noexcept {
    return {};
  }

  /**
   * Handles values passed using co_yield and stores their respective
   * string representations inside a buffer.
   *
   * @tparam T The Type being passed
   * @param value The value being passed via co_yield
   * @return Whether or not the coroutine should get suspended (currently based
   * on isBufferLargeEnough()), wrapped inside a suspend_sometimes class.
   */
  suspend_sometimes yield_value(const Streamable auto& value) noexcept {
    // whenever the buffer size exceeds the threshold the coroutine
    // is suspended, thus we can safely assume the value is read
    // before resuming
    if (isBufferLargeEnough()) {
      _value.str("");
      // clear() only clears error bits,
      // str("") is what actually clears the buffer
      _value.clear();
    }
    _value << value;
    return suspend_sometimes{isBufferLargeEnough()};
  }

  void unhandled_exception() { _exception = std::current_exception(); }

  void return_void() {}

  std::string_view value() const noexcept { return _value.view(); }

  // Don't allow any use of 'co_await' inside the generator coroutine.
  template <typename U>
  qlever_stdOrExp::suspend_never await_transform(U&& value) = delete;

  void rethrow_if_exception() {
    if (_exception) {
      std::rethrow_exception(_exception);
    }
  }

 private:
  bool isBufferLargeEnough() {
    return _value.view().length() >= MIN_BUFFER_SIZE;
  }
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

 private:
  qlever_stdOrExp::coroutine_handle<promise_type> _coroutine = nullptr;

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

  /**
   * Resumes the promise until the coroutine suspends.
   * @return A view of the accumulated values since the last suspension.
   */
  std::string_view next() {
    if (!hasNext()) {
      throw std::runtime_error("Coroutine is not active");
    }
    _coroutine.resume();
    if (_coroutine.done()) {
      _coroutine.promise().rethrow_if_exception();
    }
    return _coroutine.promise().value();
  }

  /**
   * Indicates if the coroutine is done processing.
   * @return False, if the coroutine is done, true otherwise.
   */
  bool hasNext() { return _coroutine && !_coroutine.done(); }

 private:
  friend class detail::stream_generator_promise<MIN_BUFFER_SIZE>;
  explicit basic_stream_generator(
      qlever_stdOrExp::coroutine_handle<promise_type> coroutine) noexcept
      : _coroutine{coroutine} {}
};

namespace detail {
template <size_t MIN_BUFFER_SIZE>
inline basic_stream_generator<MIN_BUFFER_SIZE>
stream_generator_promise<MIN_BUFFER_SIZE>::get_return_object() noexcept {
  using coroutine_handle = qlever_stdOrExp::coroutine_handle<
      stream_generator_promise<MIN_BUFFER_SIZE>>;
  return basic_stream_generator{coroutine_handle::from_promise(*this)};
}
}  // namespace detail

// Use 1MB buffer size by default
using stream_generator = basic_stream_generator<1u << 20>;
}  // namespace ad_utility::stream_generator

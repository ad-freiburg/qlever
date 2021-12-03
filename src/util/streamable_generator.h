// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <exception>
#include <sstream>

#include "./ostringstream.h"

// coroutines are still experimental in clang, adapt the appropriate
// namespaces.
#ifdef __clang__
#include <experimental/coroutine>
namespace qlever_stdOrExp = std::experimental;
#else
#include <coroutine>
namespace qlever_stdOrExp = std;
#endif

namespace ad_utility::stream_generator {

template <typename T>
concept Streamable = requires(T x, std::ostream& os) {
  os << x;
};

class stream_generator;

namespace detail {
class suspend_sometimes {
  const bool suspend;

 public:
  explicit suspend_sometimes(const bool suspend) : suspend(suspend) {}
  bool await_ready() const noexcept { return !suspend; }
  constexpr void await_suspend(
      qlever_stdOrExp::coroutine_handle<>) const noexcept {}
  constexpr void await_resume() const noexcept {}
};

class stream_generator_promise {
 public:
  using value_type = std::string_view;
  using reference_type = value_type;
  using pointer_type = value_type*;

  stream_generator_promise() = default;

  stream_generator get_return_object() noexcept;

  constexpr qlever_stdOrExp::suspend_always initial_suspend() const noexcept {
    return {};
  }
  constexpr qlever_stdOrExp::suspend_always final_suspend() const noexcept {
    return {};
  }

  template <Streamable T>
  suspend_sometimes yield_value(T&& value) noexcept {
    // whenever the buffer size exceeds the threshold the coroutine
    // is suspended, thus we can safely assume the value is read
    // before resuming
    if (isBufferLargeEnough()) {
      m_value.str("");
      // clear() only clears error bits,
      // str("") is what actually clears the buffer
      m_value.clear();
    }
    m_value << value;
    return suspend_sometimes(isBufferLargeEnough());
  }

  void unhandled_exception() { m_exception = std::current_exception(); }

  void return_void() {}

  reference_type value() const noexcept { return m_value.view(); }

  // Don't allow any use of 'co_await' inside the generator coroutine.
  template <typename U>
  qlever_stdOrExp::suspend_never await_transform(U&& value) = delete;

  void rethrow_if_exception() {
    if (m_exception) {
      std::rethrow_exception(m_exception);
    }
  }

 private:
  ad_utility::streams::ostringstream m_value;
  std::exception_ptr m_exception;

  bool isBufferLargeEnough() {
    // TODO number is arbitrary, fine-tune in the future
    return m_value.view().length() >= 1000;
  }
};
}  // namespace detail

class [[nodiscard]] stream_generator {
 public:
  using promise_type = detail::stream_generator_promise;
  using value_type = std::string_view;

  stream_generator() noexcept : m_coroutine(nullptr) {}

  stream_generator(stream_generator&& other) noexcept
      : m_coroutine(other.m_coroutine) {
    other.m_coroutine = nullptr;
  }

  stream_generator(const stream_generator& other) = delete;

  ~stream_generator() {
    if (m_coroutine) {
      m_coroutine.destroy();
    }
  }

  stream_generator& operator=(stream_generator other) noexcept {
    swap(other);
    return *this;
  }

  std::string_view next() {
    if (m_coroutine) {
      m_coroutine.resume();
      if (m_coroutine.done()) {
        m_coroutine.promise().rethrow_if_exception();
      }
    }
    return m_coroutine.promise().value();
  }

  bool hasNext() { return !m_coroutine.done(); }

  void swap(stream_generator& other) noexcept {
    std::swap(m_coroutine, other.m_coroutine);
  }

 private:
  friend class detail::stream_generator_promise;

  explicit stream_generator(
      qlever_stdOrExp::coroutine_handle<promise_type> coroutine) noexcept
      : m_coroutine(coroutine) {}

  qlever_stdOrExp::coroutine_handle<promise_type> m_coroutine;
};

namespace detail {
inline stream_generator stream_generator_promise::get_return_object() noexcept {
  using coroutine_handle =
      qlever_stdOrExp::coroutine_handle<stream_generator_promise>;
  return stream_generator{coroutine_handle::from_promise(*this)};
}
}  // namespace detail
}  // namespace ad_utility::stream_generator

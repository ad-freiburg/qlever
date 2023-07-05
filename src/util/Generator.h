///////////////////////////////////////////////////////////////////////////////
// Copyright (c) Lewis Baker, Johannes Kalmbach (functionality to add details).
// Licenced under MIT license. See LICENSE.txt for details.
///////////////////////////////////////////////////////////////////////////////
#ifndef CPPCORO_GENERATOR_HPP_INCLUDED
#define CPPCORO_GENERATOR_HPP_INCLUDED

#include <exception>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>

// Coroutines are still experimental in clang libcpp, therefore adapt the
// appropriate namespaces by including the convenience header.
#include "util/Coroutines.h"
#include "util/TypeTraits.h"

namespace cppcoro {
template <typename T, typename Details>
class generator;

// This struct can be `co_await`ed inside a `generator` to obtain a reference to
// the details object ( the value of which is a template parameter to the
// generator). For an example see `GeneratorTest.cpp`.
struct GetDetails {};
static constexpr GetDetails getDetails;

namespace detail {
template <typename T, typename Details = int>
class generator_promise {
 public:
  // Even if the generator only yields `const` values, the `value_type`
  // shouldn't be `const` because otherwise several static checks when
  // interacting with the STL fail.
  using value_type = std::remove_cvref_t<T>;
  using reference_type = std::conditional_t<std::is_reference_v<T>, T, T&>;
  using pointer_type = std::remove_reference_t<T>*;

  generator_promise() = default;

  generator<T, Details> get_return_object() noexcept;

  constexpr std::suspend_always initial_suspend() const noexcept { return {}; }
  constexpr std::suspend_always final_suspend() const noexcept { return {}; }

  template <typename U = T,
            std::enable_if_t<!std::is_rvalue_reference<U>::value, int> = 0>
  std::suspend_always yield_value(std::remove_reference_t<T>& value) noexcept {
    m_value = std::addressof(value);
    return {};
  }

  std::suspend_always yield_value(std::remove_reference_t<T>&& value) noexcept {
    m_value = std::addressof(value);
    return {};
  }

  void unhandled_exception() { m_exception = std::current_exception(); }

  void return_void() {}

  reference_type value() const noexcept {
    return static_cast<reference_type>(*m_value);
  }

  // Don't allow any use of 'co_await' inside the generator coroutine.
  template <typename U>
  std::suspend_never await_transform(U&& value) = delete;

  void rethrow_if_exception() {
    if (m_exception) {
      std::rethrow_exception(m_exception);
    }
  }

  // The machinery to expose the stored `Details` via
  // `co_await cppcoro::getDetails`.
  struct DetailAwaiter {
    generator_promise& promise_;
    bool await_ready() { return true; }
    bool await_suspend(std::coroutine_handle<>) noexcept { return false; }
    Details& await_resume() noexcept { return promise_.details(); }
  };

  DetailAwaiter await_transform(
      [[maybe_unused]] ad_utility::SimilarTo<GetDetails> auto&& detail) {
    return {*this};
  }

  Details& details() { return m_details; }

 private:
  pointer_type m_value;
  std::exception_ptr m_exception;
  Details m_details{};
};

struct generator_sentinel {};

template <typename T, typename Details>
class generator_iterator {
  using promise_type = generator_promise<T, Details>;
  using coroutine_handle = std::coroutine_handle<promise_type>;

 public:
  using iterator_category = std::input_iterator_tag;
  // What type should we use for counting elements of a potentially infinite
  // sequence?
  using difference_type = std::ptrdiff_t;
  using value_type = typename promise_type::value_type;
  using reference = typename promise_type::reference_type;
  using pointer = typename promise_type::pointer_type;

  // Iterator needs to be default-constructible to satisfy the Range concept.
  generator_iterator() noexcept : m_coroutine(nullptr) {}

  explicit generator_iterator(coroutine_handle coroutine) noexcept
      : m_coroutine(coroutine) {}

  friend bool operator==(const generator_iterator& it,
                         generator_sentinel) noexcept {
    return !it.m_coroutine || it.m_coroutine.done();
  }

  friend bool operator!=(const generator_iterator& it,
                         generator_sentinel s) noexcept {
    return !(it == s);
  }

  friend bool operator==(generator_sentinel s,
                         const generator_iterator& it) noexcept {
    return (it == s);
  }

  friend bool operator!=(generator_sentinel s,
                         const generator_iterator& it) noexcept {
    return it != s;
  }

  generator_iterator& operator++() {
    m_coroutine.resume();
    if (m_coroutine.done()) {
      m_coroutine.promise().rethrow_if_exception();
    }

    return *this;
  }

  // Need to provide post-increment operator to implement the 'Range' concept.
  void operator++(int) { (void)operator++(); }

  reference operator*() const noexcept { return m_coroutine.promise().value(); }

  pointer operator->() const noexcept { return std::addressof(operator*()); }

 private:
  coroutine_handle m_coroutine;
};
}  // namespace detail

template <typename T, typename Details = int>
class [[nodiscard]] generator {
 public:
  using promise_type = detail::generator_promise<T, Details>;
  using iterator = detail::generator_iterator<T, Details>;
  using value_type = typename iterator::value_type;

  generator() noexcept : m_coroutine(nullptr) {}

  generator(generator&& other) noexcept : m_coroutine(other.m_coroutine) {
    other.m_coroutine = nullptr;
  }

  generator(const generator& other) = delete;

  ~generator() {
    if (m_coroutine) {
      m_coroutine.destroy();
    }
  }

  generator& operator=(generator other) noexcept {
    swap(other);
    return *this;
  }

  iterator begin() {
    if (m_coroutine) {
      m_coroutine.resume();
      if (m_coroutine.done()) {
        m_coroutine.promise().rethrow_if_exception();
      }
    }

    return iterator{m_coroutine};
  }

  detail::generator_sentinel end() noexcept {
    return detail::generator_sentinel{};
  }

  void swap(generator& other) noexcept {
    std::swap(m_coroutine, other.m_coroutine);
  }

  const Details& details() { return m_coroutine.promise().details(); }

 private:
  friend class detail::generator_promise<T, Details>;

  explicit generator(std::coroutine_handle<promise_type> coroutine) noexcept
      : m_coroutine(coroutine) {}

  std::coroutine_handle<promise_type> m_coroutine;
};

template <typename T>
void swap(generator<T>& a, generator<T>& b) {
  a.swap(b);
}

namespace detail {
template <typename T, typename Details>
generator<T, Details>
generator_promise<T, Details>::get_return_object() noexcept {
  using coroutine_handle = std::coroutine_handle<generator_promise<T, Details>>;
  return generator<T, Details>{coroutine_handle::from_promise(*this)};
}
}  // namespace detail

template <typename FUNC, typename T>
generator<
    std::invoke_result_t<FUNC&, typename generator<T>::iterator::reference>>
fmap(FUNC func, generator<T> source) {
  for (auto&& value : source) {
    co_yield std::invoke(func, static_cast<decltype(value)>(value));
  }
}
}  // namespace cppcoro

#endif

// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once
#include <iostream>
#include <sstream>
#include <type_traits>

/**
 * This header aims to provide a drop-in replacement
 * for std::ostringstream in case the library doesn't
 * provide a view() member function.
 * Can be dropped once C++20 is fully adopted.
 */
namespace ad_utility::streams {

/**
 * A concept to determine if a Type has a view() member function.
 * @tparam T The Type to check.
 */
template <typename T>
concept HasView = requires(T t) {
  t.view();
};

namespace implementation {
using base_buffer =
    std::basic_stringbuf<char, std::char_traits<char>, std::allocator<char>>;

/**
 * A rudimentary re-implementation of std::basic_stringbuf guaranteed to have
 * a view() member function.
 */
class CustomBuffer : public base_buffer {
 public:
  /**
   * If this method exists for the base class this implementation
   * would hide it, but in that case this function is unused so
   * it doesn't matter. This method works for libc++ and stdlibc++
   * but shouldn't be viewed as a general solution.
   */
  std::string_view view() const {
    return {pbase(), static_cast<size_t>(std::max(pptr(), egptr()) - pbase())};
  }
};

using base = std::basic_ostream<char, std::char_traits<char>>;

/**
 * A rudimentary re-implementation of std::osstringstream guaranteed to have a
 * view() member function.
 */
class ostringstream : public base {
  std::conditional_t<HasView<base_buffer>, base_buffer, CustomBuffer> _buf;

 public:
  ostringstream() : base{&_buf} {}
  auto view() const { return _buf.view(); }
  void str(const std::string& string) { _buf.str(string); }
};
}  // namespace implementation

/**
 * uses std::ostringstream if it has a view() member function, a custom
 * implementation otherwise.
 */
using ostringstream =
    std::conditional_t<HasView<std::ostringstream>, std::ostringstream,
                       implementation::ostringstream>;

#ifdef _LIBCPP_VERSION
static_assert(
    !HasView<std::ostringstream>,
    "libc++ has support for std::ostringstream::view(), remove this header!");
#endif
}  // namespace ad_utility::streams

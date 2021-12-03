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

template <typename T>
concept HasView = requires(T t) {
  t.view();
};

namespace implementation {
  using base_buffer =
      std::basic_stringbuf<char, std::char_traits<char>, std::allocator<char>>;
  class CustomBuffer : public base_buffer {
   public:
    // If this method exists for the base class this implementation
    // would hide it, but in that case this function is unused so
    // it doesn't matter
    std::string_view view() const {
      return {pbase(),
              static_cast<size_t>(std::max(pptr(), egptr()) - pbase())};
    }
  };

  using base = std::basic_ostream<char, std::char_traits<char>>;
  class ostringstream : public base {
    std::conditional_t<HasView<base_buffer>, base_buffer, CustomBuffer> _buf;

   public:
    ostringstream() : base{&_buf} {}
    auto view() const { return _buf.view(); }
    void str(const std::string& string) { _buf.str(string); }
  };
}  // namespace implementation
using ostringstream =
    std::conditional_t<HasView<std::ostringstream>, std::ostringstream,
                       implementation::ostringstream>;
}  // namespace ad_utility::streams

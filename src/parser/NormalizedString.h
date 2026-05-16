// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#ifndef QLEVER_SRC_PARSER_NORMALIZEDSTRING_H
#define QLEVER_SRC_PARSER_NORMALIZEDSTRING_H

#include <cstring>
#include <ios>
#include <string>
#include <string_view>

#include "backports/three_way_comparison.h"

struct NormalizedChar {
  char c_;

  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(NormalizedChar, c_)
};

// `std::basic_string<T>` and `std::basic_string_view<T>` look up
// `std::char_traits<T>` for any operations that touch character semantics.
// Newer libc++ removed the implicit fallback specializations and now only
// ships `char_traits` for the standard character types
// (`char`, `wchar_t`, `char8_t`, `char16_t`, `char32_t`). For user-defined
// types like `NormalizedChar` we have to provide our own specialization;
// [namespace.std] explicitly allows this when the type is user-defined.
//
// The implementations forward to the wrapped `c_` so the resulting traits
// behave exactly like `std::char_traits<char>` would over the same byte
// sequence.
template <>
struct std::char_traits<NormalizedChar> {
  using char_type = NormalizedChar;
  using int_type = int;
  using off_type = std::streamoff;
  using pos_type = std::streampos;
  using state_type = std::mbstate_t;

  static constexpr void assign(char_type& c1, const char_type& c2) noexcept {
    c1 = c2;
  }
  static constexpr bool eq(char_type c1, char_type c2) noexcept {
    return c1.c_ == c2.c_;
  }
  static constexpr bool lt(char_type c1, char_type c2) noexcept {
    return c1.c_ < c2.c_;
  }

  static constexpr int compare(const char_type* s1, const char_type* s2,
                               std::size_t n) noexcept {
    for (std::size_t i = 0; i < n; ++i) {
      if (s1[i].c_ < s2[i].c_) return -1;
      if (s2[i].c_ < s1[i].c_) return 1;
    }
    return 0;
  }

  static constexpr std::size_t length(const char_type* s) noexcept {
    std::size_t i = 0;
    while (s[i].c_ != '\0') ++i;
    return i;
  }

  static constexpr const char_type* find(const char_type* s, std::size_t n,
                                         const char_type& a) noexcept {
    for (std::size_t i = 0; i < n; ++i) {
      if (s[i].c_ == a.c_) return s + i;
    }
    return nullptr;
  }

  static char_type* move(char_type* s1, const char_type* s2,
                         std::size_t n) noexcept {
    if (n == 0) return s1;
    return static_cast<char_type*>(std::memmove(s1, s2, n));
  }

  static char_type* copy(char_type* s1, const char_type* s2,
                         std::size_t n) noexcept {
    if (n == 0) return s1;
    return static_cast<char_type*>(std::memcpy(s1, s2, n));
  }

  static char_type* assign(char_type* s, std::size_t n, char_type a) noexcept {
    for (std::size_t i = 0; i < n; ++i) s[i] = a;
    return s;
  }

  static constexpr int_type to_int_type(char_type c) noexcept {
    return static_cast<int_type>(static_cast<unsigned char>(c.c_));
  }
  static constexpr char_type to_char_type(int_type c) noexcept {
    return char_type{static_cast<char>(c)};
  }
  static constexpr bool eq_int_type(int_type c1, int_type c2) noexcept {
    return c1 == c2;
  }
  static constexpr int_type eof() noexcept { return static_cast<int_type>(-1); }
  static constexpr int_type not_eof(int_type c) noexcept {
    return c == eof() ? 0 : c;
  }
};

// A bespoke string representation that ensures the content
// is correctly encoded and does not contain invalid characters
using NormalizedString = std::basic_string<NormalizedChar>;

// A string view representation of above described normalized strings
using NormalizedStringView = std::basic_string_view<NormalizedChar>;

// Returns the given NormalizedStringView as a string_view.
inline std::string_view asStringViewUnsafe(
    NormalizedStringView normalizedStringView) {
  return {reinterpret_cast<const char*>(normalizedStringView.data()),
          normalizedStringView.size()};
}
inline NormalizedStringView asNormalizedStringViewUnsafe(
    std::string_view input) {
  return {reinterpret_cast<const NormalizedChar*>(input.data()), input.size()};
}

#endif  // QLEVER_SRC_PARSER_NORMALIZEDSTRING_H

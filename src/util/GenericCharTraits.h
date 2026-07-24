//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_GENERICCHARTRAITS_H
#define QLEVER_SRC_UTIL_GENERICCHARTRAITS_H

#include <absl/base/casts.h>

#include <cstddef>
#include <string>
#include <type_traits>

namespace ad_utility {

// `std::basic_string<T>` and `std::basic_string_view<T>` look up
// `std::char_traits<T>` for any operation that touches character semantics.
// Newer versions of libc++ removed the implicit fallback specializations and
// now only ship `char_traits` for the standard character types (`char`,
// `wchar_t`, `char8_t`, `char16_t`, `char32_t`). For other character types we
// therefore have to provide our own traits. `GenericCharTraits` makes any
// trivially copyable, single-byte `CharType` behave exactly like `char` would:
// the operations that involve a `CharType` simply reinterpret the byte and
// forward to `std::char_traits<char>` (which is where the actual, well-tested
// logic lives). The members that only deal with `int_type` (`eof`, `not_eof`,
// `eq_int_type`) as well as the `..._type` aliases are inherited unchanged.
template <typename CharType>
struct GenericCharTraits : std::char_traits<char> {
  static_assert(sizeof(CharType) == sizeof(char) &&
                    alignof(CharType) == alignof(char),
                "GenericCharTraits only works for single-byte character types");
  static_assert(std::is_trivially_copyable_v<CharType>,
                "GenericCharTraits requires a trivially copyable character "
                "type");

  using char_type = CharType;

 private:
  using Base = std::char_traits<char>;
  // Reinterpret a `char_type` (value or pointer) as the `char` that the
  // forwarded-to `Base` operations expect, and back.
  static char asChar(char_type c) noexcept { return absl::bit_cast<char>(c); }
  static char_type asCharType(char c) noexcept {
    return absl::bit_cast<char_type>(c);
  }

  // ___________________________________________________________________________
  static const char* asChar(const char_type* p) noexcept {
    return reinterpret_cast<const char*>(p);
  }

  // ___________________________________________________________________________
  static char* asChar(char_type* p) noexcept {
    return reinterpret_cast<char*>(p);
  }

  // This is only used to cast back a `const char*` that was cast from `const
  // char_type*`.
  static const char_type* asCharType(const char* p) noexcept {
    return reinterpret_cast<const char_type*>(p);
  }

  // This is only used to cast back a `char*` that was cast from `char_type*`.
  static char_type* asCharType(char* p) noexcept {
    return reinterpret_cast<char_type*>(p);
  }

 public:
  // ___________________________________________________________________________
  static void assign(char_type& c1, const char_type& c2) noexcept { c1 = c2; }

  // ___________________________________________________________________________
  static char_type* assign(char_type* s, std::size_t n, char_type a) noexcept {
    Base::assign(asChar(s), n, asChar(a));
    return s;
  }

  // ___________________________________________________________________________
  static bool eq(char_type c1, char_type c2) noexcept {
    return Base::eq(asChar(c1), asChar(c2));
  }

  // ___________________________________________________________________________
  static bool lt(char_type c1, char_type c2) noexcept {
    return Base::lt(asChar(c1), asChar(c2));
  }

  // ___________________________________________________________________________
  static int compare(const char_type* s1, const char_type* s2,
                     std::size_t n) noexcept {
    return Base::compare(asChar(s1), asChar(s2), n);
  }

  // ___________________________________________________________________________
  static std::size_t length(const char_type* s) noexcept {
    return Base::length(asChar(s));
  }

  // ___________________________________________________________________________
  static const char_type* find(const char_type* s, std::size_t n,
                               const char_type& a) noexcept {
    return asCharType(Base::find(asChar(s), n, asChar(a)));
  }

  // ___________________________________________________________________________
  static char_type* move(char_type* s1, const char_type* s2,
                         std::size_t n) noexcept {
    return asCharType(Base::move(asChar(s1), asChar(s2), n));
  }

  // ___________________________________________________________________________
  static char_type* copy(char_type* s1, const char_type* s2,
                         std::size_t n) noexcept {
    return asCharType(Base::copy(asChar(s1), asChar(s2), n));
  }

  // ___________________________________________________________________________
  static int_type to_int_type(char_type c) noexcept {
    return Base::to_int_type(asChar(c));
  }

  // ___________________________________________________________________________
  static char_type to_char_type(int_type c) noexcept {
    return asCharType(Base::to_char_type(c));
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GENERICCHARTRAITS_H

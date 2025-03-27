//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_CTREHELPERS_H
#define QLEVER_SRC_UTIL_CTREHELPERS_H

#include <ctre-unicode.hpp>

/// Helper function for ctre: concatenation of fixed_strings
template <size_t A, size_t B>
constexpr ctll::fixed_string<A + B> operator+(const ctll::fixed_string<A>& a,
                                              const ctll::fixed_string<B>& b) {
  char32_t comb[A + B + 1] = {};
  for (size_t i = 0; i < A; ++i) {  // omit the trailing 0 of the first string
    comb[i] = a.content[i];
  }
  for (size_t i = 0; i < B; ++i) {
    comb[i + A] = b.content[i];
  }
  // the input array must be zero-terminated
  comb[A + B] = '\0';
  return ctll::fixed_string(comb);
}

/// Helper function for ctre: concatenation of fixed_strings
template <size_t A, size_t B>
constexpr ctll::fixed_string<A + B - 1> operator+(
    const ctll::fixed_string<A>& a, const char (&b)[B]) {
  auto bStr = ctll::fixed_string(b);
  return a + bStr;
}

/// Helper function for ctre: concatenation of fixed_strings
template <size_t A, size_t B>
constexpr ctll::fixed_string<A + B - 1> operator+(
    const char (&a)[A], const ctll::fixed_string<B>& b) {
  auto aStr = ctll::fixed_string(a);
  return aStr + b;
}

/// Create a regex group by putting the argument in parentheses
template <size_t N>
static constexpr auto grp(const ctll::fixed_string<N>& s) {
  return ctll::fixed_string("(") + s + ctll::fixed_string(")");
}

/// Create a regex character class by putting the argument in square brackets
template <size_t N>
static constexpr auto cls(const ctll::fixed_string<N>& s) {
  return ctll::fixed_string("[") + s + ctll::fixed_string("]");
}

#endif  // QLEVER_SRC_UTIL_CTREHELPERS_H

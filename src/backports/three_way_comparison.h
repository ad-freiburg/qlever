//
// Created by kalmbacj on 9/2/25.
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef THREE_WAY_COMPARISON_H
#define THREE_WAY_COMPARISON_H

// TODO<joka921>
// 1. what do we do about the constexprness? Does it just work to make every
// operator `constexpr` in our codebase?
// TODO<BMW> Also add some unit tests for these macros.

// Macros that define the relational operators < , >=, etc. as well as the
// equality operators `==` and `!=` for a class. The class has to have a magic
// member function `constexpr auto tieForComparison() const { ...}` that returns
// a comparable object (typically using `std::tie` if multiple members are to be
// compared). The `tieForComparison()` function should be declared close to the
// data members, s.t. one remembers to register additional members also in that
// function.
// Example usage:
/*
 * class S {
 *   int x;
 *   int y;
 *   constexpr auto tieForComparison() const {
 *     return std::tie(x, y);
 *   }
 *  public:
 *   QL_DEFINE_THREEWAY_COMPARISON(S);
 *   QL_DEFINE_EQUALITY_COMPARISON(S);
 * };
 *
 */

#include <tuple>

#ifdef QLEVER_CPP_17

#define QL_DEFINE_CLASS_MEMBERS_AS_TIE(...) \
  constexpr auto membersAsTie() const { return std::make_tuple(__VA_ARGS__); }

#define CPP_equality_operator(T)                             \
  friend constexpr bool operator==(const T& a, const T& b) { \
    return a.membersAsTie() == b.membersAsTie();             \
  }                                                          \
  friend constexpr bool operator!=(const T& a, const T& b) { \
    return a.membersAsTie() != b.membersAsTie();             \
  }

#define CPP_threeway_operator(T)                             \
  friend constexpr bool operator<(const T& a, const T& b) {  \
    return a.membersAsTie() < b.membersAsTie();              \
  }                                                          \
  friend constexpr bool operator<=(const T& a, const T& b) { \
    return a.membersAsTie() <= b.membersAsTie();             \
  }                                                          \
  friend constexpr bool operator>(const T& a, const T& b) {  \
    return a.membersAsTie() > b.membersAsTie();              \
  }                                                          \
  friend constexpr bool operator>=(const T& a, const T& b) { \
    return a.membersAsTie() >= b.membersAsTie();             \
  }                                                          \
  CPP_equality_operator(T)

#define QL_DEFINE_THREEWAY_OPERATOR(T, ...)   \
 private:                                     \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE(__VA_ARGS__) \
 public:                                      \
  CPP_threeway_operator(T)

#define QL_DEFINE_EQUALITY_OPERATORS(T, ...)  \
 private:                                     \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE(__VA_ARGS__) \
 public:                                      \
  CPP_equality_operator(T)

#else

#define QL_DEFINE_THREEWAY_OPERATOR(T, ...) \
  constexpr auto operator<=>(const T&) const = default;

#define QL_DEFINE_EQUALITY_OPERATORS(T, ...) \
  constexpr bool operator==(const T&) const = default;

#endif

#endif  // THREE_WAY_COMPARISON_H

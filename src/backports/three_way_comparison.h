//
// Created by kalmbacj on 9/2/25.
//

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
#define QL_DEFINE_THREEWAY_COMPARISON(T)                     \
  friend constexpr bool operator<(const T& a, const T& b) {  \
    return a.tieForComparison() < b.tieForComparison();      \
  }                                                          \
  friend constexpr bool operator<=(const T& a, const T& b) { \
    return a.tieForComparison() <= b.tieForComparison();     \
  }                                                          \
  friend constexpr bool operator>(const T& a, const T& b) {  \
    return a.tieForComparison() > b.tieForComparison();      \
  }                                                          \
  friend constexpr bool operator>=(const T& a, const T& b) { \
    return a.tieForComparison() >= b.tieForComparison();     \
  }

#define QL_DEFINE_EQUALITY_COMPARISON(T)                     \
  friend constexpr bool operator==(const T& a, const T& b) { \
    return a.tieForComparison() == b.tieForComparison();     \
  }                                                          \
  friend constexpr bool operator!=(const T& a, const T& b) { \
    return a.tieForComparison() != b.tieForComparison();     \
  }

#endif  // THREE_WAY_COMPARISON_H

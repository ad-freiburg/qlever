//
// Created by kalmbacj on 9/2/25.
//

#ifndef THREE_WAY_COMPARISON_H
#define THREE_WAY_COMPARISON_H

// TODO<joka921>
// 1. what do we do about the constexprness?
// 2. this is not `defaulted`, choose a better name.
#define QL_DEFINE_DEFAULT_THREEWAY_COMPARISON(T)             \
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

#define QL_DEFINE_DEFAULT_EQUALITY_COMPARISON(T)             \
  friend constexpr bool operator==(const T& a, const T& b) { \
    return a.tieForComparison() == b.tieForComparison();     \
  }                                                          \
  friend constexpr bool operator!=(const T& a, const T& b) { \
    return a.tieForComparison() != b.tieForComparison();     \
  }

#endif  // THREE_WAY_COMPARISON_H

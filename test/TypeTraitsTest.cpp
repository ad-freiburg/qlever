//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/util/TypeTraits.h"

using namespace ad_utility;
TEST(TypeTraits, IsSimilar) {
  static_assert(isSimilar<int, const int&>);
  static_assert(isSimilar<int&, const int&>);
  static_assert(isSimilar<volatile int&, const int>);
  static_assert(!isSimilar<volatile int&, const int*>);
  ASSERT_TRUE(true);
}

TEST(TypeTraits, IsContained) {
  using tup = std::tuple<int, char>;
  using nested = std::tuple<tup>;
  static_assert(isTypeContainedIn<int, tup>);
  static_assert(isTypeContainedIn<char, tup>);
  static_assert(isTypeContainedIn<tup, nested>);
  static_assert(!isTypeContainedIn<tup, char>);
  static_assert(!isTypeContainedIn<unsigned int, tup>);
  static_assert(!isTypeContainedIn<int, int>);
  ASSERT_TRUE(true);
}

TEST(TypeTraits, IsInstantiation) {
  static_assert(isVector<std::vector<int>>);
  static_assert(!isVector<std::tuple<int>>);
  static_assert(!isVector<int>);

  static_assert(isTuple<std::tuple<int>>);
  static_assert(isTuple<std::tuple<int, bool>>);
  static_assert(!isTuple<std::variant<int, bool>>);
  static_assert(!isTuple<int>);

  static_assert(isVariant<std::variant<int>>);
  static_assert(isVariant<std::variant<int, bool>>);
  static_assert(!isVariant<std::tuple<int, bool>>);
  static_assert(!isVariant<int>);
  ASSERT_TRUE(true);
}

template <typename>
struct X {};

TEST(TypeTraits, Lift) {
  using T = std::tuple<int>;
  using LT = LiftTuple<X, T>;
  static_assert(std::is_same_v<std::tuple<X<int>>, LT>);

  using V = std::variant<int>;
  using LV = LiftedVariant<X, V>;
  static_assert(std::is_same_v<std::variant<X<int>>, LV>);

  using TT = std::tuple<int, bool, short>;
  using LTT = LiftTuple<X, TT>;
  static_assert(std::is_same_v<std::tuple<X<int>, X<bool>, X<short>>, LTT>);

  using VV = std::variant<int, bool, short>;
  using LVV = LiftedVariant<X, VV>;
  static_assert(std::is_same_v<std::variant<X<int>, X<bool>, X<short>>, LVV>);
  ASSERT_TRUE(true);
}

TEST(TypeTraits, TupleToVariant) {
  using T = std::tuple<int>;
  using V = TupleToVariant<T>;
  static_assert(std::is_same_v<V, std::variant<int>>);

  using TT = std::tuple<int, short, bool>;
  using VV = TupleToVariant<TT>;
  static_assert(std::is_same_v<VV, std::variant<int, short, bool>>);
  ASSERT_TRUE(true);
}

TEST(TypeTraits, TupleCat) {
  using T = std::tuple<int, short>;
  using T2 = std::tuple<bool, long, size_t>;
  using T3 = std::tuple<>;

  static_assert(std::is_same_v<T, TupleCat<T>>);
  static_assert(std::is_same_v<T2, TupleCat<T2>>);
  static_assert(std::is_same_v<T3, TupleCat<T3>>);

  static_assert(std::is_same_v<T, TupleCat<T, T3>>);
  static_assert(std::is_same_v<T2, TupleCat<T2, T3>>);

  static_assert(std::is_same_v<std::tuple<int, short, bool, long, size_t>,
                               TupleCat<T, T2>>);
  static_assert(std::is_same_v<std::tuple<int, short, bool, long, size_t>,
                               TupleCat<T, T3, T2>>);
  ASSERT_TRUE(true);
}
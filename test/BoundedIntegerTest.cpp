//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>


#include "../src/engine/datatypes/BoundedInteger.h"

#include <gtest/gtest.h>
#include <iostream>

template<size_t N>
auto unchanged(int64_t x) {
    using I = ad_utility::NBitInteger<N>;
  ASSERT_EQ(x, I::fromNBit(I::toNBit(x)));
}

template<size_t N>
auto changed(int64_t x) {
  using I = ad_utility::NBitInteger<N>;
  ASSERT_NE(x, I::fromNBit(I::toNBit(x)));
}

template<size_t N>
void addition(int64_t a, int64_t b) {
  using I = ad_utility::NBitInteger<N>;
  auto resultInside = I::fromNBit(I::toNBit(I::toNBit(a) + I::toNBit(b)));
  auto resultOutside = I::fromNBit(I::toNBit(a + b));
  ASSERT_EQ(resultInside, resultOutside);
}

template<size_t N>
void subtraction(int64_t a, int64_t b) {
  using I = ad_utility::NBitInteger<N>;
  auto resultInside = I::fromNBit(I::toNBit(I::toNBit(a) - I::toNBit(b)));
  auto resultOutside = I::fromNBit(I::toNBit(a - b));
  ASSERT_EQ(resultInside, resultOutside);
}

template<size_t N>
void multiplication(int64_t a, int64_t b) {
  using I = ad_utility::NBitInteger<N>;
  auto resultInside = I::fromNBit(I::toNBit(I::toNBit(a) * I::toNBit(b)));
  auto resultOutside = I::fromNBit(I::toNBit(a * b));
  ASSERT_EQ(resultInside, resultOutside);
}

template<size_t N>
void testNumeric(int64_t a, int64_t b) {
  addition<N>(a, b);
  //subtraction<N>(a, b);
  //multiplication<N>(a, b);
}



TEST(NBitInteger, SingleBit) {
  using N = ad_utility::NBitInteger<1>;
  ASSERT_EQ(N::MinInteger(), -1);
  ASSERT_EQ(N::MaxInteger(), 0);
  unchanged<1>(0);
  unchanged<1>(-1);
  for (int i = 1; i < 100000; ++i) {
    changed<1>(i);
  }
  for (int i = -2; i > -100000; --i) {
    changed<1>(i);
  }

  for (int i = -10; i < 10; ++i) {
    for (int j = -10; j < 10; ++j) {
      std::cout << i << ' ' << j << std::endl;
      //testNumeric<1>(i, j);
      testNumeric<2>(i, j);
      //testNumeric<3>(i, j);
      //testNumeric<4>(i, j);
    }
  }

}
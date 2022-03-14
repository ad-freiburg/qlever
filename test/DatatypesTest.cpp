//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <bitset>
#include <cmath>

#include "../../src/engine/datatypes/Datatypes.h"

using namespace ad_utility;
using namespace ad_utility::datatypes;
static auto print = [](auto d) {
  auto b = std::bit_cast<std::bitset<64>>(d);
  std::cout << b << std::endl;
};

TEST(FancyId, Double) {
  auto test = [](double d, bool testExact = false) {
    auto result = NBitInteger::Double(d).getDoubleUnchecked();

    if (std::isnan(d)) {
      ASSERT_TRUE(std::isnan(result));
    } else if (testExact) {
      ASSERT_EQ(d, NBitInteger::Double(d).getDoubleUnchecked());
    } else {
      ASSERT_FLOAT_EQ(d, NBitInteger::Double(d).getDoubleUnchecked());
    }
  };

  auto testAll = [&](const std::vector<double>& ds, bool testExact = false) {
    for (auto d : ds) {
      test(d, testExact);
    }
  };

  using limits = std::numeric_limits<double>;
  static_assert(limits::is_iec559);
  auto inf = limits::infinity();
  auto nan = limits::quiet_NaN();
  auto nan2 = limits::signaling_NaN();
  auto min = limits::min();
  auto max = limits::max();
  auto denorm = limits::denorm_min();

  testAll({0.0, 1.0, -1.0, -17}, true);
  testAll({1.2345, 1254.123 - 51234.2, 2.239e-12});

  testAll({0.1});
  test(0.5, true);

  testAll({inf, -inf, nan, nan2}, true);
  testAll({min, max, denorm});
}

TEST(FancyId, Int) {
  auto inOut = [](int64_t i) {
    return NBitInteger::Integer(i).getIntegerUnchecked();
  };

  auto testEq = [&](int64_t i) { ASSERT_EQ(i, inOut(i)); };

  auto testAll = [&](const std::vector<int64_t>& is) {
    for (auto i : is) {
      testEq(i);
    }
  };

  testAll({0, 1, -1});

  auto min = fancyIdLimits::minInteger;
  auto max = fancyIdLimits::maxInteger;

  testAll({min, min + 1, max, max - 1});

  ASSERT_EQ(inOut(min - 1), max);
  ASSERT_EQ(inOut(max + 1), min);

  for (auto i = 1; i < 1000000; ++i) {
    if (inOut(min - i) != max - (i - 1)) {
      std::cout << (min - i) << std::endl;
      print(min - i);
      print(inOut(min - i));

      print(max - (i - 1));
      print(inOut(max - (i - 1)));
    }
    ASSERT_EQ(inOut(min - i), max - (i - 1));
  }

  for (auto i = max - 1000000; i < max + 100000; ++i) {
    ASSERT_EQ(inOut(min - i), max - (i - 1));
  }

  for (auto i = max - 1000000; i < max + 100000; ++i) {
    ASSERT_EQ(inOut(min - i), max - (i - 1));
  }

  auto outerMax = std::numeric_limits<int64_t>::max();
  auto outerMin = std::numeric_limits<int64_t>::min();

  ASSERT_EQ(inOut(outerMax + 1), inOut(outerMax) + inOut(1));
  ASSERT_EQ(inOut(outerMin + 1), inOut(outerMin) + inOut(1));
}

TEST(Date, FirstTests) {
  Date d{2005, 11, 28};
  ASSERT_EQ(d.year(), 2005);
  ASSERT_EQ(d.month(), 11);
  ASSERT_EQ(d.day(), 28);
}

TEST(BitPacking, FirstTests) {
  using B = BoundedInteger<-24, 38>;
  B b{11};
  ASSERT_EQ(b.get(), 11);
  ASSERT_EQ(B::fromUncheckedBits(b.toBits()).get(), 11);
}

TEST(BitPacking, numBitsRequired) {
  auto test = [](auto range) {
    auto expected = static_cast<uint8_t>(std::floor(std::log2(range - 1)) + 1);
    ASSERT_EQ(numBitsRequired(range), expected);
  };

  for (size_t i = 2; i <= 5'000'000; ++i) {
    test(i);
  }
}

TEST(BitPacking, BitMasks) {
  ASSERT_EQ(bitMaskForLowerBits(0), 0);
  ASSERT_EQ(bitMaskForLowerBits(1), 1);
  ASSERT_EQ(bitMaskForLowerBits(2), 3);
  ASSERT_EQ(bitMaskForLowerBits(3), 7);
  ASSERT_EQ(bitMaskForLowerBits(4), 15);
  ASSERT_EQ(bitMaskForLowerBits(5), 31);

  ASSERT_EQ(bitMaskForLowerBits(64), static_cast<uint64_t>(-1));
}

TEST(BitPacking, Systematic) {
  auto testSingleValue = []<typename B>(int64_t value, B*) {
    B b{value};
    ASSERT_EQ(b.get(), value);
    auto bits = b.toBits();
    ASSERT_EQ(bits, bitMaskForLowerBits(B::numBits) & bits);
    B c = B::fromUncheckedBits(bits);
    ASSERT_EQ(c.get(), value);
  };

  auto testAll = [&]<typename B>(B* dummy) {
    for (auto i = B::min; i <= B::max; ++i) {
      testSingleValue(i, dummy);
    }
  };

  BoundedInteger<0, 31>* a;
  BoundedInteger<0, 32>* b;
  BoundedInteger<0, 240000>* c;
  BoundedInteger<0, 24000000>* d;

  BoundedInteger<-24, 24>* e;
  BoundedInteger<-2400, 2400>* f;
  BoundedInteger<-240'000, 240'000>* g;
  BoundedInteger<-24'000'000, 24'000'000>* h;

  BoundedInteger<-24, 0>* i;
  BoundedInteger<-2400, 0>* j;
  BoundedInteger<-240'000, 0>* k;
  BoundedInteger<-24'000'000, 0>* l;

  BoundedInteger<-2 * 24, -24>* m;
  BoundedInteger<-2 * 2400, -2400>* n;
  BoundedInteger<-2 * 240'000, -240'000>* o;
  BoundedInteger<-2 * 24'000'000, -24'000'000>* p;

  testAll(a);
  testAll(b);
  testAll(c);
  testAll(d);
  testAll(e);
  testAll(f);
  testAll(g);
  testAll(h);
  testAll(i);
  testAll(j);
  testAll(k);
  testAll(l);
  testAll(m);
  testAll(n);
  testAll(o);
  testAll(p);
}
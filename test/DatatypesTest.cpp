//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <bitset>
#include <cmath>

#include "../../src/engine/datatypes/Datatypes.h"

using namespace ad_utility::datatypes;
static auto print = [](auto d) {
  auto b = std::bit_cast<std::bitset<64>>(d);
  std::cout << b << std::endl;
};

TEST(FancyId, Double) {
  auto test = [](double d, bool testExact = false) {
    auto result = FancyId::Double(d).getDoubleUnchecked();

    if (std::isnan(d)) {
      ASSERT_TRUE(std::isnan(result));
    } else if (testExact) {
      ASSERT_EQ(d, FancyId::Double(d).getDoubleUnchecked());
    } else {
      ASSERT_FLOAT_EQ(d, FancyId::Double(d).getDoubleUnchecked());
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
    return FancyId::Integer(i).getIntegerUnchecked();
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

}

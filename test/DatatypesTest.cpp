//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>
#include "../../src/engine/datatypes/Datatypes.h"
#include <bitset>
#include <cmath>

using ad_utility::FancyId;

TEST(FancyId, Double) {
  auto test = [](double d, bool testExact=false) {
    auto result = FancyId::Double(d).getDoubleUnchecked();

    if (std::isnan(d)) {
      ASSERT_TRUE(std::isnan(result));
    } else if (testExact) {
      ASSERT_EQ(d, FancyId::Double(d).getDoubleUnchecked());
    } else {
      ASSERT_FLOAT_EQ(d, FancyId::Double(d).getDoubleUnchecked());

    }

  };

  auto testAll = [&](const std::vector<double>& ds, bool testExact=false) {
    for (auto d : ds) {
      test(d, testExact);
    }
  };

  auto print = [](auto d) {
    auto b = std::bit_cast<std::bitset<64>>(d);
    std::cout << b << std::endl;
  };

  auto printBeforeAfter = [&](double d) {
    print(d);
    print(FancyId::Double(d).data());
    print(FancyId::Double(d).getDoubleUnchecked());
    std::cout << std::endl;
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
  testAll({1.2345, 1254.123 -51234.2, 2.239e-12});

  testAll({0.1});
  test(0.5, true);

  testAll({inf, -inf, nan, nan2}, true);
  testAll({min, max, denorm});
}



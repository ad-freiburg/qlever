//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>
#include "../src/util/CoroToStateMachine.h"
#include "../src/util/Log.h"

using ad_utility::CoroToStateMachine;

CoroToStateMachine<int> s (int initial, int& target) {
  using C = CoroToStateMachine<int>;
  target += initial;

  while (co_await C::ValueWasPushed{}) {
    target += co_await C::NextValue{};
  }

  target += initial;
}

TEST(CoroToStateMachine, Initial) {
  int target = 0;
  int compare = 0;

  auto z = s(42, target);
  compare += 42;
  EXPECT_EQ(target, compare);

  for (int i = 0; i < 2000; ++i) {
    compare += i;
    z.push(i);
    EXPECT_EQ(target, compare);
  }

 z.finish();
 compare += 42;
 ASSERT_EQ(target, compare);

}

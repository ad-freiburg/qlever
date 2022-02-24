//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/util/Forward.h"

template <typename Expected, typename T>
void tester(T&& t) {
  static_assert(std::is_same_v<Expected, decltype(AD_FWD(t))>);
}

TEST(Forward, ExpectedTypes) {
  int val = 0;
  const int cval = 0;
  int& ref = val;
  const int& cref = val;
  int&& movedRef = std::move(val);

  tester<int&>(val);
  tester<int&&>(std::move(val));
  tester<int&&>(AD_FWD(val));

  tester<const int&>(cval);
  tester<const int&&>(AD_FWD(cval));
  tester<const int&&>(std::move(cval));

  tester<int&>(ref);
  tester<const int&>(cref);

  // subtle:: rvalue references are lvalues themselves, so we also have to call
  // std::move() or forward on them if we want to move.
  tester<int&>(movedRef);
  tester<int&&>(std::move(movedRef));
  tester<int&&>(AD_FWD(movedRef));
  tester<int&&>(42);
}

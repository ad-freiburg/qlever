//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/util/Forward.h"

template <typename Expected, typename T>
void tester(T&& t) {
  static_assert(std::is_same_v<Expected, decltype(t)>);
  auto innerTester = [](auto&& innerT) {
    static_assert(std::is_same_v<Expected, decltype(innerT)>);
  };
  innerTester(AD_FWD(t));
}

TEST(Forward, ExpectedTypes) {
  int intVal = 0;
  const int constIntVal = 0;
  int& intRef = intVal;
  const int& constIntRef = intVal;
  int&& movedIntRef = std::move(intVal);

  tester<int&>(intVal);
  tester<int&&>(std::move(intVal));
  tester<int&&>(AD_FWD(intVal));

  tester<const int&>(constIntVal);
  tester<const int&&>(AD_FWD(constIntVal));
  tester<const int&&>(std::move(constIntVal));

  tester<int&>(intRef);
  tester<const int&>(constIntRef);

  // subtle:: rvalue references are lvalues themselves, so we also have to call
  // std::move() or forward on them if we want to move.
  tester<int&>(movedIntRef);
  tester<int&&>(std::move(movedIntRef));
  tester<int&&>(AD_FWD(movedIntRef));
  tester<int&&>(42);
}

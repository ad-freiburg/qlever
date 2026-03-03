//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/CtreHelpers.h"

// Test that two `ctll::fixed_string`s are equal. This function is necessary
// because `ctll::fixed_string` is not compatible with `GTest/GMock` directly.
template <typename T>
static void testEqual(T s1, T s2) {
  ASSERT_EQ(s1.size(), s2.size());
  for (size_t i = 0; i < s1.size(); ++i) {
    ASSERT_EQ(s1[i], s2[i]);
  }
}

using ctll::fixed_string;

TEST(CtreHelpers, Concatenation) {
  testEqual(fixed_string{"hello"} + fixed_string{"World"},
            fixed_string{"helloWorld"});
  testEqual(fixed_string{"hello"} + "World", fixed_string{"helloWorld"});
  testEqual("hello" + fixed_string{"World"}, fixed_string{"helloWorld"});
}

TEST(CtreHelpers, GrpAndCls) {
  testEqual(grp(fixed_string{"bim"}), fixed_string{"(bim)"});
  testEqual(cls(fixed_string{"bim"}), fixed_string{"[bim]"});
}

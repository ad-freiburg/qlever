// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <functional>
#include <list>
#include <string>
#include <vector>

#include "backports/numeric.h"

// _____________________________________________________________________________
TEST(NumericBackport, transformReduceInnerProduct) {
  std::vector<int> a{1, 2, 3};
  std::list<int> b{4, 5, 6};
  EXPECT_EQ(ql::transform_reduce(a.begin(), a.end(), b.begin(), 10),
            10 + 4 + 10 + 18);
  // The empty range yields the initial value.
  EXPECT_EQ(ql::transform_reduce(a.begin(), a.begin(), b.begin(), 10), 10);
}

// _____________________________________________________________________________
TEST(NumericBackport, transformReduceBinary) {
  std::vector<int> a{1, 2, 2, 3};
  // Count the adjacent pairs that differ, which is how the function is used in
  // `countDistinctIds`.
  auto numChanges = ql::transform_reduce(
      a.begin() + 1, a.end(), a.begin(), size_t{0}, std::plus<>{},
      [](int x, int y) { return static_cast<size_t>(x != y); });
  EXPECT_EQ(numChanges, 2u);
  EXPECT_EQ(ql::transform_reduce(a.begin(), a.end(), a.begin(), 1,
                                 std::multiplies<>{}, std::plus<>{}),
            2 * 4 * 4 * 6);
  EXPECT_EQ(ql::transform_reduce(a.begin(), a.begin(), a.begin(), 7,
                                 std::plus<>{}, std::multiplies<>{}),
            7);
}

// _____________________________________________________________________________
TEST(NumericBackport, transformReduceUnary) {
  std::vector<std::string> words{"a", "bcd", "ef"};
  EXPECT_EQ(
      ql::transform_reduce(words.begin(), words.end(), size_t{1}, std::plus<>{},
                           [](const std::string& s) { return s.size(); }),
      7u);
  EXPECT_EQ(ql::transform_reduce(words.begin(), words.begin(), size_t{1},
                                 std::plus<>{},
                                 [](const std::string& s) { return s.size(); }),
            1u);
}

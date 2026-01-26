// Copyright 2021 - 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "backports/functional.h"

// _____________________________________________________________________________
TEST(BackportFunctional, Identity) {
  auto id = ql::identity{};

  EXPECT_EQ(id(1), 1);

  std::vector<int> v{3, 4};
  auto cpy = v;

  // Takes an lvalue, shouldn't move.
  EXPECT_EQ(id(v), cpy);
  EXPECT_EQ(cpy, v);

  // This `move` only returns a reference,
  // so no actual move semantics apply.
  EXPECT_EQ(id(std::move(v)), cpy);
  EXPECT_FALSE(v.empty());

  // In this case, `v` is actually moved,
  // so we expect it to be empty after the call.
  auto v2 = id(std::move(v));
  EXPECT_EQ(v2, cpy);
  EXPECT_TRUE(v.empty());
}

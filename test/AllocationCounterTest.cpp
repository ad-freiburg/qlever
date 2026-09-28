// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <memory>

#include "util/AllocationCounter.h"

namespace allocationCounter = ad_utility::allocationCounter;

// _____________________________________________________________________________
TEST(AllocationCounter, CountsOperatorNewOnlyWhenEnabled) {
  const auto before = allocationCounter::current();
  auto values = std::make_unique<uint64_t[]>(1000);
  values[0] = 1;
  const auto diff = allocationCounter::current() - before;
  if constexpr (allocationCounter::enabled) {
    EXPECT_GE(diff.numAllocations_, 1u);
    EXPECT_GE(diff.numBytes_, 1000 * sizeof(uint64_t));
  } else {
    EXPECT_EQ(before.numAllocations_, 0u);
    EXPECT_EQ(diff.numAllocations_, 0u);
    EXPECT_EQ(diff.numBytes_, 0u);
  }
}

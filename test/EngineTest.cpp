// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Co-Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>

#include "engine/Engine.h"
#include "engine/idTable/IdTable.h"
#include "util/AllocatorTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

// _____________________________________________________________________________
TEST(Engine, countDistinct) {
  auto alloc = ad_utility::testing::makeAllocator();
  IdTable t1(alloc);
  t1.setNumColumns(0);
  auto noop = []() {};
  EXPECT_EQ(0u, Engine::countDistinct(t1, noop));
  t1.setNumColumns(3);
  EXPECT_EQ(0u, Engine::countDistinct(t1, noop));

  // 0 columns, but multiple rows;
  t1.setNumColumns(0);
  t1.resize(1);
  EXPECT_EQ(1u, Engine::countDistinct(t1, noop));
  t1.resize(5);
  EXPECT_EQ(1u, Engine::countDistinct(t1, noop));

  t1 = makeIdTableFromVector(
      {{0, 0}, {0, 0}, {1, 3}, {1, 4}, {1, 4}, {4, 4}, {4, 5}, {4, 7}});
  EXPECT_EQ(6u, Engine::countDistinct(t1, noop));

  t1 = makeIdTableFromVector(
      {{0, 0}, {1, 4}, {1, 3}, {1, 4}, {1, 4}, {4, 4}, {4, 5}, {4, 7}});

  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    AD_EXPECT_THROW_WITH_MESSAGE(Engine::countDistinct(t1, noop),
                                 ::testing::HasSubstr("must be sorted"));
  }
}

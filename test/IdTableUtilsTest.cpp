// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Co-Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>

#include "engine/idTable/IdTable.h"
#include "index/IdTableUtils.h"
#include "util/AllocatorTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

// _____________________________________________________________________________
TEST(IdTableUtils, countDistinct) {
  auto alloc = ad_utility::testing::makeAllocator();
  IdTable t1(alloc);
  t1.setNumColumns(0);
  auto noop = []() {};
  EXPECT_EQ(0u, IdTableUtils::countDistinct(t1, noop));
  t1.setNumColumns(3);
  EXPECT_EQ(0u, IdTableUtils::countDistinct(t1, noop));

  // 0 columns, but multiple rows;
  t1.setNumColumns(0);
  t1.resize(1);
  EXPECT_EQ(1u, IdTableUtils::countDistinct(t1, noop));
  t1.resize(5);
  EXPECT_EQ(1u, IdTableUtils::countDistinct(t1, noop));

  t1 = makeIdTableFromVector(
      {{0, 0}, {0, 0}, {1, 3}, {1, 4}, {1, 4}, {4, 4}, {4, 5}, {4, 7}});
  EXPECT_EQ(6u, IdTableUtils::countDistinct(t1, noop));

  t1 = makeIdTableFromVector(
      {{0, 0}, {1, 4}, {1, 3}, {1, 4}, {1, 4}, {4, 4}, {4, 5}, {4, 7}});

  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    AD_EXPECT_THROW_WITH_MESSAGE(IdTableUtils::countDistinct(t1, noop),
                                 ::testing::HasSubstr("must be sorted"));
  }
}

// _____________________________________________________________________________
TEST(IdTableUtils, containsLocalVocabIds) {
  // An empty table and a table without `Id`s of type `LocalVocabIndex`.
  IdTable table{2, ad_utility::testing::makeAllocator()};
  EXPECT_FALSE(IdTableUtils::containsLocalVocabIds(table));
  table = makeIdTableFromVector({{0, 1}, {2, 3}});
  EXPECT_FALSE(IdTableUtils::containsLocalVocabIds(table));
  EXPECT_FALSE(IdTableUtils::containsLocalVocabIds(table.asStaticView<0>()));

  // An `Id` of type `LocalVocabIndex` in the last row of the last column, and
  // in the first row of the first column.
  LocalVocabEntry entry = LocalVocabEntry::literalWithoutQuotes(
      "word", ad_utility::testing::getQec()->getLocalVocabContext());
  Id localVocabId = Id::makeFromLocalVocabIndex(&entry);
  table(1, 1) = localVocabId;
  EXPECT_TRUE(IdTableUtils::containsLocalVocabIds(table));
  EXPECT_TRUE(IdTableUtils::containsLocalVocabIds(table.asStaticView<0>()));
  table(1, 1) = Id::makeFromInt(3);
  table(0, 0) = localVocabId;
  EXPECT_TRUE(IdTableUtils::containsLocalVocabIds(table));
}

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "./IdColumnTestHelpers.h"
#include "backports/algorithm.h"
#include "engine/idTable/IdColumn.h"
#include "engine/idTable/IdColumnIterator.h"
#include "engine/idTable/IdColumnVector.h"
#include "engine/idTable/IdRef.h"

using namespace columnBasedIdTable;
using testHelpers::sampleIds;
using testHelpers::testAllocator;
using testHelpers::TestAllocator;

// _____________________________________________________________________________
TEST(IdColumnIteratorTest, defaultConstructionAndDereference) {
  IdColumnIterator defaultIt;
  static_assert(std::is_same_v<decltype(*defaultIt), IdRef>);
  static_assert(std::is_same_v<IdColumnIterator::value_type, Id>);
  static_assert(std::is_same_v<IdColumnIterator::iterator_category,
                               std::random_access_iterator_tag>);

  auto ids = sampleIds();
  const IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  const auto view = vec.asConstView();

  const auto it = view.begin();
  EXPECT_EQ(static_cast<Id>(*it), ids.front());
  EXPECT_EQ(static_cast<Id>(it[2]), ids.at(2));
}

// _____________________________________________________________________________
TEST(IdColumnIteratorTest, incrementAndDecrement) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  auto view = vec.asConstView();

  auto it = view.begin();
  EXPECT_EQ(static_cast<Id>(*it), ids.at(0));
  ++it;
  EXPECT_EQ(static_cast<Id>(*it), ids.at(1));

  auto old = it++;
  EXPECT_EQ(static_cast<Id>(*old), ids.at(1));
  EXPECT_EQ(static_cast<Id>(*it), ids.at(2));

  --it;
  EXPECT_EQ(static_cast<Id>(*it), ids.at(1));
  auto beforeDecrement = it--;
  EXPECT_EQ(static_cast<Id>(*beforeDecrement), ids.at(1));
  EXPECT_EQ(static_cast<Id>(*it), ids.at(0));
}

// _____________________________________________________________________________
TEST(IdColumnIteratorTest, randomAccessArithmetic) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  auto view = vec.asConstView();
  auto begin = view.begin();

  // `+=`/`-=` and the free `+`/`-` (both operand orders for `+`).
  auto it = begin;
  it += 3;
  EXPECT_EQ(static_cast<Id>(*it), ids.at(3));
  it -= 1;
  EXPECT_EQ(static_cast<Id>(*it), ids.at(2));

  EXPECT_EQ(static_cast<Id>(*(begin + 4)), ids.at(4));
  EXPECT_EQ(static_cast<Id>(*(4 + begin)), ids.at(4));
  EXPECT_EQ(static_cast<Id>(*((begin + 6) - 2)), ids.at(4));

  // Iterator difference.
  EXPECT_EQ((begin + 5) - begin, 5);
  EXPECT_EQ(view.end() - view.begin(), static_cast<ptrdiff_t>(ids.size()));
}

// _____________________________________________________________________________
TEST(IdColumnIteratorTest, comparisonOperators) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  auto view = vec.asConstView();
  auto begin = view.begin();
  auto second = begin + 1;

  EXPECT_TRUE(begin == begin);
  EXPECT_TRUE(begin != second);
  EXPECT_TRUE(begin < second);
  EXPECT_TRUE(begin <= second);
  EXPECT_TRUE(begin <= begin);
  EXPECT_TRUE(second > begin);
  EXPECT_TRUE(second >= begin);
  EXPECT_TRUE(begin >= begin);
  EXPECT_FALSE(second < begin);
}

// _____________________________________________________________________________
TEST(IdColumnIteratorTest, iterSwapSwapsTheReferencedValues) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  const auto view = vec.asView();
  ql::ranges::iter_swap(view.begin() + 1, view.begin() + 2);
  EXPECT_EQ(static_cast<Id>(vec[1]), ids.at(2));
  EXPECT_EQ(static_cast<Id>(vec[2]), ids.at(1));
}

// _____________________________________________________________________________
TEST(IdColumnIteratorTest, supportsRangeBasedForAndSort) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};

  std::vector<Id> viaIteration;
  for (Id id : vec.asConstView()) {
    viaIteration.push_back(id);
  }
  EXPECT_THAT(viaIteration, ::testing::ElementsAreArray(ids));

  // Sorting through the mutable view/iterator swaps the referenced values.
  auto mutableView = vec.asView();
  ql::ranges::sort(mutableView, {},
                   [](const Id id) { return getBitsCompat(id); });
  std::vector<Id> sortedIds = ids;
  ql::ranges::sort(sortedIds, {},
                   [](const Id id) { return getBitsCompat(id); });
  std::vector afterSort(mutableView.begin(), mutableView.end());
  EXPECT_THAT(afterSort, ::testing::ElementsAreArray(sortedIds));
}

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "IdColumnTestHelpers.h"
#include "backports/algorithm.h"
#include "engine/idTable/splitLayout/IdColumn.h"
#include "engine/idTable/splitLayout/IdColumnIterator.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "engine/idTable/splitLayout/IdRef.h"

using namespace columnBasedIdTable::splitLayout;
using testHelpers::sampleIds;
using testHelpers::testAllocator;

// Test the iterator traits, dereferencing, and the subscript operator.
TEST(IdColumnIteratorTest, traitsAndDereference) {
  static_assert(std::is_same_v<decltype(*IdColumnIterator{}), IdRef>);
  static_assert(std::is_same_v<IdColumnIterator::value_type, Id>);
  static_assert(std::is_same_v<IdColumnIterator::iterator_category,
                               std::random_access_iterator_tag>);

  // `*it` and `it[n]` yield the referenced elements.
  auto ids = sampleIds();
  const IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  const auto it = vec.asConstView().begin();
  EXPECT_EQ(*it, ids.front());
  EXPECT_EQ(it[2], ids.at(2));
}

// Test the increment and decrement operators, both prefix and postfix.
TEST(IdColumnIteratorTest, incrementAndDecrement) {
  auto ids = sampleIds();
  const IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  auto it = vec.asConstView().begin();

  // Prefix increment moves to the next element, postfix increment returns
  // the old position.
  EXPECT_EQ(*it, ids.at(0));
  ++it;
  EXPECT_EQ(*it, ids.at(1));
  auto old = it++;
  EXPECT_EQ(*old, ids.at(1));
  EXPECT_EQ(*it, ids.at(2));

  // The same for the decrements.
  --it;
  EXPECT_EQ(*it, ids.at(1));
  auto beforeDecrement = it--;
  EXPECT_EQ(*beforeDecrement, ids.at(1));
  EXPECT_EQ(*it, ids.at(0));
}

// Test the random-access arithmetic and the iterator difference.
TEST(IdColumnIteratorTest, randomAccessArithmetic) {
  auto ids = sampleIds();
  const IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  const auto view = vec.asConstView();
  const auto begin = view.begin();

  // `+=` and `-=`.
  auto it = begin;
  it += 3;
  EXPECT_EQ(*it, ids.at(3));
  it -= 1;
  EXPECT_EQ(*it, ids.at(2));

  // The free `+` (both operand orders) and `-`.
  EXPECT_EQ(*(begin + 4), ids.at(4));
  EXPECT_EQ(*(4 + begin), ids.at(4));
  EXPECT_EQ(*((begin + 6) - 2), ids.at(4));

  // The difference of two iterators.
  EXPECT_EQ((begin + 5) - begin, 5);
  EXPECT_EQ(view.end() - view.begin(), static_cast<ptrdiff_t>(ids.size()));
}

// Test the comparison operators. They only compare the payload pointers and
// check in expensive-check builds that the datatype pointers agree, so the
// same position reached in two ways must compare equal and different
// positions must not.
TEST(IdColumnIteratorTest, comparisonOperators) {
  auto ids = sampleIds();
  const IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  const auto view = vec.asConstView();
  const auto begin = view.begin();
  const auto second = begin + 1;

  // All six operators between the first and the second position.
  EXPECT_TRUE(begin == begin);
  EXPECT_TRUE(begin != second);
  EXPECT_TRUE(begin < second);
  EXPECT_TRUE(begin <= second);
  EXPECT_TRUE(begin <= begin);
  EXPECT_TRUE(second > begin);
  EXPECT_TRUE(second >= begin);
  EXPECT_TRUE(begin >= begin);
  EXPECT_FALSE(second < begin);
  EXPECT_TRUE(begin.compareThreeWay(second) < 0);

  // The third position reached via `+ 2` and via two increments.
  const auto viaArithmetic = begin + 2;
  auto viaIncrement = begin;
  ++viaIncrement;
  ++viaIncrement;
  EXPECT_TRUE(viaArithmetic == viaIncrement);
  EXPECT_FALSE(viaArithmetic != viaIncrement);
  EXPECT_TRUE(viaArithmetic.compareThreeWay(viaIncrement) == 0);
}

// Test that the mutable iterator converts to the const iterator, but not
// the other way round.
TEST(IdColumnIteratorTest, mutableIteratorConvertsToConstIterator) {
  static_assert(std::is_convertible_v<IdColumnIterator, ConstIdColumnIterator>);
  static_assert(
      !std::is_convertible_v<ConstIdColumnIterator, IdColumnIterator>);
  static_assert(std::is_same_v<IdColumnRef::iterator, IdColumnIterator>);
  static_assert(
      std::is_same_v<IdColumnRef::const_iterator, ConstIdColumnIterator>);

  // The converted iterator refers to the same position.
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  const ConstIdColumnIterator it = vec.asView().begin() + 2;
  EXPECT_EQ(*it, ids.at(2));
  EXPECT_TRUE(it == vec.asConstView().begin() + 2);
}

// Test that `iter_swap` swaps the referenced values.
TEST(IdColumnIteratorTest, iterSwapSwapsTheReferencedValues) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};
  const auto view = vec.asView();
  ql::ranges::iter_swap(view.begin() + 1, view.begin() + 2);
  EXPECT_EQ(vec[1], ids.at(2));
  EXPECT_EQ(vec[2], ids.at(1));
}

// Test the iterator in a range-based `for` loop and in `ranges::sort`.
TEST(IdColumnIteratorTest, rangeBasedForAndSort) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};

  // The loop over the const view yields the ids in order.
  std::vector<Id> viaIteration;
  for (Id id : vec.asConstView()) {
    viaIteration.push_back(id);
  }
  EXPECT_THAT(viaIteration, ::testing::ElementsAreArray(ids));

  // Sorting the mutable view sorts the referenced values like sorting the
  // ids themselves.
  const auto byBits = [](const Id id) { return getBitsCompat(id); };
  const auto mutableView = vec.asView();
  ql::ranges::sort(mutableView, {}, byBits);
  std::vector<Id> sortedIds = ids;
  ql::ranges::sort(sortedIds, {}, byBits);
  std::vector afterSort(mutableView.begin(), mutableView.end());
  EXPECT_THAT(afterSort, ::testing::ElementsAreArray(sortedIds));
}

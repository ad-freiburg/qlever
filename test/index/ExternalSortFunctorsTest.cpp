// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <random>
#include <vector>

#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/ExternalSortFunctors.h"

namespace {

// A random `Id` from a small domain (so that there are many ties), of one of
// several datatypes that can be compared bitwise.
Id randomId(std::mt19937_64& engine) {
  std::uniform_int_distribution<int64_t> value{0, 4};
  std::uniform_int_distribution<int> datatype{0, 5};
  int64_t v = value(engine);
  switch (datatype(engine)) {
    case 0:
      return Id::makeFromVocabIndex(VocabIndex::make(v));
    case 1:
      return Id::makeFromInt(v - 2);
    case 2:
      return Id::makeFromDouble(static_cast<double>(v) - 2.5);
    case 3:
      return Id::makeFromBlankNodeIndex(BlankNodeIndex::make(v));
    case 4:
      return Id::makeFromBool(v % 2 == 0);
    default:
      return Id::makeUndefined();
  }
}

// The comparison of `SortTriple` before it was changed to compare arrays of
// bits: a chain of three-way comparisons via `compareWithoutLocalVocab`, with
// the graph column as the last tie breaker.
template <int i0, int i1, int i2, bool hasGraphColumn>
bool referenceLess(const std::vector<Id>& a, const std::vector<Id>& b) {
  auto compare = [](const Id& x, const Id& y) {
    return x.compareWithoutLocalVocab(y);
  };
  auto c1 = compare(a[i0], b[i0]);
  if (c1 != 0) {
    return c1 < 0;
  }
  auto c2 = compare(a[i1], b[i1]);
  if (c2 != 0) {
    return c2 < 0;
  }
  auto c3 = compare(a[i2], b[i2]);
  if constexpr (!hasGraphColumn) {
    return c3 < 0;
  } else {
    if (c3 != 0) {
      return c3 < 0;
    }
    static constexpr auto g = ADDITIONAL_COLUMN_GRAPH_ID;
    return compare(a[g], b[g]) < 0;
  }
}

// Check that `SortTriple<i0, i1, i2, hasGraphColumn>` agrees with
// `referenceLess` on all pairs of a set of random rows (with a payload column
// after the graph column, which must be ignored).
template <int i0, int i1, int i2, bool hasGraphColumn>
void testSortTriple(unsigned seed) {
  std::mt19937_64 engine{seed};
  std::vector<std::vector<Id>> rows;
  for (size_t i = 0; i < 300; ++i) {
    std::vector<Id> row;
    for (size_t c = 0; c < ADDITIONAL_COLUMN_GRAPH_ID + 2; ++c) {
      row.push_back(randomId(engine));
    }
    rows.push_back(std::move(row));
  }
  SortTriple<i0, i1, i2, hasGraphColumn> comparator;
  size_t numLess = 0;
  for (const auto& a : rows) {
    for (const auto& b : rows) {
      bool expected = referenceLess<i0, i1, i2, hasGraphColumn>(a, b);
      ASSERT_EQ(comparator(a, b), expected);
      numLess += expected;
    }
  }
  // The rows are neither all equal nor all different in their keys.
  EXPECT_GT(numLess, 0u);
}
}  // namespace

// _____________________________________________________________________________
TEST(ExternalSortFunctors, sortTripleMatchesThreeWayComparison) {
  testSortTriple<0, 1, 2, true>(1);
  testSortTriple<1, 0, 2, true>(2);
  testSortTriple<2, 0, 1, true>(3);
  testSortTriple<1, 0, 2, false>(4);
  testSortTriple<2, 1, 0, false>(5);
}

// _____________________________________________________________________________
TEST(ExternalSortFunctors, bitwiseKeyColumns) {
  EXPECT_THAT(SortByPSO::bitwiseKeyColumns,
              ::testing::ElementsAre(1, 0, 2, ADDITIONAL_COLUMN_GRAPH_ID));
  EXPECT_THAT(SortByOSP::bitwiseKeyColumns,
              ::testing::ElementsAre(2, 0, 1, ADDITIONAL_COLUMN_GRAPH_ID));
  EXPECT_THAT(SortByPSONoGraphColumn::bitwiseKeyColumns,
              ::testing::ElementsAre(1, 0, 2));
}

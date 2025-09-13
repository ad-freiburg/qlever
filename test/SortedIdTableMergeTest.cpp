// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "index/SortedIdTableMerge.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"

using namespace ad_utility::testing;

namespace {

TEST(SortedIdTableMerge, ErrorChecks) {
  // Row sizes differ
  std::vector<IdTable> idTables;
  idTables.push_back(makeIdTableFromVector({{0}, {1}}));
  idTables.push_back(makeIdTableFromVector({{0, 0}, {1, 1}}));
  AD_EXPECT_THROW_WITH_MESSAGE(
      (sortedIdTableMerge::mergeIdTables<2, 1>(
          idTables, makeAllocator(), {0},
          sortedIdTableMerge::DirectComparator{})),
      ::testing::HasSubstr(
          "All idTables to merge should have the same number of columns. First "
          "idTable has: 1 columns. Failed table had: 2 columns"));

  // Empty IdTables
  std::vector<IdTable> emptyIdTables{};
  AD_EXPECT_THROW_WITH_MESSAGE(
      (sortedIdTableMerge::mergeIdTables<1, 1>(
          emptyIdTables, makeAllocator(), {0},
          sortedIdTableMerge::DirectComparator{})),
      ::testing::HasSubstr(
          "mergeIdTables shouldn't be called with no idTables to merge."));
}

TEST(SortedIdTableMerge, SimplePermutation) {
  // Only one IdTable
  std::vector<IdTable> idTables;
  idTables.push_back(makeIdTableFromVector({{0, 0}, {1, 1}}));
  auto merged = sortedIdTableMerge::mergeIdTables<2, 1>(
      idTables, makeAllocator(), {0}, sortedIdTableMerge::DirectComparator{});
  EXPECT_EQ(merged.size(), 2);
  EXPECT_EQ(merged.numColumns(), 2);
  EXPECT_THAT(merged, matchesIdTableFromVector({{0, 0}, {1, 1}}));

  // Two IdTables
  std::vector<IdTable> idTables2;
  idTables2.push_back(makeIdTableFromVector({{0, 1}, {1, 1}}));
  idTables2.push_back(makeIdTableFromVector({{0, 2}, {1, 0}}));
  auto merged2 = sortedIdTableMerge::mergeIdTables<2, 2>(
      idTables2, makeAllocator(), {0, 1},
      sortedIdTableMerge::DirectComparator{});
  EXPECT_EQ(merged2.size(), 4);
  EXPECT_EQ(merged2.numColumns(), 2);
  EXPECT_THAT(merged2,
              matchesIdTableFromVector({{0, 1}, {0, 2}, {1, 0}, {1, 1}}));

  // Three IdTables
  std::vector<IdTable> idTables3;
  idTables3.push_back(makeIdTableFromVector({{2, 1}, {3, 1}}));
  idTables3.push_back(makeIdTableFromVector({{0, 2}, {1, 0}}));
  idTables3.push_back(makeIdTableFromVector({{0, 1}, {1, 1}}));
  auto merged3 = sortedIdTableMerge::mergeIdTables<2, 2>(
      idTables3, makeAllocator(), {0, 1},
      sortedIdTableMerge::DirectComparator{});
  EXPECT_EQ(merged3.size(), 6);
  EXPECT_EQ(merged3.numColumns(), 2);
  EXPECT_THAT(merged3, matchesIdTableFromVector(
                           {{0, 1}, {0, 2}, {1, 0}, {1, 1}, {2, 1}, {3, 1}}));

  // Four IdTables only sorted on the first col
  std::vector<IdTable> idTables4;
  idTables4.push_back(makeIdTableFromVector({{2, 1}, {3, 0}}));
  idTables4.push_back(makeIdTableFromVector({{1, 1}, {4, 0}}));
  idTables4.push_back(makeIdTableFromVector({{8, 1}, {9, 2}}));
  idTables4.push_back(makeIdTableFromVector({{5, 3}, {6, 2}}));
  auto merged4 = sortedIdTableMerge::mergeIdTables<2, 1>(
      idTables4, makeAllocator(), {0}, sortedIdTableMerge::DirectComparator{});
  EXPECT_EQ(merged4.size(), 8);
  EXPECT_EQ(merged4.numColumns(), 2);
  EXPECT_THAT(
      merged4,
      matchesIdTableFromVector(
          {{1, 1}, {2, 1}, {3, 0}, {4, 0}, {5, 3}, {6, 2}, {8, 1}, {9, 2}}));
}

TEST(SortedIdTableMerge, CustomPermutation) {
  std::vector<IdTable> idTable;
  idTable.push_back(makeIdTableFromVector({{3, 0, 1}, {2, 1, 1}}));
  idTable.push_back(makeIdTableFromVector({{4, 0, 1}, {1, 1, 1}}));
  idTable.push_back(makeIdTableFromVector({{8, 1, 1}, {9, 2, 1}}));
  idTable.push_back(makeIdTableFromVector({{6, 2, 1}, {5, 3, 1}}));
  auto merged = sortedIdTableMerge::mergeIdTables<3, 2>(
      idTable, makeAllocator(), {1, 0}, sortedIdTableMerge::DirectComparator{});
  EXPECT_EQ(merged.size(), 8);
  EXPECT_EQ(merged.numColumns(), 3);
  EXPECT_THAT(merged, matchesIdTableFromVector({{0, 3, 1},
                                                {0, 4, 1},
                                                {1, 1, 1},
                                                {1, 2, 1},
                                                {1, 8, 1},
                                                {2, 6, 1},
                                                {2, 9, 1},
                                                {3, 5, 1}}));
}

}  // namespace

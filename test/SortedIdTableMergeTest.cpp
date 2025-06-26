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
      SortedIdTableMerge::mergeIdTables(std::move(idTables), makeAllocator(),
                                        10_MB),
      ::testing::HasSubstr(
          "All idTables to merge should have the same number of columns. First "
          "table had: 1 columns. Failed table had: 2 columns"));

  // Empty IdTables
  std::vector<IdTable> emptyIdTables{};
  AD_EXPECT_THROW_WITH_MESSAGE(
      SortedIdTableMerge::mergeIdTables(std::move(emptyIdTables),
                                        makeAllocator(), 10_MB),
      ::testing::HasSubstr(
          "This method shouldn't be called without idTables to merge"));
}

TEST(SortedIdTableMerge, StandardComparator) {
  // Only one IdTable
  std::vector<IdTable> idTables;
  idTables.push_back(makeIdTableFromVector({{0, 0}, {1, 1}}));
  auto merged = SortedIdTableMerge::mergeIdTables(std::move(idTables),
                                                  makeAllocator(), 10_MB);
  EXPECT_EQ(merged.size(), 2);
  EXPECT_EQ(merged.numColumns(), 2);
  EXPECT_THAT(merged, matchesIdTableFromVector({{0, 0}, {1, 1}}));

  // Two IdTables
  std::vector<IdTable> idTables2;
  idTables2.push_back(makeIdTableFromVector({{0, 1}, {1, 1}}));
  idTables2.push_back(makeIdTableFromVector({{0, 2}, {1, 0}}));
  auto merged2 = SortedIdTableMerge::mergeIdTables(std::move(idTables2),
                                                   makeAllocator(), 10_MB);
  EXPECT_EQ(merged2.size(), 4);
  EXPECT_EQ(merged2.numColumns(), 2);
  EXPECT_THAT(merged2,
              matchesIdTableFromVector({{0, 1}, {0, 2}, {1, 0}, {1, 1}}));

  // Three IdTables
  std::vector<IdTable> idTables3;
  idTables3.push_back(makeIdTableFromVector({{2, 1}, {3, 1}}));
  idTables3.push_back(makeIdTableFromVector({{0, 2}, {1, 0}}));
  idTables3.push_back(makeIdTableFromVector({{0, 1}, {1, 0}}));
  auto merged3 = SortedIdTableMerge::mergeIdTables(std::move(idTables3),
                                                   makeAllocator(), 10_MB);
  EXPECT_EQ(merged3.size(), 6);
  EXPECT_EQ(merged3.numColumns(), 2);
  EXPECT_THAT(merged3, matchesIdTableFromVector(
                           {{0, 1}, {0, 2}, {1, 0}, {1, 0}, {2, 1}, {3, 1}}));
}

TEST(SortedIdTableMerge, CustomComparator) {
  auto comparator = [](const std::vector<ValueId>& a,
                       const std::vector<ValueId>& b) {
    if (a[1].getBits() < b[1].getBits()) {
      return true;
    } else if (a[1].getBits() > b[1].getBits()) {
      return false;
    }
    if (a[0].getBits() < b[0].getBits()) {
      return true;
    } else if (a[0].getBits() > b[0].getBits()) {
      return false;
    }
    if (a[2].getBits() < b[2].getBits()) {
      return true;
    } else if (a[2].getBits() > b[2].getBits()) {
      return false;
    }
    return true;
  };

  // Only one IdTable
  std::vector<IdTable> idTables;
  idTables.push_back(makeIdTableFromVector({{0, 0, 0}, {1, 1, 1}}));
  auto merged = SortedIdTableMerge::mergeIdTables(
      std::move(idTables), makeAllocator(), 10_MB, false, comparator);
  EXPECT_EQ(merged.size(), 2);
  EXPECT_EQ(merged.numColumns(), 3);
  EXPECT_THAT(merged, matchesIdTableFromVector({{0, 0, 0}, {1, 1, 1}}));

  // Two IdTables
  std::vector<IdTable> idTables2;
  idTables2.push_back(makeIdTableFromVector({{0, 1, 0}, {1, 1, 1}}));
  idTables2.push_back(makeIdTableFromVector({{1, 0, 1}, {0, 2, 0}}));
  auto merged2 = SortedIdTableMerge::mergeIdTables(
      std::move(idTables2), makeAllocator(), 10_MB, false, comparator);
  EXPECT_EQ(merged2.size(), 4);
  EXPECT_EQ(merged2.numColumns(), 3);
  EXPECT_THAT(merged2, matchesIdTableFromVector(
                           {{1, 0, 1}, {0, 1, 0}, {1, 1, 1}, {0, 2, 0}}));

  // Three IdTables
  std::vector<IdTable> idTables3;
  idTables3.push_back(makeIdTableFromVector({{2, 1, 0}, {3, 1, 0}}));
  idTables3.push_back(makeIdTableFromVector({{1, 0, 1}, {0, 2, 0}}));
  idTables3.push_back(makeIdTableFromVector({{1, 0, 1}, {2, 1, 1}}));
  auto merged3 = SortedIdTableMerge::mergeIdTables(
      std::move(idTables3), makeAllocator(), 10_MB, false, comparator);
  EXPECT_EQ(merged3.size(), 6);
  EXPECT_EQ(merged3.numColumns(), 3);
  EXPECT_THAT(
      merged3,
      matchesIdTableFromVector(
          {{1, 0, 1}, {1, 0, 1}, {2, 1, 0}, {2, 1, 1}, {3, 1, 0}, {0, 2, 0}}));
}

TEST(SortedIdTableMerge, Distinct) {
  std::vector<IdTable> idTables3;
  idTables3.push_back(makeIdTableFromVector({{2, 1}, {3, 1}}));
  idTables3.push_back(makeIdTableFromVector({{0, 2}, {1, 0}}));
  idTables3.push_back(makeIdTableFromVector({{0, 1}, {1, 0}}));
  auto merged3 = SortedIdTableMerge::mergeIdTables(
      std::move(idTables3), makeAllocator(), 10_MB, true);
  EXPECT_EQ(merged3.size(), 5);
  EXPECT_EQ(merged3.numColumns(), 2);
  EXPECT_THAT(merged3, matchesIdTableFromVector(
                           {{0, 1}, {0, 2}, {1, 0}, {2, 1}, {3, 1}}));
}

}  // namespace

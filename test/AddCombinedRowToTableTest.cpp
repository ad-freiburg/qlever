//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "engine/AddCombinedRowToTable.h"

namespace {
static constexpr auto U = Id::makeUndefined();

void testWithAllBuffersizes(const auto& testFunction) {
  for (auto bufferSize : std::views::iota(0, 10)) {
    testFunction(bufferSize);
  }
  testFunction(100'000);
}
}  // namespace

// _______________________________________________________________________________
TEST(AddCombinedRowToTable, OneJoinColumn) {
  auto testWithBufferSize = [](size_t bufferSize) {
    auto left = makeIdTableFromVector({{3, 4}, {7, 8}, {11, 10}, {14, 11}});
    auto right =
        makeIdTableFromVector({{7, 14, 0}, {9, 10, 1}, {14, 8, 2}, {33, 5, 3}});
    auto result = makeIdTableFromVector({});
    result.setNumColumns(4);
    auto adder = ad_utility::AddCombinedRowToIdTable(
        1, left.asStaticView<0>(), right.asStaticView<0>(), std::move(result),
        bufferSize);
    adder.addRow(1, 0);
    adder.setOnlyLeftInputForOptionalJoin(left);
    adder.addOptionalRow(2);
    adder.setInput(left, right);
    adder.addRow(3, 2);
    auto numUndefined = adder.numUndefinedPerColumn();
    result = std::move(adder).resultTable();

    auto expected =
        makeIdTableFromVector({{7, 8, 14, 0}, {11, 10, U, U}, {14, 11, 8, 2}});
    ASSERT_EQ(result, expected);

    auto expectedUndefined = std::vector<size_t>{0, 0, 1, 1};
    ASSERT_EQ(numUndefined, expectedUndefined);
  };
  testWithAllBuffersizes(testWithBufferSize);
}

// _______________________________________________________________________________
TEST(AddCombinedRowToTable, TwoJoinColumns) {
  auto testWithBufferSize = [](size_t bufferSize) {
    auto left = makeIdTableFromVector({{3, 4}, {7, 8}, {11, 10}, {14, U}});
    auto right =
        makeIdTableFromVector({{U, 8, 0}, {9, 10, 1}, {14, 11, 2}, {33, 5, 3}});
    auto result = makeIdTableFromVector({});
    result.setNumColumns(3);
    auto adder = ad_utility::AddCombinedRowToIdTable(
        2, left.asStaticView<0>(), right.asStaticView<0>(), std::move(result),
        bufferSize);
    adder.addRow(1, 0);
    adder.addOptionalRow(2);
    adder.addRow(3, 2);
    auto numUndefined = adder.numUndefinedPerColumn();
    result = std::move(adder).resultTable();

    auto expected =
        makeIdTableFromVector({{7, 8, 0}, {11, 10, U}, {14, 11, 2}});
    ASSERT_EQ(result, expected);

    auto expectedUndefined = std::vector<size_t>{0, 0, 1};
    ASSERT_EQ(numUndefined, expectedUndefined);
  };
  testWithAllBuffersizes(testWithBufferSize);
}

// _______________________________________________________________________________
TEST(AddCombinedRowToTable, UndefInInput) {
  auto testWithBufferSize = [](size_t bufferSize) {
    auto left = makeIdTableFromVector({{U, 5}, {2, U}, {3, U}, {4, U}});
    auto right = makeIdTableFromVector({{1}, {3}, {4}, {U}});
    auto result = makeIdTableFromVector({});
    result.setNumColumns(2);
    auto adder = ad_utility::AddCombinedRowToIdTable(
        1, left.asStaticView<0>(), right.asStaticView<0>(), std::move(result),
        bufferSize);
    adder.addRow(0, 0);
    adder.addRow(0, 1);
    adder.addRow(2, 1);
    adder.addRow(0, 2);
    adder.addRow(3, 2);
    adder.addRow(0, 3);
    auto numUndefined = adder.numUndefinedPerColumn();
    result = std::move(adder).resultTable();

    auto expected =
        makeIdTableFromVector({{1, 5}, {3, 5}, {3, U}, {4, 5}, {4, U}, {U, 5}});
    ASSERT_EQ(result, expected);

    auto expectedUndefined = std::vector<size_t>{1, 2};
    ASSERT_EQ(numUndefined, expectedUndefined);
  };
  testWithAllBuffersizes(testWithBufferSize);
}

// _______________________________________________________________________________
TEST(AddCombinedRowToTable, setInput) {
  auto testWithBufferSize = [](size_t bufferSize) {
    {
      auto result = makeIdTableFromVector({});
      result.setNumColumns(2);
      auto adder =
          ad_utility::AddCombinedRowToIdTable(1, std::move(result), bufferSize);
      // It is okay to flush even if no inputs were specified, as long as we
      // haven't pushed any rows yet.
      EXPECT_NO_THROW(adder.flush());
      if (ad_utility::areExpensiveChecksEnabled || bufferSize == 0) {
        EXPECT_ANY_THROW(adder.addRow(0, 0));
      } else {
        adder.addRow(0, 0);
        EXPECT_ANY_THROW(adder.flush());
      }
    }

    auto result = makeIdTableFromVector({});
    result.setNumColumns(3);
    auto adder =
        ad_utility::AddCombinedRowToIdTable(1, std::move(result), bufferSize);
    auto left = makeIdTableFromVector({{U, 5}, {2, U}, {3, U}, {4, U}});
    auto right = makeIdTableFromVector({{1, 2}, {3, 4}, {4, 7}, {U, 8}});
    adder.setInput(left, right);
    adder.addRow(0, 0);
    adder.addRow(0, 1);
    adder.addRow(2, 1);
    adder.addRow(0, 2);
    adder.addRow(3, 2);
    adder.addRow(0, 3);
    adder.setInput(right, left);
    adder.addRow(0, 0);
    adder.addRow(1, 0);
    adder.addRow(1, 2);
    adder.addRow(2, 0);
    adder.addRow(2, 3);
    adder.addRow(3, 0);
    result = std::move(adder).resultTable();

    auto expected = makeIdTableFromVector({{1, 5, 2},
                                           {3, 5, 4},
                                           {3, U, 4},
                                           {4, 5, 7},
                                           {4, U, 7},
                                           {U, 5, 8},
                                           {1, 2, 5},
                                           {3, 4, 5},
                                           {3, 4, U},
                                           {4, 7, 5},
                                           {4, 7, U},
                                           {U, 8, 5}});
    ASSERT_EQ(result, expected);
  };
  testWithAllBuffersizes(testWithBufferSize);
}

// _______________________________________________________________________________
TEST(AddCombinedRowToTable, cornerCases) {
  auto testWithBufferSize = [](size_t bufferSize) {
    auto result = makeIdTableFromVector({});
    result.setNumColumns(3);
    auto adder =
        ad_utility::AddCombinedRowToIdTable(2, std::move(result), bufferSize);
    auto left = makeIdTableFromVector({{U, 5}, {2, U}, {3, U}, {4, U}});
    auto right = makeIdTableFromVector({{1, 2}, {3, 4}, {4, 7}, {U, 8}});
    // We have specified two join columns and our inputs have two columns each,
    // so the result should also have two columns, but it has three.
    EXPECT_ANY_THROW(adder.setInput(left, right));

    left = makeIdTableFromVector({{1}, {2}, {3}});

    // Left has only one column, but we have specified two join columns.
    EXPECT_ANY_THROW(adder.setInput(left, right));
    // The same test with the arguments switched.
    EXPECT_ANY_THROW(adder.setInput(right, left));
  };
  testWithAllBuffersizes(testWithBufferSize);
}

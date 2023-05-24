//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "engine/AddCombinedRowToTable.h"

namespace {
static constexpr auto U = Id::makeUndefined();
}

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
    adder.addOptionalRow(2);
    adder.addRow(3, 2);
    auto numUndefined = adder.numUndefinedPerColumn();
    result = std::move(adder).resultTable();

    auto expected =
        makeIdTableFromVector({{7, 8, 14, 0}, {11, 10, U, U}, {14, 11, 8, 2}});
    ASSERT_EQ(result, expected);

    auto expectedUndefined = std::vector<size_t>{0, 0, 1, 1};
    ASSERT_EQ(numUndefined, expectedUndefined);
  };
  testWithBufferSize(100'000);
  testWithBufferSize(1);
  testWithBufferSize(2);
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
  testWithBufferSize(100'000);
  testWithBufferSize(1);
  testWithBufferSize(2);
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
  testWithBufferSize(100'000);
  testWithBufferSize(1);
  testWithBufferSize(2);
}

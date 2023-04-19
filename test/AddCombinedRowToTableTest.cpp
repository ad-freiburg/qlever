//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "engine/AddCombinedRowToTable.h"

TEST(AddCombinedRowToTable, AddRows) {
  auto left = makeIdTableFromVector({{3, 4}, {7, 8}, {11, 10}, {14, 11}});
  auto right =
      makeIdTableFromVector({{7, 14, 0}, {9, 10, 1}, {14, 8, 2}, {33, 5, 3}});
  auto result = makeIdTableFromVector({});
  result.setNumColumns(3);
  auto adder = ad_utility::AddCombinedRowToIdTable(
      1, left.asStaticView<0>(), right.asStaticView<0>(), std::move(result));
  adder.addRow(1, 0);
  adder.addOptionalRow(2);
  adder.addRow(3, 2);
  result = std::move(adder).resultTable();

  // auto expected = makeIdTableFromVector({{7, 8, 14, 0}, })
}

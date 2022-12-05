//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/IdTableColumnBased.h"
#include "gtest/gtest.h"

using namespace columnBasedIdTable;

Id I(int i) { return Id::makeFromInt(i); }

TEST(IdTableColumnBased, Row) {
  Id id1 = Id::makeFromInt(1);
  Id id2 = Id::makeFromInt(2);
  Id id3 = Id::makeFromInt(3);
  Row row{std::vector{&id1, &id2, &id3}};

  row[1] = Id::makeFromInt(42);
  ASSERT_EQ(I(42), id2);

  Row row2 = row;
  ASSERT_EQ(I(1), row2[0]);
  ASSERT_EQ(I(42), row2[1]);
  ASSERT_EQ(I(3), row2[2]);

  row2[2] = I(5);
  ASSERT_EQ(I(5), row2[2]);
  ASSERT_EQ(I(3), row[2]);
  ASSERT_EQ(I(3), id3);

  // TODO<joka921> Also add tests for static rows and for const rows and for all
  // member functions.
}

TEST(IdTableColumnBased, IdTable) {
  IdTable table;
  table.setCols(2);
  table.resize(4);
  for (size_t i = 0; i < 4; ++i) {
    table(i, 0) = I(3 - i);
    table(i, 1) = I(i);
  }

  /*
  auto it = table.begin();

  auto it2 = it;

  (*it2)[0] = I(42);
  ASSERT_EQ(table(0, 0), I(42));

  ++it2;
  swap(*it, *it2);
  ASSERT_EQ(table(1, 0), I(42));
   */

  /*
  std::sort(table.begin(), table.end(),
            [](const auto& r1, const auto& r2) { return r1[0] < r2[0]; });
            */

  std::reverse(table.begin(), table.end());
  // Does not yet fulfill the requirements of a random_access_range, but
  // hopefully we'll get there.
  // std::ranges::sort(table, [](const auto& r1, const auto& r2) {return r1[0] <
  // r2[0];});

  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(table(i, 0), I(i)) << i;
    EXPECT_EQ(table(i, 1), I(3 - i)) << i;
  }
}
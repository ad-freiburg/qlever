//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>


#include "gtest/gtest.h"

#include "engine/IdTableColumnBased.h"

using namespace columnBasedIdTable;

Id I(int i) {
  return Id::makeFromInt(i);
}

TEST(IdTableColumnBase, Row) {
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

}
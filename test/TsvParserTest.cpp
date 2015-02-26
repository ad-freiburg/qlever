// Copyright 2014, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <vector>
#include "../src/parser/TsvParser.h"
#include "gtest/gtest.h"

using std::vector;

TEST(TsvParserTest, testGetLine) {
  TsvParser p("data/test_tsv_file_1.tsv");
  array<string, 3> a;
  vector<array<string, 3>> v;
  while (p.getLine(a)) {
    v.push_back(a);
  }
  ASSERT_EQ(3, v.size());
  ASSERT_EQ("l0c0", v[0][0]);
  ASSERT_EQ("l0c1", v[0][1]);
  ASSERT_EQ("l0c2", v[0][2]);
  ASSERT_EQ("l1c0", v[1][0]);
  ASSERT_EQ("l1c1", v[1][1]);
  ASSERT_EQ("l1c2", v[1][2]);
  ASSERT_EQ("l2c0", v[2][0]);
  ASSERT_EQ("l2c1", v[2][1]);
  ASSERT_EQ("l2c2", v[2][2]);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
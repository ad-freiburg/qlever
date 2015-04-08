// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include "../src/engine/Engine.h"


TEST(EngineTest, joinTest) {
  Engine e;
  vector<array<Id, 2>> a;
  a.push_back(array<Id, 2>{{1, 1}});
  a.push_back(array<Id, 2>{{1, 3}});
  a.push_back(array<Id, 2>{{2, 1}});
  a.push_back(array<Id, 2>{{2, 2}});
  a.push_back(array<Id, 2>{{4, 1}});
  vector<array<Id, 2>> b;
  b.push_back(array<Id, 2>{{1, 3}});
  b.push_back(array<Id, 2>{{1, 8}});
  b.push_back(array<Id, 2>{{3, 1}});
  b.push_back(array<Id, 2>{{4, 2}});
  vector<array<Id, 3>> res;
  e.join(a, 0, b, 0, &res);


  ASSERT_EQ(1, res[0][0]);
  ASSERT_EQ(1, res[0][1]);
  ASSERT_EQ(3, res[0][2]);

  ASSERT_EQ(1, res[1][0]);
  ASSERT_EQ(1, res[1][1]);
  ASSERT_EQ(8, res[1][2]);

  ASSERT_EQ(1, res[2][0]);
  ASSERT_EQ(3, res[2][1]);
  ASSERT_EQ(3, res[2][2]);

  ASSERT_EQ(1, res[3][0]);
  ASSERT_EQ(3, res[3][1]);
  ASSERT_EQ(8, res[3][2]);

  ASSERT_EQ(5, res.size());

  ASSERT_EQ(4, res[4][0]);
  ASSERT_EQ(1, res[4][1]);
  ASSERT_EQ(2, res[4][2]);
};

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


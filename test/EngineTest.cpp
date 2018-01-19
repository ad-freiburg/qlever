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


  ASSERT_EQ(1u, res[0][0]);
  ASSERT_EQ(1u, res[0][1]);
  ASSERT_EQ(3u, res[0][2]);

  ASSERT_EQ(1u, res[1][0]);
  ASSERT_EQ(1u, res[1][1]);
  ASSERT_EQ(8u, res[1][2]);

  ASSERT_EQ(1u, res[2][0]);
  ASSERT_EQ(3u, res[2][1]);
  ASSERT_EQ(3u, res[2][2]);

  ASSERT_EQ(1u, res[3][0]);
  ASSERT_EQ(3u, res[3][1]);
  ASSERT_EQ(8u, res[3][2]);

  ASSERT_EQ(5u, res.size());

  ASSERT_EQ(4u, res[4][0]);
  ASSERT_EQ(1u, res[4][1]);
  ASSERT_EQ(2u, res[4][2]);

  res.clear();
  for (size_t i = 1; i <= 10000; ++i) {
    b.push_back(array<Id, 2>{{4 + i, 2 + i}});
  }
  a.push_back(array<Id, 2>{{400000, 200000}});
  b.push_back(array<Id, 2>{{400000, 200000}});
  e.join(a, 0, b, 0, &res);
  ASSERT_EQ(6u, res.size());

  a.clear();
  b.clear();
  res.clear();

  for (size_t i = 1; i <= 10000; ++i) {
    a.push_back(array<Id, 2>{{4 + i, 2 + i}});
  }
  a.push_back(array<Id, 2>{{40000, 200000}});
  b.push_back(array<Id, 2>{{40000, 200000}});

  for (size_t i = 1; i <= 10000; ++i) {
    a.push_back(array<Id, 2>{{40000 + i, 2 + i}});
  }
  a.push_back(array<Id, 2>{{4000001, 200000}});
  b.push_back(array<Id, 2>{{4000001, 200000}});
  e.join(a, 0, b, 0, &res);
  ASSERT_EQ(2u, res.size());
};

TEST(EngineTest, optionalJoinTest) {
  Engine e;
  vector<array<Id, 3>> a;
  a.push_back(array<Id, 3>{{4, 1, 2}});
  a.push_back(array<Id, 3>{{2, 1, 3}});
  a.push_back(array<Id, 3>{{1, 1, 4}});
  a.push_back(array<Id, 3>{{2, 2, 1}});
  a.push_back(array<Id, 3>{{1, 3, 1}});
  vector<array<Id, 3>> b;
  b.push_back(array<Id, 3>{{3, 3, 1}});
  b.push_back(array<Id, 3>{{1, 8, 1}});
  b.push_back(array<Id, 3>{{4, 2, 2}});
  b.push_back(array<Id, 3>{{1, 1, 3}});
  vector<array<Id, 4>> res;
  vector<array<size_t, 2>> jcls;
  jcls.push_back(array<size_t, 2>{{1, 2}});
  jcls.push_back(array<size_t, 2>{{2, 1}});
  e.optionalJoin<vector<array<Id, 3>>, vector<array<Id, 3>>, Id, 4>
      (a, b, false, true, jcls, &res);

  ASSERT_EQ(5u, res.size());

  ASSERT_EQ(4u, res[0][0]);
  ASSERT_EQ(1u, res[0][1]);
  ASSERT_EQ(2u, res[0][2]);
  ASSERT_EQ(ID_NO_VALUE, res[0][3]);

  ASSERT_EQ(2u, res[1][0]);
  ASSERT_EQ(1u, res[1][1]);
  ASSERT_EQ(3u, res[1][2]);
  ASSERT_EQ(3u, res[1][3]);

  ASSERT_EQ(1u, res[2][0]);
  ASSERT_EQ(1u, res[2][1]);
  ASSERT_EQ(4u, res[2][2]);
  ASSERT_EQ(ID_NO_VALUE, res[2][3]);

  ASSERT_EQ(2u, res[3][0]);
  ASSERT_EQ(2u, res[3][1]);
  ASSERT_EQ(1u, res[3][2]);
  ASSERT_EQ(ID_NO_VALUE, res[3][3]);

  ASSERT_EQ(1u, res[4][0]);
  ASSERT_EQ(3u, res[4][1]);
  ASSERT_EQ(1u, res[4][2]);
  ASSERT_EQ(1u, res[4][3]);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


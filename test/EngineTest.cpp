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
  e.optionalJoin<vector<array<Id, 3>>, vector<array<Id, 3>>, array<Id, 4>, 4>
      (a, b, false, true, jcls, &res, 4);

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

  vector<vector<Id>> va;
  va.push_back(vector<Id>{{1, 2, 3, 4, 5, 6}});
  va.push_back(vector<Id>{{1, 2, 3, 7, 5, 6}});
  va.push_back(vector<Id>{{7, 6, 5, 4, 3, 2}});

  vector<array<Id, 3>> vb;
  vb.push_back(array<Id, 3>{{2, 3, 4}});
  vb.push_back(array<Id, 3>{{2, 3, 5}});
  vb.push_back(array<Id, 3>{{6, 7, 4}});

  vector<vector<Id>> vres;
  jcls.clear();
  jcls.push_back(array<size_t, 2>{{1, 0}});
  jcls.push_back(array<size_t, 2>{{2, 1}});

  // The template size parameter can be at most 6 (the maximum number
  // of fixed size columns plus one).
  e.optionalJoin<vector<vector<Id>>, vector<array<Id, 3>>, vector<Id>, 6>
      (va, vb, true, false, jcls, &vres, 7);

  ASSERT_EQ(5u, vres.size());
  ASSERT_EQ(7u, vres[0].size());
  ASSERT_EQ(7u, vres[1].size());
  ASSERT_EQ(7u, vres[2].size());
  ASSERT_EQ(7u, vres[3].size());
  ASSERT_EQ(7u, vres[4].size());

  vector<Id> r{1, 2, 3, 4, 5, 6, 4};
  ASSERT_EQ(r, vres[0]);
  r = {1, 2, 3, 4, 5, 6, 5};
  ASSERT_EQ(r, vres[1]);
  r = {1, 2, 3, 7, 5, 6, 4};
  ASSERT_EQ(r, vres[2]);
  r = {1, 2, 3, 7, 5, 6, 5};
  ASSERT_EQ(r, vres[3]);
  r = {ID_NO_VALUE, 6, 7, ID_NO_VALUE, ID_NO_VALUE, ID_NO_VALUE, 4};
  ASSERT_EQ(r, vres[4]);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


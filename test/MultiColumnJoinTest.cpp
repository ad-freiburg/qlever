// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <array>
#include <vector>

#include <gtest/gtest.h>
#include "../src/engine/MultiColumnJoin.h"

TEST(EngineTest, multiColumnJoinTest) {
  using std::array;
  using std::vector;

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
  vector<array<Id, 2>> jcls;
  jcls.push_back(array<Id, 2>{{1, 2}});
  jcls.push_back(array<Id, 2>{{2, 1}});

  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  MultiColumnJoin::computeMultiColumnJoin<vector<array<Id, 3>>,
                                          vector<array<Id, 3>>, array<Id, 4>>(
      a, b, jcls, &res, 4u);

  ASSERT_EQ(2u, res.size());

  ASSERT_EQ(2u, res[0][0]);
  ASSERT_EQ(1u, res[0][1]);
  ASSERT_EQ(3u, res[0][2]);
  ASSERT_EQ(3u, res[0][3]);

  ASSERT_EQ(1u, res[1][0]);
  ASSERT_EQ(3u, res[1][1]);
  ASSERT_EQ(1u, res[1][2]);
  ASSERT_EQ(1u, res[1][3]);

  // Test the multi column join with variable sized data.
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
  jcls.push_back(array<Id, 2>{{1, 0}});
  jcls.push_back(array<Id, 2>{{2, 1}});

  // The template size parameter can be at most 6 (the maximum number
  // of fixed size columns plus one).
  MultiColumnJoin::computeMultiColumnJoin<vector<vector<Id>>,
                                          vector<array<Id, 3>>, vector<Id>>(
      va, vb, jcls, &vres, 7u);

  ASSERT_EQ(4u, vres.size());
  ASSERT_EQ(7u, vres[0].size());
  ASSERT_EQ(7u, vres[1].size());
  ASSERT_EQ(7u, vres[2].size());
  ASSERT_EQ(7u, vres[3].size());

  vector<Id> r{1, 2, 3, 4, 5, 6, 4};
  ASSERT_EQ(r, vres[0]);
  r = {1, 2, 3, 4, 5, 6, 5};
  ASSERT_EQ(r, vres[1]);
  r = {1, 2, 3, 7, 5, 6, 4};
  ASSERT_EQ(r, vres[2]);
  r = {1, 2, 3, 7, 5, 6, 5};
  ASSERT_EQ(r, vres[3]);
}

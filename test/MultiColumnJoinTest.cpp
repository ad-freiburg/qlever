// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "./util/AllocatorTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/MultiColumnJoin.h"

using ad_utility::testing::makeAllocator;
namespace {
auto V = ad_utility::testing::VocabId;
}

TEST(EngineTest, multiColumnJoinTest) {
  using std::array;
  using std::vector;

  IdTable a(3, makeAllocator());
  a.push_back({V(4), V(1), V(2)});
  a.push_back({V(2), V(1), V(3)});
  a.push_back({V(1), V(1), V(4)});
  a.push_back({V(2), V(2), V(1)});
  a.push_back({V(1), V(3), V(1)});
  IdTable b(3, makeAllocator());
  b.push_back({V(3), V(3), V(1)});
  b.push_back({V(1), V(8), V(1)});
  b.push_back({V(4), V(2), V(2)});
  b.push_back({V(1), V(1), V(3)});
  IdTable res(4, makeAllocator());
  vector<array<ColumnIndex, 2>> jcls;
  jcls.push_back(array<ColumnIndex, 2>{{1, 2}});
  jcls.push_back(array<ColumnIndex, 2>{{2, 1}});

  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  int aWidth = a.numColumns();
  int bWidth = b.numColumns();
  int resWidth = res.numColumns();
  CALL_FIXED_SIZE((std::array{aWidth, bWidth, resWidth}),
                  MultiColumnJoin::computeMultiColumnJoin, a, b, jcls, &res);

  ASSERT_EQ(2u, res.size());

  ASSERT_EQ(V(2u), res[0][0]);
  ASSERT_EQ(V(1u), res[0][1]);
  ASSERT_EQ(V(3u), res[0][2]);
  ASSERT_EQ(V(3u), res[0][3]);

  ASSERT_EQ(V(1u), res[1][0]);
  ASSERT_EQ(V(3u), res[1][1]);
  ASSERT_EQ(V(1u), res[1][2]);
  ASSERT_EQ(V(1u), res[1][3]);

  // Test the multi column join with variable sized data.
  IdTable va(6, makeAllocator());
  va.push_back({V(1), V(2), V(3), V(4), V(5), V(6)});
  va.push_back({V(1), V(2), V(3), V(7), V(5), V(6)});
  va.push_back({V(7), V(6), V(5), V(4), V(3), V(2)});

  IdTable vb(3, makeAllocator());
  vb.push_back({V(2), V(3), V(4)});
  vb.push_back({V(2), V(3), V(5)});
  vb.push_back({V(6), V(7), V(4)});

  IdTable vres(7, makeAllocator());
  jcls.clear();
  jcls.push_back({1, 0});
  jcls.push_back({2, 1});

  // The template size parameter can be at most 6 (the maximum number
  // of fixed size columns plus one).
  aWidth = va.numColumns();
  bWidth = vb.numColumns();
  resWidth = vres.numColumns();
  CALL_FIXED_SIZE((std::array{aWidth, bWidth, resWidth}),
                  MultiColumnJoin::computeMultiColumnJoin, va, vb, jcls, &vres);

  ASSERT_EQ(4u, vres.size());
  ASSERT_EQ(7u, vres.numColumns());

  IdTable wantedRes(7, makeAllocator());
  wantedRes.push_back({V(1), V(2), V(3), V(4), V(5), V(6), V(4)});
  wantedRes.push_back({V(1), V(2), V(3), V(4), V(5), V(6), V(5)});
  wantedRes.push_back({V(1), V(2), V(3), V(7), V(5), V(6), V(4)});
  wantedRes.push_back({V(1), V(2), V(3), V(7), V(5), V(6), V(5)});

  ASSERT_EQ(wantedRes[0], vres[0]);
  ASSERT_EQ(wantedRes[1], vres[1]);
  ASSERT_EQ(wantedRes[2], vres[2]);
  ASSERT_EQ(wantedRes[3], vres[3]);
}

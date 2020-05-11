// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "../src/engine/CallFixedSize.h"
#include "../src/engine/Minus.h"

TEST(EngineTest, minusTest) {
  using std::array;
  using std::vector;

  IdTable a(3);
  a.push_back({8, 1, 2});
  a.push_back({1, 2, 1});
  a.push_back({2, 1, 4});
  a.push_back({5, 4, 1});
  a.push_back({8, 2, 3});
  a.push_back({ID_NO_VALUE, 2, 1});
  IdTable b(4);
  b.push_back({1, 2, 4, 5});
  b.push_back({3, 3, 1, 5});
  b.push_back({1, 8, 1, 5});
  b.push_back({4, ID_NO_VALUE, 2, 5});
  IdTable res(3);
  vector<array<Id, 2>> jcls;
  jcls.push_back(array<Id, 2>{{0, 1}});
  jcls.push_back(array<Id, 2>{{1, 0}});

  IdTable wantedRes(3);
  wantedRes.push_back({1, 2, 1});
  wantedRes.push_back({8, 2, 3});
  wantedRes.push_back({ID_NO_VALUE, 2, 1});

  // Subtract b from a on the column pairs 1,2 and 2,1 (entries from columns 1
  // of a have to equal those of column 2 of b and vice versa).
  int aWidth = a.cols();
  int bWidth = b.cols();
  int resWidth = res.cols();
  CALL_FIXED_SIZE_3(aWidth, bWidth, resWidth, Minus::computeMinus, a, b, jcls,
                    &res);

  ASSERT_EQ(wantedRes.size(), res.size());

  ASSERT_EQ(wantedRes[0], res[0]);
  ASSERT_EQ(wantedRes[1], res[1]);
  ASSERT_EQ(wantedRes[2], res[2]);

  // Test minus with variable sized data.
  IdTable va(6);
  va.push_back({1, 2, 3, 4, 5, 6});
  va.push_back({1, 2, 3, 7, 5, 6});
  va.push_back({7, 6, 5, 4, 3, 2});

  IdTable vb(3);
  vb.push_back({2, 3, 4});
  vb.push_back({2, 3, 5});
  vb.push_back({6, 7, 4});

  IdTable vres(6);
  jcls.clear();
  jcls.push_back({1, 0});
  jcls.push_back({2, 1});

  // The template size parameter can be at most 6 (the maximum number
  // of fixed size columns plus one).
  aWidth = va.cols();
  bWidth = vb.cols();
  resWidth = vres.cols();
  CALL_FIXED_SIZE_3(aWidth, bWidth, resWidth, Minus::computeMinus, va, vb, jcls,
                    &vres);

  wantedRes = IdTable(6);
  wantedRes.push_back({7, 6, 5, 4, 3, 2});
  ASSERT_EQ(wantedRes.size(), vres.size());
  ASSERT_EQ(wantedRes.cols(), vres.cols());

  ASSERT_EQ(wantedRes[0], vres[0]);
}

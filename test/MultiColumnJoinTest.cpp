// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "../src/engine/CallFixedSize.h"
#include "../src/engine/MultiColumnJoin.h"
ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}
auto I = [](const auto& id) { return Id::make(id); };

TEST(EngineTest, multiColumnJoinTest) {
  using std::array;
  using std::vector;

  IdTable a(3, allocator());
  a.push_back({I(4), I(1), I(2)});
  a.push_back({I(2), I(1), I(3)});
  a.push_back({I(1), I(1), I(4)});
  a.push_back({I(2), I(2), I(1)});
  a.push_back({I(1), I(3), I(1)});
  IdTable b(3, allocator());
  b.push_back({I(3), I(3), I(1)});
  b.push_back({I(1), I(8), I(1)});
  b.push_back({I(4), I(2), I(2)});
  b.push_back({I(1), I(1), I(3)});
  IdTable res(4, allocator());
  vector<array<ColumnIndex, 2>> jcls;
  jcls.push_back(array<ColumnIndex, 2>{{1, 2}});
  jcls.push_back(array<ColumnIndex, 2>{{2, 1}});

  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  int aWidth = a.cols();
  int bWidth = b.cols();
  int resWidth = res.cols();
  CALL_FIXED_SIZE_3(aWidth, bWidth, resWidth,
                    MultiColumnJoin::computeMultiColumnJoin, a, b, jcls, &res);

  ASSERT_EQ(2u, res.size());

  ASSERT_EQ(I(2u), res[0][0]);
  ASSERT_EQ(I(1u), res[0][1]);
  ASSERT_EQ(I(3u), res[0][2]);
  ASSERT_EQ(I(3u), res[0][3]);

  ASSERT_EQ(I(1u), res[1][0]);
  ASSERT_EQ(I(3u), res[1][1]);
  ASSERT_EQ(I(1u), res[1][2]);
  ASSERT_EQ(I(1u), res[1][3]);

  // Test the multi column join with variable sized data.
  IdTable va(6, allocator());
  va.push_back({I(1), I(2), I(3), I(4), I(5), I(6)});
  va.push_back({I(1), I(2), I(3), I(7), I(5), I(6)});
  va.push_back({I(7), I(6), I(5), I(4), I(3), I(2)});

  IdTable vb(3, allocator());
  vb.push_back({I(2), I(3), I(4)});
  vb.push_back({I(2), I(3), I(5)});
  vb.push_back({I(6), I(7), I(4)});

  IdTable vres(7, allocator());
  jcls.clear();
  jcls.push_back({1, 0});
  jcls.push_back({2, 1});

  // The template size parameter can be at most 6 (the maximum number
  // of fixed size columns plus one).
  aWidth = va.cols();
  bWidth = vb.cols();
  resWidth = vres.cols();
  CALL_FIXED_SIZE_3(aWidth, bWidth, resWidth,
                    MultiColumnJoin::computeMultiColumnJoin, va, vb, jcls,
                    &vres);

  ASSERT_EQ(4u, vres.size());
  ASSERT_EQ(7u, vres.cols());

  IdTable wantedRes(7, allocator());
  wantedRes.push_back({I(1), I(2), I(3), I(4), I(5), I(6), I(4)});
  wantedRes.push_back({I(1), I(2), I(3), I(4), I(5), I(6), I(5)});
  wantedRes.push_back({I(1), I(2), I(3), I(7), I(5), I(6), I(4)});
  wantedRes.push_back({I(1), I(2), I(3), I(7), I(5), I(6), I(5)});

  ASSERT_EQ(wantedRes[0], vres[0]);
  ASSERT_EQ(wantedRes[1], vres[1]);
  ASSERT_EQ(wantedRes[2], vres[2]);
  ASSERT_EQ(wantedRes[3], vres[3]);
}

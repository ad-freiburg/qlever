// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "../src/engine/CallFixedSize.h"
#include "../src/engine/Engine.h"
#include "../src/engine/Join.h"
#include "../src/engine/OptionalJoin.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
};

TEST(EngineTest, joinTest) {
  IdTable a(2, allocator());
  a.push_back({I(1), I(1)});
  a.push_back({I(1), I(3)});
  a.push_back({I(2), I(1)});
  a.push_back({I(2), I(2)});
  a.push_back({I(4), I(1)});
  IdTable b(2, allocator());
  b.push_back({I(1), I(3)});
  b.push_back({I(1), I(8)});
  b.push_back({I(3), I(1)});
  b.push_back({I(4), I(2)});
  IdTable res(3, allocator());
  int lwidth = a.cols();
  int rwidth = b.cols();
  int reswidth = a.cols() + b.cols() - 1;
  Join J{Join::InvalidOnlyForTestingJoinTag{}};
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, J.join, a, 0, b, 0, &res);

  ASSERT_EQ(I(1), res(0, 0));
  ASSERT_EQ(I(1), res(0, 1));
  ASSERT_EQ(I(3), res(0, 2));
  ASSERT_EQ(I(1), res(1, 0));
  ASSERT_EQ(I(1), res(1, 1));
  ASSERT_EQ(I(8), res(1, 2));
  ASSERT_EQ(I(1), res(2, 0));
  ASSERT_EQ(I(3), res(2, 1));
  ASSERT_EQ(I(3), res(2, 2));
  ASSERT_EQ(I(1), res(3, 0));
  ASSERT_EQ(I(3), res(3, 1));
  ASSERT_EQ(I(8), res(3, 2));
  ASSERT_EQ(5u, res.size());
  ASSERT_EQ(I(4), res(4, 0));
  ASSERT_EQ(I(1), res(4, 1));
  ASSERT_EQ(I(2), res(4, 2));

  res.clear();
  for (size_t i = 1; i <= 10000; ++i) {
    b.push_back({I(4 + i), I(2 + i)});
  }
  a.push_back({I(400000), I(200000)});
  b.push_back({I(400000), I(200000)});

  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, J.join, a, 0, b, 0, &res);
  ASSERT_EQ(6u, res.size());

  a.clear();
  b.clear();
  res.clear();

  for (size_t i = 1; i <= 10000; ++i) {
    a.push_back({I(4 + i), I(2 + i)});
  }
  a.push_back({I(40000), I(200000)});
  b.push_back({I(40000), I(200000)});

  for (size_t i = 1; i <= 10000; ++i) {
    a.push_back({I(40000 + i), I(2 + i)});
  }
  a.push_back({I(4000001), I(200000)});
  b.push_back({I(4000001), I(200000)});
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, J.join, a, 0, b, 0, &res);
  ASSERT_EQ(2u, res.size());

  a.clear();
  b.clear();
  res.clear();

  IdTable c(1, allocator());
  c.push_back({I(0)});

  b.push_back({I(0), I(1)});
  b.push_back({I(0), I(2)});
  b.push_back({I(1), I(3)});
  b.push_back({I(1), I(4)});

  lwidth = b.cols();
  rwidth = c.cols();
  reswidth = b.cols() + c.cols() - 1;
  // reset the IdTable.
  res = IdTable(reswidth, allocator());
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, J.join, b, 0, c, 0, &res);

  ASSERT_EQ(2u, res.size());

  ASSERT_EQ(I(0), res(0, 0));
  ASSERT_EQ(I(1), res(0, 1));

  ASSERT_EQ(I(0), res(1, 0));
  ASSERT_EQ(I(2), res(1, 1));
};

TEST(EngineTest, optionalJoinTest) {
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
  CALL_FIXED_SIZE_3(aWidth, bWidth, resWidth, OptionalJoin::optionalJoin, a, b,
                    false, true, jcls, &res);

  ASSERT_EQ(5u, res.size());

  ASSERT_EQ(I(4), res(0, 0));
  ASSERT_EQ(I(1), res(0, 1));
  ASSERT_EQ(I(2), res(0, 2));
  ASSERT_EQ(ID_NO_VALUE, res(0, 3));

  ASSERT_EQ(I(2), res(1, 0));
  ASSERT_EQ(I(1), res(1, 1));
  ASSERT_EQ(I(3), res(1, 2));
  ASSERT_EQ(I(3), res(1, 3));

  ASSERT_EQ(I(1), res(2, 0));
  ASSERT_EQ(I(1), res(2, 1));
  ASSERT_EQ(I(4), res(2, 2));
  ASSERT_EQ(ID_NO_VALUE, res(2, 3));

  ASSERT_EQ(I(2), res(3, 0));
  ASSERT_EQ(I(2), res(3, 1));
  ASSERT_EQ(I(1), res(3, 2));
  ASSERT_EQ(ID_NO_VALUE, res(3, 3));

  ASSERT_EQ(I(1), res(4, 0));
  ASSERT_EQ(I(3), res(4, 1));
  ASSERT_EQ(I(1), res(4, 2));
  ASSERT_EQ(I(1), res(4, 3));

  // Test the optional join with variable sized data.
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
  jcls.push_back(array<ColumnIndex, 2>{{1, 0}});
  jcls.push_back(array<ColumnIndex, 2>{{2, 1}});

  aWidth = va.cols();
  bWidth = vb.cols();
  resWidth = vres.cols();
  CALL_FIXED_SIZE_3(aWidth, bWidth, resWidth, OptionalJoin::optionalJoin, va,
                    vb, true, false, jcls, &vres);

  ASSERT_EQ(5u, vres.size());
  ASSERT_EQ(7u, vres.cols());

  vector<Id> r{I(1), I(2), I(3), I(4), I(5), I(6), I(4)};
  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(r[i], vres[0][i]);
  }
  r = {I(1), I(2), I(3), I(4), I(5), I(6), I(5)};
  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(r[i], vres[1][i]);
  }
  r = {I(1), I(2), I(3), I(7), I(5), I(6), I(4)};
  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(r[i], vres(2, i));
  }
  r = {I(1), I(2), I(3), I(7), I(5), I(6), I(5)};
  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(r[i], vres(3, i));
  }
  r = {ID_NO_VALUE, I(6), I(7), ID_NO_VALUE, ID_NO_VALUE, ID_NO_VALUE, I(4)};
  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(r[i], vres(4, i));
  }
}

TEST(EngineTest, distinctTest) {
  IdTable inp(4, allocator());
  IdTable res(4, allocator());

  inp.push_back({I(1), I(1), I(3), I(7)});
  inp.push_back({I(6), I(1), I(3), I(6)});
  inp.push_back({I(2), I(2), I(3), I(5)});
  inp.push_back({I(3), I(6), I(5), I(4)});
  inp.push_back({I(1), I(6), I(5), I(1)});

  std::vector<size_t> keepIndices = {1, 2};
  CALL_FIXED_SIZE_1(4, Engine::distinct, inp, keepIndices, &res);

  ASSERT_EQ(3u, res.size());
  ASSERT_EQ(inp[0], res[0]);
  ASSERT_EQ(inp[2], res[1]);
  ASSERT_EQ(inp[3], res[2]);
}

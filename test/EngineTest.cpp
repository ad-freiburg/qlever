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

TEST(EngineTest, joinTest) {
  IdTable a(2);
  a.push_back({1, 1});
  a.push_back({1, 3});
  a.push_back({2, 1});
  a.push_back({2, 2});
  a.push_back({4, 1});
  IdTable b(2);
  b.push_back({1, 3});
  b.push_back({1, 8});
  b.push_back({3, 1});
  b.push_back({4, 2});
  IdTable res(3);
  int lwidth = a.cols();
  int rwidth = b.cols();
  int reswidth = a.cols() + b.cols() - 1;
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, Join::join, a, 0, b, 0, &res);

  ASSERT_EQ(1u, res(0, 0));
  ASSERT_EQ(1u, res(0, 1));
  ASSERT_EQ(3u, res(0, 2));

  ASSERT_EQ(1u, res(1, 0));
  ASSERT_EQ(1u, res(1, 1));
  ASSERT_EQ(8u, res(1, 2));

  ASSERT_EQ(1u, res(2, 0));
  ASSERT_EQ(3u, res(2, 1));
  ASSERT_EQ(3u, res(2, 2));

  ASSERT_EQ(1u, res(3, 0));
  ASSERT_EQ(3u, res(3, 1));
  ASSERT_EQ(8u, res(3, 2));

  ASSERT_EQ(5u, res.size());

  ASSERT_EQ(4u, res(4, 0));
  ASSERT_EQ(1u, res(4, 1));
  ASSERT_EQ(2u, res(4, 2));

  res.clear();
  for (size_t i = 1; i <= 10000; ++i) {
    b.push_back({4 + i, 2 + i});
  }
  a.push_back({400000, 200000});
  b.push_back({400000, 200000});

  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, Join::join, a, 0, b, 0, &res);
  ASSERT_EQ(6u, res.size());

  a.clear();
  b.clear();
  res.clear();

  for (size_t i = 1; i <= 10000; ++i) {
    a.push_back({4 + i, 2 + i});
  }
  a.push_back({40000, 200000});
  b.push_back({40000, 200000});

  for (size_t i = 1; i <= 10000; ++i) {
    a.push_back({40000 + i, 2 + i});
  }
  a.push_back({4000001, 200000});
  b.push_back({4000001, 200000});
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, Join::join, a, 0, b, 0, &res);
  ASSERT_EQ(2u, res.size());

  a.clear();
  b.clear();
  res.clear();

  IdTable c(1);
  c.push_back({0});

  b.push_back({0, 1});
  b.push_back({0, 2});
  b.push_back({1, 3});
  b.push_back({1, 4});

  lwidth = b.cols();
  rwidth = c.cols();
  reswidth = b.cols() + c.cols() - 1;
  // reset the IdTable.
  res = IdTable(reswidth);
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, Join::join, b, 0, c, 0, &res);

  ASSERT_EQ(2u, res.size());

  ASSERT_EQ(0u, res(0, 0));
  ASSERT_EQ(1u, res(0, 1));

  ASSERT_EQ(0u, res(1, 0));
  ASSERT_EQ(2u, res(1, 1));
};

TEST(EngineTest, optionalJoinTest) {
  IdTable a(3);
  a.push_back({4, 1, 2});
  a.push_back({2, 1, 3});
  a.push_back({1, 1, 4});
  a.push_back({2, 2, 1});
  a.push_back({1, 3, 1});
  IdTable b(3);
  b.push_back({3, 3, 1});
  b.push_back({1, 8, 1});
  b.push_back({4, 2, 2});
  b.push_back({1, 1, 3});
  IdTable res(4);
  vector<array<Id, 2>> jcls;
  jcls.push_back(array<Id, 2>{{1, 2}});
  jcls.push_back(array<Id, 2>{{2, 1}});

  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  int aWidth = a.cols();
  int bWidth = b.cols();
  int resWidth = res.cols();
  CALL_FIXED_SIZE_3(aWidth, bWidth, resWidth, OptionalJoin::optionalJoin, a, b,
                    false, true, jcls, &res);

  ASSERT_EQ(5u, res.size());

  ASSERT_EQ(4u, res(0, 0));
  ASSERT_EQ(1u, res(0, 1));
  ASSERT_EQ(2u, res(0, 2));
  ASSERT_EQ(ID_NO_VALUE, res(0, 3));

  ASSERT_EQ(2u, res(1, 0));
  ASSERT_EQ(1u, res(1, 1));
  ASSERT_EQ(3u, res(1, 2));
  ASSERT_EQ(3u, res(1, 3));

  ASSERT_EQ(1u, res(2, 0));
  ASSERT_EQ(1u, res(2, 1));
  ASSERT_EQ(4u, res(2, 2));
  ASSERT_EQ(ID_NO_VALUE, res(2, 3));

  ASSERT_EQ(2u, res(3, 0));
  ASSERT_EQ(2u, res(3, 1));
  ASSERT_EQ(1u, res(3, 2));
  ASSERT_EQ(ID_NO_VALUE, res(3, 3));

  ASSERT_EQ(1u, res(4, 0));
  ASSERT_EQ(3u, res(4, 1));
  ASSERT_EQ(1u, res(4, 2));
  ASSERT_EQ(1u, res(4, 3));

  // Test the optional join with variable sized data.
  IdTable va(6);
  va.push_back({1, 2, 3, 4, 5, 6});
  va.push_back({1, 2, 3, 7, 5, 6});
  va.push_back({7, 6, 5, 4, 3, 2});

  IdTable vb(3);
  vb.push_back({2, 3, 4});
  vb.push_back({2, 3, 5});
  vb.push_back({6, 7, 4});

  IdTable vres(7);
  jcls.clear();
  jcls.push_back(array<Id, 2>{{1, 0}});
  jcls.push_back(array<Id, 2>{{2, 1}});

  aWidth = va.cols();
  bWidth = vb.cols();
  resWidth = vres.cols();
  CALL_FIXED_SIZE_3(aWidth, bWidth, resWidth, OptionalJoin::optionalJoin, va,
                    vb, true, false, jcls, &vres);

  ASSERT_EQ(5u, vres.size());
  ASSERT_EQ(7u, vres.cols());

  vector<Id> r{1, 2, 3, 4, 5, 6, 4};
  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(r[i], vres[0][i]);
  }
  r = {1, 2, 3, 4, 5, 6, 5};
  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(r[i], vres[1][i]);
  }
  r = {1, 2, 3, 7, 5, 6, 4};
  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(r[i], vres(2, i));
  }
  r = {1, 2, 3, 7, 5, 6, 5};
  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(r[i], vres(3, i));
  }
  r = {ID_NO_VALUE, 6, 7, ID_NO_VALUE, ID_NO_VALUE, ID_NO_VALUE, 4};
  for (size_t i = 0; i < 7; i++) {
    ASSERT_EQ(r[i], vres(4, i));
  }
}

TEST(EngineTest, distinctTest) {
  IdTable inp(4);
  IdTable res(4);

  inp.push_back({1, 1, 3, 7});
  inp.push_back({6, 1, 3, 6});
  inp.push_back({2, 2, 3, 5});
  inp.push_back({3, 6, 5, 4});
  inp.push_back({1, 6, 5, 1});

  std::vector<size_t> keepIndices = {1, 2};
  CALL_FIXED_SIZE_1(4, Engine::distinct, inp, keepIndices, &res);

  ASSERT_EQ(3u, res.size());
  ASSERT_EQ(inp[0], res[0]);
  ASSERT_EQ(inp[2], res[1]);
  ASSERT_EQ(inp[3], res[2]);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

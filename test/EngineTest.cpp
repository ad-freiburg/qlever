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

/*
 * Creates an IdTable for table described in talbeContent and returns it.
*/
template<size_t TABLE_WIDTH>
IdTable makeIdTableFromVector(std::vector<std::array<size_t, TABLE_WIDTH>> tableContent) {
  IdTable result(TABLE_WIDTH, allocator());

  // Copying the content into the table.
  for (const auto& row: tableContent) {
    const size_t backIndex = result.size();
    result.push_back();

    for (size_t c = 0; c < TABLE_WIDTH; c++) {
      result(backIndex, c) = I(row[c]);
    }
  }

  return result;
}

TEST(EngineTest, joinTest) {
  std::vector<std::array<size_t, 2>> ids {{1, 1}, {1, 3}, {2, 1}, {2, 2}, {4, 1}};
  IdTable a = makeIdTableFromVector(ids);
  ids = {{1, 3}, {1, 8}, {3, 1}, {4, 2}};
  IdTable b = makeIdTableFromVector(ids);
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

  ids = {{0, 1}, {0, 2}, {1, 3}, {1, 4}};
  b = makeIdTableFromVector(ids);

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
  std::vector<std::array<size_t, 3>> ids {{4, 1, 2}, {2, 1, 3}, {1, 1, 4}, {2, 2, 1}, {1, 3, 1}};
  IdTable a = makeIdTableFromVector(ids);
  ids = {{3, 3, 1}, {1, 8, 1}, {4, 2, 2}, {1, 1, 3}};
  IdTable b = makeIdTableFromVector(ids);
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
  std::vector<std::array<size_t, 6>> vaId {{1, 2, 3, 4, 5, 6}, {1, 2, 3, 7, 5 ,6}, {7, 6, 5, 4, 3, 2}};
  IdTable va = makeIdTableFromVector(vaId);

  std::vector<std::array<size_t, 3>> vbId {{2, 3, 4}, {2, 3, 5}, {6, 7, 4}};
  IdTable vb = makeIdTableFromVector(vbId);

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
  std::vector<std::array<size_t, 4>> ids {{1, 1, 3, 7}, {6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}};
  IdTable inp = makeIdTableFromVector(ids);

  IdTable res(4, allocator());

  std::vector<size_t> keepIndices = {1, 2};
  CALL_FIXED_SIZE_1(4, Engine::distinct, inp, keepIndices, &res);

  ASSERT_EQ(3u, res.size());
  ASSERT_EQ(inp[0], res[0]);
  ASSERT_EQ(inp[2], res[1]);
  ASSERT_EQ(inp[3], res[2]);
}

TEST(EngineTest, hashJoinTest) {
  // Create two IdTables, that are sorted.
 std::vector<std::array<size_t, 2>> ids {{1, 1}, {1, 3}, {2, 1}, {2, 2}, {4, 1}};
  IdTable a = makeIdTableFromVector(ids);
  ids = {{1, 3}, {1, 8}, {3, 1}, {4, 2}};
  IdTable b = makeIdTableFromVector(ids);
  IdTable res(3, allocator());
  int lwidth = a.cols();
  int rwidth = b.cols();
  int reswidth = a.cols() + b.cols() - 1;
  Join J{Join::InvalidOnlyForTestingJoinTag{}};
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, J.hashJoin, a, 0, b, 0, &res);

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

  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, J.hashJoin, a, 0, b, 0, &res);
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
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, J.hashJoin, a, 0, b, 0, &res);
  ASSERT_EQ(2u, res.size());

  a.clear();
  b.clear();
  res.clear();

  IdTable c(1, allocator());
  c.push_back({I(0)});

  ids = {{0, 1}, {0, 2}, {1, 3}, {1, 4}};
  b = makeIdTableFromVector(ids);

  lwidth = b.cols();
  rwidth = c.cols();
  reswidth = b.cols() + c.cols() - 1;
  // reset the IdTable.
  res = IdTable(reswidth, allocator());
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, J.hashJoin, b, 0, c, 0, &res);

  ASSERT_EQ(2u, res.size());

  ASSERT_EQ(I(0), res(0, 0));
  ASSERT_EQ(I(1), res(0, 1));

  ASSERT_EQ(I(0), res(1, 0));
  ASSERT_EQ(I(2), res(1, 1));
};

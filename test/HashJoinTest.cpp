// Code for test copied and adjusted from EngineTest.cpp: joinTest.
#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "../src/engine/CallFixedSize.h"
#include "../src/engine/Engine.h"
#include "../src/engine/Join.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
};

TEST(EngineTest, hashJoinTest) {
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

  b.push_back({I(0), I(1)});
  b.push_back({I(0), I(2)});
  b.push_back({I(1), I(3)});
  b.push_back({I(1), I(4)});

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


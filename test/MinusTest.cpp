// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "../src/engine/CallFixedSize.h"
#include "../src/engine/Minus.h"

auto table(size_t cols) {
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1'000'000)};
  return IdTable(cols, std::move(alloc));
}
auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
};

TEST(EngineTest, minusTest) {
  using std::array;
  using std::vector;

  IdTable a = table(3);
  a.push_back({I(1), I(2), I(1)});
  a.push_back({I(2), I(1), I(4)});
  a.push_back({I(5), I(4), I(1)});
  a.push_back({I(8), I(1), I(2)});
  a.push_back({I(8), I(2), I(3)});

  IdTable b = table(4);
  b.push_back({I(1), I(2), I(7), I(5)});
  b.push_back({I(3), I(3), I(1), I(5)});
  b.push_back({I(1), I(8), I(1), I(5)});

  IdTable res = table(3);

  vector<array<ColumnIndex, 2>> jcls;
  jcls.push_back(array<ColumnIndex, 2>{{0, 1}});
  jcls.push_back(array<ColumnIndex, 2>{{1, 0}});

  IdTable wantedRes = table(3);
  wantedRes.push_back({I(1), I(2), I(1)});
  wantedRes.push_back({I(5), I(4), I(1)});
  wantedRes.push_back({I(8), I(2), I(3)});

  // Subtract b from a on the column pairs 1,2 and 2,1 (entries from columns 1
  // of a have to equal those of column 2 of b and vice versa).
  int aWidth = a.numColumns();
  int bWidth = b.numColumns();
  Minus m{Minus::OnlyForTestingTag{}};
  CALL_FIXED_SIZE((std::array{aWidth, bWidth}), &Minus::computeMinus, m, a, b,
                  jcls, &res);

  ASSERT_EQ(wantedRes.size(), res.size());

  ASSERT_EQ(wantedRes[0], res[0]);
  ASSERT_EQ(wantedRes[1], res[1]);
  ASSERT_EQ(wantedRes[2], res[2]);

  // Test subtracting without matching columns
  res.clear();
  jcls.clear();
  CALL_FIXED_SIZE((std::array{aWidth, bWidth}), &Minus::computeMinus, m, a, b,
                  jcls, &res);
  ASSERT_EQ(a.size(), res.size());
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], res[i]);
  }

  // Test minus with variable sized data.
  IdTable va = table(6);
  va.push_back({I(1), I(2), I(3), I(4), I(5), I(6)});
  va.push_back({I(1), I(2), I(3), I(7), I(5), I(6)});
  va.push_back({I(7), I(6), I(5), I(4), I(3), I(2)});

  IdTable vb = table(3);
  vb.push_back({I(2), I(3), I(4)});
  vb.push_back({I(2), I(3), I(5)});
  vb.push_back({I(6), I(7), I(4)});

  IdTable vres = table(6);
  jcls.clear();
  jcls.push_back({1, 0});
  jcls.push_back({2, 1});

  // The template size parameter can be at most 6 (the maximum number
  // of fixed size columns plus one).
  aWidth = va.numColumns();
  bWidth = vb.numColumns();
  CALL_FIXED_SIZE((std::array{aWidth, bWidth}), &Minus::computeMinus, m, va, vb,
                  jcls, &vres);

  wantedRes = table(6);
  wantedRes.push_back({I(7), I(6), I(5), I(4), I(3), I(2)});
  ASSERT_EQ(wantedRes.size(), vres.size());
  ASSERT_EQ(wantedRes.numColumns(), vres.numColumns());

  ASSERT_EQ(wantedRes[0], vres[0]);
}

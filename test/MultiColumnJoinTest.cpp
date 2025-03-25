// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <absl/strings/str_split.h>
#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "engine/MultiColumnJoin.h"
#include "util/AllocatorTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

using ad_utility::testing::makeAllocator;
namespace {
auto V = ad_utility::testing::VocabId;
}

TEST(EngineTest, multiColumnJoinTest) {
  using std::array;
  using std::vector;

  IdTable a = makeIdTableFromVector(
      {{4, 1, 2}, {2, 1, 3}, {1, 1, 4}, {2, 2, 1}, {1, 3, 1}});
  IdTable b =
      makeIdTableFromVector({{3, 3, 1}, {1, 8, 1}, {4, 2, 2}, {1, 1, 3}});
  IdTable res(4, makeAllocator());
  vector<array<ColumnIndex, 2>> jcls;
  jcls.push_back(array<ColumnIndex, 2>{{1, 2}});
  jcls.push_back(array<ColumnIndex, 2>{{2, 1}});

  auto* qec = ad_utility::testing::getQec();

  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  MultiColumnJoin{qec, idTableToExecutionTree(qec, a),
                  idTableToExecutionTree(qec, b)}
      .computeMultiColumnJoin(a, b, jcls, &res);

  auto expected = makeIdTableFromVector({{2, 1, 3, 3}, {1, 3, 1, 1}});
  ASSERT_EQ(expected, res);

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

  MultiColumnJoin{qec, idTableToExecutionTree(qec, a),
                  idTableToExecutionTree(qec, b)}
      .computeMultiColumnJoin(va, vb, jcls, &vres);

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

// _____________________________________________________________________________
TEST(MultiColumnJoin, clone) {
  auto* qec = ad_utility::testing::getQec();
  IdTable a = makeIdTableFromVector({{4, 1, 2}});
  MultiColumnJoin join{qec, idTableToExecutionTree(qec, a),
                       idTableToExecutionTree(qec, a)};

  auto clone = join.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(join, IsDeepCopy(*clone));

  std::string_view prefix = "MultiColumnJoin on ";
  EXPECT_THAT(join.getDescriptor(), ::testing::StartsWith(prefix));
  EXPECT_THAT(clone->getDescriptor(), ::testing::StartsWith(prefix));
  // Order of join columns is not deterministic.
  auto getVars = [prefix](std::string_view string) {
    string.remove_prefix(prefix.length());
    std::vector<std::string> vars;
    for (const auto& split : absl::StrSplit(string, ' ', absl::SkipEmpty())) {
      vars.emplace_back(split);
    }
    ql::ranges::sort(vars);
    return vars;
  };
  EXPECT_EQ(getVars(clone->getDescriptor()), getVars(join.getDescriptor()));
}

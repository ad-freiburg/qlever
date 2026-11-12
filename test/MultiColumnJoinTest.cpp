// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <absl/strings/str_split.h>
#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "engine/operation/IndexScan.h"
#include "engine/operation/MultiColumnJoin.h"
#include "engine/operation/NeutralOptional.h"
#include "global/Constants.h"
#include "util/AllocatorTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

using ad_utility::testing::makeAllocator;
namespace {
auto V = ad_utility::testing::VocabId;

// Join `left` and `right` on the `joinColumns` via
// `MultiColumnJoin::computeMultiColumnJoin` and return the result.
IdTable computeJoin(
    const IdTable& left, const IdTable& right,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns) {
  auto* qec = ad_utility::testing::getQec();
  IdTable result(left.numColumns() + right.numColumns() - joinColumns.size(),
                 makeAllocator());
  MultiColumnJoin{qec, idTableToExecutionTree(qec, left),
                  idTableToExecutionTree(qec, right)}
      .computeMultiColumnJoin(left.asStaticView<0>(), right.asStaticView<0>(),
                              joinColumns, &result);
  return result;
}
}  // namespace

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
      .computeMultiColumnJoin(a.asStaticView<0>(), b.asStaticView<0>(), jcls,
                              &res);

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
      .computeMultiColumnJoin(va.asStaticView<0>(), vb.asStaticView<0>(), jcls,
                              &vres);

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

// Test the join with UNDEF values in the join columns. An UNDEF value matches
// every value, and the join value of a result row is the one of the side that
// is not UNDEF. Such inputs take the generic path of `computeMultiColumnJoin`
// (see `isCheap` there), which requires that no join column contains UNDEF.
TEST(MultiColumnJoin, undefInJoinColumns) {
  constexpr auto U = Id::makeUndefined();
  std::vector<std::array<ColumnIndex, 2>> jcls;
  jcls.push_back(std::array<ColumnIndex, 2>{{0, 0}});
  jcls.push_back(std::array<ColumnIndex, 2>{{1, 1}});
  auto join = [&jcls](const IdTable& left, const IdTable& right) {
    return computeJoin(left, right, jcls);
  };

  // UNDEF only in a join column of the right input.
  {
    auto left = makeIdTableFromVector({{1, 10}, {2, 20}});
    auto right = makeIdTableFromVector({{U, 10, 100}, {2, 20, 200}});
    auto expected = makeIdTableFromVector({{1, 10, 100}, {2, 20, 200}});
    EXPECT_EQ(expected, join(left, right));
  }
  // UNDEF only in a join column of the left input.
  {
    auto left = makeIdTableFromVector({{U, 10}, {2, 20}});
    auto right = makeIdTableFromVector({{1, 10, 100}, {2, 20, 200}});
    auto expected = makeIdTableFromVector({{1, 10, 100}, {2, 20, 200}});
    EXPECT_EQ(expected, join(left, right));
  }
  // UNDEF in a join column of both inputs.
  {
    auto left = makeIdTableFromVector({{U, 10}, {2, 20}});
    auto right = makeIdTableFromVector({{1, 10, 100}, {2, U, 200}});
    auto expected =
        makeIdTableFromVector({{1, 10, 100}, {2, 10, 200}, {2, 20, 200}});
    EXPECT_EQ(expected, join(left, right));
  }
}

// Test the join when one input is much smaller than the other, s.t. the
// galloping join is used (see `GALLOP_THRESHOLD`), once with the smaller input
// on the left and once with the smaller input on the right. Also test that an
// UNDEF value in the smaller input still matches every value of the larger
// input, and the join on three columns.
TEST(MultiColumnJoin, galloping) {
  constexpr auto U = Id::makeUndefined();
  std::vector<std::array<ColumnIndex, 2>> jcls{{0, 0}, {1, 1}};

  // Each pair `(k, k + 1)` with `k` in `[0, 2500)` occurs twice in `large`.
  VectorTable largeInput;
  for (int64_t i = 0; i < 5000; ++i) {
    largeInput.push_back({i / 2, i / 2 + 1, i});
  }
  auto large = makeIdTableFromVector(largeInput);
  // The second row matches `large` only in the first join column, and the last
  // row is larger than all rows of `large`.
  auto small = makeIdTableFromVector(
      {{3, 4, 10}, {500, 0, 12}, {500, 501, 11}, {2500, 2501, 13}});
  ASSERT_GT(large.size() / small.size(), GALLOP_THRESHOLD);

  // The smaller input on the left.
  EXPECT_EQ(computeJoin(small, large, jcls),
            makeIdTableFromVector({{3, 4, 10, 6},
                                   {3, 4, 10, 7},
                                   {500, 501, 11, 1000},
                                   {500, 501, 11, 1001}}));
  // The smaller input on the right.
  EXPECT_EQ(computeJoin(large, small, jcls),
            makeIdTableFromVector({{3, 4, 6, 10},
                                   {3, 4, 7, 10},
                                   {500, 501, 1000, 11},
                                   {500, 501, 1001, 11}}));

  // An UNDEF value in a join column of the smaller input must match every
  // value of `large` (the galloping join cannot handle UNDEF values, so the
  // generic join must be used despite the size ratio).
  auto smallUndef = makeIdTableFromVector({{U, 2500, 14}, {3, 4, 10}});
  EXPECT_EQ(computeJoin(smallUndef, large, jcls),
            makeIdTableFromVector({{3, 4, 10, 6},
                                   {3, 4, 10, 7},
                                   {2499, 2500, 14, 4998},
                                   {2499, 2500, 14, 4999}}));
  EXPECT_EQ(computeJoin(large, smallUndef, jcls),
            makeIdTableFromVector({{3, 4, 6, 10},
                                   {3, 4, 7, 10},
                                   {2499, 2500, 4998, 14},
                                   {2499, 2500, 4999, 14}}));

  // Three join columns, where the second row of the smaller input matches
  // `large` in the first two join columns but not in the third.
  std::vector<std::array<ColumnIndex, 2>> jcls3{{0, 0}, {1, 1}, {2, 2}};
  auto small3 = makeIdTableFromVector(
      {{3, 4, 6, 50}, {3, 4, 8, 51}, {500, 501, 1001, 52}});
  EXPECT_EQ(computeJoin(small3, large, jcls3),
            makeIdTableFromVector({{3, 4, 6, 50}, {500, 501, 1001, 52}}));
  EXPECT_EQ(computeJoin(large, small3, jcls3),
            makeIdTableFromVector({{3, 4, 6, 50}, {500, 501, 1001, 52}}));
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

// _____________________________________________________________________________
TEST(MultiColumnJoin, columnOriginatesFromGraphOrUndef) {
  using ad_utility::triple_component::Iri;
  auto* qec = ad_utility::testing::getQec();
  // Not in graph no undef
  auto values1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?c"}});
  auto values2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  // Not in graph, potentially undef
  auto values3 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{Id::makeUndefined(), Id::makeUndefined()}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?c"}});
  auto values4 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{Id::makeUndefined(), Id::makeUndefined()}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  // In graph, no undef
  auto index1 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Variable{"?c"}});
  auto index2 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Variable{"?b"}});
  // In graph, potential undef
  auto index3 = ad_utility::makeExecutionTree<NeutralOptional>(
      qec, ad_utility::makeExecutionTree<IndexScan>(
               qec, Permutation::PSO,
               SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                                  Variable{"?c"}}));
  auto index4 = ad_utility::makeExecutionTree<NeutralOptional>(
      qec, ad_utility::makeExecutionTree<IndexScan>(
               qec, Permutation::PSO,
               SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                                  Variable{"?b"}}));

  auto testWithTrees =
      [qec](std::shared_ptr<QueryExecutionTree> left,
            std::shared_ptr<QueryExecutionTree> right, bool a, bool b, bool c,
            ad_utility::source_location location = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(location);

        MultiColumnJoin join{qec, std::move(left), std::move(right), false};
        EXPECT_EQ(join.columnOriginatesFromGraphOrUndef(Variable{"?a"}), a);
        EXPECT_EQ(join.columnOriginatesFromGraphOrUndef(Variable{"?b"}), b);
        EXPECT_EQ(join.columnOriginatesFromGraphOrUndef(Variable{"?c"}), c);
        EXPECT_THROW(
            join.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
            ad_utility::Exception);
      };

  testWithTrees(index3, index4, true, true, true);
  testWithTrees(index3, index2, true, true, true);
  testWithTrees(index3, values4, false, false, true);
  testWithTrees(index3, values2, false, false, true);
  testWithTrees(index1, index4, true, true, true);
  testWithTrees(index1, index2, true, true, true);
  testWithTrees(index1, values4, true, false, true);
  testWithTrees(index1, values2, true, false, true);
  testWithTrees(values4, index3, false, false, true);
  testWithTrees(values4, index1, true, false, true);
  testWithTrees(values4, values3, false, false, false);
  testWithTrees(values4, values1, false, false, false);
  testWithTrees(values2, index3, false, false, true);
  testWithTrees(values2, index1, true, false, true);
  testWithTrees(values2, values3, false, false, false);
  testWithTrees(values2, values1, false, false, false);
}

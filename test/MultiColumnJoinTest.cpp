// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <absl/strings/str_split.h>
#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "engine/IndexScan.h"
#include "engine/MultiColumnJoin.h"
#include "engine/NeutralOptional.h"
#include "util/AllocatorTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

using ad_utility::testing::makeAllocator;
namespace {
auto V = ad_utility::testing::VocabId;
auto iri = [](std::string_view s) {
  return TripleComponent::Iri::fromIriref(s);
};
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

// _____________________________________________________________________________
TEST(MultiColumnJoin, prefilteringWithTwoIndexScans) {
  // Create a dataset with overlap in subjects between two predicates.
  // This tests that both IndexScans can be prefiltered when joining.
  std::string kg;
  for (size_t i = 0; i < 15; ++i) {
    kg += absl::StrCat("<s", i, "> <p1> <o", i, "> .\n");
  }
  for (size_t i = 5; i < 20; ++i) {
    kg += absl::StrCat("<s", i, "> <p2> <o2_", i, "> .\n");
  }

  auto qec = ad_utility::testing::getQec(kg);
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::lazyIndexScanMaxSizeMaterialization_>(1);
  qec->getQueryTreeCache().clearAll();

  using V = Variable;
  auto scan1 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{V{"?s"}, iri("<p1>"), V{"?o1"}});
  auto scan2 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{V{"?s"}, iri("<p2>"), V{"?o2"}});

  auto join = ad_utility::makeExecutionTree<MultiColumnJoin>(qec, scan1, scan2);

  auto result = join->getResult();

  // Verify result correctness: only subjects s5-s14 appear in both (10 rows)
  ASSERT_TRUE(result->isFullyMaterialized());
  EXPECT_EQ(result->idTable().size(), 10);

  // Verify that the operation was recognized as using IndexScans by checking
  // runtime info exists
  const auto& scan1Rti = scan1->getRootOperation()->getRuntimeInfoPointer();
  const auto& scan2Rti = scan2->getRootOperation()->getRuntimeInfoPointer();
  ASSERT_NE(scan1Rti, nullptr);
  ASSERT_NE(scan2Rti, nullptr);
}

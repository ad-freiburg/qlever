//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "./PrefilterExpressionTestHelpers.h"
#include "engine/Filter.h"
#include "engine/IndexScan.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

using ::testing::ElementsAre;
using ::testing::Eq;

namespace {
// Shorthand for makeFromBool
ValueId asBool(bool value) { return Id::makeFromBool(value); }

// Convert a generator to a vector for easier comparison in assertions
std::vector<IdTable> toVector(Result::LazyResult generator) {
  std::vector<IdTable> result;
  for (auto& pair : generator) {
    // IMPORTANT: The `LocalVocab` contained in the pair will be destroyed at
    // the end of the iteration. The underlying assumption is that the
    // `LocalVocab` will be empty and the `IdTable` won't contain any dangling
    // references.
    result.push_back(std::move(pair.idTable_));
  }
  return result;
}

// Shorthand helper function
ad_utility::triple_component::Iri iri(std::string_view string) {
  return TripleComponent::Iri::fromIriref(string);
}

// _____________________________________________________________________________
void checkSetPrefilterExpressionVariablePair(
    QueryExecutionContext* qec, const Permutation::Enum& permutation,
    SparqlTripleSimple triple,
    std::unique_ptr<sparqlExpression::SparqlExpression> sparqlExpr,
    bool prefilterIsApplicable, bool enablePrefilterForFilter = true) {
  [[maybe_unused]] const auto& rtp =
      setRuntimeParameterForTest<"enable-prefilter-on-index-scans">(
          enablePrefilterForFilter);
  auto subtree =
      ad_utility::makeExecutionTree<IndexScan>(qec, permutation, triple);
  Filter filter{qec, subtree, {std::move(sparqlExpr), "Expression ?x"}};
  const auto& optUpdatedSubtree = filter.getSubtree();
  if (prefilterIsApplicable && enablePrefilterForFilter) {
    EXPECT_NE(subtree, optUpdatedSubtree);
    EXPECT_FALSE(optUpdatedSubtree->getRootOperation()->canResultBeCached());
  } else {
    EXPECT_EQ(subtree, optUpdatedSubtree);
    EXPECT_TRUE(optUpdatedSubtree->getRootOperation()->canResultBeCached());
  }
}

}  // namespace

// _____________________________________________________________________________
TEST(Filter, verifyPredicateIsAppliedCorrectlyOnLazyEvaluation) {
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();
  std::vector<IdTable> idTables;
  idTables.push_back(makeIdTableFromVector(
      {{true}, {true}, {false}, {false}, {true}}, asBool));
  idTables.push_back(makeIdTableFromVector({{true}, {false}}, asBool));
  idTables.push_back(IdTable{1, ad_utility::makeUnlimitedAllocator<Id>()});
  idTables.push_back(
      makeIdTableFromVector({{false}, {false}, {false}}, asBool));
  idTables.push_back(makeIdTableFromVector({{true}}, asBool));

  ValuesForTesting values{qec, std::move(idTables), {Variable{"?x"}}};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};
  Filter filter{
      qec,
      std::make_shared<QueryExecutionTree>(std::move(subTree)),
      {std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
       "Expression ?x"}};

  auto result = filter.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
  ASSERT_FALSE(result->isFullyMaterialized());
  auto generator = result->idTables();

  auto referenceTable1 =
      makeIdTableFromVector({{true}, {true}, {true}}, asBool);
  auto referenceTable2 = makeIdTableFromVector({{true}}, asBool);

  auto m = matchesIdTable;
  EXPECT_THAT(
      toVector(std::move(generator)),
      ElementsAre(m(referenceTable1), m(referenceTable2), m(referenceTable2)));
}

// _____________________________________________________________________________
TEST(Filter, verifyPredicateIsAppliedCorrectlyOnNonLazyEvaluation) {
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();
  IdTable idTable = makeIdTableFromVector({{true},
                                           {true},
                                           {false},
                                           {false},
                                           {true},
                                           {true},
                                           {false},
                                           {false},
                                           {false},
                                           {false},
                                           {true}},
                                          asBool);

  ValuesForTesting values{qec, std::move(idTable), {Variable{"?x"}}, false,
                          {},  LocalVocab{},       std::nullopt,     true};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};
  Filter filter{
      qec,
      std::make_shared<QueryExecutionTree>(std::move(subTree)),
      {std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
       "Expression ?x"}};

  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());

  EXPECT_EQ(
      result->idTable(),
      makeIdTableFromVector({{true}, {true}, {true}, {true}, {true}}, asBool));
}

// _____________________________________________________________________________
TEST(Filter,
     verifyPredicateIsAppliedCorrectlyOnNonLazyEvaluationWithLazyChild) {
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();
  std::vector<IdTable> idTables;
  idTables.push_back(makeIdTableFromVector(
      {{true}, {true}, {false}, {false}, {true}}, asBool));
  idTables.push_back(makeIdTableFromVector({{true}, {false}}, asBool));
  idTables.push_back(IdTable{1, ad_utility::makeUnlimitedAllocator<Id>()});
  idTables.push_back(
      makeIdTableFromVector({{false}, {false}, {false}}, asBool));
  idTables.push_back(makeIdTableFromVector({{true}}, asBool));

  ValuesForTesting values{qec, std::move(idTables), {Variable{"?x"}}};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};
  Filter filter{
      qec,
      std::make_shared<QueryExecutionTree>(std::move(subTree)),
      {std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
       "Expression ?x"}};

  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());

  EXPECT_EQ(
      result->idTable(),
      makeIdTableFromVector({{true}, {true}, {true}, {true}, {true}}, asBool));
}

// _____________________________________________________________________________
TEST(Filter, verifySetPrefilterExpressionVariablePairForIndexScanChild) {
  using namespace makeFilterExpression;
  using namespace makeSparqlExpression;
  using namespace ad_utility::testing;
  std::string kg = "<a> <p> 22.5 .";
  QueryExecutionContext* qec = ad_utility::testing::getQec(kg);
  // For the following tests a <PrefilterExpression, Variable> pair should be
  // assigned to the IndexScan child (prefiltering is possible) with Filter
  // construction.
  checkSetPrefilterExpressionVariablePair(
      qec, Permutation::POS, {Variable{"?x"}, iri("<p>"), Variable{"?z"}},
      ltSprql(Variable{"?z"}, IntId(10)), true);
  // If the runtime parameter `enable-prefilter-on-index-scans` is set to
  // false, we expect that no prefilter is set although it would be possible
  // (last argument is set to false).
  checkSetPrefilterExpressionVariablePair(
      qec, Permutation::POS, {Variable{"?x"}, iri("<p>"), Variable{"?z"}},
      ltSprql(Variable{"?z"}, IntId(10)), true, false);
  checkSetPrefilterExpressionVariablePair(
      qec, Permutation::POS, {Variable{"?x"}, iri("<p>"), Variable{"?z"}},
      andSprqlExpr(neqSprql(Variable{"?z"}, IntId(10)),
                   gtSprql(Variable{"?y"}, DoubleId(0))),
      true);
  checkSetPrefilterExpressionVariablePair(
      qec, Permutation::PSO,
      {makeSparqlExpression::Iri::fromIriref("<a>"), iri("<p>"),
       Variable{"?z"}},
      eqSprql(Variable{"?z"}, DoubleId(22.5)), true);
  // If the runtime parameter `enable-prefilter-on-index-scans` is set to
  // false, we expect that no prefilter is set although it would be possible
  // (last argument is set to false).
  checkSetPrefilterExpressionVariablePair(
      qec, Permutation::PSO,
      {makeSparqlExpression::Iri::fromIriref("<a>"), iri("<p>"),
       Variable{"?z"}},
      eqSprql(Variable{"?z"}, DoubleId(22.5)), true, false);

  // We expect that no <PrefilterExpression, Variable> pair is assigned
  // (no prefilter procedure applicable) with Filter construction.
  checkSetPrefilterExpressionVariablePair(
      qec, Permutation::PSO, {Variable{"?x"}, iri("<p>"), Variable{"?z"}},
      eqSprql(Variable{"?z"}, DoubleId(22.5)), false);
  checkSetPrefilterExpressionVariablePair(
      qec, Permutation::POS, {Variable{"?x"}, iri("<p>"), Variable{"?z"}},
      gtSprql(Variable{"?x"}, VocabId(10)), false);
}

// _____________________________________________________________________________
TEST(Filter, lazyChildMaterializedResultBinaryFilter) {
  using namespace makeSparqlExpression;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();
  std::vector<IdTable> idTables;
  auto I = ad_utility::testing::IntId;
  idTables.push_back(makeIdTableFromVector({{1}, {2}, {3}, {3}, {4}}, I));
  idTables.push_back(makeIdTableFromVector({{4}, {5}}, I));
  idTables.push_back(makeIdTableFromVector({{6}, {7}}, I));
  idTables.push_back(makeIdTableFromVector({{8}, {8}}, I));

  auto varX = Variable{"?x"};
  auto expr = notSprqlExpr(ltSprql(varX, I(5)));

  ValuesForTesting values{
      qec, std::move(idTables), {Variable{"?x"}}, false, {0}};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};
  Filter filter{qec,
                std::make_shared<QueryExecutionTree>(std::move(subTree)),
                {std::move(expr), "!?x < 5"}};

  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());

  EXPECT_EQ(result->idTable(),
            makeIdTableFromVector({{5}, {6}, {7}, {8}, {8}}, I));
}

// _____________________________________________________________________________
TEST(Filter, clone) {
  using namespace makeSparqlExpression;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  std::vector<IdTable> idTables;
  auto I = ad_utility::testing::IntId;
  idTables.push_back(makeIdTableFromVector({{1}}, I));

  ValuesForTesting values{
      qec, std::move(idTables), {Variable{"?x"}}, false, {0}};
  QueryExecutionTree subTree{
      qec, std::make_shared<ValuesForTesting>(std::move(values))};
  Filter filter{qec,
                std::make_shared<QueryExecutionTree>(std::move(subTree)),
                {ltSprql(Variable{"?x"}, I(5)), "!?x < 5"}};

  auto clone = filter.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(filter, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), filter.getDescriptor());
}

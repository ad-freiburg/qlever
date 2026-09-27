//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "./PrefilterExpressionTestHelpers.h"
#include "engine/Filter.h"
#include "engine/IndexScan.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/BlankNodeExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RandomExpression.h"
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
  [[maybe_unused]] const auto& rtp = setRuntimeParameterForTest<
      &RuntimeParameters::enablePrefilterOnIndexScans_>(
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
      result->idTableView(),
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
      result->idTableView(),
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

  EXPECT_EQ(result->idTableView(),
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

// _____________________________________________________________________________
TEST(Filter, isDeterministic) {
  using namespace sparqlExpression;
  QueryExecutionContext* qec = ad_utility::testing::getQec();

  auto makeTree = [qec]() {
    return ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, IdTable{1, qec->getAllocator()},
        std::vector<std::optional<Variable>>{Variable{"?x"}});
  };

  // Deterministic expression.
  Filter detFilter{
      qec,
      makeTree(),
      {std::make_unique<VariableExpression>(Variable{"?x"}), "?x"}};
  EXPECT_TRUE(detFilter.isDeterministic());

  // Non-deterministic expression.
  Filter nonDetFilter{
      qec, makeTree(), {std::make_unique<RandomExpression>(), "RAND()"}};
  EXPECT_FALSE(nonDetFilter.isDeterministic());
}

// _____________________________________________________________________________
TEST(Filter, getRunLengthEvaluationColumn) {
  using namespace makeSparqlExpression;
  using namespace sparqlExpression;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  auto I = ad_utility::testing::IntId;
  auto makeFilter = [qec](SparqlExpression::Ptr expression) {
    auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1, 2}}, ad_utility::testing::IntId),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}});
    return Filter{qec, std::move(values), {std::move(expression), "expr"}};
  };
  Variable x{"?x"};
  Variable y{"?y"};

  // Disabled by default.
  EXPECT_EQ(makeFilter(ltSprql(x, I(5))).getRunLengthEvaluationColumn(),
            std::nullopt);

  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::filterRunLengthEvaluation_>(true);
  auto columnY = makeFilter(ltSprql(y, I(5))).getRunLengthEvaluationColumn();
  ASSERT_TRUE(columnY.has_value());
  EXPECT_EQ(columnY->first, y);
  EXPECT_EQ(columnY->second.columnIndex_, 1u);
  // A variable that occurs twice.
  auto columnX = makeFilter(andSprqlExpr(ltSprql(x, I(5)), gtSprql(x, I(1))))
                     .getRunLengthEvaluationColumn();
  ASSERT_TRUE(columnX.has_value());
  EXPECT_EQ(columnX->first, x);
  EXPECT_EQ(columnX->second.columnIndex_, 0u);

  // Two variables.
  EXPECT_EQ(makeFilter(andSprqlExpr(ltSprql(x, I(5)), gtSprql(y, I(1))))
                .getRunLengthEvaluationColumn(),
            std::nullopt);
  // No variable, and a variable that the input does not bind.
  EXPECT_EQ(makeFilter(std::make_unique<RandomExpression>())
                .getRunLengthEvaluationColumn(),
            std::nullopt);
  EXPECT_EQ(
      makeFilter(ltSprql(Variable{"?z"}, I(5))).getRunLengthEvaluationColumn(),
      std::nullopt);
  // Not deterministic.
  EXPECT_EQ(makeFilter(std::make_unique<LessThanExpression>(
                           std::array<SparqlExpression::Ptr, 2>{
                               std::make_unique<RandomExpression>(),
                               std::make_unique<VariableExpression>(x)}))
                .getRunLengthEvaluationColumn(),
            std::nullopt);
}

// _____________________________________________________________________________
TEST(Filter, runLengthEvaluationGivesSameResult) {
  using namespace makeSparqlExpression;
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  auto I = ad_utility::testing::IntId;
  Variable x{"?x"};

  // Blocks with long runs, short runs (per-row fallback), all rows passing,
  // no row passing, and an empty block. The second column is unique per row,
  // so that wrongly copied rows are detected.
  auto makeBlocks = [&I]() {
    std::vector<IdTable> blocks;
    blocks.push_back(makeIdTableFromVector(
        {{1, 0}, {1, 1}, {1, 2}, {5, 3}, {5, 4}, {6, 5}, {6, 6}, {2, 7}}, I));
    blocks.push_back(makeIdTableFromVector({{1, 8}, {7, 9}, {2, 10}}, I));
    blocks.push_back(
        makeIdTableFromVector({{8, 11}, {8, 12}, {9, 13}, {9, 14}}, I));
    blocks.push_back(makeIdTableFromVector({{3, 15}, {3, 16}}, I));
    blocks.push_back(IdTable{2, ad_utility::makeUnlimitedAllocator<Id>()});
    return blocks;
  };

  // Filter `makeBlocks()` by `NOT(?x < 5)` or, with `sorted`, by `?x = 5`
  // on input that the filter is told to be sorted by `?x`.
  auto compute = [&](bool runLength, bool lazy, bool sorted) {
    qec->getQueryTreeCache().clearAll();
    auto cleanup = setRuntimeParameterForTest<
        &RuntimeParameters::filterRunLengthEvaluation_>(runLength);
    std::vector<IdTable> blocks = makeBlocks();
    if (sorted) {
      blocks.clear();
      blocks.push_back(makeIdTableFromVector(
          {{1, 0}, {1, 1}, {5, 2}, {5, 3}, {5, 4}, {6, 5}, {6, 6}}, I));
    }
    auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(blocks),
        std::vector<std::optional<Variable>>{x, Variable{"?y"}}, false,
        sorted ? std::vector<ColumnIndex>{0} : std::vector<ColumnIndex>{});
    auto expression =
        sorted ? eqSprql(x, I(5)) : notSprqlExpr(ltSprql(x, I(5)));
    Filter filter{qec, std::move(values), {std::move(expression), "expr"}};
    EXPECT_EQ(filter.getRunLengthEvaluationColumn().has_value(), runLength);
    auto result =
        filter.getResult(false, lazy ? ComputationMode::LAZY_IF_SUPPORTED
                                     : ComputationMode::FULLY_MATERIALIZED);
    IdTable table{2, ad_utility::makeUnlimitedAllocator<Id>()};
    if (result->isFullyMaterialized()) {
      table.insertAtEnd(result->idTableView());
    } else {
      for (auto& pair : result->idTables()) {
        table.insertAtEnd(pair.idTable_);
      }
    }
    return table;
  };

  auto expected = makeIdTableFromVector({{5, 3},
                                         {5, 4},
                                         {6, 5},
                                         {6, 6},
                                         {7, 9},
                                         {8, 11},
                                         {8, 12},
                                         {9, 13},
                                         {9, 14}},
                                        I);
  auto expectedSorted = makeIdTableFromVector({{5, 2}, {5, 3}, {5, 4}}, I);
  for (bool lazy : {false, true}) {
    EXPECT_EQ(compute(false, lazy, false), expected);
    EXPECT_EQ(compute(true, lazy, false), expected);
    EXPECT_EQ(compute(false, lazy, true), expectedSorted);
    EXPECT_EQ(compute(true, lazy, true), expectedSorted);
  }
}

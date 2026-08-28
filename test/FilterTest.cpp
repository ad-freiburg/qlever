//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "./PrefilterExpressionTestHelpers.h"
#include "engine/Filter.h"
#include "engine/IndexScan.h"
#include "engine/Sort.h"
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

// Return true iff the root operation of `tree` is of type `Op`.
template <typename Op>
bool rootIs(const QueryExecutionTree& tree) {
  return std::dynamic_pointer_cast<Op>(tree.getRootOperation()) != nullptr;
}

// Return the only child of `tree`. Throws if `tree` has no children.
const QueryExecutionTree& onlyChild(const QueryExecutionTree& tree) {
  return *tree.getRootOperation()->getChildren().at(0);
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
TEST(Filter, makeSortedTree) {
  using namespace makeSparqlExpression;
  auto I = ad_utility::testing::IntId;
  QueryExecutionContext* qec = ad_utility::testing::getQec();

  auto makeFilter = [qec, I](std::shared_ptr<QueryExecutionTree> subtree) {
    return Filter{
        qec, std::move(subtree), {ltSprql(Variable{"?z"}, I(5)), "?z < 5"}};
  };
  auto makeValues = [qec, I](std::vector<ColumnIndex> sortedColumns) {
    return ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1, 2}}, I),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?z"}},
        false, std::move(sortedColumns));
  };
  // Check that `filter.makeSortedTree(sortColumns)` pushed the sorting down to
  // the child, and return the resulting tree.
  auto checkPushedDown = [](const Filter& filter,
                            const std::vector<ColumnIndex>& sortColumns) {
    auto tree = filter.makeSortedTree(sortColumns);
    EXPECT_TRUE(tree.has_value());
    if (tree.has_value()) {
      EXPECT_TRUE(rootIs<Filter>(*tree.value()));
      EXPECT_TRUE(tree.value()->getRootOperation()->isSortedBy(sortColumns));
      // A `Filter` doesn't change the columns of its child.
      EXPECT_EQ(tree.value()->getVariableColumns(),
                filter.getSubtree()->getVariableColumns());
    }
    return tree;
  };

  // An `IndexScan` can change its sort order by changing its permutation, so
  // the sorting is pushed down into the scan and no `Sort` is required at all.
  {
    // A prefiltered `IndexScan` is tied to its permutation and hence cannot be
    // re-sorted, so we disable the prefilter here.
    auto rtp = setRuntimeParameterForTest<
        &RuntimeParameters::enablePrefilterOnIndexScans_>(false);
    auto scan = ad_utility::makeExecutionTree<IndexScan>(
        qec, Permutation::PSO,
        SparqlTripleSimple{Variable{"?x"}, iri("<p>"), Variable{"?z"}});
    ASSERT_EQ(scan->resultSortedOn(), (std::vector<ColumnIndex>{0, 1}));
    auto filter = makeFilter(scan);

    // The `Filter` already is sorted by `{0}`, so requesting that sort order
    // violates the precondition of `makeSortedTree`.
    EXPECT_THROW(filter.makeSortedTree({0}), ad_utility::Exception);

    auto tree = checkPushedDown(filter, {1});
    ASSERT_TRUE(tree.has_value());
    // Only the sort order changed, the scan still is a scan.
    EXPECT_EQ(tree.value()->resultSortedOn(), (std::vector<ColumnIndex>{1, 0}));
    EXPECT_TRUE(rootIs<IndexScan>(onlyChild(*tree.value())));
  }

  // A `ValuesForTesting` cannot change its sort order, so an explicit `Sort` is
  // required. That `Sort` is not pushed below the `Filter`, because sorting
  // only the rows that pass the filter is cheaper.
  {
    auto filter = makeFilter(makeValues({0}));
    EXPECT_FALSE(filter.makeSortedTree({1}).has_value());
  }

  // If the child already is a `Sort`, the sorting is pushed down, because
  // re-sorting the child doesn't add another `Sort`, but only changes the sort
  // order of the existing one.
  {
    auto sortedValues = ad_utility::makeExecutionTree<Sort>(
        qec, makeValues({}), std::vector<ColumnIndex>{1});
    auto filter = makeFilter(sortedValues);
    ASSERT_EQ(filter.resultSortedOn(), (std::vector<ColumnIndex>{1}));

    auto tree = checkPushedDown(filter, {0});
    ASSERT_TRUE(tree.has_value());
    // The `Sort` of the child was replaced instead of stacking a second `Sort`
    // on top of it.
    ASSERT_TRUE(rootIs<Sort>(onlyChild(*tree.value())));
    EXPECT_FALSE(rootIs<Sort>(onlyChild(onlyChild(*tree.value()))));
  }
}

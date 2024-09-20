//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/GroupBy.h"
#include "engine/LazyGroupBy.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"

namespace {
auto I = ad_utility::testing::IntId;
}

// _____________________________________________________________________________
class LazyGroupByTest : public ::testing::Test {
 protected:
  // Unlimited allocator.
  ad_utility::AllocatorWithLimit<Id> ua =
      ad_utility::makeUnlimitedAllocator<Id>();
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
  // Initialize dummy instance of GroupBy to be used for testing. The actual
  // operations will never get executed because we manually test `LazyGroupBy`
  // here.
  Variable xVar_{"?x"};
  Variable yVar_{"?y"};
  sparqlExpression::SparqlExpressionPimpl sparqlExpression_{
      makeAddExpression(
          std::make_unique<sparqlExpression::VariableExpression>(yVar_),
          std::make_unique<sparqlExpression::SumExpression>(
              false,
              std::make_unique<sparqlExpression::VariableExpression>(xVar_))),
      "?y + SUM(?x)"};
  std::shared_ptr<QueryExecutionTree> subtree_ =
      std::make_shared<QueryExecutionTree>(
          qec_,
          std::make_shared<ValuesForTesting>(
              qec_, IdTable{2, ad_utility::makeAllocatorWithLimit<Id>(0_B)},
              std::vector{std::optional{xVar_}, std::optional{yVar_}}));
  GroupBy groupBy_{qec_, {yVar_}, {}, subtree_};

  std::vector<GroupBy::Aggregate> aggregates_{
      {std::move(sparqlExpression_), 1}};
  LocalVocab localVocab_{};
  LazyGroupBy lazyGroupBy_{
      localVocab_,
      groupBy_.computeUnsequentialProcessingMetadata(aggregates_)
          ->aggregateAliases_,
      groupBy_.allocator(), 1};

  sparqlExpression::EvaluationContext makeEvaluationContext(
      const IdTable& idTable) const {
    sparqlExpression::EvaluationContext evaluationContext{
        *qec_,
        subtree_->getVariableColumns(),
        idTable,
        groupBy_.allocator(),
        localVocab_,
        std::make_shared<ad_utility::CancellationHandle<>>(),
        std::chrono::steady_clock::time_point::max()};

    evaluationContext._groupedVariables = ad_utility::HashSet<Variable>{yVar_};
    evaluationContext._previousResultsFromSameGroup.resize(2);
    evaluationContext._isPartOfGroupBy = true;
    return evaluationContext;
  }
};

// _____________________________________________________________________________
TEST_F(LazyGroupByTest, verifyEmptyGroupsAreAggregatedCorrectly) {
  IdTable resultTable{2, ua};
  GroupBy::GroupBlock block{{0, I(7)}};
  IdTable idTable{1, ad_utility::makeAllocatorWithLimit<Id>(0_B)};
  auto evaluationContext = makeEvaluationContext(idTable);

  lazyGroupBy_.processBlock(evaluationContext, 0, 0);
  lazyGroupBy_.commitRow(resultTable, evaluationContext, block, groupBy_);

  // The grouped variable has the value 7 here + 0 is still 7.
  EXPECT_EQ(resultTable, makeIdTableFromVector({{7, 7}}, I));

  block.at(0).second = I(9);
  lazyGroupBy_.commitRow(resultTable, evaluationContext, block, groupBy_);

  // The grouped variable has the value 9 here + 0 is still 9.
  EXPECT_EQ(resultTable, makeIdTableFromVector({{7, 7}, {9, 9}}, I));
}

// _____________________________________________________________________________
TEST_F(LazyGroupByTest, verifyGroupsAreAggregatedCorrectly) {
  IdTable resultTable{2, ua};
  GroupBy::GroupBlock block{{0, I(7)}};
  IdTable idTable = makeIdTableFromVector({{2}, {3}, {5}, {7}}, I);
  auto evaluationContext = makeEvaluationContext(idTable);

  lazyGroupBy_.processBlock(evaluationContext, 1, 3);
  lazyGroupBy_.commitRow(resultTable, evaluationContext, block, groupBy_);

  // The `7` is the current group, and the aggregate computes the SUM between
  // the elements at index 1 and 2, which is  7 + 3 + 5 = 15.
  EXPECT_EQ(resultTable, makeIdTableFromVector({{7, 15}}, I));

  // the new group starts with a value of 0
  lazyGroupBy_.processBlock(evaluationContext, 0, 1);  // add 2 -> 2
  lazyGroupBy_.processBlock(evaluationContext, 1, 3);  // add 3 + 5 = 8 -> 10
  lazyGroupBy_.processBlock(evaluationContext, 3, 4);  // add 7 -> 17
  lazyGroupBy_.processBlock(evaluationContext, 4, 4);  // add 0 (empty) -> 17
  block.at(0).second = I(9);                           // add 9 -> 26
  lazyGroupBy_.commitRow(resultTable, evaluationContext, block, groupBy_);

  EXPECT_EQ(resultTable, makeIdTableFromVector({{7, 15}, {9, 26}}, I));
}

// _____________________________________________________________________________
TEST_F(LazyGroupByTest, verifyCommitWorksWhenOriginalIdTableIsGone) {
  IdTable resultTable{2, ua};
  GroupBy::GroupBlock block{{0, I(3)}};
  {
    IdTable idTable = makeIdTableFromVector({{2}, {3}, {5}, {7}}, I);
    auto evaluationContext = makeEvaluationContext(idTable);

    lazyGroupBy_.processBlock(evaluationContext, 1, 3);
  }
  IdTable idTable = makeIdTableFromVector({}, I);
  auto evaluationContext = makeEvaluationContext(idTable);
  lazyGroupBy_.commitRow(resultTable, evaluationContext, block, groupBy_);

  // 3 + 3 + 5 = 11
  EXPECT_EQ(resultTable, makeIdTableFromVector({{3, 11}}, I));
}

// _____________________________________________________________________________
TEST(LazyGroupBy, verifyGroupConcatIsCorrectlyInitialized) {
  auto* qec = ad_utility::testing::getQec();
  Variable variable{"?someVariable"};
  sparqlExpression::SparqlExpressionPimpl sparqlExpression{
      std::make_unique<sparqlExpression::GroupConcatExpression>(
          false,
          std::make_unique<sparqlExpression::VariableExpression>(variable),
          "|"),
      "SUM(?someVariable)"};
  auto subtree = std::make_shared<QueryExecutionTree>(
      qec, std::make_shared<ValuesForTesting>(
               qec, IdTable{1, ad_utility::makeAllocatorWithLimit<Id>(0_B)},
               std::vector{std::optional{variable}}));
  GroupBy groupBy{qec, {variable}, {}, subtree};
  std::vector<GroupBy::Aggregate> aggregates{{std::move(sparqlExpression), 0}};
  LocalVocab localVocab{};
  LazyGroupBy lazyGroupBy{
      localVocab,
      groupBy.computeUnsequentialProcessingMetadata(aggregates)
          ->aggregateAliases_,
      groupBy.allocator(), 1};
  using namespace ::testing;
  using Gc = GroupConcatAggregationData;
  EXPECT_THAT(lazyGroupBy.aggregationData_.getAggregationDataVariant(0),
              VariantWith<sparqlExpression::VectorWithMemoryLimit<Gc>>(
                  ElementsAre(AD_FIELD(Gc, separator_, Eq("|")))));
}

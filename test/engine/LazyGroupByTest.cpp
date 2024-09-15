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
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"

namespace {
auto I = ad_utility::testing::IntId;
}

// _____________________________________________________________________________
class LazyGroupByTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
  Variable variable_{"?someVariable"};
  sparqlExpression::SparqlExpressionPimpl sparqlExpression_{
      std::make_unique<sparqlExpression::SumExpression>(
          false,
          std::make_unique<sparqlExpression::VariableExpression>(variable_)),
      "SUM(?someVariable)"};
  std::shared_ptr<QueryExecutionTree> subtree_ =
      std::make_shared<QueryExecutionTree>(
          qec_,
          std::make_shared<ValuesForTesting>(
              qec_, IdTable{1, ad_utility::makeAllocatorWithLimit<Id>(0_B)},
              std::vector{std::optional{variable_}}));
  GroupBy groupBy_{qec_, {variable_}, {}, subtree_};
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

    evaluationContext._groupedVariables =
        ad_utility::HashSet<Variable>{variable_};
    evaluationContext._previousResultsFromSameGroup.resize(2);
    evaluationContext._isPartOfGroupBy = true;
    return evaluationContext;
  }
};

// _____________________________________________________________________________
TEST_F(LazyGroupByTest, verifyEmptyGroupsAreAggregatedCorrectly) {
  IdTable resultTable{2, ad_utility::makeUnlimitedAllocator<Id>()};
  std::vector<std::pair<size_t, Id>> block{{0, I(7)}};
  IdTable idTable{1, ad_utility::makeAllocatorWithLimit<Id>(0_B)};
  auto evaluationContext = makeEvaluationContext(idTable);

  lazyGroupBy_.processNextBlock(evaluationContext, 0, 0);
  lazyGroupBy_.commitRow(resultTable, evaluationContext, block, groupBy_);

  EXPECT_EQ(resultTable, makeIdTableFromVector({{7, 0}}, I));

  lazyGroupBy_.commitRow(resultTable, evaluationContext, block, groupBy_);

  EXPECT_EQ(resultTable, makeIdTableFromVector({{7, 0}, {7, 0}}, I));
}

// _____________________________________________________________________________
TEST_F(LazyGroupByTest, verifyGroupsAreAggregatedCorrectly) {
  IdTable resultTable{2, ad_utility::makeUnlimitedAllocator<Id>()};
  std::vector<std::pair<size_t, Id>> block{{0, I(7)}};
  IdTable idTable = makeIdTableFromVector({{2}, {3}, {5}, {7}}, I);
  auto evaluationContext = makeEvaluationContext(idTable);

  lazyGroupBy_.processNextBlock(evaluationContext, 1, 3);
  lazyGroupBy_.commitRow(resultTable, evaluationContext, block, groupBy_);

  EXPECT_EQ(resultTable, makeIdTableFromVector({{7, 8}}, I));

  lazyGroupBy_.processNextBlock(evaluationContext, 0, 1);
  lazyGroupBy_.processNextBlock(evaluationContext, 1, 3);
  lazyGroupBy_.processNextBlock(evaluationContext, 3, 4);
  lazyGroupBy_.processNextBlock(evaluationContext, 4, 4);
  block.at(0).second = I(9);
  lazyGroupBy_.commitRow(resultTable, evaluationContext, block, groupBy_);

  EXPECT_EQ(resultTable, makeIdTableFromVector({{7, 8}, {9, 17}}, I));
}

// _____________________________________________________________________________
TEST_F(LazyGroupByTest, verifyCommitWorksWhenOriginalIdTableIsGone) {
  IdTable resultTable{2, ad_utility::makeUnlimitedAllocator<Id>()};
  std::vector<std::pair<size_t, Id>> block{{0, I(3)}};
  {
    IdTable idTable = makeIdTableFromVector({{2}, {3}, {5}, {7}}, I);
    auto evaluationContext = makeEvaluationContext(idTable);

    lazyGroupBy_.processNextBlock(evaluationContext, 1, 3);
  }
  IdTable idTable = makeIdTableFromVector({}, I);
  auto evaluationContext = makeEvaluationContext(idTable);
  lazyGroupBy_.commitRow(resultTable, evaluationContext, block, groupBy_);

  EXPECT_EQ(resultTable, makeIdTableFromVector({{3, 8}}, I));
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
                  ElementsAre(Field("separator_", &Gc::separator_, Eq("|")))));
}

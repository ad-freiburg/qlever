//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/Bind.h"
#include "engine/sparqlExpressions/LiteralExpression.h"

using namespace sparqlExpression;

// _____________________________________________________________________________
TEST(Bind, computeResult) {
  using Vars = std::vector<std::optional<Variable>>;
  auto* qec = ad_utility::testing::getQec();
  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}, {2}, {3}, {4}}), Vars{Variable{"?a"}});
  Bind bind{
      qec,
      std::move(valuesTree),
      {SparqlExpressionPimpl{
           std::make_unique<VariableExpression>(Variable{"?a"}), "?a as ?b"},
       Variable{"?b"}}};

  {
    qec->getQueryTreeCache().clearAll();
    auto result = bind.getResult(false, ComputationMode::FULLY_MATERIALIZED);
    ASSERT_TRUE(result->isFullyMaterialized());
    EXPECT_EQ(result->idTable(),
              makeIdTableFromVector({{1, 1}, {2, 2}, {3, 3}, {4, 4}}));
  }

  {
    qec->getQueryTreeCache().clearAll();
    auto result = bind.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
    ASSERT_FALSE(result->isFullyMaterialized());
    auto& idTables = result->idTables();
    auto begin = idTables.begin();
    ASSERT_NE(begin, idTables.end());
    EXPECT_EQ(*begin, makeIdTableFromVector({{1, 1}, {2, 2}, {3, 3}, {4, 4}}));
    EXPECT_EQ(++begin, idTables.end());
  }
}

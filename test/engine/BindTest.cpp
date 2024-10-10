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
using Vars = std::vector<std::optional<Variable>>;

namespace {
Bind makeBindForIdTable(QueryExecutionContext* qec, IdTable idTable) {
  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(idTable), Vars{Variable{"?a"}});
  return {
      qec,
      std::move(valuesTree),
      {SparqlExpressionPimpl{
           std::make_unique<VariableExpression>(Variable{"?a"}), "?a as ?b"},
       Variable{"?b"}}};
}

IdTable getSingleIdTable(cppcoro::generator<IdTable>& generator) {
  std::optional<IdTable> result = std::nullopt;
  for (IdTable& idTable : generator) {
    if (result.has_value()) {
      ADD_FAILURE() << "More than one IdTable was generated";
      break;
    }
    result = std::move(idTable);
  }
  if (!result.has_value()) {
    throw std::runtime_error{"No IdTable was generated"};
  }
  return std::move(result).value();
}
}  // namespace

// _____________________________________________________________________________
TEST(Bind, computeResult) {
  auto* qec = ad_utility::testing::getQec();
  Bind bind =
      makeBindForIdTable(qec, makeIdTableFromVector({{1}, {2}, {3}, {4}}));

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
    EXPECT_EQ(getSingleIdTable(result->idTables()),
              makeIdTableFromVector({{1, 1}, {2, 2}, {3, 3}, {4, 4}}));
  }
}

// _____________________________________________________________________________
TEST(Bind, computeResultWithTableWithoutRows) {
  auto* qec = ad_utility::testing::getQec();
  Bind bind = makeBindForIdTable(
      qec, IdTable{1, ad_utility::makeUnlimitedAllocator<Id>()});

  {
    qec->getQueryTreeCache().clearAll();
    auto result = bind.getResult(false, ComputationMode::FULLY_MATERIALIZED);
    ASSERT_TRUE(result->isFullyMaterialized());
    EXPECT_EQ(result->idTable(),
              (IdTable{2, ad_utility::makeUnlimitedAllocator<Id>()}));
  }

  {
    qec->getQueryTreeCache().clearAll();
    auto result = bind.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
    ASSERT_FALSE(result->isFullyMaterialized());
    EXPECT_EQ(getSingleIdTable(result->idTables()),
              (IdTable{2, ad_utility::makeUnlimitedAllocator<Id>()}));
  }
}

// _____________________________________________________________________________
TEST(Bind, computeResultWithTableWithoutColumns) {
  auto val = Id::makeFromInt(42);
  auto* qec = ad_utility::testing::getQec();
  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{}, {}}), Vars{});
  Bind bind{
      qec,
      std::move(valuesTree),
      {SparqlExpressionPimpl{std::make_unique<IdExpression>(val), "42 as ?b"},
       Variable{"?b"}}};

  {
    qec->getQueryTreeCache().clearAll();
    auto result = bind.getResult(false, ComputationMode::FULLY_MATERIALIZED);
    ASSERT_TRUE(result->isFullyMaterialized());
    EXPECT_EQ(result->idTable(), makeIdTableFromVector({{val}, {val}}));
  }

  {
    qec->getQueryTreeCache().clearAll();
    auto result = bind.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
    ASSERT_FALSE(result->isFullyMaterialized());
    EXPECT_EQ(getSingleIdTable(result->idTables()),
              makeIdTableFromVector({{val}, {val}}));
  }
}

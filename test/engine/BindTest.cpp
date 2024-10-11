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

void expectBindYieldsIdTable(
    QueryExecutionContext* qec, Bind& bind, const IdTable& expected,
    ad_utility::source_location loc = ad_utility::source_location::current()) {
  auto trace = generateLocationTrace(loc);

  {
    qec->getQueryTreeCache().clearAll();
    auto result = bind.getResult(false, ComputationMode::FULLY_MATERIALIZED);
    ASSERT_TRUE(result->isFullyMaterialized());
    EXPECT_EQ(result->idTable(), expected);
  }

  {
    qec->getQueryTreeCache().clearAll();
    auto result = bind.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
    ASSERT_FALSE(result->isFullyMaterialized());
    auto& idTables = result->idTables();
    auto iterator = idTables.begin();
    ASSERT_NE(iterator, idTables.end());
    EXPECT_EQ(*iterator, expected);
    EXPECT_EQ(++iterator, idTables.end());
  }
}
}  // namespace

// _____________________________________________________________________________
TEST(Bind, computeResult) {
  auto* qec = ad_utility::testing::getQec();
  Bind bind =
      makeBindForIdTable(qec, makeIdTableFromVector({{1}, {2}, {3}, {4}}));

  expectBindYieldsIdTable(
      qec, bind, makeIdTableFromVector({{1, 1}, {2, 2}, {3, 3}, {4, 4}}));
}

// _____________________________________________________________________________
TEST(Bind, computeResultWithTableWithoutRows) {
  auto* qec = ad_utility::testing::getQec();
  Bind bind = makeBindForIdTable(
      qec, IdTable{1, ad_utility::makeUnlimitedAllocator<Id>()});

  expectBindYieldsIdTable(qec, bind,
                          IdTable{2, ad_utility::makeUnlimitedAllocator<Id>()});
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

  expectBindYieldsIdTable(qec, bind, makeIdTableFromVector({{val}, {val}}));
}

// _____________________________________________________________________________
TEST(
    Bind,
    computeResultProducesLazyResultWhenFullyMaterializedSubResultIsTooLargeAndRequested) {
  auto val = Id::makeFromInt(42);
  IdTable::row_type row{1};
  row[0] = val;
  auto* qec = ad_utility::testing::getQec();
  IdTable table{1, ad_utility::makeUnlimitedAllocator<Id>()};
  table.resize(Bind::CHUNK_SIZE + 1);
  std::ranges::fill(table, row);
  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, table.clone(), Vars{Variable{"?a"}}, false,
      std::vector<ColumnIndex>{}, LocalVocab{}, std::nullopt, true);
  Bind bind{
      qec,
      std::move(valuesTree),
      {SparqlExpressionPimpl{std::make_unique<IdExpression>(val), "42 as ?b"},
       Variable{"?b"}}};

  table.addEmptyColumn();
  row = IdTable::row_type{2};
  row[0] = val;
  row[1] = val;
  std::ranges::fill(table, row);
  {
    qec->getQueryTreeCache().clearAll();
    auto result = bind.getResult(false, ComputationMode::FULLY_MATERIALIZED);
    ASSERT_TRUE(result->isFullyMaterialized());
    EXPECT_EQ(result->idTable(), table);
  }

  {
    table.resize(Bind::CHUNK_SIZE);
    qec->getQueryTreeCache().clearAll();
    auto result = bind.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
    ASSERT_FALSE(result->isFullyMaterialized());
    auto& idTables = result->idTables();
    auto iterator = idTables.begin();
    ASSERT_NE(iterator, idTables.end());
    EXPECT_EQ(*iterator, table);
    ASSERT_NE(++iterator, idTables.end());
    EXPECT_EQ(*iterator, makeIdTableFromVector({{val, val}}));
    EXPECT_EQ(++iterator, idTables.end());
  }
}

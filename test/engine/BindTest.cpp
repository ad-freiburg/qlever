//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/Bind.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"

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
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
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
    auto idTables = result->idTables();
    auto iterator = idTables.begin();
    ASSERT_NE(iterator, idTables.end());
    EXPECT_EQ(iterator->idTable_, expected);
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
  ql::ranges::fill(table, row);
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
  ql::ranges::fill(table, row);
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
    auto idTables = result->idTables();
    auto iterator = idTables.begin();
    ASSERT_NE(iterator, idTables.end());
    EXPECT_EQ(iterator->idTable_, table);
    ASSERT_NE(++iterator, idTables.end());
    EXPECT_EQ(iterator->idTable_, makeIdTableFromVector({{val, val}}));
    EXPECT_EQ(++iterator, idTables.end());
  }
}

// _____________________________________________________________________________
TEST(Bind, clone) {
  auto* qec = ad_utility::testing::getQec();
  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, IdTable{1, qec->getAllocator()}, Vars{Variable{"?a"}}, false,
      std::vector<ColumnIndex>{}, LocalVocab{}, std::nullopt, true);
  Bind bind{
      qec,
      std::move(valuesTree),
      {SparqlExpressionPimpl{
           std::make_unique<IdExpression>(Id::makeFromInt(42)), "42 as ?b"},
       Variable{"?b"}}};

  auto clone = bind.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(bind, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), bind.getDescriptor());
}

// _____________________________________________________________________________
TEST(Bind, limitIsPropagated) {
  auto* qec = ad_utility::testing::getQec();
  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0}, {1}, {2}}, &Id::makeFromInt),
      Vars{Variable{"?a"}}, false, std::vector<ColumnIndex>{}, LocalVocab{},
      std::nullopt, true);
  Bind bind{
      qec,
      std::move(valuesTree),
      {SparqlExpressionPimpl{
           std::make_unique<IdExpression>(Id::makeFromInt(42)), "42 as ?b"},
       Variable{"?b"}}};

  bind.applyLimitOffset({1, 1});

  auto result = bind.computeResultOnlyForTesting();
  const auto& idTable = result.idTable();

  EXPECT_EQ(idTable, makeIdTableFromVector({{1, 42}}, &Id::makeFromInt));
}

// _____________________________________________________________________________
TEST(Bind, undefStatusForConstantInteger) {
  auto* qec = ad_utility::testing::getQec();
  Variable inputVar{"?x"};
  Variable targetVar{"?newCol"};

  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{42}}, &Id::makeFromInt), Vars{inputVar});

  // Create BIND(3 AS ?newCol)
  auto constantExpr = std::make_unique<IdExpression>(Id::makeFromInt(3));
  SparqlExpressionPimpl pimpl{std::move(constantExpr), "3"};
  Bind bind{qec, std::move(valuesTree), {std::move(pimpl), targetVar}};

  // Check that the variable to column map has the correct undef status
  auto varColMap = bind.getExternallyVisibleVariableColumns();

  ASSERT_TRUE(varColMap.contains(targetVar));
  auto& colInfo = varColMap.at(targetVar);

  // The constant 3 should always be defined
  EXPECT_EQ(colInfo.mightContainUndef_,
            ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined);
}

// _____________________________________________________________________________
TEST(Bind, undefStatusForDivisionByZero) {
  auto* qec = ad_utility::testing::getQec();
  Variable inputVar{"?x"};
  Variable targetVar{"?newCol"};

  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{42}}, &Id::makeFromInt), Vars{inputVar});

  // Create BIND(1/0 AS ?newCol)
  auto oneExpr = std::make_unique<IdExpression>(Id::makeFromInt(1));
  auto zeroExpr = std::make_unique<IdExpression>(Id::makeFromInt(0));
  auto divExpr = makeDivideExpression(std::move(oneExpr), std::move(zeroExpr));
  SparqlExpressionPimpl pimpl{std::move(divExpr), "1/0"};
  Bind bind{qec, std::move(valuesTree), {std::move(pimpl), targetVar}};

  // Check that the variable to column map has the correct undef status
  auto varColMap = bind.getExternallyVisibleVariableColumns();

  ASSERT_TRUE(varColMap.contains(targetVar));
  auto& colInfo = varColMap.at(targetVar);

  // Division by zero can produce undefined, so it should be PossiblyUndefined
  EXPECT_EQ(colInfo.mightContainUndef_,
            ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined);
}

// _____________________________________________________________________________
TEST(Bind, undefStatusForAlwaysDefinedVariable) {
  auto* qec = ad_utility::testing::getQec();
  Variable inputVar{"?x"};
  Variable targetVar{"?y"};

  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{42}}, &Id::makeFromInt), Vars{inputVar});

  // Create BIND(?x AS ?y) where ?x is AlwaysDefined
  auto varExpr = std::make_unique<VariableExpression>(inputVar);
  SparqlExpressionPimpl pimpl{std::move(varExpr), inputVar.name()};
  Bind bind{qec, std::move(valuesTree), {std::move(pimpl), targetVar}};

  // Check that the variable to column map has the correct undef status
  auto varColMap = bind.getExternallyVisibleVariableColumns();

  ASSERT_TRUE(varColMap.contains(targetVar));
  auto& colInfo = varColMap.at(targetVar);

  // Since ?x is AlwaysDefined (from ValuesForTesting), ?y should be too
  EXPECT_EQ(colInfo.mightContainUndef_,
            ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined);
}

// _____________________________________________________________________________
TEST(Bind, undefStatusForStringLiteral) {
  auto* qec = ad_utility::testing::getQec();
  Variable inputVar{"?x"};
  Variable targetVar{"?str"};

  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{42}}, &Id::makeFromInt), Vars{inputVar});

  // Create BIND("hello" AS ?str)
  auto literal =
      ad_utility::triple_component::Literal::literalWithoutQuotes("hello");
  auto literalExpr = std::make_unique<StringLiteralExpression>(literal);
  SparqlExpressionPimpl pimpl{std::move(literalExpr), "\"hello\""};
  Bind bind{qec, std::move(valuesTree), {std::move(pimpl), targetVar}};

  // Check that the variable to column map has the correct undef status
  auto varColMap = bind.getExternallyVisibleVariableColumns();

  ASSERT_TRUE(varColMap.contains(targetVar));
  auto& colInfo = varColMap.at(targetVar);

  // String literals should always be defined
  EXPECT_EQ(colInfo.mightContainUndef_,
            ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined);
}

// _____________________________________________________________________________
TEST(Bind, undefStatusForArithmeticOnConstants) {
  auto* qec = ad_utility::testing::getQec();
  Variable inputVar{"?x"};
  Variable targetVar{"?result"};

  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{42}}, &Id::makeFromInt), Vars{inputVar});

  // Create BIND(2 * 3 AS ?result)
  auto twoExpr = std::make_unique<IdExpression>(Id::makeFromInt(2));
  auto threeExpr = std::make_unique<IdExpression>(Id::makeFromInt(3));
  auto mulExpr =
      makeMultiplyExpression(std::move(twoExpr), std::move(threeExpr));
  SparqlExpressionPimpl pimpl{std::move(mulExpr), "2*3"};
  Bind bind{qec, std::move(valuesTree), {std::move(pimpl), targetVar}};

  // Check that the variable to column map has the correct undef status
  auto varColMap = bind.getExternallyVisibleVariableColumns();

  ASSERT_TRUE(varColMap.contains(targetVar));
  auto& colInfo = varColMap.at(targetVar);

  // Arithmetic expressions don't have isResultAlwaysDefined implemented yet,
  // so they default to PossiblyUndefined. This will change once NaryExpression
  // implements isResultAlwaysDefined.
  EXPECT_EQ(colInfo.mightContainUndef_,
            ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined);
}

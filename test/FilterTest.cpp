//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "engine/Filter.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "util/IndexTestHelpers.h"

using ::testing::ElementsAre;

IdTable makeIdTable(std::vector<bool> bools) {
  IdTable idTable{1, ad_utility::makeUnlimitedAllocator<Id>()};
  for (bool b : bools) {
    idTable.push_back({Id::makeFromBool(b)});
  }
  return idTable;
}

columnBasedIdTable::Row<Id> makeRow(bool b) {
  columnBasedIdTable::Row<Id> row{1};
  row[0] = Id::makeFromBool(b);
  return row;
}

// _____________________________________________________________________________
TEST(Filter, verifyPredicateIsAppliedCorrectlyOnLazyEvaluation) {
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();
  std::vector<IdTable> idTables;
  idTables.push_back(makeIdTable({true, true, false, false, true}));
  idTables.push_back(makeIdTable({true, false}));
  idTables.push_back(makeIdTable({}));
  idTables.push_back(makeIdTable({false, false, false}));
  idTables.push_back(makeIdTable({true}));

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
  auto& generator = result->idTables();

  auto iterator = generator.begin();
  ASSERT_NE(iterator, generator.end());
  EXPECT_THAT(*iterator,
              ElementsAre(makeRow(true), makeRow(true), makeRow(true)));

  ++iterator;
  ASSERT_NE(iterator, generator.end());
  EXPECT_THAT(*iterator, ElementsAre(makeRow(true)));

  ++iterator;
  ASSERT_NE(iterator, generator.end());
  EXPECT_THAT(*iterator, ElementsAre());

  ++iterator;
  ASSERT_NE(iterator, generator.end());
  EXPECT_THAT(*iterator, ElementsAre());

  ++iterator;
  ASSERT_NE(iterator, generator.end());
  EXPECT_THAT(*iterator, ElementsAre(makeRow(true)));

  ++iterator;
  EXPECT_EQ(iterator, generator.end());
}

// _____________________________________________________________________________
TEST(Filter, verifyPredicateIsAppliedCorrectlyOnNonLazyEvaluation) {
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();
  std::vector<IdTable> idTables;
  idTables.push_back(makeIdTable({true, true, false, false, true}));
  idTables.push_back(makeIdTable({true, false}));
  idTables.push_back(makeIdTable({}));
  idTables.push_back(makeIdTable({false, false, false}));
  idTables.push_back(makeIdTable({true}));

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
  EXPECT_THAT(result->idTable(),
              ElementsAre(makeRow(true), makeRow(true), makeRow(true),
                          makeRow(true), makeRow(true)));
}

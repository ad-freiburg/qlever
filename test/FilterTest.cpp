//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "engine/Filter.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

using ::testing::ElementsAre;
using ::testing::Eq;

namespace {
// Shorthand for makeFromBool
ValueId asBool(bool value) { return Id::makeFromBool(value); }

// Convert a generator to a vector for easier comparison in assertions
std::vector<IdTable> toVector(cppcoro::generator<IdTable> generator) {
  std::vector<IdTable> result;
  for (auto& table : generator) {
    result.push_back(std::move(table));
  }
  return result;
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
  auto& generator = result->idTables();

  auto referenceTable1 =
      makeIdTableFromVector({{true}, {true}, {true}}, asBool);
  auto referenceTable2 = makeIdTableFromVector({{true}}, asBool);
  IdTable referenceTable3{0, ad_utility::makeUnlimitedAllocator<Id>()};

  EXPECT_THAT(toVector(std::move(generator)),
              ElementsAre(Eq(std::cref(referenceTable1)),
                          Eq(std::cref(referenceTable2)),
                          Eq(std::cref(referenceTable3)),
                          Eq(std::cref(referenceTable3)),
                          Eq(std::cref(referenceTable2))));
}

// _____________________________________________________________________________
TEST(Filter, verifyPredicateIsAppliedCorrectlyOnNonLazyEvaluation) {
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

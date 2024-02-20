//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <vector>

#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/ResultTable.h"
#include "engine/Values.h"
#include "engine/idTable/IdTable.h"
#include "util/IndexTestHelpers.h"

using TC = TripleComponent;
using ValuesComponents = std::vector<std::vector<TripleComponent>>;

// Check the basic methods of the `Values` clause.
TEST(Values, basicMethods) {
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
  ValuesComponents values{{TC{1}, TC{2}, TC{3}},
                          {TC{5}, TC{2}, TC{3}},
                          {TC{7}, TC{42}, TC{3}},
                          {TC{7}, TC{42}, TC::UNDEF{}}};
  Values valuesOp(testQec,
                  {{Variable{"?x"}, Variable{"?y"}, Variable{"?z"}}, values});
  EXPECT_FALSE(valuesOp.knownEmptyResult());
  EXPECT_EQ(valuesOp.getSizeEstimate(), 4u);
  EXPECT_EQ(valuesOp.getCostEstimate(), 4u);
  EXPECT_EQ(valuesOp.getDescriptor(), "Values with variables ?x\t?y\t?z");
  EXPECT_TRUE(valuesOp.resultSortedOn().empty());
  EXPECT_EQ(valuesOp.getResultWidth(), 3u);
  // Col 0: 1, 5, 42, 42
  EXPECT_FLOAT_EQ(valuesOp.getMultiplicity(0), 4.0 / 3.0);
  // Col 2: 2, 2, 3, 3
  EXPECT_FLOAT_EQ(valuesOp.getMultiplicity(1), 2.0f);
  // Col 3: always `3`.
  EXPECT_FLOAT_EQ(valuesOp.getMultiplicity(2), 2.0f);
  // Out-of-bounds columns always return 1.
  EXPECT_FLOAT_EQ(valuesOp.getMultiplicity(4), 1.0f);

  using V = Variable;
  VariableToColumnMap expectedVariables{
      {V{"?x"}, makeAlwaysDefinedColumn(0)},
      {V{"?y"}, makeAlwaysDefinedColumn(1)},
      {V{"?z"}, makePossiblyUndefinedColumn(2)}};
  EXPECT_THAT(valuesOp.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
}

// Check some corner cases for an empty VALUES clause.
TEST(Values, emptyValuesClause) {
  auto testQec = ad_utility::testing::getQec();
  Values emptyValuesOp(testQec, {});
  EXPECT_TRUE(emptyValuesOp.knownEmptyResult());
  // The current implementation always returns `1.0` for nonexisting columns.
  EXPECT_FLOAT_EQ(emptyValuesOp.getMultiplicity(32), 1.0);
}

// Check that `computeResult`, given a parsed VALUES clause, computes the
// correct result table.
TEST(Values, computeResult) {
  auto testQec = ad_utility::testing::getQec("<x> <x> <x> .");
  ValuesComponents values{{TC{12}, TC{"<x>"}}, {TC::UNDEF{}, TC{"<y>"}}};
  Values valuesOperation(testQec, {{Variable{"?x"}, Variable{"?y"}}, values});
  auto result = valuesOperation.getResult();
  const auto& table = result->idTable();
  Id x;
  bool success = testQec->getIndex().getId("<x>", &x);
  AD_CORRECTNESS_CHECK(success);
  auto I = ad_utility::testing::IntId;
  auto L = ad_utility::testing::LocalVocabId;
  auto U = Id::makeUndefined();
  ASSERT_EQ(table, makeIdTableFromVector({{I(12), x}, {U, L(0)}}));
}

// Check that if the number of variables and the number of values in each row
// are not all equal, an exception is thrown.
TEST(Values, illegalInput) {
  auto qec = ad_utility::testing::getQec();
  ValuesComponents values{{TC{12}, TC{"<x>"}}, {TC::UNDEF{}}};
  ASSERT_ANY_THROW(Values(qec, {{Variable{"?x"}, Variable{"?y"}}, values}));
}

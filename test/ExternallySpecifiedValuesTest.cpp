//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Generated with Claude Code

#include <gtest/gtest.h>

#include <vector>

#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/ExternallySpecifiedValues.h"
#include "engine/Operation.h"
#include "engine/Result.h"
#include "engine/idTable/IdTable.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

using TC = TripleComponent;
using ValuesComponents = std::vector<std::vector<TripleComponent>>;

namespace {
auto iri = ad_utility::testing::iri;
}

// Check the basic methods of the `ExternallySpecifiedValues` operation.
TEST(ExternallySpecifiedValues, basicMethods) {
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
  ValuesComponents values{{TC{1}, TC{2}, TC{3}},
                          {TC{5}, TC{2}, TC{3}},
                          {TC{7}, TC{42}, TC{3}}};
  ExternallySpecifiedValues externalValuesOp(
      testQec, {{Variable{"?x"}, Variable{"?y"}, Variable{"?z"}}, values},
      "test-id");

  // Check identifier
  EXPECT_EQ(externalValuesOp.getIdentifier(), "test-id");

  // Check that knownEmptyResult always returns false (even with empty values)
  EXPECT_FALSE(externalValuesOp.knownEmptyResult());

  // Check that the operation is uncachable
  EXPECT_FALSE(externalValuesOp.canResultBeCached());

  // Check other basic methods inherited from Values
  EXPECT_EQ(externalValuesOp.getSizeEstimate(), 3u);
  EXPECT_EQ(externalValuesOp.getCostEstimate(), 3u);
  EXPECT_EQ(externalValuesOp.getDescriptor(),
            "External values with identifier 'test-id' and variables ?x\t?y\t?z");
  EXPECT_TRUE(externalValuesOp.resultSortedOn().empty());
  EXPECT_EQ(externalValuesOp.getResultWidth(), 3u);
}

// Check that knownEmptyResult returns false even with empty values
TEST(ExternallySpecifiedValues, knownEmptyResultWithEmptyValues) {
  auto testQec = ad_utility::testing::getQec();
  ValuesComponents emptyValues{};
  ExternallySpecifiedValues externalValuesOp(
      testQec, {{Variable{"?x"}, Variable{"?y"}}, emptyValues}, "empty-id");

  // Should return false even though values are empty
  EXPECT_FALSE(externalValuesOp.knownEmptyResult());
}

// Check that `computeResult` works correctly
TEST(ExternallySpecifiedValues, computeResult) {
  auto testQec = ad_utility::testing::getQec("<x> <x> <x> .");
  ValuesComponents values{{TC{12}, TC{iri("<x>")}},
                          {TC::UNDEF{}, TC{iri("<y>")}}};
  ExternallySpecifiedValues valuesOperation(
      testQec, {{Variable{"?x"}, Variable{"?y"}}, values}, "result-test");

  auto result = valuesOperation.getResult();
  const auto& table = result->idTable();
  Id x = ad_utility::testing::makeGetId(testQec->getIndex())("<x>");
  auto I = ad_utility::testing::IntId;
  auto l = result->localVocab().getIndexOrNullopt(
      ad_utility::triple_component::LiteralOrIri::iriref("<y>"));
  ASSERT_TRUE(l.has_value());
  auto U = Id::makeUndefined();
  ASSERT_EQ(table,
            makeIdTableFromVector(
                {{I(12), x}, {U, Id::makeFromLocalVocabIndex(l.value())}}));
}

// Test the updateValues method
TEST(ExternallySpecifiedValues, updateValues) {
  auto testQec = ad_utility::testing::getQec();
  ValuesComponents initialValues{{TC{1}, TC{2}}, {TC{3}, TC{4}}};
  ExternallySpecifiedValues externalValuesOp(
      testQec, {{Variable{"?x"}, Variable{"?y"}}, initialValues}, "update-test");

  // Check initial size
  EXPECT_EQ(externalValuesOp.getSizeEstimate(), 2u);

  // Update with new values (same variables)
  ValuesComponents newValues{{TC{10}, TC{20}}, {TC{30}, TC{40}}, {TC{50}, TC{60}}};
  parsedQuery::SparqlValues updatedSparqlValues{
      {Variable{"?x"}, Variable{"?y"}}, newValues};

  externalValuesOp.updateValues(std::move(updatedSparqlValues));

  // Check that the size changed
  EXPECT_EQ(externalValuesOp.getSizeEstimate(), 3u);
}

// Test that updateValues fails with different variables
TEST(ExternallySpecifiedValues, updateValuesFailsWithDifferentVariables) {
  auto testQec = ad_utility::testing::getQec();
  ValuesComponents initialValues{{TC{1}, TC{2}}};
  ExternallySpecifiedValues externalValuesOp(
      testQec, {{Variable{"?x"}, Variable{"?y"}}, initialValues},
      "mismatch-test");

  // Try to update with different variables - should fail
  ValuesComponents newValues{{TC{10}, TC{20}, TC{30}}};
  parsedQuery::SparqlValues wrongSparqlValues{
      {Variable{"?x"}, Variable{"?y"}, Variable{"?z"}}, newValues};

  EXPECT_DEATH(externalValuesOp.updateValues(std::move(wrongSparqlValues)), "");
}

// Test that updateValues fails with same variables but different order
TEST(ExternallySpecifiedValues, updateValuesFailsWithDifferentOrder) {
  auto testQec = ad_utility::testing::getQec();
  ValuesComponents initialValues{{TC{1}, TC{2}}};
  ExternallySpecifiedValues externalValuesOp(
      testQec, {{Variable{"?x"}, Variable{"?y"}}, initialValues}, "order-test");

  // Try to update with variables in different order - should fail
  ValuesComponents newValues{{TC{10}, TC{20}}};
  parsedQuery::SparqlValues wrongOrderSparqlValues{
      {Variable{"?y"}, Variable{"?x"}}, newValues};

  EXPECT_DEATH(externalValuesOp.updateValues(std::move(wrongOrderSparqlValues)),
               "");
}

// Test clone functionality
TEST(ExternallySpecifiedValues, clone) {
  auto testQec = ad_utility::testing::getQec("<x> <x> <x> .");
  ValuesComponents values{{TC{12}, TC{iri("<x>")}},
                          {TC::UNDEF{}, TC{iri("<y>")}}};
  ExternallySpecifiedValues valuesOperation(
      testQec, {{Variable{"?x"}, Variable{"?y"}}, values}, "clone-test");

  auto clone = valuesOperation.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(valuesOperation, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), valuesOperation.getDescriptor());

  // Check that the cloned operation is also an ExternallySpecifiedValues
  auto* clonedExternal = dynamic_cast<ExternallySpecifiedValues*>(clone.get());
  ASSERT_NE(clonedExternal, nullptr);
  EXPECT_EQ(clonedExternal->getIdentifier(), "clone-test");
}

// Test getExternalValues functionality
TEST(ExternallySpecifiedValues, getExternalValues) {
  auto testQec = ad_utility::testing::getQec();
  ValuesComponents values{{TC{1}, TC{2}}};
  ExternallySpecifiedValues externalValuesOp(
      testQec, {{Variable{"?x"}, Variable{"?y"}}, values}, "collect-test");

  std::vector<ExternallySpecifiedValues*> collected;
  externalValuesOp.getExternalValues(collected);

  ASSERT_EQ(collected.size(), 1u);
  EXPECT_EQ(collected[0], &externalValuesOp);
  EXPECT_EQ(collected[0]->getIdentifier(), "collect-test");
}

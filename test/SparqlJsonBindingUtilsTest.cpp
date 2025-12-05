// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/IndexTestHelpers.h"
#include "util/SparqlJsonBindingUtils.h"

using namespace sparqlJsonBindingUtils;

// Fixture that sets up a test index for testing.
class SparqlJsonBindingUtilsTest : public ::testing::Test {
 protected:
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
};

// Test parsing a URI binding.
TEST_F(SparqlJsonBindingUtilsTest, parseUri) {
  nlohmann::json binding = {{"type", "uri"},
                            {"value", "http://example.org/foo"}};

  ad_utility::HashMap<std::string, Id> blankNodeMap;
  LocalVocab localVocab;

  TripleComponent tc = bindingToTripleComponent(
      binding, testQec->getIndex(), blankNodeMap, &localVocab,
      testQec->getIndex().getBlankNodeManager());

  ASSERT_TRUE(tc.isIri());
  EXPECT_EQ(tc.getIri().toStringRepresentation(), "<http://example.org/foo>");
}

// Test parsing a plain literal binding.
TEST_F(SparqlJsonBindingUtilsTest, parsePlainLiteral) {
  nlohmann::json binding = {{"type", "literal"}, {"value", "doof"}};

  ad_utility::HashMap<std::string, Id> blankNodeMap;
  LocalVocab localVocab;

  TripleComponent tc = bindingToTripleComponent(
      binding, testQec->getIndex(), blankNodeMap, &localVocab,
      testQec->getIndex().getBlankNodeManager());

  ASSERT_TRUE(tc.isLiteral());
  EXPECT_EQ(tc.getLiteral().toStringRepresentation(), "\"doof\"");
}

// Test parsing a language-tagged literal binding.
TEST_F(SparqlJsonBindingUtilsTest, parseLanguageTaggedLiteral) {
  nlohmann::json binding = {
      {"type", "literal"}, {"value", "doof"}, {"xml:lang", "en"}};

  ad_utility::HashMap<std::string, Id> blankNodeMap;
  LocalVocab localVocab;

  TripleComponent tc = bindingToTripleComponent(
      binding, testQec->getIndex(), blankNodeMap, &localVocab,
      testQec->getIndex().getBlankNodeManager());

  ASSERT_TRUE(tc.isLiteral());
  EXPECT_EQ(tc.getLiteral().toStringRepresentation(), "\"doof\"@en");
}

// Test parsing a typed literal binding (integer).
TEST_F(SparqlJsonBindingUtilsTest, parseTypedLiteralInteger) {
  nlohmann::json binding = {
      {"type", "literal"},
      {"value", "42"},
      {"datatype", "http://www.w3.org/2001/XMLSchema#int"}};

  ad_utility::HashMap<std::string, Id> blankNodeMap;
  LocalVocab localVocab;

  TripleComponent tc = bindingToTripleComponent(
      binding, testQec->getIndex(), blankNodeMap, &localVocab,
      testQec->getIndex().getBlankNodeManager());

  ASSERT_TRUE(tc.isInt());
  EXPECT_EQ(tc.getInt(), 42);
}

// Test parsing a typed literal binding using the deprecated "typed-literal"
// type (used by Virtuoso).
TEST_F(SparqlJsonBindingUtilsTest, parseTypedLiteralDeprecated) {
  nlohmann::json binding = {
      {"type", "typed-literal"},
      {"value", "3.14"},
      {"datatype", "http://www.w3.org/2001/XMLSchema#double"}};

  ad_utility::HashMap<std::string, Id> blankNodeMap;
  LocalVocab localVocab;

  TripleComponent tc = bindingToTripleComponent(
      binding, testQec->getIndex(), blankNodeMap, &localVocab,
      testQec->getIndex().getBlankNodeManager());

  ASSERT_TRUE(tc.isDouble());
  EXPECT_DOUBLE_EQ(tc.getDouble(), 3.14);
}

// Test parsing a blank node binding.
TEST_F(SparqlJsonBindingUtilsTest, parseBlankNode) {
  nlohmann::json binding1 = {{"type", "bnode"}, {"value", "b0"}};
  nlohmann::json binding2 = {{"type", "bnode"}, {"value", "b1"}};
  nlohmann::json binding3 = {{"type", "bnode"}, {"value", "b0"}};  // Same as b0

  ad_utility::HashMap<std::string, Id> blankNodeMap;
  LocalVocab localVocab;

  TripleComponent tc1 = bindingToTripleComponent(
      binding1, testQec->getIndex(), blankNodeMap, &localVocab,
      testQec->getIndex().getBlankNodeManager());

  TripleComponent tc2 = bindingToTripleComponent(
      binding2, testQec->getIndex(), blankNodeMap, &localVocab,
      testQec->getIndex().getBlankNodeManager());

  TripleComponent tc3 = bindingToTripleComponent(
      binding3, testQec->getIndex(), blankNodeMap, &localVocab,
      testQec->getIndex().getBlankNodeManager());

  // For blank nodes, the TripleComponent is constructed from an Id directly.
  // tc1 and tc3 should be the same (same blank node label)
  ASSERT_TRUE(tc1.isId());
  ASSERT_TRUE(tc3.isId());
  EXPECT_EQ(tc1.getId(), tc3.getId());

  // tc2 should be different
  ASSERT_TRUE(tc2.isId());
  EXPECT_NE(tc1.getId(), tc2.getId());
}

// Test that missing "type" field throws an error.
TEST_F(SparqlJsonBindingUtilsTest, missingTypeField) {
  nlohmann::json binding = {{"value", "http://example.org/doof"}};

  ad_utility::HashMap<std::string, Id> blankNodeMap;
  LocalVocab localVocab;

  EXPECT_THROW(bindingToTripleComponent(
                   binding, testQec->getIndex(), blankNodeMap, &localVocab,
                   testQec->getIndex().getBlankNodeManager()),
               std::runtime_error);
}

// Test that missing "value" field throws an error.
TEST_F(SparqlJsonBindingUtilsTest, missingValueField) {
  nlohmann::json binding = {{"type", "uri"}};

  ad_utility::HashMap<std::string, Id> blankNodeMap;
  LocalVocab localVocab;

  EXPECT_THROW(bindingToTripleComponent(
                   binding, testQec->getIndex(), blankNodeMap, &localVocab,
                   testQec->getIndex().getBlankNodeManager()),
               std::runtime_error);
}

// Test that unknown type throws an error.
TEST_F(SparqlJsonBindingUtilsTest, unknownType) {
  nlohmann::json binding = {{"type", "unknown"},
                            {"value", "http://example.org/foo"}};

  ad_utility::HashMap<std::string, Id> blankNodeMap;
  LocalVocab localVocab;

  EXPECT_THROW(bindingToTripleComponent(
                   binding, testQec->getIndex(), blankNodeMap, &localVocab,
                   testQec->getIndex().getBlankNodeManager()),
               std::runtime_error);
}

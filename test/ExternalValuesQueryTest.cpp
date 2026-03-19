//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Generated with Claude Code

#include <gtest/gtest.h>

#include "parser/ExternalValuesQuery.h"
#include "parser/SparqlTriple.h"

using parsedQuery::ExternalValuesException;
using parsedQuery::ExternalValuesQuery;

// Helper to create a triple with an IRI predicate.
static SparqlTriple makeTriple(std::string_view predIri,
                               TripleComponent object) {
  TripleComponent subject = TripleComponent::UNDEF{};
  auto predicate =
      PropertyPath::fromIri(TripleComponent::Iri::fromIriref(predIri));
  return {subject, predicate, std::move(object)};
}

// Test addParameter with <identifier>.
TEST(ExternalValuesQuery, addParameterIdentifier) {
  ExternalValuesQuery query;
  auto triple = makeTriple(
      "<identifier>",
      TripleComponent::Literal::fromStringRepresentation("\"myId\""));
  query.addParameter(triple);
  EXPECT_EQ(query.identifier_, "myId");
}

// Test that setting <identifier> twice throws.
TEST(ExternalValuesQuery, addParameterIdentifierTwice) {
  ExternalValuesQuery query;
  auto triple = makeTriple(
      "<identifier>",
      TripleComponent::Literal::fromStringRepresentation("\"myId\""));
  query.addParameter(triple);
  EXPECT_THROW(query.addParameter(triple), ExternalValuesException);
}

// Test that <identifier> with a non-literal throws.
TEST(ExternalValuesQuery, addParameterIdentifierNonLiteral) {
  ExternalValuesQuery query;
  auto triple = makeTriple("<identifier>", Variable{"?x"});
  EXPECT_THROW(query.addParameter(triple), ExternalValuesException);
}

// Test that <identifier> with an empty string throws.
TEST(ExternalValuesQuery, addParameterIdentifierEmpty) {
  ExternalValuesQuery query;
  auto triple =
      makeTriple("<identifier>",
                 TripleComponent::Literal::fromStringRepresentation("\"\""));
  EXPECT_THROW(query.addParameter(triple), ExternalValuesException);
}

// Test addParameter with <variables>.
TEST(ExternalValuesQuery, addParameterVariables) {
  ExternalValuesQuery query;
  query.addParameter(makeTriple("<variables>", Variable{"?x"}));

  ASSERT_EQ(query.variables_.size(), 1u);
  EXPECT_EQ(query.variables_[0], Variable{"?x"});
}

// Test addParameter with multiple variables.
TEST(ExternalValuesQuery, addParameterMultipleVariables) {
  ExternalValuesQuery query;
  query.addParameter(makeTriple("<variables>", Variable{"?x"}));
  query.addParameter(makeTriple("<variables>", Variable{"?y"}));
  query.addParameter(makeTriple("<variables>", Variable{"?z"}));

  ASSERT_EQ(query.variables_.size(), 3u);
  EXPECT_EQ(query.variables_[0], Variable{"?x"});
  EXPECT_EQ(query.variables_[1], Variable{"?y"});
  EXPECT_EQ(query.variables_[2], Variable{"?z"});
}

// Test addParameter with non-variable object for <variables> throws.
TEST(ExternalValuesQuery, addParameterVariablesNonVariable) {
  ExternalValuesQuery query;
  auto triple = makeTriple(
      "<variables>", TripleComponent::Iri::fromIriref("<http://example.com>"));
  EXPECT_THROW(query.addParameter(triple), ExternalValuesException);
}

// Test addParameter with unknown predicate throws.
TEST(ExternalValuesQuery, addParameterUnknownPredicate) {
  ExternalValuesQuery query;
  auto triple = makeTriple("<unknown>", Variable{"?x"});
  EXPECT_THROW(query.addParameter(triple), ExternalValuesException);
}

// Test validate succeeds with identifier and variables set.
TEST(ExternalValuesQuery, validateSuccess) {
  ExternalValuesQuery query;
  query.addParameter(makeTriple(
      "<identifier>",
      TripleComponent::Literal::fromStringRepresentation("\"myId\"")));
  query.addParameter(makeTriple("<variables>", Variable{"?x"}));
  EXPECT_NO_THROW(query.validate());
}

// Test validate fails without identifier.
TEST(ExternalValuesQuery, validateMissingIdentifier) {
  ExternalValuesQuery query;
  query.addParameter(makeTriple("<variables>", Variable{"?x"}));
  EXPECT_THROW(query.validate(), ExternalValuesException);
}

// Test validate fails without variables.
TEST(ExternalValuesQuery, validateMissingVariables) {
  ExternalValuesQuery query;
  query.addParameter(makeTriple(
      "<identifier>",
      TripleComponent::Literal::fromStringRepresentation("\"myId\"")));
  EXPECT_THROW(query.validate(), ExternalValuesException);
}

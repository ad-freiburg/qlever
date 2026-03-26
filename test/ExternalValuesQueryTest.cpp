// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "./util/TripleComponentTestHelpers.h"
#include "parser/ExternalValuesQuery.h"
#include "parser/MagicServiceIriConstants.h"
#include "parser/SparqlTriple.h"
#include "util/GTestHelpers.h"

namespace {
using parsedQuery::ExternalValuesException;
using parsedQuery::ExternalValuesQuery;
using namespace ::testing;
using V = Variable;

auto lit = ad_utility::testing::tripleComponentLiteral;
auto iri = ad_utility::testing::iri;
using ::testing::HasSubstr;

// Helper to create a triple with an IRI predicate.
static SparqlTriple makeTriple(std::string_view predIri,
                               TripleComponent object) {
  TripleComponent subject = TripleComponent::UNDEF{};
  auto predicate =
      PropertyPath::fromIri(TripleComponent::Iri::fromIriref(predIri));
  return {subject, predicate, std::move(object)};
}

// Create a triple with the predicate `<identifier>`.
static SparqlTriple idTriple(TripleComponent object) {
  return makeTriple("<identifier>", std::move(object));
}

// Create a triple with the predicate `<variables>`.
static SparqlTriple varTriple(TripleComponent object) {
  return makeTriple("<variables>", std::move(object));
}

struct ExternalValuesQueryTest : public ::testing::Test {
 protected:
  ExternalValuesQuery query;
  SparqlTriple defaultIdTriple = idTriple(lit("myId"));
  SparqlTriple defaultVarTriple = varTriple(V{"?x"});
};

// _____________________________________________________________________________
TEST_F(ExternalValuesQueryTest, name) {
  EXPECT_EQ(query.name(), "external values query");
}

// Test addParameter with <identifier>.
TEST_F(ExternalValuesQueryTest, addParameterIdentifier) {
  query.addParameter(defaultIdTriple);
  EXPECT_EQ(query.identifier_, "myId");
}

// Test that setting <identifier> twice throws.
TEST_F(ExternalValuesQueryTest, addParameterIdentifierTwice) {
  query.addParameter(defaultIdTriple);
  EXPECT_THROW(query.addParameter(defaultIdTriple), ExternalValuesException);
}

// Test that <identifier> with a non-literal throws.
TEST_F(ExternalValuesQueryTest, addParameterIdentifierNonLiteral) {
  EXPECT_THROW(query.addParameter(makeTriple("<identifier>", V{"?x"})),
               ExternalValuesException);
}

// Test that <identifier> with an empty string throws.
TEST_F(ExternalValuesQueryTest, addParameterIdentifierEmpty) {
  EXPECT_THROW(query.addParameter(idTriple(lit(""))), ExternalValuesException);
}

// Test addParameter with <variables>.
TEST_F(ExternalValuesQueryTest, addParameterVariables) {
  query.addParameter(defaultVarTriple);
  EXPECT_THAT(query.variables_, ElementsAre(V{"?x"}));
}

// Test addParameter with multiple variables.
TEST_F(ExternalValuesQueryTest, addParameterMultipleVariables) {
  query.addParameter(varTriple(V{"?x"}));
  query.addParameter(varTriple(V{"?y"}));
  query.addParameter(varTriple(V{"?z"}));
  EXPECT_THAT(query.variables_, ElementsAre(V{"?x"}, V{"?y"}, V{"?z"}));
}

// Test addParameter with non-variable object for <variables> throws.
TEST_F(ExternalValuesQueryTest, addParameterVariablesNonVariable) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      query.addParameter(varTriple(iri("<http://example.com>"))),
      HasSubstr("expects a variable"));
}

// Test addParameter with unknown predicate throws.
TEST(ExternalValuesQuery, addParameterUnknownPredicatemeter) {
  ExternalValuesQuery query;
  auto triple = makeTriple("<unknown>", Variable{"?x"});
  AD_EXPECT_THROW_WITH_MESSAGE(query.addParameter(triple),
                               HasSubstr("Unknown parameter"));
}

// Test validate succeeds with identifier and variables set.
TEST_F(ExternalValuesQueryTest, validateSuccess) {
  query.addParameter(defaultIdTriple);
  query.addParameter(defaultVarTriple);
  EXPECT_NO_THROW(query.validate());
}

// Test validate fails without identifier.
TEST_F(ExternalValuesQueryTest, validateMissingIdentifier) {
  query.addParameter(defaultVarTriple);
  AD_EXPECT_THROW_WITH_MESSAGE(query.validate(),
                               HasSubstr("requires an <identifier>"));
}

// Test validate fails without variables.
TEST_F(ExternalValuesQueryTest, validateMissingVariables) {
  query.addParameter(defaultIdTriple);
  AD_EXPECT_THROW_WITH_MESSAGE(
      query.validate(),
      HasSubstr("requires at least one <variables> parameter"));
}

// Test the (deprecated) specification of the identifier via the IRI directly.
TEST_F(ExternalValuesQueryTest, deprecatedIdentifierSpecification) {
  query = ExternalValuesQuery{iri(EXTERNAL_VALUES_IRI)};
  EXPECT_TRUE(query.identifier_.empty());
  AD_EXPECT_THROW_WITH_MESSAGE(ExternalValuesQuery(iri("<invalidServiceIri>")),
                               ::testing::HasSubstr("unexpected SERVICE IRI"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ExternalValuesQuery(iri(absl::StrCat(EXTERNAL_VALUES_IRI_PREFIX, ">"))),
      ::testing::HasSubstr("must not be empty"));
  ExternalValuesQuery query2(
      iri(absl::StrCat(EXTERNAL_VALUES_IRI_PREFIX, "blubb>")));
  EXPECT_EQ(query2.identifier_, "blubb");
}
}  // namespace

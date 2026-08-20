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

// Create a triple with the predicate `<name>`.
static SparqlTriple idTriple(TripleComponent object) {
  return makeTriple("<name>", std::move(object));
}

// Create a triple with the predicate `<variable>`.
static SparqlTriple varTriple(TripleComponent object) {
  return makeTriple("<variable>", std::move(object));
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

// Test addParameter with <name>.
TEST_F(ExternalValuesQueryTest, addParameterName) {
  query.addParameter(defaultIdTriple);
  EXPECT_EQ(query.name_, "myId");
}

// Test that setting <name> twice throws.
TEST_F(ExternalValuesQueryTest, addParameterNameTwice) {
  query.addParameter(defaultIdTriple);
  EXPECT_THROW(query.addParameter(defaultIdTriple), ExternalValuesException);
}

// Test that <name> with a non-literal throws.
TEST_F(ExternalValuesQueryTest, addParameterNameNonLiteral) {
  EXPECT_THROW(query.addParameter(makeTriple("<name>", V{"?x"})),
               ExternalValuesException);
}

// Test that <name> with an empty string throws.
TEST_F(ExternalValuesQueryTest, addParameterNameEmpty) {
  EXPECT_THROW(query.addParameter(idTriple(lit(""))), ExternalValuesException);
}

// Test addParameter with <variable>.
TEST_F(ExternalValuesQueryTest, addParameterVariable) {
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

// Test addParameter with non-variable object for <variable> throws.
TEST_F(ExternalValuesQueryTest, addParameterVariableNonVariable) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      query.addParameter(varTriple(iri("<http://example.com>"))),
      HasSubstr("expects a variable"));
}

// Test addParameter with unknown predicate throws.
TEST(ExternalValuesQuery, addParameterUnknownPredicate) {
  ExternalValuesQuery query;
  auto triple = makeTriple("<unknown>", Variable{"?x"});
  AD_EXPECT_THROW_WITH_MESSAGE(query.addParameter(triple),
                               HasSubstr("Unknown parameter"));
}

// Test validate succeeds with name and variables set.
TEST_F(ExternalValuesQueryTest, validateSuccess) {
  query.addParameter(defaultIdTriple);
  query.addParameter(defaultVarTriple);
  EXPECT_NO_THROW(query.validate());
}

// Test validate fails without name.
TEST_F(ExternalValuesQueryTest, validateMissingName) {
  query.addParameter(defaultVarTriple);
  AD_EXPECT_THROW_WITH_MESSAGE(query.validate(),
                               HasSubstr("requires a <name>"));
}

// Test validate fails without variables.
TEST_F(ExternalValuesQueryTest, validateMissingVariables) {
  query.addParameter(defaultIdTriple);
  AD_EXPECT_THROW_WITH_MESSAGE(
      query.validate(),
      HasSubstr("requires at least one <variable> parameter"));
}

// Test the (deprecated) specification of the name via the IRI directly.
TEST_F(ExternalValuesQueryTest, deprecatedNameSpecification) {
  query = ExternalValuesQuery{iri(EXTERNAL_VALUES_IRI)};
  EXPECT_TRUE(query.name_.empty());
  AD_EXPECT_THROW_WITH_MESSAGE(ExternalValuesQuery(iri("<invalidServiceIri>")),
                               ::testing::HasSubstr("unexpected SERVICE IRI"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ExternalValuesQuery(iri(absl::StrCat(EXTERNAL_VALUES_IRI_PREFIX, ">"))),
      ::testing::HasSubstr("must not be empty"));
  ExternalValuesQuery query2(
      iri(absl::StrCat(EXTERNAL_VALUES_IRI_PREFIX, "blubb>")));
  EXPECT_EQ(query2.name_, "blubb");
}
}  // namespace

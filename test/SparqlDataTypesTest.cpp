// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gmock/gmock.h>

#include "engine/ConstructTripleInstantiator.h"
#include "parser/data/Types.h"

TEST(SparqlDataTypesTest, BlankNodeInvalidLabelsThrowException) {
  EXPECT_THROW(BlankNode(false, ""), ad_utility::Exception);
  EXPECT_THROW(BlankNode(false, "label with spaces"), ad_utility::Exception);
  EXPECT_THROW(BlankNode(false, "trailing-dash-"), ad_utility::Exception);
  EXPECT_THROW(BlankNode(false, "-leading-dash"), ad_utility::Exception);
  EXPECT_THROW(BlankNode(false, "trailing.dots."), ad_utility::Exception);
  EXPECT_THROW(BlankNode(false, ".leading.dots"), ad_utility::Exception);
}

TEST(SparqlDataTypesTest, BlankNodeGeneratesCorrectRdfTerm) {
  using namespace qlever::constructExport;
  BatchEvaluationResult unused{};

  auto instantiate = [&](const PrecomputedBlankNode& node, size_t rowId) {
    return *ConstructTripleInstantiator::instantiateTerm(PreprocessedTerm{node},
                                                         unused, 0, rowId)
                .value();
  };

  PrecomputedBlankNode nodeA{"_:u", "_a"};
  EXPECT_EQ(instantiate(nodeA, 0), "_:u0_a");
  EXPECT_EQ(instantiate(nodeA, 10), "_:u10_a");
  EXPECT_EQ(instantiate(nodeA, 12), "_:u12_a");

  PrecomputedBlankNode nodeB{"_:g", "_b"};
  EXPECT_EQ(instantiate(nodeB, 0), "_:g0_b");
}

TEST(SparqlDataTypesTest, IriInvalidSyntaxThrowsException) {
  EXPECT_THROW(Iri{"http://linkwithoutangularbrackets"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<<nestedangularbrackets>>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<duplicatedangularbracker>>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<<duplicatedangularbracker>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<noend"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"nostart>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<\"withdoublequote>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<{withcurlybrace>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<}withcurlybrace>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<|withpipesymbol>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<^withcaret>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<\\withbackslash>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<`withbacktick>"}, ad_utility::Exception);
  // U+0000 (NULL) to U+0020 (Space) are all forbidden characters
  // but the following two are probably the most common cases
  EXPECT_THROW(Iri{"<with whitespace>"}, ad_utility::Exception);
  EXPECT_THROW(Iri{"<with\r\nnewline>"}, ad_utility::Exception);
}

TEST(SparqlDataTypesTest, IriValidIriIsPreserved) {
  ASSERT_EQ(Iri{"<http://valid-iri>"}.iri(), "<http://valid-iri>");
}

TEST(SparqlDataTypesTest, LiteralBooleanIsCorrectlyFormatted) {
  EXPECT_EQ(Literal{"true"}.literal(), "true");
  EXPECT_EQ(Literal{false}.literal(), "false");
}

TEST(SparqlDataTypesTest, LiteralStringIsCorrectlyFormatted) {
  EXPECT_EQ(Literal{"abcdef"}.literal(), "abcdef");
  EXPECT_EQ(Literal{"\U0001f937\U0001f3fc\u200d\u2642\ufe0f"}.literal(),
            "🤷🏼‍♂️");
}

TEST(SparqlDataTypesTest, LiteralNumberIsCorrectlyFormatted) {
  EXPECT_EQ(Literal{1234567890}.literal(), "1234567890");
  EXPECT_EQ(Literal{-1337}.literal(), "-1337");
  EXPECT_EQ(Literal{1.3}.literal(), "1.3");
}

TEST(SparqlDataTypesTest, VariableNormalizesDollarSign) {
  Variable varWithQuestionMark{"?abc"};
  EXPECT_EQ(varWithQuestionMark.name(), "?abc");

  Variable varWithDollarSign{"$abc"};
  EXPECT_EQ(varWithQuestionMark.name(), "?abc");
}

TEST(SparqlDataTypesTest, VariableInvalidNamesThrowException) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP()
        << "validity of variable names is only checked with expensive checks";
  }
  EXPECT_THROW(Variable("no_leading_var_or_dollar", true),
               ad_utility::Exception);
  EXPECT_THROW(Variable("", true), ad_utility::Exception);
  EXPECT_THROW(Variable("? var with space", true), ad_utility::Exception);
  EXPECT_THROW(Variable("?", true), ad_utility::Exception);
  EXPECT_THROW(Variable("$", true), ad_utility::Exception);
}

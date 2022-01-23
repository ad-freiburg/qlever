// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/parser/data/BlankNode.h"
#include "../src/parser/data/Iri.h"
#include "../src/parser/data/Literal.h"
#include "../src/parser/data/Variable.h"

TEST(SparqlDataTypesTest, BlankNodeInvalidLabelsThrowException) {
  EXPECT_THROW(BlankNode(false, ""), ad_semsearch::Exception);
  EXPECT_THROW(BlankNode(false, "label with spaces"), ad_semsearch::Exception);
  EXPECT_THROW(BlankNode(false, "trailing-dash-"), ad_semsearch::Exception);
  EXPECT_THROW(BlankNode(false, "-leading-dash"), ad_semsearch::Exception);
  EXPECT_THROW(BlankNode(false, "trailing.dots."), ad_semsearch::Exception);
  EXPECT_THROW(BlankNode(false, ".leading.dots"), ad_semsearch::Exception);
}

TEST(SparqlDataTypesTest, IriInvalidSyntaxThrowsException) {
  EXPECT_THROW(Iri{"http://linkwithoutangularbrackets"},
               ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<<nestedangularbrackets>>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<duplicatedangularbracker>>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<<duplicatedangularbracker>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<noend"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"nostart>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<\"withdoublequote>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<{withcurlybrace>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<}withcurlybrace>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<|withpipesymbol>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<^withcaret>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<\\withbackslash>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<`withbacktick>"}, ad_semsearch::Exception);
  // U+0000 (NULL) to U+0020 (Space) are all forbidden characters
  // but the following two are probably the most common cases
  EXPECT_THROW(Iri{"<with whitespace>"}, ad_semsearch::Exception);
  EXPECT_THROW(Iri{"<with\r\nnewline>"}, ad_semsearch::Exception);
}

TEST(SparqlDataTypesTest, IriValidIriIsPreserved) {
  ASSERT_EQ(Iri{"<http://valid-iri>"}.iri(), "<http://valid-iri>");
}

TEST(SparqlDataTypesTest, LiteralBooleanIsCorrectlyFormatted) {
  EXPECT_EQ(Literal{true}.literal(), "true");
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
  EXPECT_THROW(Variable{"no_leading_var_or_dollar"}, ad_semsearch::Exception);
  EXPECT_THROW(Variable{""}, ad_semsearch::Exception);
  EXPECT_THROW(Variable{"? var with space"}, ad_semsearch::Exception);
  EXPECT_THROW(Variable{"?"}, ad_semsearch::Exception);
  EXPECT_THROW(Variable{"$"}, ad_semsearch::Exception);
}

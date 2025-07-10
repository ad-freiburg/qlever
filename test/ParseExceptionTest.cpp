//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "SparqlAntlrParserTestHelpers.h"
#include "parser/SparqlParser.h"
#include "util/ParseException.h"
#include "util/SourceLocation.h"

// _____________________________________________________________________________
TEST(ParseException, coloredError) {
  auto exampleQuery = "SELECT A ?var WHERE";
  EXPECT_EQ((ExceptionMetadata{exampleQuery, 7, 7, 1, 7}).coloredError(),
            "SELECT \x1B[1m\x1B[4m\x1B[31mA\x1B[0m ?var WHERE");
  EXPECT_EQ((ExceptionMetadata{exampleQuery, 9, 12, 1, 9}).coloredError(),
            "SELECT A \x1B[1m\x1B[4m\x1B[31m?var\x1B[0m WHERE");
  // Start index is greater than stop index.
  EXPECT_ANY_THROW(
      (ExceptionMetadata{exampleQuery, 8, 6, 1, 3}).coloredError());
}

// _____________________________________________________________________________
TEST(ParseException, illegalConstructorArguments) {
  auto exampleQuery = "SELECT A ?var WHERE";
  // Start index is greater than stop index.
  EXPECT_ANY_THROW((ParseException{
      "illegal query", ExceptionMetadata{exampleQuery, 8, 6, 1, 3}}));
}

// _____________________________________________________________________________
void expectParseExceptionWithMetadata(
    const std::string& input, const std::optional<ExceptionMetadata>& metadata,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto trace = generateLocationTrace(l);
  try {
    SparqlParser::parseQuery(input);
    FAIL();  // Should be unreachable.
  } catch (const ParseException& e) {
    // The constructor has to be bracketed because EXPECT_EQ is a macro.
    EXPECT_EQ(e.metadata(), metadata);
  }
}

// _____________________________________________________________________________
TEST(ParseException, MetadataGeneration) {
  // A is not a valid argument for select.
  expectParseExceptionWithMetadata(
      "SELECT A ?a WHERE { ?a ?b ?c }",
      {{"SELECT A ?a WHERE { ?a ?b ?c }", 7, 7, 1, 7}});
  // Error is the undefined Prefix "a".
  expectParseExceptionWithMetadata(
      "SELECT * WHERE { ?a a:b ?b }",
      {{"SELECT * WHERE { ?a a:b ?b }", 20, 22, 1, 20}});
  // "%" doesn't match any valid token. So in this case we will get an Error
  // from the Lexer.
  expectParseExceptionWithMetadata("SELECT * WHERE { % }",
                                   {{"SELECT * WHERE { % }", 17, 17, 1, 17}});
  // Error is the undefined Prefix "f".
  expectParseExceptionWithMetadata(
      "SELECT * WHERE {\n ?a ?b ?c . \n f:d ?d ?e\n}",
      {{"SELECT * WHERE {\n ?a ?b ?c . \n f:d ?d ?e\n}", 31, 33, 3, 1}});
}

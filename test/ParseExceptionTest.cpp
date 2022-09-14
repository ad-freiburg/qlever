//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "SparqlAntlrParserTestHelpers.h"
#include "parser/ParseException.h"
#include "parser/SparqlParser.h"

TEST(ParseException, coloredError) {
  auto exampleQuery = "SELECT A ?var WHERE";
  EXPECT_EQ((ExceptionMetadata{exampleQuery, 7, 7, 1, 7}).coloredError(),
            "SELECT \x1B[1m\x1B[4m\x1B[31mA\x1B[0m ?var WHERE");
  EXPECT_EQ((ExceptionMetadata{exampleQuery, 9, 12, 1, 9}).coloredError(),
            "SELECT A \x1B[1m\x1B[4m\x1B[31m?var\x1B[0m WHERE");
}

void expectParseExceptionWithMetadata(
    const string& input, const std::optional<ExceptionMetadata>& metadata) {
  try {
    SparqlParser(input).parse();
    FAIL();  // Should be unreachable.
  } catch (const ParseException& e) {
    // The constructor has to be bracketed because EXPECT_EQ is a macro.
    EXPECT_EQ(e.metadata(), metadata);
  }
}

TEST(ParseException, MetadataGeneration) {
  // The SparqlLexer changes the input that has already been read into a token
  // when it is outputted with getUnconsumedInput (which is used for ANtLR).
  // A is not a valid argument for select.
  expectParseExceptionWithMetadata(
      "SELECT A ?a WHERE { ?a ?b ?c }",
      {{"select   A ?a WHERE { ?a ?b ?c }", 9, 9, 1, 9}});
  // The ANTLR Parser currently doesn't always have the whole query.
  // Error is the undefined Prefix "a".
  expectParseExceptionWithMetadata(
      "SELECT * WHERE { ?a a:b ?b }",
      {{"select   * WHERE { ?a a:b ?b }", 22, 24, 1, 22}});
  // "%" doesn't match any valid token. So in this case we will get an Error
  // from the Lexer.
  expectParseExceptionWithMetadata("SELECT * WHERE { % }",
                                   {{"select   * WHERE { % }", 19, 19, 1, 19}});
  // Error is the undefined Prefix "f".
  expectParseExceptionWithMetadata(
      "SELECT * WHERE {\n ?a ?b ?c . \n f:d ?d ?e\n}",
      {{"select   * WHERE {\n ?a ?b ?c . \n f:d ?d ?e\n}", 33, 35, 3, 1}});
}

//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include <gtest/gtest.h>
#include <parser/ParseException.h>
#include <parser/SparqlParser.h>

TEST(ParseException, coloredError) {
  auto exampleQuery = "SELECT A ?var WHERE";
  EXPECT_EQ((ExceptionMetadata{exampleQuery, 7, 7}).coloredError(),
            "SELECT \x1B[1m\x1B[4m\x1B[31mA\x1B[0m ?var WHERE");
  EXPECT_EQ((ExceptionMetadata{exampleQuery, 9, 12}).coloredError(),
            "SELECT A \x1B[1m\x1B[4m\x1B[31m?var\x1B[0m WHERE");
}

TEST(ParseException, MetadataGeneration) {
  // The SparqlLexer changes the input that has already been read into a token
  // when it is outputted with getUnconsumedInput (which is used for ANtLR).
  {
    try {
      SparqlParser("SELECT A ?a WHERE { ?a ?b ?c }").parse();
    } catch (const ParseException& e) {
      EXPECT_TRUE(e.metadata().has_value());
      EXPECT_EQ(e.metadata().value(),
                (ExceptionMetadata{"select   A ?a WHERE { ?a ?b ?c }", 9, 9}));
    }
  }
  // The ANTLR Parser currently doesn't always have the whole query.
  {
    try {
      SparqlParser("SELECT * WHERE { ?a a:b ?b }").parse();
    } catch (const ParseException& e) {
      EXPECT_TRUE(e.metadata().has_value());
      EXPECT_EQ(e.metadata().value(),
                (ExceptionMetadata{"?a  a:b ?b }", 4, 6}));
    }
  }
}

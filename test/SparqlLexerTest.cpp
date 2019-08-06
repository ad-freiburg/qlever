// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include <gtest/gtest.h>
#include "../src/parser/SparqlLexer.h"

#include <re2/re2.h>

TEST(SparqlLexerTest, basicTest) {
  //  ASSERT_TRUE(
  //      re2::RE2::FullMatch("<is-a>/(<a>|<b>)*+0^",
  //      "(((<[^<>\"{}|^`\\[\\]\\-\\x00-\\x20]*>)|[?*+/|()^0-9])+)"));

  //    ASSERT_TRUE(
  //        re2::RE2::FullMatch("<is-a>/(<a>|<b>)*",
  //        "(((<[^<>\"{}|^`\\[\\]\x00-\\x20]*>)|[?*+/|()^0-9])+)"));

  std::string query =
      ""
      "PREFIX wd: <http://www.wikidata.org/entity/>"
      "SELECT ?a ?b (COUNT(?c) as ?count) WHERE {"
      "  ?a wd:test ?b ."
      "  OPTIONAL {"
      "  { ?a <is-a> ?b } UNION{?a <is-b> ?b}"
      "  FILTER langMatches(lang(?rname), \"en\")"
      "}"
      "}"
      "ORDER BY ASC(?a) DESC(?b)"
      "GROUP BY(?a)";

  SparqlLexer lexer(query);
  lexer.expect("prefix");
  lexer.expect("wd:");
  lexer.expect("<http://www.wikidata.org/entity/>");

  lexer.expect("select");
  lexer.expect("?a");
  lexer.expect("?b");
  lexer.expect("(");
  lexer.expect("count");
  lexer.expect("(");
  lexer.expect("?c");
  lexer.expect(")");
  lexer.expect("as");
  lexer.expect("?count");
  lexer.expect(")");
  lexer.expect("where");
  lexer.expect("{");

  lexer.expect("?a");
  lexer.expect("wd:test");
  lexer.expect("?b");
  lexer.expect(".");

  lexer.expect("optional");
  lexer.expect("{");

  lexer.expect("{");
  lexer.expect("?a");
  lexer.expect("<is-a>");
  lexer.expect("?b");
  lexer.expect("}");
  lexer.expect("union");
  lexer.expect("{");
  lexer.expect("?a");
  lexer.expect("<is-b>");
  lexer.expect("?b");
  lexer.expect("}");

  lexer.expect("filter");
  lexer.expect("langmatches");
  lexer.expect("(");
  lexer.expect("lang");
  lexer.expect("(");
  lexer.expect("?rname");
  lexer.expect(")");
  lexer.expect(",");
  lexer.expect("\"en\"");
  lexer.expect(")");

  lexer.expect("}");

  lexer.expect("}");

  lexer.expect("order");
  lexer.expect("by");
  lexer.expect("asc");
  lexer.expect("(");
  lexer.expect("?a");
  lexer.expect(")");
  lexer.expect("desc");
  lexer.expect("(");
  lexer.expect("?b");
  lexer.expect(")");

  lexer.expect("group");
  lexer.expect("by");
  lexer.expect("(");
  lexer.expect("?a");
  lexer.expect(")");

  SparqlLexer lexer2(query);
  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::IRI);
  lexer2.expect(SparqlToken::Type::IRI);

  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::AGGREGATE);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::SYMBOL);

  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::IRI);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::SYMBOL);

  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::SYMBOL);

  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::IRI);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::IRI);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::SYMBOL);

  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::RDFLITERAL);
  lexer2.expect(SparqlToken::Type::SYMBOL);

  lexer2.expect(SparqlToken::Type::SYMBOL);

  lexer2.expect(SparqlToken::Type::SYMBOL);

  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::SYMBOL);

  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::KEYWORD);
  lexer2.expect(SparqlToken::Type::SYMBOL);
  lexer2.expect(SparqlToken::Type::VARIABLE);
  lexer2.expect(SparqlToken::Type::SYMBOL);
}

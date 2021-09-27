// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include <gtest/gtest.h>
#include <re2/re2.h>

#include "../src/parser/SparqlLexer.h"

TEST(SparqlLexerTest, unescapeLiteral) {
  std::string input = R"("^\"biff")";
  SparqlLexer lexer(input);
  lexer.expect(SparqlToken::Type::RDFLITERAL);
  ASSERT_EQ(R"("^"biff")", lexer.current().raw);
}

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

  lexer.expect("order by");
  lexer.expect("asc");
  lexer.expect("(");
  lexer.expect("?a");
  lexer.expect(")");
  lexer.expect("desc");
  lexer.expect("(");
  lexer.expect("?b");
  lexer.expect(")");

  lexer.expect("group by");
  lexer.expect("(");
  lexer.expect("?a");
  lexer.expect(")");

  SparqlLexer lexer2(query);
  lexer2.expect(SparqlToken::Type::KEYWORD);  // prefix
  lexer2.expect(SparqlToken::Type::IRI);      // wd:
  lexer2.expect(SparqlToken::Type::IRI);  // <http://www.wikidata.org/entity/>

  lexer2.expect(SparqlToken::Type::KEYWORD);    // select
  lexer2.expect(SparqlToken::Type::VARIABLE);   // ?a
  lexer2.expect(SparqlToken::Type::VARIABLE);   // ?b
  lexer2.expect(SparqlToken::Type::SYMBOL);     // (
  lexer2.expect(SparqlToken::Type::AGGREGATE);  // count
  lexer2.expect(SparqlToken::Type::SYMBOL);     // (
  lexer2.expect(SparqlToken::Type::VARIABLE);   // ?c
  lexer2.expect(SparqlToken::Type::SYMBOL);     // )
  lexer2.expect(SparqlToken::Type::KEYWORD);    // as
  lexer2.expect(SparqlToken::Type::VARIABLE);   // ?count
  lexer2.expect(SparqlToken::Type::SYMBOL);     // )
  lexer2.expect(SparqlToken::Type::KEYWORD);    // where
  lexer2.expect(SparqlToken::Type::SYMBOL);     // {

  lexer2.expect(SparqlToken::Type::VARIABLE);  // ?a
  lexer2.expect(SparqlToken::Type::IRI);       // wd:test
  lexer2.expect(SparqlToken::Type::VARIABLE);  // ?b
  lexer2.expect(SparqlToken::Type::SYMBOL);    // .

  lexer2.expect(SparqlToken::Type::KEYWORD);  // optional
  lexer2.expect(SparqlToken::Type::SYMBOL);   // {

  lexer2.expect(SparqlToken::Type::SYMBOL);    // {
  lexer2.expect(SparqlToken::Type::VARIABLE);  // ?a
  lexer2.expect(SparqlToken::Type::IRI);       // <is-a>
  lexer2.expect(SparqlToken::Type::VARIABLE);  // ?b
  lexer2.expect(SparqlToken::Type::SYMBOL);    // }
  lexer2.expect(SparqlToken::Type::KEYWORD);   // union
  lexer2.expect(SparqlToken::Type::SYMBOL);    // {
  lexer2.expect(SparqlToken::Type::VARIABLE);  // ?a
  lexer2.expect(SparqlToken::Type::IRI);       // <is-b>
  lexer2.expect(SparqlToken::Type::VARIABLE);  // ?b
  lexer2.expect(SparqlToken::Type::SYMBOL);    // }

  lexer2.expect(SparqlToken::Type::KEYWORD);     // filter
  lexer2.expect(SparqlToken::Type::KEYWORD);     // langmatches
  lexer2.expect(SparqlToken::Type::SYMBOL);      // (
  lexer2.expect(SparqlToken::Type::KEYWORD);     // lang
  lexer2.expect(SparqlToken::Type::SYMBOL);      // (
  lexer2.expect(SparqlToken::Type::VARIABLE);    // ?rname
  lexer2.expect(SparqlToken::Type::SYMBOL);      // )
  lexer2.expect(SparqlToken::Type::SYMBOL);      // ,
  lexer2.expect(SparqlToken::Type::RDFLITERAL);  // "en"
  lexer2.expect(SparqlToken::Type::SYMBOL);      // )

  lexer2.expect(SparqlToken::Type::SYMBOL);  // }

  lexer2.expect(SparqlToken::Type::SYMBOL);  // }

  lexer2.expect(SparqlToken::Type::ORDER_BY);  // order by
  lexer2.expect(SparqlToken::Type::KEYWORD);   // asc
  lexer2.expect(SparqlToken::Type::SYMBOL);    // (
  lexer2.expect(SparqlToken::Type::VARIABLE);  // ?a
  lexer2.expect(SparqlToken::Type::SYMBOL);    // )
  lexer2.expect(SparqlToken::Type::KEYWORD);   // desc
  lexer2.expect(SparqlToken::Type::SYMBOL);    // (
  lexer2.expect(SparqlToken::Type::VARIABLE);  // ?b
  lexer2.expect(SparqlToken::Type::SYMBOL);    // )

  lexer2.expect(SparqlToken::Type::GROUP_BY);  // group by
  lexer2.expect(SparqlToken::Type::SYMBOL);    // (
  lexer2.expect(SparqlToken::Type::VARIABLE);  // ?a
  lexer2.expect(SparqlToken::Type::SYMBOL);    // )
}

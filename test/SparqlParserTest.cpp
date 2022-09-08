// Copyright 2014, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <variant>

#include "../src/global/Constants.h"
#include "../src/parser/SparqlParser.h"
#include "SparqlAntlrParserTestHelpers.h"

namespace m = matchers;
namespace p = parsedQuery;

const ParsedQuery pqDummy = []() {
  ParsedQuery pq;
  pq._prefixes.emplace_back("xsd", "<http://www.w3.org/2001/XMLSchema#>");
  return pq;
}();

TEST(ParserTest, testParse) {
  try {
    {
      auto pq = SparqlParser("SELECT ?x WHERE {?x ?y ?z}").parse();
      ASSERT_TRUE(pq.hasSelectClause());
      const auto& selectClause = pq.selectClause();
      ASSERT_EQ(1u, pq._prefixes.size());
      ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
      ASSERT_EQ(1u, pq._rootGraphPattern._graphPatterns.size());
      ASSERT_EQ(
          1u,
          pq._rootGraphPattern._graphPatterns[0].getBasic()._triples.size());
    }

    {
      auto pq = SparqlParser(
                    "PREFIX : <http://rdf.myprefix.com/>\n"
                    "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
                    "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
                    "SELECT ?x ?z \n "
                    "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y <nsx:rel2> "
                    "<http://abc.de>}")
                    .parse();
      ASSERT_TRUE(pq.hasSelectClause());
      const auto& selectClause2 = pq.selectClause();
      ASSERT_EQ(4u, pq._prefixes.size());
      ASSERT_EQ(2u, selectClause2.getSelectedVariables().size());
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._triples;
      ASSERT_EQ(3u, triples.size());

      ASSERT_THAT(pq._prefixes,
                  testing::UnorderedElementsAre(
                      SparqlPrefix{INTERNAL_PREDICATE_PREFIX_NAME,
                                   INTERNAL_PREDICATE_PREFIX_IRI},
                      SparqlPrefix{"", "<http://rdf.myprefix.com/>"},
                      SparqlPrefix{"ns", "<http://rdf.myprefix.com/ns/>"},
                      SparqlPrefix{"xxx", "<http://rdf.myprefix.com/xxx/>"}));
      ASSERT_EQ(Variable{"?x"}, selectClause2.getSelectedVariables()[0]);
      ASSERT_EQ(Variable{"?z"}, selectClause2.getSelectedVariables()[1]);
      ASSERT_EQ("?x", triples[0]._s);
      ASSERT_EQ("<http://rdf.myprefix.com/myrel>", triples[0]._p._iri);
      ASSERT_EQ("?y", triples[0]._o);
      ASSERT_EQ("?y", triples[1]._s);
      ASSERT_EQ("<http://rdf.myprefix.com/ns/myrel>", triples[1]._p._iri);
      ASSERT_EQ("?z", triples[1]._o);
      ASSERT_EQ("?y", triples[2]._s);
      ASSERT_EQ("<nsx:rel2>", triples[2]._p._iri);
      ASSERT_EQ("<http://abc.de>", triples[2]._o);
      ASSERT_EQ(std::numeric_limits<uint64_t>::max(), pq._limitOffset._limit);
      ASSERT_EQ(0, pq._limitOffset._offset);
    }

    {
      auto pq = SparqlParser(
                    "PREFIX : <http://rdf.myprefix.com/>\n"
                    "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
                    "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
                    "SELECT ?x ?z \n "
                    "WHERE \t {\n?x :myrel ?y. ?y ns:myrel ?z.\n?y <nsx:rel2> "
                    "<http://abc.de>\n}")
                    .parse();
      ASSERT_TRUE(pq.hasSelectClause());
      const auto& selectClause = pq.selectClause();
      ASSERT_EQ(4u, pq._prefixes.size());
      ASSERT_EQ(2u, selectClause.getSelectedVariables().size());
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._triples;
      ASSERT_EQ(3u, triples.size());

      ASSERT_THAT(pq._prefixes,
                  testing::UnorderedElementsAre(
                      SparqlPrefix{INTERNAL_PREDICATE_PREFIX_NAME,
                                   INTERNAL_PREDICATE_PREFIX_IRI},
                      SparqlPrefix{"", "<http://rdf.myprefix.com/>"},
                      SparqlPrefix{"ns", "<http://rdf.myprefix.com/ns/>"},
                      SparqlPrefix{"xxx", "<http://rdf.myprefix.com/xxx/>"}));
      ASSERT_EQ(Variable{"?x"}, selectClause.getSelectedVariables()[0]);
      ASSERT_EQ(Variable{"?z"}, selectClause.getSelectedVariables()[1]);
      ASSERT_EQ("?x", triples[0]._s);
      ASSERT_EQ("<http://rdf.myprefix.com/myrel>", triples[0]._p._iri);
      ASSERT_EQ("?y", triples[0]._o);
      ASSERT_EQ("?y", triples[1]._s);
      ASSERT_EQ("<http://rdf.myprefix.com/ns/myrel>", triples[1]._p._iri);
      ASSERT_EQ("?z", triples[1]._o);
      ASSERT_EQ("?y", triples[2]._s);
      ASSERT_EQ("<nsx:rel2>", triples[2]._p._iri);
      ASSERT_EQ("<http://abc.de>", triples[2]._o);
      ASSERT_EQ(std::numeric_limits<uint64_t>::max(), pq._limitOffset._limit);
      ASSERT_EQ(0, pq._limitOffset._offset);
    }

    {
      auto pq = SparqlParser(
                    "PREFIX ns: <http://ns/>"
                    "SELECT ?x ?z \n "
                    "WHERE \t {\n?x <Directed_by> ?y. ?y ns:myrel.extend ?z.\n"
                    "?y <nsx:rel2> \"Hello... World\"}")
                    .parse();
      ASSERT_TRUE(pq.hasSelectClause());
      const auto& selectClause = pq.selectClause();
      ASSERT_EQ(2u, pq._prefixes.size());
      ASSERT_EQ(2u, selectClause.getSelectedVariables().size());
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._triples;
      ASSERT_EQ(3u, triples.size());

      ASSERT_EQ(Variable{"?x"}, selectClause.getSelectedVariables()[0]);
      ASSERT_EQ(Variable{"?z"}, selectClause.getSelectedVariables()[1]);
      ASSERT_EQ("?x", triples[0]._s);
      ASSERT_EQ("<Directed_by>", triples[0]._p._iri);
      ASSERT_EQ("?y", triples[0]._o);
      ASSERT_EQ("?y", triples[1]._s);
      ASSERT_EQ("<http://ns/myrel.extend>", triples[1]._p._iri);
      ASSERT_EQ("?z", triples[1]._o);
      ASSERT_EQ("?y", triples[2]._s);
      ASSERT_EQ("<nsx:rel2>", triples[2]._p._iri);
      ASSERT_EQ("\"Hello... World\"", triples[2]._o);
      ASSERT_EQ(std::numeric_limits<uint64_t>::max(), pq._limitOffset._limit);
      ASSERT_EQ(0, pq._limitOffset._offset);
    }

    {
      auto pq = SparqlParser(
                    "SELECT ?x ?y WHERE {?x <is-a> <Actor> .  FILTER(?x != ?y)."
                    "?y <is-a> <Actor> . FILTER(?y < ?x)} LIMIT 10")
                    .parse();
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._triples;
      auto filters = pq._rootGraphPattern._filters;
      ASSERT_EQ(2u, filters.size());
      ASSERT_EQ("?x", filters[0]._lhs);
      ASSERT_EQ("?y", filters[0]._rhs);
      ASSERT_EQ(SparqlFilter::FilterType::NE, filters[0]._type);
      ASSERT_EQ("?y", filters[1]._lhs);
      ASSERT_EQ("?x", filters[1]._rhs);
      ASSERT_EQ(SparqlFilter::FilterType::LT, filters[1]._type);
      ASSERT_EQ(2u, triples.size());
    }

    {
      auto pq = SparqlParser(
                    "SELECT ?x ?y WHERE {?x <is-a> <Actor> .  FILTER(?x != ?y)."
                    "?y <is-a> <Actor>} LIMIT 10")
                    .parse();
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._triples;
      auto filters = pq._rootGraphPattern._filters;
      ASSERT_EQ(1u, filters.size());
      ASSERT_EQ("?x", filters[0]._lhs);
      ASSERT_EQ("?y", filters[0]._rhs);
      ASSERT_EQ(SparqlFilter::FilterType::NE, filters[0]._type);
      ASSERT_EQ(2u, triples.size());
    }

    {
      auto pq = SparqlParser(
                    "SELECT ?x ?y WHERE {?x <is-a> <Actor> .  FILTER(?x != ?y)."
                    "?y <is-a> <Actor>. ?c ql:contains-entity ?x."
                    "?c ql:contains-word \"coca* abuse\"} LIMIT 10")
                    .parse();
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._triples;
      auto filters = pq._rootGraphPattern._filters;
      ASSERT_EQ(1u, filters.size());
      ASSERT_EQ("?x", filters[0]._lhs);
      ASSERT_EQ("?y", filters[0]._rhs);
      ASSERT_EQ(SparqlFilter::FilterType::NE, filters[0]._type);
      ASSERT_EQ(4u, triples.size());
      ASSERT_EQ("?c", triples[2]._s);
      ASSERT_EQ(CONTAINS_ENTITY_PREDICATE, triples[2]._p._iri);
      ASSERT_EQ("?x", triples[2]._o);
      ASSERT_EQ("?c", triples[3]._s);
      ASSERT_EQ(CONTAINS_WORD_PREDICATE, triples[3]._p._iri);
      ASSERT_EQ("coca* abuse", triples[3]._o);
    }

    {
      auto pq = SparqlParser(
                    "PREFIX : <>\n"
                    "SELECT ?x ?y ?z ?c ?ql_textscore_c ?c WHERE {\n"
                    "?x :is-a :Politician .\n"
                    "?c ql:contains-entity ?x .\n"
                    "?c ql:contains-word \"friend\" .\n"
                    "?c ql:contains-entity ?y .\n"
                    "?y :is-a :Scientist .\n"
                    "FILTER(?x != ?y) .\n"
                    "} ORDER BY ?c")
                    .parse();
      ASSERT_EQ(1u, pq._rootGraphPattern._filters.size());

      // shouldn't have more checks??
    }

    {
      auto pq = SparqlParser(
                    "SELECT ?x ?z WHERE {\n"
                    "  ?x <test> ?y .\n"
                    "  OPTIONAL {\n"
                    "    ?y <test2> ?z .\n"
                    "  }\n"
                    "}")
                    .parse();

      ASSERT_EQ(2u, pq.children().size());
      const auto& opt =
          std::get<p::Optional>(pq._rootGraphPattern._graphPatterns[1]);
      auto& child = opt._child;
      const auto& triples = child._graphPatterns[0].getBasic()._triples;
      auto filters = child._filters;
      ASSERT_EQ(1u, triples.size());
      ASSERT_EQ("?y", triples[0]._s);
      ASSERT_EQ("<test2>", triples[0]._p._iri);
      ASSERT_EQ("?z", triples[0]._o);
      ASSERT_EQ(0u, filters.size());
      ASSERT_TRUE(child._optional);
    }

    {
      auto pq = SparqlParser(
                    "SELECT ?x ?z WHERE {\n"
                    "  ?x <test> ?y .\n"
                    "  OPTIONAL {\n"
                    "    ?y <test2> ?z .\n"
                    "    optional {\n"
                    "      ?a ?b ?c .\n"
                    "      FILTER(?c > 3)\n"
                    "    }\n"
                    "    optional {\n"
                    "      ?d ?e ?f\n"
                    "    }\n"
                    "  }\n"
                    "}")
                    .parse();
      ASSERT_EQ(2u, pq._rootGraphPattern._graphPatterns.size());
      const auto& optA = std::get<p::Optional>(
          pq._rootGraphPattern._graphPatterns[1]);  // throws on error
      auto& child = optA._child;
      ASSERT_EQ(3u, child._graphPatterns.size());
      const auto& opt2 =
          std::get<p::Optional>(child._graphPatterns[1]);  // throws on error
      const auto& opt3 =
          std::get<p::Optional>(child._graphPatterns[2]);  // throws on error
      const auto& child2 = opt2._child._graphPatterns[0].getBasic();
      const auto& child3 = opt3._child._graphPatterns[0].getBasic();
      ASSERT_EQ(1u, child2._triples.size());
      ASSERT_EQ(1u, opt2._child._filters.size());
      ASSERT_EQ(1u, child3._triples.size());
      ASSERT_EQ(0u, opt3._child._filters.size());
      ASSERT_TRUE(child._optional);
      ASSERT_TRUE(opt2._child._optional);
      ASSERT_TRUE(opt3._child._optional);
    }

    {
      auto pq = SparqlParser(
                    "SELECT ?a WHERE {\n"
                    "  VALUES ?a { <1> \"2\"}\n"
                    "  VALUES (?b ?c) {(<1> <2>) (\"1\" \"2\")}\n"
                    "  ?a <rel> ?b ."
                    "}")
                    .parse();
      ASSERT_EQ(3u, pq._rootGraphPattern._graphPatterns.size());
      const auto& c = pq.children()[2].getBasic();
      ASSERT_EQ(1u, c._triples.size());
      ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());
      const auto& values1 = std::get<p::Values>(pq.children()[0])._inlineValues;
      const auto& values2 = std::get<p::Values>(pq.children()[1])._inlineValues;

      vector<string> vvars = {"?a"};
      ASSERT_EQ(vvars, values1._variables);
      vector<vector<string>> vvals = {{"<1>"}, {"\"2\""}};
      ASSERT_EQ(vvals, values1._values);

      vvars = {"?b", "?c"};
      ASSERT_EQ(vvars, values2._variables);
      vvals = {{"<1>", "<2>"}, {"\"1\"", "\"2\""}};
      ASSERT_EQ(vvals, values2._values);
    }

    {
      auto pq = SparqlParser(
                    R"(SELECT ?a ?b ?c WHERE {
                        VALUES ?a { <Albert_Einstein>}
                        VALUES (?b ?c) {
        (<Marie_Curie> <Joseph_Jacobson>) (<Freiherr> <Lord_of_the_Isles>) }
                        }
                    )")
                    .parse();

      ASSERT_EQ(2u, pq.children().size());
      ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());
      const auto& values1 = std::get<p::Values>(pq.children()[0])._inlineValues;
      const auto& values2 = std::get<p::Values>(pq.children()[1])._inlineValues;

      vector<string> vvars = {"?a"};
      ASSERT_EQ(vvars, values1._variables);
      vector<vector<string>> vvals = {{"<Albert_Einstein>"}};
      ASSERT_EQ(vvals, values1._values);

      vvars = {"?b", "?c"};
      ASSERT_EQ(vvars, values2._variables);
      vvals = {{"<Marie_Curie>", "<Joseph_Jacobson>"},
               {"<Freiherr>", "<Lord_of_the_Isles>"}};
      ASSERT_EQ(vvals, values2._values);
    }

    {
      auto pq = SparqlParser(
                    ""
                    "PREFIX wd: <http://www.wikidata.org/entity/>\n"
                    "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n"
                    "SELECT ?city WHERE {\n"
                    "  VALUES ?citytype { wd:Q515 wd:Q262166}\n"
                    "  ?city wdt:P31 ?citytype .\n"
                    "}\n")
                    .parse();

      ASSERT_EQ(2u, pq.children().size());
      const auto& c = pq.children()[1].getBasic();
      ASSERT_EQ(1u, c._triples.size());
      ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());

      ASSERT_EQ(c._triples[0]._s, "?city");
      ASSERT_EQ(c._triples[0]._p._iri,
                "<http://www.wikidata.org/prop/direct/P31>");
      ASSERT_EQ(c._triples[0]._o, "?citytype");

      const auto& values1 = std::get<p::Values>(pq.children()[0])._inlineValues;
      vector<string> vvars = {"?citytype"};
      ASSERT_EQ(vvars, values1._variables);
      vector<vector<string>> vvals = {
          {"<http://www.wikidata.org/entity/Q515>"},
          {"<http://www.wikidata.org/entity/Q262166>"}};
      ASSERT_EQ(vvals, values1._values);
    }

    // TODO @joaomarques90: Finish when the required functionalities
    // are implemented
    /*
    {
     // C++ exception with description "ParseException, cause:
     // Expected a token of type AGGREGATE but got a token of
     // type RDFLITERAL (() in the input at pos 373 : (YEAR(?year))
      auto pq = SparqlParser(
                    "SELECT DISTINCT * WHERE { \n"
                    "  ?movie <directed-by> <Scott%2C%20Ridley> .\n"
                    "\t{ \n"
                    "\t SELECT ?movie WHERE { \n"
                    "\t\t\t ?movie <directed-by> ?director .\n"
                    "\t\t\t ?movie <from-year> ?year .\n"
                    "\t\t\t FILTER(regex(?title, \"^the\", \"i\" )) ."
                    "\t\t } \n"
                    "\t\t GROUP BY (YEAR(?year)) \n"
                    "\t} \n"
                    "} \n"
                    "LIMIT 10 \n"
                    "ORDER BY DESC(?movie) ASC(?year) \n")
                    .parse();
    }

    {
      auto pq = SparqlParser(
                "SELECT * WHERE { \n"
                "  VALUES ?x { 1 2 3 4 } .\n"
                "\t{ \n"
                "\t SELECT ?y ?concat WHERE { \n"
                "\t\t\t VALUES ?y { \"5\" \"6\" \"7\" \"8\" } .\n"
                "\t\t\t BIND(CONCAT(STR(?y),STR(?y)) AS ?concat) .\n"
                "\t\t } \n"
                "\t\t LIMIT 2 \n"
                "\t} \n"
                "\t BIND((?x * ?y) AS ?xy) ."
                "} \n"
                "LIMIT 5 \n"
                "OFFSET 2 \n"
                "ORDER BY DESC(?x) ASC(?concat) \n")
                .parse();
    }

     {
        auto pq = SparqlParser(
               "SELECT REDUCED * WHERE { \n"
               "  ?movie <directed-by> <Scott%2C%20Ridley> .\n"
               "\t{ \n"
               "\t SELECT ?movie WHERE { \n"
               "\t\t\t ?movie <directed-by> ?director .\n"
               "\t\t\t ?movie <from-year> ?year .\n"
               "\t\t\t FILTER(regex(?title, \"^the\", \"i\" )) ."
               "\t\t } \n"
               "\t\t GROUP BY (YEAR(?year)) \n"
               "\t\t ORDER BY DESC(?year) \n"
               "\t\t OFFSET 2 \n"
               "\t} \n"
               "} \n"
               "LIMIT 10 \n"
               "ORDER BY DESC(?movie) ASC(?year) \n")
               .parse();
    }

    {
        auto pq = SparqlParser(
               "SELECT DISTINCT * WHERE { \n"
               "  ?movie <directed-by> <Scott%2C%20Ridley> .\n"
               "\t{ \n"
               "\t SELECT * WHERE { \n"
               "\t\t\t ?movie <directed-by> ?director .\n"
               "\t\t\t ?movie <from-year> ?year .\n"
               "\t\t\t FILTER(YEAR(?year) > 2000) ."
               "\t\t } \n"
               "\t\t ORDER BY DESC(?director) \n"
               "\t} \n"
               "} \n"
               "LIMIT 20 \n"
               "OFFSET 3 \n"
               "ORDER BY DESC(?movie)\n")
               .parse();
    }
    */

    {
      auto pq = SparqlParser(
                    "SELECT REDUCED * WHERE { \n"
                    "  ?movie <directed-by> ?director .\n"
                    "} \n"
                    "ORDER BY ASC(?movie)\n"
                    "LIMIT 10 \n")
                    .parse();
      ASSERT_EQ(1u, pq._prefixes.size());
      ASSERT_EQ(1u, pq._rootGraphPattern._graphPatterns.size());

      const auto& c = pq.children()[0].getBasic();
      ASSERT_EQ(1u, c._triples.size());
      ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());

      ASSERT_EQ(c._triples[0]._s, "?movie");
      ASSERT_EQ(c._triples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c._triples[0]._o, "?director");

      ASSERT_EQ(10u, pq._limitOffset._limit);
      ASSERT_EQ(false, pq._orderBy[0].isDescending_);
      ASSERT_EQ("?movie", pq._orderBy[0].variable_);

      auto sc = get<p::SelectClause>(pq._clause);
      ASSERT_EQ(true, sc.reduced_);
      ASSERT_EQ(true, sc.isAsterisk());

      vector<string> vvars = {"?movie", "?director"};
      ASSERT_EQ(vvars, sc.getSelectedVariablesAsStrings());
    }

    {
      auto pq = SparqlParser(
                    "SELECT DISTINCT * WHERE { \n"
                    "  ?movie <directed-by> ?director .\n"
                    "} \n"
                    "ORDER BY DESC(?movie)\n"
                    "LIMIT 10 \n")
                    .parse();

      ASSERT_EQ(1u, pq._prefixes.size());
      ASSERT_EQ(1u, pq._rootGraphPattern._graphPatterns.size());

      const auto& c = pq.children()[0].getBasic();
      ASSERT_EQ(1u, c._triples.size());
      ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());

      ASSERT_EQ(c._triples[0]._s, "?movie");
      ASSERT_EQ(c._triples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c._triples[0]._o, "?director");

      ASSERT_EQ(10u, pq._limitOffset._limit);
      ASSERT_EQ(true, pq._orderBy[0].isDescending_);
      ASSERT_EQ("?movie", pq._orderBy[0].variable_);

      auto sc = get<ParsedQuery::SelectClause>(pq._clause);
      ASSERT_EQ(true, sc.distinct_);
      ASSERT_EQ(true, sc.isAsterisk());

      vector<string> vvars = {"?movie", "?director"};
      ASSERT_EQ(vvars, sc.getSelectedVariablesAsStrings());
    }

    {
      auto pq = SparqlParser(
                    "SELECT DISTINCT * WHERE { \n"
                    "  ?movie <directed-by> <Scott%2C%20Ridley> .\n"
                    "\t{ \n"
                    "\t SELECT * WHERE { \n"
                    "\t\t\t ?movie <directed-by> ?director .\n"
                    "\t\t\t ?movie <from-year> ?year .\n"
                    "\t\t\t FILTER(?year > \"00-00-2000\") ."
                    "\t\t } \n"
                    "\t\t ORDER BY DESC(?director) \n"
                    "\t} \n"
                    "} \n"
                    "ORDER BY DESC(?movie)\n"
                    "LIMIT 20 \n"
                    "OFFSET 3 \n")
                    .parse();

      ASSERT_EQ(1u, pq._prefixes.size());
      ASSERT_EQ(2u, pq._rootGraphPattern._graphPatterns.size());

      const auto& c = pq.children()[0].getBasic();
      ASSERT_EQ(1u, c._triples.size());
      ASSERT_EQ(0, pq._rootGraphPattern._filters.size());
      ASSERT_EQ(3u, pq._limitOffset._offset);

      ASSERT_EQ(c._triples[0]._s, "?movie");
      ASSERT_EQ(c._triples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c._triples[0]._o, "<Scott%2C%20Ridley>");

      ASSERT_EQ(20u, pq._limitOffset._limit);
      ASSERT_EQ(true, pq._orderBy[0].isDescending_);
      ASSERT_EQ("?movie", pq._orderBy[0].variable_);

      auto sc = get<ParsedQuery::SelectClause>(pq._clause);
      ASSERT_EQ(true, sc.distinct_);
      ASSERT_EQ(true, sc.isAsterisk());

      vector<string> vvars = {"?movie", "?director", "?year"};
      ASSERT_EQ(vvars, sc.getSelectedVariablesAsStrings());

      // -- SubQuery
      auto subQueryGroup =
          get<p::GroupGraphPattern>(pq._rootGraphPattern._graphPatterns[1]);
      auto parsed_sub_query =
          get<p::Subquery>(subQueryGroup._child._graphPatterns[0]);
      const auto& c_subquery = get<p::BasicGraphPattern>(
          parsed_sub_query.get()._rootGraphPattern._graphPatterns[0]);
      ASSERT_EQ(2u, c_subquery._triples.size());
      ASSERT_EQ(1u, parsed_sub_query.get()._rootGraphPattern._filters.size());
      ASSERT_EQ("?year",
                parsed_sub_query.get()._rootGraphPattern._filters[0]._lhs);
      ASSERT_EQ("\"00-00-2000\"",
                parsed_sub_query.get()._rootGraphPattern._filters[0]._rhs);
      ASSERT_EQ(SparqlFilter::GT,
                parsed_sub_query.get()._rootGraphPattern._filters[0]._type);
      ASSERT_EQ(0, parsed_sub_query.get()._limitOffset._offset);

      ASSERT_EQ(c_subquery._triples[0]._s, "?movie");
      ASSERT_EQ(c_subquery._triples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c_subquery._triples[0]._o, "?director");

      ASSERT_EQ(c_subquery._triples[1]._s, "?movie");
      ASSERT_EQ(c_subquery._triples[1]._p._iri, "<from-year>");
      ASSERT_EQ(c_subquery._triples[1]._o, "?year");

      ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
                parsed_sub_query.get()._limitOffset._limit);
      ASSERT_EQ(true, parsed_sub_query.get()._orderBy[0].isDescending_);
      ASSERT_EQ("?director", parsed_sub_query.get()._orderBy[0].variable_);

      auto sc_subquery =
          get<ParsedQuery::SelectClause>(parsed_sub_query.get()._clause);
      ASSERT_EQ(false, sc_subquery.distinct_);
      ASSERT_EQ(false, sc_subquery.reduced_);
      ASSERT_EQ(true, sc_subquery.isAsterisk());
      vector<string> vvars_subquery = {"?movie", "?director", "?year"};
      ASSERT_EQ(vvars_subquery, sc_subquery.getSelectedVariablesAsStrings());
    }

    {
      // Query proving Select * working for n-subQuery
      auto pq = SparqlParser(
                    "SELECT DISTINCT * WHERE { \n"
                    "  ?movie <directed-by> <Scott%2C%20Ridley> .\n"
                    "\t{ \n"
                    "\t SELECT * WHERE { \n"
                    "\t\t\t ?movie <directed-by> ?director .\n"
                    "\t\t\t { \n"
                    "\t\t\t\t SELECT ?year WHERE { \n"
                    "\t\t\t\t\t ?movie <from-year> ?year . \n"
                    "\t\t\t\t\t } \n"
                    "\t\t\t } \n"
                    "\t\t\t FILTER(?year > \"00-00-2000\") ."
                    "\t\t } \n"
                    "\t\t ORDER BY DESC(?director) \n"
                    "\t} \n"
                    "} \n"
                    "ORDER BY DESC(?movie)\n"
                    "LIMIT 20 \n"
                    "OFFSET 3 \n")
                    .parse();

      ASSERT_EQ(1u, pq._prefixes.size());
      ASSERT_EQ(2u, pq._rootGraphPattern._graphPatterns.size());

      const auto& c = pq.children()[0].getBasic();
      ASSERT_EQ(1u, c._triples.size());
      ASSERT_EQ(0, pq._rootGraphPattern._filters.size());
      ASSERT_EQ(3u, pq._limitOffset._offset);

      ASSERT_EQ(c._triples[0]._s, "?movie");
      ASSERT_EQ(c._triples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c._triples[0]._o, "<Scott%2C%20Ridley>");

      ASSERT_EQ(20u, pq._limitOffset._limit);
      ASSERT_EQ(true, pq._orderBy[0].isDescending_);
      ASSERT_EQ("?movie", pq._orderBy[0].variable_);

      auto sc = get<ParsedQuery::SelectClause>(pq._clause);
      ASSERT_EQ(true, sc.distinct_);
      ASSERT_EQ(true, sc.isAsterisk());

      vector<string> vvars = {"?movie", "?director", "?year"};
      ASSERT_EQ(vvars, sc.getSelectedVariablesAsStrings());

      // -- SubQuery (level 1)
      auto subQueryGroup =
          get<p::GroupGraphPattern>(pq._rootGraphPattern._graphPatterns[1]);
      auto parsed_sub_query =
          get<p::Subquery>(subQueryGroup._child._graphPatterns[0]);
      const auto& c_subquery = get<p::BasicGraphPattern>(
          parsed_sub_query.get()._rootGraphPattern._graphPatterns[0]);
      ASSERT_EQ(1u, c_subquery._triples.size());
      ASSERT_EQ(1u, parsed_sub_query.get()._rootGraphPattern._filters.size());
      ASSERT_EQ("?year",
                parsed_sub_query.get()._rootGraphPattern._filters[0]._lhs);
      ASSERT_EQ("\"00-00-2000\"",
                parsed_sub_query.get()._rootGraphPattern._filters[0]._rhs);
      ASSERT_EQ(SparqlFilter::GT,
                parsed_sub_query.get()._rootGraphPattern._filters[0]._type);
      ASSERT_EQ(0, parsed_sub_query.get()._limitOffset._offset);

      ASSERT_EQ(c_subquery._triples[0]._s, "?movie");
      ASSERT_EQ(c_subquery._triples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c_subquery._triples[0]._o, "?director");

      ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
                parsed_sub_query.get()._limitOffset._limit);
      ASSERT_EQ(true, parsed_sub_query.get()._orderBy[0].isDescending_);
      ASSERT_EQ("?director", parsed_sub_query.get()._orderBy[0].variable_);

      auto sc_subquery =
          get<ParsedQuery::SelectClause>(parsed_sub_query.get()._clause);
      ASSERT_EQ(false, sc_subquery.distinct_);
      ASSERT_EQ(false, sc_subquery.reduced_);
      ASSERT_EQ(true, sc_subquery.isAsterisk());
      vector<string> vvars_subquery = {"?movie", "?director", "?year"};
      ASSERT_EQ(vvars_subquery, sc_subquery.getSelectedVariablesAsStrings());

      // -- SubQuery (level 2)
      auto subsubQueryGroup = get<p::GroupGraphPattern>(
          parsed_sub_query.get()._rootGraphPattern._graphPatterns[1]);
      auto aux_parsed_sub_sub_query =
          get<p::Subquery>(subsubQueryGroup._child._graphPatterns[0]).get();
      const auto& c_sub_subquery = get<p::BasicGraphPattern>(
          aux_parsed_sub_sub_query._rootGraphPattern._graphPatterns[0]);
      ASSERT_EQ(1u, c_sub_subquery._triples.size());
      ASSERT_EQ(0u, aux_parsed_sub_sub_query._rootGraphPattern._filters.size());
      ASSERT_EQ(0, aux_parsed_sub_sub_query._limitOffset._offset);

      ASSERT_EQ(c_sub_subquery._triples[0]._s, "?movie");
      ASSERT_EQ(c_sub_subquery._triples[0]._p._iri, "<from-year>");
      ASSERT_EQ(c_sub_subquery._triples[0]._o, "?year");

      ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
                aux_parsed_sub_sub_query._limitOffset._limit);
      ASSERT_EQ(0u, aux_parsed_sub_sub_query._orderBy.size());

      auto sc_sub_subquery =
          get<ParsedQuery::SelectClause>(aux_parsed_sub_sub_query._clause);
      ASSERT_EQ(false, sc_sub_subquery.distinct_);
      ASSERT_EQ(false, sc_sub_subquery.reduced_);
      ASSERT_EQ(false, sc_sub_subquery.isAsterisk());
      vector<string> vvars_sub_subquery = {"?year"};
      ASSERT_EQ(vvars_sub_subquery,
                sc_sub_subquery.getSelectedVariablesAsStrings());
    }

    // We currently only check, that the following two queries don't throw an
    // exception.
    // TODO<RobinTF>  Also add checks for the correct semantics.
    {
      // Check Parse Construct (1)
      auto pq_1 = SparqlParser(
          "PREFIX foaf:   <http://xmlns.com/foaf/0.1/> \n"
          "PREFIX org:    <http://example.com/ns#> \n"
          "CONSTRUCT { ?x foaf:name ?name } \n"
          "WHERE  { ?x org:employeeName ?name }");
      pq_1.parse();

      // Check Parse Construct (2)
      auto pq_2 = SparqlParser(
          "PREFIX foaf:    <http://xmlns.com/foaf/0.1/>\n"
          "PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>\n"
          "CONSTRUCT   { <http://example.org/person#Alice> vcard:FN ?name }\n"
          "WHERE       { ?x foaf:name ?name } ");
      pq_2.parse();
    }

    {
      // Check if the correct ParseException is thrown after
      // GroupBy with Select '*'
      auto pq = SparqlParser(
          "SELECT DISTINCT * WHERE { \n"
          "  ?a <b> ?c .\n"
          "} \n"
          "GROUP BY ?a ?c \n");
      ASSERT_THROW(pq.parse(), ParseException);
    }

    {
      // Check if the correct ParseException is thrown after:
      // Select [var_name]+ '*'
      auto pq = SparqlParser(
          "SELECT DISTINCT ?a * WHERE { \n"
          "  ?a <b> ?c .\n"
          "} \n");
      ASSERT_THROW(pq.parse(), ParseException);
    }

    {
      // Check if the correct ParseException is thrown after:
      // Select '*' [var_name]+
      auto pq = SparqlParser(
          "SELECT DISTINCT * ?a WHERE { \n"
          "  ?a <b> ?c .\n"
          "} \n");
      ASSERT_THROW(pq.parse(), ParseException);
    }

    {
      // Check if the correct ParseException is thrown after: Select ['*']{2,}
      auto pq = SparqlParser(
          "SELECT DISTINCT * * WHERE { \n"
          "  ?a <b> ?c .\n"
          "} \n");
      ASSERT_THROW(pq.parse(), ParseException);
    }

  } catch (const ad_semsearch::Exception& e) {
    FAIL() << e.getFullErrorMessage();
  }
}

TEST(ParserTest, testFilterWithoutDot) {
  ParsedQuery pq =
      SparqlParser(
          "PREFIX fb: <http://rdf.freebase.com/ns/>\n"
          "\n"
          "SELECT DISTINCT ?1 WHERE {\n"
          " fb:m.0fkvn fb:government.government_office_category.officeholders "
          "?0 "
          ".\n"
          " ?0 fb:government.government_position_held.jurisdiction_of_office "
          "fb:m.0vmt .\n"
          " ?0 fb:government.government_position_held.office_holder ?1 .\n"
          " FILTER (?1 != fb:m.0fkvn)\n"
          " FILTER (?1 != fb:m.0vmt)\n"
          "FILTER (?1 != fb:m.018mts) \n"
          "} LIMIT 300")
          .parse();
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(2u, pq._prefixes.size());
  ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
  ASSERT_EQ(1u, pq.children().size());
  const auto& c = pq.children()[0].getBasic();
  ASSERT_EQ(3u, c._triples.size());
  const auto& filters = pq._rootGraphPattern._filters;
  ASSERT_EQ(3u, filters.size());
  ASSERT_EQ("?1", filters[0]._lhs);
  ASSERT_EQ("<http://rdf.freebase.com/ns/m.0fkvn>", filters[0]._rhs);
  ASSERT_EQ(SparqlFilter::FilterType::NE, filters[0]._type);
  ASSERT_EQ("?1", filters[1]._lhs);
  ASSERT_EQ("<http://rdf.freebase.com/ns/m.0vmt>", filters[1]._rhs);
  ASSERT_EQ(SparqlFilter::FilterType::NE, filters[1]._type);
  ASSERT_EQ("?1", filters[2]._lhs);
  ASSERT_EQ("<http://rdf.freebase.com/ns/m.018mts>", filters[2]._rhs);
  ASSERT_EQ(SparqlFilter::FilterType::NE, filters[2]._type);
}

TEST(ParserTest, testExpandPrefixes) {
  ParsedQuery pq = SparqlParser(
                       "PREFIX : <http://rdf.myprefix.com/>\n"
                       "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
                       "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
                       "SELECT ?x ?z \n WHERE \t {?x :myrel ?y. ?y ns:myrel "
                       "?z.?y <nsx:rel2> <http://abc.de>}")
                       .parse();
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(1u, pq.children().size());
  const auto& c = pq.children()[0].getBasic();
  ASSERT_EQ(4u, pq._prefixes.size());
  ASSERT_EQ(2u, selectClause.getSelectedVariables().size());
  ASSERT_EQ(3u, c._triples.size());
  ASSERT_THAT(pq._prefixes,
              testing::UnorderedElementsAre(
                  SparqlPrefix{INTERNAL_PREDICATE_PREFIX_NAME,
                               INTERNAL_PREDICATE_PREFIX_IRI},
                  SparqlPrefix{"", "<http://rdf.myprefix.com/>"},
                  SparqlPrefix{"ns", "<http://rdf.myprefix.com/ns/>"},
                  SparqlPrefix{"xxx", "<http://rdf.myprefix.com/xxx/>"}));
  ASSERT_EQ(Variable{"?x"}, selectClause.getSelectedVariables()[0]);
  ASSERT_EQ(Variable{"?z"}, selectClause.getSelectedVariables()[1]);
  ASSERT_EQ("?x", c._triples[0]._s);
  ASSERT_EQ("<http://rdf.myprefix.com/myrel>", c._triples[0]._p._iri);
  ASSERT_EQ("?y", c._triples[0]._o);
  ASSERT_EQ("?y", c._triples[1]._s);
  ASSERT_EQ("<http://rdf.myprefix.com/ns/myrel>", c._triples[1]._p._iri);
  ASSERT_EQ("?z", c._triples[1]._o);
  ASSERT_EQ("?y", c._triples[2]._s);
  ASSERT_EQ("<nsx:rel2>", c._triples[2]._p._iri);
  ASSERT_EQ("<http://abc.de>", c._triples[2]._o);
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(), pq._limitOffset._limit);
  ASSERT_EQ(0, pq._limitOffset._offset);
}

TEST(ParserTest, testLiterals) {
  ParsedQuery pq =
      SparqlParser(
          "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT * WHERE { "
          "true <test:myrel> 10 . 10.2 <test:myrel> \"2000-00-00\"^^xsd:date }")
          .parse();
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(1u, pq.children().size());
  const auto& c = pq.children()[0].getBasic();
  ASSERT_EQ(2u, pq._prefixes.size());
  ASSERT_TRUE(selectClause.isAsterisk());
  ASSERT_EQ(2u, c._triples.size());
  ASSERT_EQ("\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            c._triples[0]._s);
  ASSERT_EQ("<test:myrel>", c._triples[0]._p._iri);
  ASSERT_EQ(10, c._triples[0]._o);
  ASSERT_EQ(10.2, c._triples[1]._s);
  ASSERT_EQ("<test:myrel>", c._triples[1]._p._iri);
  ASSERT_EQ(":v:date:0000000000000002000-00-00T00:00:00", c._triples[1]._o);
}

TEST(ParserTest, testSolutionModifiers) {
  {
    ParsedQuery pq =
        SparqlParser("SELECT ?x WHERE \t {?x <test:myrel> ?y}").parse();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, pq._prefixes.size());
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), pq._limitOffset._limit);
    ASSERT_EQ(0, pq._limitOffset._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(selectClause.distinct_);
    ASSERT_FALSE(selectClause.reduced_);
  }

  {
    auto pq = SparqlParser("SELECT ?x WHERE \t {?x <test:myrel> ?y} LIMIT 10")
                  .parse();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq._prefixes.size());
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(10ul, pq._limitOffset._limit);
    ASSERT_EQ(0, pq._limitOffset._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(selectClause.distinct_);
    ASSERT_FALSE(selectClause.reduced_);
  }

  {
    auto pq = SparqlParser(
                  "SELECT ?x WHERE \t {?x <test:myrel> ?y}\n"
                  "LIMIT 10 OFFSET 15")
                  .parse();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, pq._prefixes.size());
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(selectClause.distinct_);
    ASSERT_FALSE(selectClause.reduced_);
  }

  {
    auto pq = SparqlParser(
                  "SELECT DISTINCT ?x ?y WHERE \t {?x <test:myrel> ?y}\n"
                  "ORDER BY ?y LIMIT 10 OFFSET 15")
                  .parse();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, pq._prefixes.size());
    ASSERT_EQ(2u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(1), pq._orderBy.size());
    ASSERT_EQ("?y", pq._orderBy[0].variable_);
    ASSERT_FALSE(pq._orderBy[0].isDescending_);
    ASSERT_TRUE(selectClause.distinct_);
    ASSERT_FALSE(selectClause.reduced_);
  }

  {
    auto pq = SparqlParser(
                  "SELECT DISTINCT ?x ?ql_textscore_x ?y WHERE \t {?x "
                  "<test:myrel> ?y}\n"
                  "ORDER BY ASC(?y) DESC(?ql_textscore_x) LIMIT 10 OFFSET 15")
                  .parse();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, pq._prefixes.size());
    ASSERT_EQ(3u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(Variable{"?ql_textscore_x"},
              selectClause.getSelectedVariables()[1]);
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(2), pq._orderBy.size());
    ASSERT_EQ("?y", pq._orderBy[0].variable_);
    ASSERT_FALSE(pq._orderBy[0].isDescending_);
    ASSERT_EQ("?ql_textscore_x", pq._orderBy[1].variable_);
    ASSERT_TRUE(pq._orderBy[1].isDescending_);
    ASSERT_TRUE(selectClause.distinct_);
    ASSERT_FALSE(selectClause.reduced_);
  }

  {
    auto pq = SparqlParser(
                  "SELECT REDUCED ?x ?y WHERE \t {?x <test:myrel> ?y}\n"
                  "ORDER BY DESC(?x) ASC(?y) LIMIT 10 OFFSET 15")
                  .parse();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, pq._prefixes.size());
    ASSERT_EQ(2u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(1u, c._triples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(2), pq._orderBy.size());
    ASSERT_EQ("?x", pq._orderBy[0].variable_);
    ASSERT_TRUE(pq._orderBy[0].isDescending_);
    ASSERT_EQ("?y", pq._orderBy[1].variable_);
    ASSERT_FALSE(pq._orderBy[1].isDescending_);
    ASSERT_FALSE(selectClause.distinct_);
    ASSERT_TRUE(selectClause.reduced_);
  }

  {
    auto pq =
        SparqlParser("SELECT ?x ?y WHERE {?x <is-a> <Actor>} LIMIT 10").parse();
    ASSERT_EQ(10u, pq._limitOffset._limit);
  }

  {
    auto pq = SparqlParser(
                  "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"
                  "SELECT DISTINCT ?movie WHERE { \n"
                  "\n"
                  "?movie <from-year> \"2000-00-00\"^^xsd:date .\n"
                  "\n"
                  "?movie <directed-by> <Scott%2C%20Ridley> .   }  LIMIT 50")
                  .parse();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(2u, pq._prefixes.size());
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(Variable{"?movie"}, selectClause.getSelectedVariables()[0]);
    ASSERT_EQ(2u, c._triples.size());
    ASSERT_EQ("?movie", c._triples[0]._s);
    ASSERT_EQ("<from-year>", c._triples[0]._p._iri);
    ASSERT_EQ(":v:date:0000000000000002000-00-00T00:00:00", c._triples[0]._o);
    ASSERT_EQ("?movie", c._triples[1]._s);
    ASSERT_EQ("<directed-by>", c._triples[1]._p._iri);
    ASSERT_EQ("<Scott%2C%20Ridley>", c._triples[1]._o);
  }

  {
    auto pq = SparqlParser(
                  "PREFIX xsd: <http://www.w3.org/2010/XMLSchema#>"
                  "SELECT DISTINCT ?movie WHERE { \n"
                  "\n"
                  "?movie <from-year> \"00-00-2000\"^^xsd:date .\n"
                  "\n"
                  "?movie <directed-by> <Scott%2C%20Ridley> .   }  LIMIT 50")
                  .parse();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(2u, pq._prefixes.size());
    ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
    ASSERT_EQ(Variable{"?movie"}, selectClause.getSelectedVariables()[0]);
    ASSERT_EQ(2u, c._triples.size());
    ASSERT_EQ("?movie", c._triples[0]._s);
    ASSERT_EQ("<from-year>", c._triples[0]._p._iri);
    ASSERT_EQ("\"00-00-2000\"^^<http://www.w3.org/2010/XMLSchema#date>",
              c._triples[0]._o);
    ASSERT_EQ("?movie", c._triples[1]._s);
    ASSERT_EQ("<directed-by>", c._triples[1]._p._iri);
    ASSERT_EQ("<Scott%2C%20Ridley>", c._triples[1]._o);
  }

  {
    auto pq = SparqlParser(
                  "SELECT ?r (AVG(?r) as ?avg) WHERE {"
                  "?a <http://schema.org/name> ?b ."
                  "?a ql:has-relation ?r }"
                  "GROUP BY ?r "
                  "ORDER BY ?avg")
                  .parse();
    ASSERT_EQ(1u, pq.children().size());
    ASSERT_EQ(1u, pq._orderBy.size());
    EXPECT_THAT(pq, m::GroupByVariables({Variable{"?r"}}));
    ASSERT_EQ("?avg", pq._orderBy[0].variable_);
    ASSERT_FALSE(pq._orderBy[0].isDescending_);
  }

  {
    auto pq = SparqlParser(
                  "SELECT ?r (COUNT(DISTINCT ?r) as ?count) WHERE {"
                  "?a <http://schema.org/name> ?b ."
                  "?a ql:has-relation ?r }"
                  "GROUP BY ?r "
                  "ORDER BY ?count")
                  .parse();
    ASSERT_EQ(1u, pq._orderBy.size());
    EXPECT_THAT(pq, m::GroupByVariables({Variable{"?r"}}));
    ASSERT_EQ("?count", pq._orderBy[0].variable_);
    ASSERT_FALSE(pq._orderBy[0].isDescending_);
  }

  {
    auto pq =
        SparqlParser(
            "SELECT ?r (GROUP_CONCAT(?r;SEPARATOR=\"Cake\") as ?concat) WHERE {"
            "?a <http://schema.org/name> ?b ."
            "?a ql:has-relation ?r }"
            "GROUP BY ?r "
            "ORDER BY ?concat")
            .parse();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& aliases = pq.selectClause().getAliases();
    ASSERT_EQ(1u, aliases.size());
    ASSERT_EQ("(GROUP_CONCAT(?r;SEPARATOR=\"Cake\") as ?concat)",
              aliases[0].getDescriptor());
  }
}

TEST(ParserTest, testGroupByAndAlias) {
  ParsedQuery pq =
      SparqlParser(
          "SELECT (COUNT(?a) as ?count) WHERE { ?b <rel> ?a } GROUP BY ?b")
          .parse();
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(1u, selectClause.getSelectedVariables().size());
  ASSERT_EQ(Variable{"?count"}, selectClause.getSelectedVariables()[0]);

  const auto& aliases = selectClause.getAliases();
  ASSERT_EQ(1u, aliases.size());
  ASSERT_TRUE(aliases[0]._expression.isAggregate({}));
  ASSERT_EQ("(COUNT(?a) as ?count)", aliases[0].getDescriptor());
  EXPECT_THAT(pq, m::GroupByVariables({Variable{"?b"}}));
}

TEST(ParserTest, Bind) {
  ParsedQuery pq =
      SparqlParser("SELECT ?a WHERE { BIND (10 - 5 as ?a) . }").parse();
  ASSERT_TRUE(pq.hasSelectClause());
  ASSERT_EQ(pq.children().size(), 1);
  p::GraphPatternOperation child = pq.children()[0];
  ASSERT_TRUE(holds_alternative<p::Bind>(child));
  p::Bind bind = get<p::Bind>(child);
  ASSERT_EQ(bind._target, "?a");
  ASSERT_EQ(bind._expression.getDescriptor(), "10-5");
}

TEST(ParserTest, Order) {
  {
    ParsedQuery pq =
        SparqlParser("SELECT ?x ?y WHERE { ?x <test/myrel> ?y }").parse();
    ASSERT_TRUE(pq._orderBy.empty());
    ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1);
    ASSERT_TRUE(holds_alternative<p::BasicGraphPattern>(
        pq._rootGraphPattern._graphPatterns[0]));
  }
  {
    ParsedQuery pq =
        SparqlParser("SELECT ?x ?y WHERE { ?x <test/myrel> ?y } ORDER BY ?x")
            .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], m::VariableOrderKey("?x", false));
    ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1);
    ASSERT_TRUE(holds_alternative<p::BasicGraphPattern>(
        pq._rootGraphPattern._graphPatterns[0]));
  }
  {
    ParsedQuery pq =
        SparqlParser(
            "SELECT ?x ?y WHERE { ?x <test/myrel> ?y } ORDER BY ASC(?y)")
            .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], m::VariableOrderKey("?y", false));
    ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1);
    ASSERT_TRUE(holds_alternative<p::BasicGraphPattern>(
        pq._rootGraphPattern._graphPatterns[0]));
  }
  {
    ParsedQuery pq =
        SparqlParser(
            "SELECT ?x ?y WHERE { ?x <test/myrel> ?y } ORDER BY DESC(?foo)")
            .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], m::VariableOrderKey("?foo", true));
    ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1);
    ASSERT_TRUE(holds_alternative<p::BasicGraphPattern>(
        pq._rootGraphPattern._graphPatterns[0]));
  }
  {
    ParsedQuery pq =
        SparqlParser(
            "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY ?x ORDER BY ?x")
            .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], m::VariableOrderKey("?x", false));
    ASSERT_EQ(pq._rootGraphPattern._graphPatterns.size(), 1);
    ASSERT_TRUE(holds_alternative<p::BasicGraphPattern>(
        pq._rootGraphPattern._graphPatterns[0]));
  }
  {
    ParsedQuery pq = SparqlParser(
                         "SELECT ?x (COUNT(?y) as ?c) WHERE { ?x <test/myrel> "
                         "?y } GROUP BY ?x ORDER BY ?c")
                         .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], m::VariableOrderKey("?c", false));
  }
  {
    ParsedQuery pq =
        SparqlParser(
            "SELECT ?x ?y WHERE { ?x <test/myrel> ?y } ORDER BY (?x - ?y)")
            .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    auto variant = pq._rootGraphPattern._graphPatterns[1];
    ASSERT_TRUE(holds_alternative<p::Bind>(variant));
    auto helperBind = get<p::Bind>(variant);
    ASSERT_EQ(helperBind._expression.getDescriptor(), "?x-?y");
    ASSERT_EQ(pq._orderBy[0].variable_, helperBind._target);
  }
  {
    // Ordering by variables that are not grouped is not allowed.
    EXPECT_THROW(
        SparqlParser(
            "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY ?x ORDER BY ?y")
            .parse(),
        ParseException);
  }
  {
    // Ordering by an expression while grouping is currently not supported.
    EXPECT_THROW(SparqlParser("SELECT ?y WHERE { ?x <test/myrel> ?y } GROUP BY "
                              "?y ORDER BY (?x - ?y)")
                     .parse(),
                 ParseException);
  }
  {
    // Ordering by an expression while grouping is currently not supported.
    EXPECT_THROW(SparqlParser("SELECT ?y WHERE { ?x <test/myrel> ?y } GROUP BY "
                              "?y ORDER BY (2 * ?y)")
                     .parse(),
                 ParseException);
  }
}

TEST(ParserTest, Group) {
  {
    ParsedQuery pq =
        SparqlParser("SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY ?x")
            .parse();
    EXPECT_THAT(pq, m::GroupByVariables({Variable{"?x"}}));
  }
  {
    // grouping by a variable
    ParsedQuery pq =
        SparqlParser("SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY ?y ?x")
            .parse();
    EXPECT_THAT(pq, m::GroupByVariables({Variable{"?y"}, Variable{"?x"}}));
  }
  {
    // grouping by an expression
    ParsedQuery pq =
        SparqlParser(
            "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY (?x - ?y) ?x")
            .parse();
    auto variant = pq._rootGraphPattern._graphPatterns[1];
    ASSERT_TRUE(holds_alternative<p::Bind>(variant));
    auto helperBind = get<p::Bind>(variant);
    ASSERT_THAT(helperBind, m::BindExpression("?x-?y"));
    EXPECT_THAT(pq, m::GroupByVariables(
                        {Variable{helperBind._target}, Variable{"?x"}}));
  }
  {
    // grouping by an expression with an alias
    ParsedQuery pq = SparqlParser(
                         "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY (?x "
                         "- ?y AS ?foo) ?x")
                         .parse();
    EXPECT_THAT(pq._rootGraphPattern._graphPatterns[1],
                m::Bind("?foo", "?x-?y"));
    EXPECT_THAT(pq, m::GroupByVariables({Variable{"?foo"}, Variable{"?x"}}));
  }
  {
    // grouping by a builtin call
    ParsedQuery pq =
        SparqlParser(
            "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY COUNT(?x) ?x")
            .parse();
    auto variant = pq._rootGraphPattern._graphPatterns[1];
    ASSERT_TRUE(holds_alternative<p::Bind>(variant));
    auto helperBind = get<p::Bind>(variant);
    ASSERT_THAT(helperBind, m::BindExpression("COUNT(?x)"));
    EXPECT_THAT(pq, m::GroupByVariables(
                        {Variable{helperBind._target}, Variable{"?x"}}));
  }
  {
    // grouping by a function call
    ParsedQuery pq = SparqlParser(
                         "SELECT ?x WHERE { ?x <test/myrel> ?y } GROUP BY "
                         "<http://www.opengis.net/def/function/geosparql/"
                         "latitude> (?test) ?x")
                         .parse();
    auto variant = pq._rootGraphPattern._graphPatterns[1];
    ASSERT_TRUE(holds_alternative<p::Bind>(variant));
    auto helperBind = get<p::Bind>(variant);
    ASSERT_THAT(
        helperBind,
        m::BindExpression(
            "<http://www.opengis.net/def/function/geosparql/latitude>(?test)"));
    EXPECT_THAT(pq, m::GroupByVariables(
                        {Variable{helperBind._target}, Variable{"?x"}}));
  }
  {
    // selection of a variable that is not grouped/aggregated
    EXPECT_THROW(
        SparqlParser("SELECT ?x ?y WHERE { ?x <test/myrel> ?y } GROUP BY ?x")
            .parse(),
        ParseException);
  }
}

TEST(ParserTest, Prefix) {
  ParsedQuery pq =
      SparqlParser(
          "PREFIX descriptor: <foo> SELECT ?var WHERE { ?var <bar> <foo> }")
          .parse();
  ASSERT_THAT(pq._prefixes,
              testing::UnorderedElementsAre(
                  SparqlPrefix{"ql", "<QLever-internal-function/>"},
                  SparqlPrefix{"descriptor", "<foo>"}));
}

TEST(ParserTest, ParseFilterExpression) {
  auto f = SparqlParser::parseFilterExpression("(LANG(?x) = \"en\")", {});
  ASSERT_EQ(f, (SparqlFilter{SparqlFilter::LANG_MATCHES, "?x", "\"en\""}));

  f = SparqlParser::parseFilterExpression("(?x <= 42.3)", {});
  ASSERT_EQ(f, (SparqlFilter{SparqlFilter::LE, "?x", "42.3"}));

  f = SparqlParser::parseFilterExpression("(?x = me:you)",
                                          {{"me", "<www.me.de/>"}});
  ASSERT_EQ(f, (SparqlFilter{SparqlFilter::EQ, "?x", "<www.me.de/you>"}));
}

TEST(ParserTest, LanguageFilterPostProcessing) {
  {
    ParsedQuery q =
        SparqlParser(
            "SELECT * WHERE {?x <label> ?y . FILTER (LANG(?y) = \"en\")}")
            .parse();
    ASSERT_TRUE(q._rootGraphPattern._filters.empty());
    const auto& triples =
        q._rootGraphPattern._graphPatterns[0].getBasic()._triples;
    ASSERT_EQ(1u, triples.size());
    ASSERT_EQ((SparqlTriple{"?x", PropertyPath::fromIri("@en@<label>"), "?y"}),
              triples[0]);
  }
  {
    ParsedQuery q =
        SparqlParser(
            "SELECT * WHERE {<somebody> ?p ?y . FILTER (LANG(?y) = \"en\")}")
            .parse();
    ASSERT_TRUE(q._rootGraphPattern._filters.empty());
    const auto& triples =
        q._rootGraphPattern._graphPatterns[0].getBasic()._triples;
    ASSERT_EQ(2u, triples.size());
    ASSERT_EQ((SparqlTriple{"<somebody>", PropertyPath::fromIri("?p"), "?y"}),
              triples[0]);
    ASSERT_EQ(
        (SparqlTriple{
            "?y", PropertyPath::fromIri("<QLever-internal-function/langtag>"),
            "<QLever-internal-function/@en>"}),
        triples[1]);
  }
}

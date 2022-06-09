// Copyright 2014, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <variant>

#include "../src/global/Constants.h"
#include "../src/parser/PropertyPathParser.h"
#include "../src/parser/SparqlParser.h"
#include "SparqlAntlrParserTestHelpers.h"

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
      ASSERT_GT(pq.asString().size(), 0u);
      ASSERT_EQ(0u, pq._prefixes.size());
      ASSERT_EQ(1u, selectClause._varsOrAsterisk.getSelectedVariables().size());
      ASSERT_EQ(1u, pq._rootGraphPattern._children.size());
      ASSERT_EQ(1u, pq._rootGraphPattern._children[0]
                        .getBasic()
                        ._whereClauseTriples.size());
    }

    {
      auto pq = SparqlParser(
                    "PREFIX : <http://rdf.myprefix.com/>\n"
                    "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
                    "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
                    "SELECT ?x ?z \n "
                    "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y nsx:rel2 "
                    "<http://abc.de>}")
                    .parse();
      ASSERT_TRUE(pq.hasSelectClause());
      const auto& selectClause2 = pq.selectClause();
      ASSERT_EQ(3u, pq._prefixes.size());
      ASSERT_EQ(2u,
                selectClause2._varsOrAsterisk.getSelectedVariables().size());
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._whereClauseTriples;
      ASSERT_EQ(3u, triples.size());

      ASSERT_EQ("", pq._prefixes[0]._prefix);
      ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
      ASSERT_EQ("ns", pq._prefixes[1]._prefix);
      ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
      ASSERT_EQ("?x", selectClause2._varsOrAsterisk.getSelectedVariables()[0]);
      ASSERT_EQ("?z", selectClause2._varsOrAsterisk.getSelectedVariables()[1]);
      ASSERT_EQ("?x", triples[0]._s);
      ASSERT_EQ(":myrel", triples[0]._p._iri);
      ASSERT_EQ("?y", triples[0]._o);
      ASSERT_EQ("?y", triples[1]._s);
      ASSERT_EQ("ns:myrel", triples[1]._p._iri);
      ASSERT_EQ("?z", triples[1]._o);
      ASSERT_EQ("?y", triples[2]._s);
      ASSERT_EQ("nsx:rel2", triples[2]._p._iri);
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
                    "WHERE \t {\n?x :myrel ?y. ?y ns:myrel ?z.\n?y nsx:rel2 "
                    "<http://abc.de>\n}")
                    .parse();
      ASSERT_TRUE(pq.hasSelectClause());
      const auto& selectClause = pq.selectClause();
      ASSERT_EQ(3u, pq._prefixes.size());
      ASSERT_EQ(2u, selectClause._varsOrAsterisk.getSelectedVariables().size());
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._whereClauseTriples;
      ASSERT_EQ(3u, triples.size());

      ASSERT_EQ("", pq._prefixes[0]._prefix);
      ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
      ASSERT_EQ("ns", pq._prefixes[1]._prefix);
      ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
      ASSERT_EQ("?x", selectClause._varsOrAsterisk.getSelectedVariables()[0]);
      ASSERT_EQ("?z", selectClause._varsOrAsterisk.getSelectedVariables()[1]);
      ASSERT_EQ("?x", triples[0]._s);
      ASSERT_EQ(":myrel", triples[0]._p._iri);
      ASSERT_EQ("?y", triples[0]._o);
      ASSERT_EQ("?y", triples[1]._s);
      ASSERT_EQ("ns:myrel", triples[1]._p._iri);
      ASSERT_EQ("?z", triples[1]._o);
      ASSERT_EQ("?y", triples[2]._s);
      ASSERT_EQ("nsx:rel2", triples[2]._p._iri);
      ASSERT_EQ("<http://abc.de>", triples[2]._o);
      ASSERT_EQ(std::numeric_limits<uint64_t>::max(), pq._limitOffset._limit);
      ASSERT_EQ(0, pq._limitOffset._offset);
    }

    {
      auto pq = SparqlParser(
                    "PREFIX ns: <http://ns/>"
                    "SELECT ?x ?z \n "
                    "WHERE \t {\n?x <Directed_by> ?y. ?y ns:myrel.extend ?z.\n"
                    "?y nsx:rel2 \"Hello... World\"}")
                    .parse();
      ASSERT_TRUE(pq.hasSelectClause());
      const auto& selectClause = pq.selectClause();
      ASSERT_EQ(1u, pq._prefixes.size());
      ASSERT_EQ(2u, selectClause._varsOrAsterisk.getSelectedVariables().size());
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._whereClauseTriples;
      ASSERT_EQ(3u, triples.size());

      pq.expandPrefixes();

      ASSERT_EQ("?x", selectClause._varsOrAsterisk.getSelectedVariables()[0]);
      ASSERT_EQ("?z", selectClause._varsOrAsterisk.getSelectedVariables()[1]);
      ASSERT_EQ("?x", triples[0]._s);
      ASSERT_EQ("<Directed_by>", triples[0]._p._iri);
      ASSERT_EQ("?y", triples[0]._o);
      ASSERT_EQ("?y", triples[1]._s);
      ASSERT_EQ("<http://ns/myrel.extend>", triples[1]._p._iri);
      ASSERT_EQ("?z", triples[1]._o);
      ASSERT_EQ("?y", triples[2]._s);
      ASSERT_EQ("nsx:rel2", triples[2]._p._iri);
      ASSERT_EQ("\"Hello... World\"", triples[2]._o);
      ASSERT_EQ(std::numeric_limits<uint64_t>::max(), pq._limitOffset._limit);
      ASSERT_EQ(0, pq._limitOffset._offset);
    }

    {
      auto pq = SparqlParser(
                    "SELECT ?x ?y WHERE {?x <is-a> <Actor> .  FILTER(?x != ?y)."
                    "?y <is-a> <Actor> . FILTER(?y < ?x)} LIMIT 10")
                    .parse();
      pq.expandPrefixes();
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._whereClauseTriples;
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
      pq.expandPrefixes();
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._whereClauseTriples;
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
      pq.expandPrefixes();
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._whereClauseTriples;
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
                    "SELECT ?x ?y ?z TEXT(?c) SCORE(?c) ?c WHERE {\n"
                    "?x :is-a :Politician .\n"
                    "?c ql:contains-entity ?x .\n"
                    "?c ql:contains-word \"friend\" .\n"
                    "?c ql:contains-entity ?y .\n"
                    "?y :is-a :Scientist .\n"
                    "FILTER(?x != ?y) .\n"
                    "} ORDER BY ?c")
                    .parse();
      pq.expandPrefixes();
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
          pq._rootGraphPattern._children[1]
              .get<GraphPatternOperation::Optional>();  // throws on error
      auto& child = opt._child;
      const auto& triples = child._children[0].getBasic()._whereClauseTriples;
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
      ASSERT_EQ(2u, pq._rootGraphPattern._children.size());
      const auto& optA =
          pq._rootGraphPattern._children[1]
              .get<GraphPatternOperation::Optional>();  // throws on error
      auto& child = optA._child;
      ASSERT_EQ(3u, child._children.size());
      const auto& opt2 =
          child._children[1]
              .get<GraphPatternOperation::Optional>();  // throws on error
      const auto& opt3 =
          child._children[2]
              .get<GraphPatternOperation::Optional>();  // throws on error
      const auto& child2 = opt2._child._children[0].getBasic();
      const auto& child3 = opt3._child._children[0].getBasic();
      ASSERT_EQ(1u, child2._whereClauseTriples.size());
      ASSERT_EQ(1u, opt2._child._filters.size());
      ASSERT_EQ(1u, child3._whereClauseTriples.size());
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
      ASSERT_EQ(3u, pq._rootGraphPattern._children.size());
      const auto& c = pq.children()[2].getBasic();
      ASSERT_EQ(1u, c._whereClauseTriples.size());
      ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());
      const auto& values1 =
          pq.children()[0].get<GraphPatternOperation::Values>()._inlineValues;
      const auto& values2 =
          pq.children()[1].get<GraphPatternOperation::Values>()._inlineValues;

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
      const auto& values1 =
          pq.children()[0].get<GraphPatternOperation::Values>()._inlineValues;
      const auto& values2 =
          pq.children()[1].get<GraphPatternOperation::Values>()._inlineValues;

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
      ASSERT_EQ(1u, c._whereClauseTriples.size());
      ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());

      ASSERT_EQ(c._whereClauseTriples[0]._s, "?city");
      ASSERT_EQ(c._whereClauseTriples[0]._p._iri, "wdt:P31");
      ASSERT_EQ(c._whereClauseTriples[0]._o, "?citytype");

      const auto& values1 =
          pq.children()[0].get<GraphPatternOperation::Values>()._inlineValues;
      vector<string> vvars = {"?citytype"};
      ASSERT_EQ(vvars, values1._variables);
      vector<vector<string>> vvals = {{"wd:Q515"}, {"wd:Q262166"}};
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
                    "LIMIT 10 \n"
                    "ORDER BY ASC(?movie)\n")
                    .parse();
      ASSERT_EQ(0u, pq._prefixes.size());
      ASSERT_EQ(1u, pq._rootGraphPattern._children.size());

      const auto& c = pq.children()[0].getBasic();
      ASSERT_EQ(1u, c._whereClauseTriples.size());
      ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());

      ASSERT_EQ(c._whereClauseTriples[0]._s, "?movie");
      ASSERT_EQ(c._whereClauseTriples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c._whereClauseTriples[0]._o, "?director");

      ASSERT_EQ(10u, pq._limitOffset._limit);
      ASSERT_EQ(false, pq._orderBy[0].isDescending_);
      ASSERT_EQ("?movie", pq._orderBy[0].variable_);

      auto sc = get<ParsedQuery::SelectClause>(pq._clause);
      ASSERT_EQ(true, sc._reduced);
      ASSERT_EQ(true, sc._varsOrAsterisk.isAllVariablesSelected());

      vector<string> vvars = {"?movie", "?director"};
      ASSERT_EQ(vvars, sc._varsOrAsterisk.getSelectedVariables());
    }

    {
      auto pq = SparqlParser(
                    "SELECT DISTINCT * WHERE { \n"
                    "  ?movie <directed-by> ?director .\n"
                    "} \n"
                    "LIMIT 10 \n"
                    "ORDER BY DESC(?movie)\n")
                    .parse();

      ASSERT_EQ(0u, pq._prefixes.size());
      ASSERT_EQ(1u, pq._rootGraphPattern._children.size());

      const auto& c = pq.children()[0].getBasic();
      ASSERT_EQ(1u, c._whereClauseTriples.size());
      ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());

      ASSERT_EQ(c._whereClauseTriples[0]._s, "?movie");
      ASSERT_EQ(c._whereClauseTriples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c._whereClauseTriples[0]._o, "?director");

      ASSERT_EQ(10u, pq._limitOffset._limit);
      ASSERT_EQ(true, pq._orderBy[0].isDescending_);
      ASSERT_EQ("?movie", pq._orderBy[0].variable_);

      auto sc = get<ParsedQuery::SelectClause>(pq._clause);
      ASSERT_EQ(true, sc._distinct);
      ASSERT_EQ(true, sc._varsOrAsterisk.isAllVariablesSelected());

      vector<string> vvars = {"?movie", "?director"};
      ASSERT_EQ(vvars, sc._varsOrAsterisk.getSelectedVariables());
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
                    "LIMIT 20 \n"
                    "OFFSET 3 \n"
                    "ORDER BY DESC(?movie)\n")
                    .parse();

      ASSERT_EQ(0u, pq._prefixes.size());
      ASSERT_EQ(2u, pq._rootGraphPattern._children.size());

      const auto& c = pq.children()[0].getBasic();
      ASSERT_EQ(1u, c._whereClauseTriples.size());
      ASSERT_EQ(0, pq._rootGraphPattern._filters.size());
      ASSERT_EQ(3u, pq._limitOffset._offset);

      ASSERT_EQ(c._whereClauseTriples[0]._s, "?movie");
      ASSERT_EQ(c._whereClauseTriples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c._whereClauseTriples[0]._o, "<Scott%2C%20Ridley>");

      ASSERT_EQ(20u, pq._limitOffset._limit);
      ASSERT_EQ(true, pq._orderBy[0].isDescending_);
      ASSERT_EQ("?movie", pq._orderBy[0].variable_);

      auto sc = get<ParsedQuery::SelectClause>(pq._clause);
      ASSERT_EQ(true, sc._distinct);
      ASSERT_EQ(true, sc._varsOrAsterisk.isAllVariablesSelected());

      vector<string> vvars = {"?movie", "?director", "?year"};
      ASSERT_EQ(vvars, sc._varsOrAsterisk.getSelectedVariables());

      // -- SubQuery
      auto parsed_sub_query = get<GraphPatternOperation::Subquery>(
          pq._rootGraphPattern._children[1].variant_);
      const auto& c_subquery = get<GraphPatternOperation::BasicGraphPattern>(
          parsed_sub_query._subquery._rootGraphPattern._children[0].variant_);
      ASSERT_EQ(2u, c_subquery._whereClauseTriples.size());
      ASSERT_EQ(1u,
                parsed_sub_query._subquery._rootGraphPattern._filters.size());
      ASSERT_EQ("?year",
                parsed_sub_query._subquery._rootGraphPattern._filters[0]._lhs);
      ASSERT_EQ("\"00-00-2000\"",
                parsed_sub_query._subquery._rootGraphPattern._filters[0]._rhs);
      ASSERT_EQ(SparqlFilter::GT,
                parsed_sub_query._subquery._rootGraphPattern._filters[0]._type);
      ASSERT_EQ(0, parsed_sub_query._subquery._limitOffset._offset);

      ASSERT_EQ(c_subquery._whereClauseTriples[0]._s, "?movie");
      ASSERT_EQ(c_subquery._whereClauseTriples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c_subquery._whereClauseTriples[0]._o, "?director");

      ASSERT_EQ(c_subquery._whereClauseTriples[1]._s, "?movie");
      ASSERT_EQ(c_subquery._whereClauseTriples[1]._p._iri, "<from-year>");
      ASSERT_EQ(c_subquery._whereClauseTriples[1]._o, "?year");

      ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
                parsed_sub_query._subquery._limitOffset._limit);
      ASSERT_EQ(true, parsed_sub_query._subquery._orderBy[0].isDescending_);
      ASSERT_EQ("?director", parsed_sub_query._subquery._orderBy[0].variable_);

      auto sc_subquery =
          get<ParsedQuery::SelectClause>(parsed_sub_query._subquery._clause);
      ASSERT_EQ(false, sc_subquery._distinct);
      ASSERT_EQ(false, sc_subquery._reduced);
      ASSERT_EQ(true, sc_subquery._varsOrAsterisk.isAllVariablesSelected());
      vector<string> vvars_subquery = {"?movie", "?director", "?year"};
      ASSERT_EQ(vvars_subquery,
                sc_subquery._varsOrAsterisk.getSelectedVariables());
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
                    "LIMIT 20 \n"
                    "OFFSET 3 \n"
                    "ORDER BY DESC(?movie)\n")
                    .parse();

      ASSERT_EQ(0u, pq._prefixes.size());
      ASSERT_EQ(2u, pq._rootGraphPattern._children.size());

      const auto& c = pq.children()[0].getBasic();
      ASSERT_EQ(1u, c._whereClauseTriples.size());
      ASSERT_EQ(0, pq._rootGraphPattern._filters.size());
      ASSERT_EQ(3u, pq._limitOffset._offset);

      ASSERT_EQ(c._whereClauseTriples[0]._s, "?movie");
      ASSERT_EQ(c._whereClauseTriples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c._whereClauseTriples[0]._o, "<Scott%2C%20Ridley>");

      ASSERT_EQ(20u, pq._limitOffset._limit);
      ASSERT_EQ(true, pq._orderBy[0].isDescending_);
      ASSERT_EQ("?movie", pq._orderBy[0].variable_);

      auto sc = get<ParsedQuery::SelectClause>(pq._clause);
      ASSERT_EQ(true, sc._distinct);
      ASSERT_EQ(true, sc._varsOrAsterisk.isAllVariablesSelected());

      vector<string> vvars = {"?movie", "?director", "?year"};
      ASSERT_EQ(vvars, sc._varsOrAsterisk.getSelectedVariables());

      // -- SubQuery (level 1)
      auto parsed_sub_query = get<GraphPatternOperation::Subquery>(
          pq._rootGraphPattern._children[1].variant_);
      const auto& c_subquery = get<GraphPatternOperation::BasicGraphPattern>(
          parsed_sub_query._subquery._rootGraphPattern._children[0].variant_);
      ASSERT_EQ(1u, c_subquery._whereClauseTriples.size());
      ASSERT_EQ(1u,
                parsed_sub_query._subquery._rootGraphPattern._filters.size());
      ASSERT_EQ("?year",
                parsed_sub_query._subquery._rootGraphPattern._filters[0]._lhs);
      ASSERT_EQ("\"00-00-2000\"",
                parsed_sub_query._subquery._rootGraphPattern._filters[0]._rhs);
      ASSERT_EQ(SparqlFilter::GT,
                parsed_sub_query._subquery._rootGraphPattern._filters[0]._type);
      ASSERT_EQ(0, parsed_sub_query._subquery._limitOffset._offset);

      ASSERT_EQ(c_subquery._whereClauseTriples[0]._s, "?movie");
      ASSERT_EQ(c_subquery._whereClauseTriples[0]._p._iri, "<directed-by>");
      ASSERT_EQ(c_subquery._whereClauseTriples[0]._o, "?director");

      ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
                parsed_sub_query._subquery._limitOffset._limit);
      ASSERT_EQ(true, parsed_sub_query._subquery._orderBy[0].isDescending_);
      ASSERT_EQ("?director", parsed_sub_query._subquery._orderBy[0].variable_);

      auto sc_subquery =
          get<ParsedQuery::SelectClause>(parsed_sub_query._subquery._clause);
      ASSERT_EQ(false, sc_subquery._distinct);
      ASSERT_EQ(false, sc_subquery._reduced);
      ASSERT_EQ(true, sc_subquery._varsOrAsterisk.isAllVariablesSelected());
      vector<string> vvars_subquery = {"?movie", "?director", "?year"};
      ASSERT_EQ(vvars_subquery,
                sc_subquery._varsOrAsterisk.getSelectedVariables());

      // -- SubQuery (level 2)
      auto parsed_sub_sub_query = get<GraphPatternOperation::Subquery>(
          pq._rootGraphPattern._children[1].variant_);
      const auto& c_sub_subquery =
          get<GraphPatternOperation::BasicGraphPattern>(
              get<GraphPatternOperation::Subquery>(
                  parsed_sub_sub_query._subquery._rootGraphPattern._children[1]
                      .variant_)
                  ._subquery._rootGraphPattern._children[0]
                  .variant_);
      auto aux_parsed_sub_sub_query =
          get<GraphPatternOperation::Subquery>(
              parsed_sub_sub_query._subquery._rootGraphPattern._children[1]
                  .variant_)
              ._subquery;
      ASSERT_EQ(1u, c_sub_subquery._whereClauseTriples.size());
      ASSERT_EQ(0u, aux_parsed_sub_sub_query._rootGraphPattern._filters.size());
      ASSERT_EQ(0, aux_parsed_sub_sub_query._limitOffset._offset);

      ASSERT_EQ(c_sub_subquery._whereClauseTriples[0]._s, "?movie");
      ASSERT_EQ(c_sub_subquery._whereClauseTriples[0]._p._iri, "<from-year>");
      ASSERT_EQ(c_sub_subquery._whereClauseTriples[0]._o, "?year");

      ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
                aux_parsed_sub_sub_query._limitOffset._limit);
      ASSERT_EQ(0u, aux_parsed_sub_sub_query._orderBy.size());

      auto sc_sub_subquery =
          get<ParsedQuery::SelectClause>(aux_parsed_sub_sub_query._clause);
      ASSERT_EQ(false, sc_sub_subquery._distinct);
      ASSERT_EQ(false, sc_sub_subquery._reduced);
      ASSERT_EQ(true,
                sc_sub_subquery._varsOrAsterisk.isManuallySelectedVariables());
      vector<string> vvars_sub_subquery = {"?year"};
      ASSERT_EQ(vvars_sub_subquery,
                sc_sub_subquery._varsOrAsterisk.getSelectedVariables());
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
  pq.expandPrefixes();
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(1u, pq._prefixes.size());
  ASSERT_EQ(1u, selectClause._varsOrAsterisk.getSelectedVariables().size());
  ASSERT_EQ(1u, pq.children().size());
  const auto& c = pq.children()[0].getBasic();
  ASSERT_EQ(3u, c._whereClauseTriples.size());
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
  ParsedQuery pq =
      SparqlParser(
          "PREFIX : <http://rdf.myprefix.com/>\n"
          "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
          "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
          "SELECT ?x ?z \n "
          "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y nsx:rel2 <http://abc.de>}")
          .parse();
  pq.expandPrefixes();
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(1u, pq.children().size());
  const auto& c = pq.children()[0].getBasic();
  ASSERT_EQ(3u, pq._prefixes.size());
  ASSERT_EQ(2u, selectClause._varsOrAsterisk.getSelectedVariables().size());
  ASSERT_EQ(3u, c._whereClauseTriples.size());
  ASSERT_EQ("", pq._prefixes[0]._prefix);
  ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
  ASSERT_EQ("ns", pq._prefixes[1]._prefix);
  ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
  ASSERT_EQ("?x", selectClause._varsOrAsterisk.getSelectedVariables()[0]);
  ASSERT_EQ("?z", selectClause._varsOrAsterisk.getSelectedVariables()[1]);
  ASSERT_EQ("?x", c._whereClauseTriples[0]._s);
  ASSERT_EQ("<http://rdf.myprefix.com/myrel>",
            c._whereClauseTriples[0]._p._iri);
  ASSERT_EQ("?y", c._whereClauseTriples[0]._o);
  ASSERT_EQ("?y", c._whereClauseTriples[1]._s);
  ASSERT_EQ("<http://rdf.myprefix.com/ns/myrel>",
            c._whereClauseTriples[1]._p._iri);
  ASSERT_EQ("?z", c._whereClauseTriples[1]._o);
  ASSERT_EQ("?y", c._whereClauseTriples[2]._s);
  ASSERT_EQ("nsx:rel2", c._whereClauseTriples[2]._p._iri);
  ASSERT_EQ("<http://abc.de>", c._whereClauseTriples[2]._o);
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(), pq._limitOffset._limit);
  ASSERT_EQ(0, pq._limitOffset._offset);
}

TEST(ParserTest, testSolutionModifiers) {
  {
    ParsedQuery pq = SparqlParser("SELECT ?x WHERE \t {?x :myrel ?y}").parse();
    pq.expandPrefixes();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(1u, selectClause._varsOrAsterisk.getSelectedVariables().size());
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), pq._limitOffset._limit);
    ASSERT_EQ(0, pq._limitOffset._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(selectClause._distinct);
    ASSERT_FALSE(selectClause._reduced);
  }

  {
    auto pq =
        SparqlParser("SELECT ?x WHERE \t {?x :myrel ?y} LIMIT 10").parse();
    pq.expandPrefixes();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(1u, selectClause._varsOrAsterisk.getSelectedVariables().size());
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ(10ul, pq._limitOffset._limit);
    ASSERT_EQ(0, pq._limitOffset._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(selectClause._distinct);
    ASSERT_FALSE(selectClause._reduced);
  }

  {
    auto pq = SparqlParser(
                  "SELECT ?x WHERE \t {?x :myrel ?y}\n"
                  "LIMIT 10 OFFSET 15")
                  .parse();
    pq.expandPrefixes();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(1u, selectClause._varsOrAsterisk.getSelectedVariables().size());
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(selectClause._distinct);
    ASSERT_FALSE(selectClause._reduced);
  }

  {
    auto pq = SparqlParser(
                  "SELECT DISTINCT ?x ?y WHERE \t {?x :myrel ?y}\n"
                  "ORDER BY ?y LIMIT 10 OFFSET 15")
                  .parse();
    pq.expandPrefixes();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(2u, selectClause._varsOrAsterisk.getSelectedVariables().size());
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(1), pq._orderBy.size());
    ASSERT_EQ("?y", pq._orderBy[0].variable_);
    ASSERT_FALSE(pq._orderBy[0].isDescending_);
    ASSERT_TRUE(selectClause._distinct);
    ASSERT_FALSE(selectClause._reduced);
  }

  // TODO Figure out how to readd SCORE in a clean way.
  //  {
  //    auto pq = SparqlParser(
  //                  "SELECT DISTINCT ?x SCORE(?x) ?y WHERE \t {?x :myrel
  //                  ?y}\n" "ORDER BY ASC(?y) DESC(SCORE(?X)) LIMIT 10 OFFSET
  //                  15") .parse();
  //    pq.expandPrefixes();
  //    ASSERT_TRUE(pq.hasSelectClause());
  //    const auto& selectClause = pq.selectClause();
  //    ASSERT_EQ(1u, pq.children().size());
  //    const auto& c = pq.children()[0].getBasic();
  //    ASSERT_EQ(0u, pq._prefixes.size());
  //    ASSERT_EQ(3u,
  //    selectClause._varsOrAsterisk.getSelectedVariables().size());
  //    ASSERT_EQ("SCORE(?x)",
  //              selectClause._varsOrAsterisk.getSelectedVariables()[1]);
  //    ASSERT_EQ(1u, c._whereClauseTriples.size());
  //    ASSERT_EQ(10u, pq._limitOffset._limit);
  //    ASSERT_EQ(15u, pq._limitOffset._offset);
  //    ASSERT_EQ(size_t(2), pq._orderBy.size());
  //    ASSERT_EQ("?y", pq._orderBy[0].variable_);
  //    ASSERT_FALSE(pq._orderBy[0].isDescending_);
  //    ASSERT_EQ("SCORE(?x)", pq._orderBy[1].variable_);
  //    ASSERT_TRUE(pq._orderBy[1].isDescending_);
  //    ASSERT_TRUE(selectClause._distinct);
  //    ASSERT_FALSE(selectClause._reduced);
  //  }

  {
    auto pq = SparqlParser(
                  "SELECT REDUCED ?x ?y WHERE \t {?x :myrel ?y}\n"
                  "ORDER BY DESC(?x) ASC(?y) LIMIT 10 OFFSET 15")
                  .parse();
    pq.expandPrefixes();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(2u, selectClause._varsOrAsterisk.getSelectedVariables().size());
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ(10u, pq._limitOffset._limit);
    ASSERT_EQ(15u, pq._limitOffset._offset);
    ASSERT_EQ(size_t(2), pq._orderBy.size());
    ASSERT_EQ("?x", pq._orderBy[0].variable_);
    ASSERT_TRUE(pq._orderBy[0].isDescending_);
    ASSERT_EQ("?y", pq._orderBy[1].variable_);
    ASSERT_FALSE(pq._orderBy[1].isDescending_);
    ASSERT_FALSE(selectClause._distinct);
    ASSERT_TRUE(selectClause._reduced);
  }

  {
    auto pq =
        SparqlParser("SELECT ?x ?y WHERE {?x <is-a> <Actor>} LIMIT 10").parse();
    pq.expandPrefixes();
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
    pq.expandPrefixes();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, pq._prefixes.size());
    ASSERT_EQ(1u, selectClause._varsOrAsterisk.getSelectedVariables().size());
    ASSERT_EQ("?movie", selectClause._varsOrAsterisk.getSelectedVariables()[0]);
    ASSERT_EQ(2u, c._whereClauseTriples.size());
    ASSERT_EQ("?movie", c._whereClauseTriples[0]._s);
    ASSERT_EQ("<from-year>", c._whereClauseTriples[0]._p._iri);
    ASSERT_EQ(":v:date:0000000000000002000-00-00T00:00:00",
              c._whereClauseTriples[0]._o);
    ASSERT_EQ("?movie", c._whereClauseTriples[1]._s);
    ASSERT_EQ("<directed-by>", c._whereClauseTriples[1]._p._iri);
    ASSERT_EQ("<Scott%2C%20Ridley>", c._whereClauseTriples[1]._o);
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
    pq.expandPrefixes();
    ASSERT_TRUE(pq.hasSelectClause());
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, pq._prefixes.size());
    ASSERT_EQ(1u, selectClause._varsOrAsterisk.getSelectedVariables().size());
    ASSERT_EQ("?movie", selectClause._varsOrAsterisk.getSelectedVariables()[0]);
    ASSERT_EQ(2u, c._whereClauseTriples.size());
    ASSERT_EQ("?movie", c._whereClauseTriples[0]._s);
    ASSERT_EQ("<from-year>", c._whereClauseTriples[0]._p._iri);
    ASSERT_EQ("\"00-00-2000\"^^<http://www.w3.org/2010/XMLSchema#date>",
              c._whereClauseTriples[0]._o);
    ASSERT_EQ("?movie", c._whereClauseTriples[1]._s);
    ASSERT_EQ("<directed-by>", c._whereClauseTriples[1]._p._iri);
    ASSERT_EQ("<Scott%2C%20Ridley>", c._whereClauseTriples[1]._o);
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
    ASSERT_EQ(1u, pq._groupByVariables.size());
    ASSERT_EQ(1u, pq._orderBy.size());
    ASSERT_EQ("?r", pq._groupByVariables[0]);
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
    ASSERT_EQ(1u, pq._groupByVariables.size());
    ASSERT_EQ(1u, pq._orderBy.size());
    ASSERT_EQ("?r", pq._groupByVariables[0]);
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
    const auto& selectClause = pq.selectClause();
    ASSERT_EQ(1u, selectClause._aliases.size());
    ASSERT_EQ("(group_concat(?r;SEPARATOR=\"Cake\") as ?concat)",
              selectClause._aliases[0].getDescriptor());
  }
}

TEST(ParserTest, testGroupByAndAlias) {
  ParsedQuery pq =
      SparqlParser(
          "SELECT (COUNT(?a) as ?count) WHERE { ?b <rel> ?a } GROUP BY ?b")
          .parse();
  ASSERT_TRUE(pq.hasSelectClause());
  const auto& selectClause = pq.selectClause();
  ASSERT_EQ(1u, selectClause._varsOrAsterisk.getSelectedVariables().size());
  ASSERT_EQ("?count", selectClause._varsOrAsterisk.getSelectedVariables()[0]);
  ASSERT_EQ(1u, selectClause._aliases.size());
  ASSERT_TRUE(selectClause._aliases[0]._expression.isAggregate({}));
  ASSERT_EQ("(count(?a) as ?count)", selectClause._aliases[0].getDescriptor());
  ASSERT_EQ(1u, pq._groupByVariables.size());
  ASSERT_EQ("?b", pq._groupByVariables[0]);
}

TEST(ParserTest, testParseLiteral) {
  using std::string;
  // Test a basic parse of a simple xsd string
  string inp = "   \"Astronaut\"^^xsd:string  \t";
  string ret = SparqlParser::parseLiteral(pqDummy, inp, true).getString();
  ASSERT_EQ("\"Astronaut\"^^<http://www.w3.org/2001/XMLSchema#string>", ret);

  inp = "\"1950-01-01T00:00:00\"^^xsd:dateTime";
  ret = SparqlParser::parseLiteral(pqDummy, inp, true).getString();
  ASSERT_EQ(":v:date:0000000000000001950-01-01T00:00:00", ret);

  // Check that `parseLiteral` fails on the following string, which is not a
  // literal.
  inp = R"(?a ?b "The \"Moon\""@en .)";
  ASSERT_THROW(SparqlParser::parseLiteral(pqDummy, inp, true), ParseException);

  // Do not escape quotation marks with the isEntireString check
  inp = R"(?a ?b "The \"Moon""@en)";
  ASSERT_THROW(SparqlParser::parseLiteral(pqDummy, inp, true, 6),
               ParseException);
}

TEST(ParserTest, propertyPaths) {
  using Op = PropertyPath::Operation;
  std::string inp = "a/b*|c|(a/b/<a/b/c>)+";
  PropertyPath result = PropertyPathParser(inp).parse();
  PropertyPath expected = PropertyPath(
      Op::ALTERNATIVE, 0, std::string(),
      {PropertyPath(Op::SEQUENCE, 0, std::string(),
                    {
                        PropertyPath(Op::IRI, 0, "a", {}),
                        PropertyPath(Op::TRANSITIVE, 0, std::string(),
                                     {PropertyPath(Op::IRI, 0, "b", {})}),
                    }),
       PropertyPath(Op::IRI, 0, "c", {}),
       PropertyPath(
           Op::TRANSITIVE_MIN, 1, std::string(),
           {PropertyPath(Op::SEQUENCE, 0, std::string(),
                         {PropertyPath(Op::IRI, 0, "a", {}),
                          PropertyPath(Op::IRI, 0, "b", {}),
                          PropertyPath(Op::IRI, 0, "<a/b/c>", {})})})});
  expected.computeCanBeNull();
  expected._can_be_null = false;
  ASSERT_EQ(expected, result);

  // Ensure whitespace is not accepted
  inp = "a | b\t / \nc";
  ASSERT_THROW(PropertyPathParser(inp).parse(), ParseException);
}

TEST(ParserTest, Bind) {
  ParsedQuery pq =
      SparqlParser("SELECT ?a WHERE { BIND (10 - 5 as ?a) . }").parse();
  ASSERT_TRUE(pq.hasSelectClause());
  ASSERT_EQ(pq.children().size(), 1);
  GraphPatternOperation child = pq.children()[0];
  ASSERT_TRUE(holds_alternative<GraphPatternOperation::Bind>(child.variant_));
  GraphPatternOperation::Bind bind =
      get<GraphPatternOperation::Bind>(child.variant_);
  ASSERT_EQ(bind._target, "?a");
  ASSERT_EQ(bind._expression.getDescriptor(), "10-5");
}

TEST(ParserTest, Order) {
  {
    ParsedQuery pq =
        SparqlParser("SELECT ?x ?y WHERE { ?x :myrel ?y }").parse();
    ASSERT_TRUE(pq._orderBy.empty());
    ASSERT_EQ(pq._rootGraphPattern._children.size(), 1);
    ASSERT_TRUE(holds_alternative<GraphPatternOperation::BasicGraphPattern>(
        pq._rootGraphPattern._children[0].variant_));
  }
  {
    ParsedQuery pq =
        SparqlParser("SELECT ?x ?y WHERE { ?x :myrel ?y } ORDER BY ?x").parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], IsVariableOrderKey("?x", false));
    ASSERT_EQ(pq._rootGraphPattern._children.size(), 1);
    ASSERT_TRUE(holds_alternative<GraphPatternOperation::BasicGraphPattern>(
        pq._rootGraphPattern._children[0].variant_));
  }
  {
    ParsedQuery pq =
        SparqlParser("SELECT ?x ?y WHERE { ?x :myrel ?y } ORDER BY ASC(?y)")
            .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], IsVariableOrderKey("?y", false));
    ASSERT_EQ(pq._rootGraphPattern._children.size(), 1);
    ASSERT_TRUE(holds_alternative<GraphPatternOperation::BasicGraphPattern>(
        pq._rootGraphPattern._children[0].variant_));
  }
  {
    ParsedQuery pq =
        SparqlParser("SELECT ?x ?y WHERE { ?x :myrel ?y } ORDER BY DESC(?foo)")
            .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], IsVariableOrderKey("?foo", true));
    ASSERT_EQ(pq._rootGraphPattern._children.size(), 1);
    ASSERT_TRUE(holds_alternative<GraphPatternOperation::BasicGraphPattern>(
        pq._rootGraphPattern._children[0].variant_));
  }
  {
    ParsedQuery pq =
        SparqlParser(
            "SELECT ?x WHERE { ?x :myrel ?y } GROUP BY ?x ORDER BY "
            "?x")
            .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], IsVariableOrderKey("?x", false));
    ASSERT_EQ(pq._rootGraphPattern._children.size(), 1);
    ASSERT_TRUE(holds_alternative<GraphPatternOperation::BasicGraphPattern>(
        pq._rootGraphPattern._children[0].variant_));
  }
  {
    ParsedQuery pq =
        SparqlParser(
            "SELECT ?x (COUNT(?y) as ?c) WHERE { ?x :myrel ?y } GROUP "
            "BY ?x ORDER BY ?c")
            .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    EXPECT_THAT(pq._orderBy[0], IsVariableOrderKey("?c", false));
  }
  {
    ParsedQuery pq =
        SparqlParser("SELECT ?x ?y WHERE { ?x :myrel ?y } ORDER BY (?x - ?y)")
            .parse();
    ASSERT_EQ(pq._orderBy.size(), 1);
    auto variant = pq._rootGraphPattern._children[1].variant_;
    ASSERT_TRUE(holds_alternative<GraphPatternOperation::Bind>(variant));
    auto helperBind = get<GraphPatternOperation::Bind>(variant);
    ASSERT_EQ(helperBind._expression.getDescriptor(), "?x-?y");
    ASSERT_EQ(pq._orderBy[0].variable_, helperBind._target);
  }
  {
    // Ordering by variables that are not grouped is not allowed.
    EXPECT_THROW(
        SparqlParser("SELECT ?x WHERE { ?x :myrel ?y } GROUP BY ?x ORDER BY "
                     "?y")
            .parse(),
        ParseException);
  }
  {
    // Ordering by an expression while grouping is currently not supported.
    EXPECT_THROW(
        SparqlParser("SELECT ?y WHERE { ?x :myrel ?y } GROUP BY ?y ORDER BY "
                     "(?x - ?y)")
            .parse(),
        ParseException);
  }
  {
    // Ordering by an expression while grouping is currently not supported.
    EXPECT_THROW(
        SparqlParser("SELECT ?y WHERE { ?x :myrel ?y } GROUP BY ?y ORDER BY "
                     "(2 * ?y)")
            .parse(),
        ParseException);
  }
}

// Copyright 2014, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include "../src/global/Constants.h"
#include "../src/parser/PropertyPathParser.h"
#include "../src/parser/SparqlParser.h"
#include "../src/util/Exception.h"

TEST(ParserTest, testParse) {
  try {
    {
      ParsedQuery pq = SparqlParser("SELECT ?x WHERE {?x ?y ?z}").parse();
      ASSERT_GT(pq.asString().size(), 0u);
      ASSERT_EQ(0u, pq._prefixes.size());
      ASSERT_EQ(1u, pq._selectClause._selectedVariables.size());
      ASSERT_EQ(1u, pq._rootGraphPattern._children.size());
      ASSERT_EQ(1u, pq._rootGraphPattern._children[0]
                        .getBasic()
                        ._whereClauseTriples.size());

      pq = SparqlParser(
               "PREFIX : <http://rdf.myprefix.com/>\n"
               "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
               "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
               "SELECT ?x ?z \n "
               "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y nsx:rel2 "
               "<http://abc.de>}")
               .parse();
      ASSERT_EQ(3u, pq._prefixes.size());
      ASSERT_EQ(2u, pq._selectClause._selectedVariables.size());
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._whereClauseTriples;
      ASSERT_EQ(3u, triples.size());

      ASSERT_EQ("", pq._prefixes[0]._prefix);
      ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
      ASSERT_EQ("ns", pq._prefixes[1]._prefix);
      ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
      ASSERT_EQ("?x", pq._selectClause._selectedVariables[0]);
      ASSERT_EQ("?z", pq._selectClause._selectedVariables[1]);
      ASSERT_EQ("?x", triples[0]._s);
      ASSERT_EQ(":myrel", triples[0]._p._iri);
      ASSERT_EQ("?y", triples[0]._o);
      ASSERT_EQ("?y", triples[1]._s);
      ASSERT_EQ("ns:myrel", triples[1]._p._iri);
      ASSERT_EQ("?z", triples[1]._o);
      ASSERT_EQ("?y", triples[2]._s);
      ASSERT_EQ("nsx:rel2", triples[2]._p._iri);
      ASSERT_EQ("<http://abc.de>", triples[2]._o);
      ASSERT_EQ("", pq._limit);
      ASSERT_EQ("", pq._offset);
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
      ASSERT_EQ(3u, pq._prefixes.size());
      ASSERT_EQ(2u, pq._selectClause._selectedVariables.size());
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._whereClauseTriples;
      ASSERT_EQ(3u, triples.size());

      ASSERT_EQ("", pq._prefixes[0]._prefix);
      ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
      ASSERT_EQ("ns", pq._prefixes[1]._prefix);
      ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
      ASSERT_EQ("?x", pq._selectClause._selectedVariables[0]);
      ASSERT_EQ("?z", pq._selectClause._selectedVariables[1]);
      ASSERT_EQ("?x", triples[0]._s);
      ASSERT_EQ(":myrel", triples[0]._p._iri);
      ASSERT_EQ("?y", triples[0]._o);
      ASSERT_EQ("?y", triples[1]._s);
      ASSERT_EQ("ns:myrel", triples[1]._p._iri);
      ASSERT_EQ("?z", triples[1]._o);
      ASSERT_EQ("?y", triples[2]._s);
      ASSERT_EQ("nsx:rel2", triples[2]._p._iri);
      ASSERT_EQ("<http://abc.de>", triples[2]._o);
      ASSERT_EQ("", pq._limit);
      ASSERT_EQ("", pq._offset);
    }

    {
      auto pq = SparqlParser(
                    "PREFIX ns: <http://ns/>"
                    "SELECT ?x ?z \n "
                    "WHERE \t {\n?x <Directed_by> ?y. ?y ns:myrel.extend ?z.\n"
                    "?y nsx:rel2 \"Hello... World\"}")
                    .parse();
      ASSERT_EQ(1u, pq._prefixes.size());
      ASSERT_EQ(2u, pq._selectClause._selectedVariables.size());
      ASSERT_EQ(1u, pq.children().size());
      const auto& triples = pq.children()[0].getBasic()._whereClauseTriples;
      ASSERT_EQ(3u, triples.size());

      pq.expandPrefixes();

      ASSERT_EQ("?x", pq._selectClause._selectedVariables[0]);
      ASSERT_EQ("?z", pq._selectClause._selectedVariables[1]);
      ASSERT_EQ("?x", triples[0]._s);
      ASSERT_EQ("<Directed_by>", triples[0]._p._iri);
      ASSERT_EQ("?y", triples[0]._o);
      ASSERT_EQ("?y", triples[1]._s);
      ASSERT_EQ("<http://ns/myrel.extend>", triples[1]._p._iri);
      ASSERT_EQ("?z", triples[1]._o);
      ASSERT_EQ("?y", triples[2]._s);
      ASSERT_EQ("nsx:rel2", triples[2]._p._iri);
      ASSERT_EQ("\"Hello... World\"", triples[2]._o);
      ASSERT_EQ("", pq._limit);
      ASSERT_EQ("", pq._offset);
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
      auto pq = SparqlParser(R"(
SELECT ?a ?b ?c WHERE {
  VALUES ?a { <Albert_Einstein>}
  VALUES (?b ?c) { (<Marie_Curie> <Joseph_Jacobson>) (<Freiherr> <Lord_of_the_Isles>) }
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
      const auto& values1 =
          pq.children()[0].get<GraphPatternOperation::Values>()._inlineValues;
      ASSERT_EQ(1u, c._whereClauseTriples.size());
      ASSERT_EQ(0u, pq._rootGraphPattern._filters.size());

      ASSERT_EQ(c._whereClauseTriples[0]._s, "?city");
      ASSERT_EQ(c._whereClauseTriples[0]._p._iri, "wdt:P31");
      ASSERT_EQ(c._whereClauseTriples[0]._s, "?city");

      vector<string> vvars = {"?citytype"};
      ASSERT_EQ(vvars, values1._variables);
      vector<vector<string>> vvals = {{"wd:Q515"}, {"wd:Q262166"}};
      ASSERT_EQ(vvals, values1._values);
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
  ASSERT_EQ(1u, pq._prefixes.size());
  ASSERT_EQ(1u, pq._selectClause._selectedVariables.size());
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
  ASSERT_EQ(1u, pq.children().size());
  const auto& c = pq.children()[0].getBasic();
  ASSERT_EQ(3u, pq._prefixes.size());
  ASSERT_EQ(2u, pq._selectClause._selectedVariables.size());
  ASSERT_EQ(3u, c._whereClauseTriples.size());
  ASSERT_EQ("", pq._prefixes[0]._prefix);
  ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
  ASSERT_EQ("ns", pq._prefixes[1]._prefix);
  ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
  ASSERT_EQ("?x", pq._selectClause._selectedVariables[0]);
  ASSERT_EQ("?z", pq._selectClause._selectedVariables[1]);
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
  ASSERT_EQ("", pq._limit);
  ASSERT_EQ("", pq._offset);
}

TEST(ParserTest, testSolutionModifiers) {
  {
    ParsedQuery pq = SparqlParser("SELECT ?x WHERE \t {?x :myrel ?y}").parse();
    pq.expandPrefixes();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(1u, pq._selectClause._selectedVariables.size());
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ("", pq._limit);
    ASSERT_EQ("", pq._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(pq._selectClause._distinct);
    ASSERT_FALSE(pq._selectClause._reduced);
  }

  {
    auto pq =
        SparqlParser("SELECT ?x WHERE \t {?x :myrel ?y} LIMIT 10").parse();
    pq.expandPrefixes();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(1u, pq._selectClause._selectedVariables.size());
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ("10", pq._limit);
    ASSERT_EQ("", pq._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(pq._selectClause._distinct);
    ASSERT_FALSE(pq._selectClause._reduced);
  }

  {
    auto pq = SparqlParser(
                  "SELECT ?x WHERE \t {?x :myrel ?y}\n"
                  "LIMIT 10 OFFSET 15")
                  .parse();
    pq.expandPrefixes();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(1u, pq._selectClause._selectedVariables.size());
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ("10", pq._limit);
    ASSERT_EQ("15", pq._offset);
    ASSERT_EQ(size_t(0), pq._orderBy.size());
    ASSERT_FALSE(pq._selectClause._distinct);
    ASSERT_FALSE(pq._selectClause._reduced);
  }

  {
    auto pq = SparqlParser(
                  "SELECT DISTINCT ?x ?y WHERE \t {?x :myrel ?y}\n"
                  "ORDER BY ?y LIMIT 10 OFFSET 15")
                  .parse();
    pq.expandPrefixes();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(2u, pq._selectClause._selectedVariables.size());
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ("10", pq._limit);
    ASSERT_EQ("15", pq._offset);
    ASSERT_EQ(size_t(1), pq._orderBy.size());
    ASSERT_EQ("?y", pq._orderBy[0]._key);
    ASSERT_FALSE(pq._orderBy[0]._desc);
    ASSERT_TRUE(pq._selectClause._distinct);
    ASSERT_FALSE(pq._selectClause._reduced);
  }

  {
    auto pq = SparqlParser(
                  "SELECT DISTINCT ?x SCORE(?x) ?y WHERE \t {?x :myrel ?y}\n"
                  "ORDER BY ASC(?y) DESC(SCORE(?x)) LIMIT 10 OFFSET 15")
                  .parse();
    pq.expandPrefixes();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(3u, pq._selectClause._selectedVariables.size());
    ASSERT_EQ("SCORE(?x)", pq._selectClause._selectedVariables[1]);
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ("10", pq._limit);
    ASSERT_EQ("15", pq._offset);
    ASSERT_EQ(size_t(2), pq._orderBy.size());
    ASSERT_EQ("?y", pq._orderBy[0]._key);
    ASSERT_FALSE(pq._orderBy[0]._desc);
    ASSERT_EQ("SCORE(?x)", pq._orderBy[1]._key);
    ASSERT_TRUE(pq._orderBy[1]._desc);
    ASSERT_TRUE(pq._selectClause._distinct);
    ASSERT_FALSE(pq._selectClause._reduced);
  }

  {
    auto pq = SparqlParser(
                  "SELECT REDUCED ?x ?y WHERE \t {?x :myrel ?y}\n"
                  "ORDER BY DESC(?x) ASC(?y) LIMIT 10 OFFSET 15")
                  .parse();
    pq.expandPrefixes();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(2u, pq._selectClause._selectedVariables.size());
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ("10", pq._limit);
    ASSERT_EQ("15", pq._offset);
    ASSERT_EQ(size_t(2), pq._orderBy.size());
    ASSERT_EQ("?x", pq._orderBy[0]._key);
    ASSERT_TRUE(pq._orderBy[0]._desc);
    ASSERT_EQ("?y", pq._orderBy[1]._key);
    ASSERT_FALSE(pq._orderBy[1]._desc);
    ASSERT_FALSE(pq._selectClause._distinct);
    ASSERT_TRUE(pq._selectClause._reduced);
  }

  {
    auto pq =
        SparqlParser("SELECT ?x ?y WHERE {?x <is-a> <Actor>} LIMIT 10").parse();
    pq.expandPrefixes();
    ASSERT_EQ("10", pq._limit);
  }

  {
    auto pq = SparqlParser(
                  "SELECT DISTINCT ?movie WHERE { \n"
                  "\n"
                  "?movie <from-year> \"00-00-2000\"^^xsd:date .\n"
                  "\n"
                  "?movie <directed-by> <Scott%2C%20Ridley> .   }  LIMIT 50")
                  .parse();
    pq.expandPrefixes();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(1u, pq._selectClause._selectedVariables.size());
    ASSERT_EQ("?movie", pq._selectClause._selectedVariables[0]);
    ASSERT_EQ(2u, c._whereClauseTriples.size());
    ASSERT_EQ("?movie", c._whereClauseTriples[0]._s);
    ASSERT_EQ("<from-year>", c._whereClauseTriples[0]._p._iri);
    ASSERT_EQ("\"00-00-2000\"^^xsd:date", c._whereClauseTriples[0]._o);
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
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(1u, pq._prefixes.size());
    ASSERT_EQ(1u, pq._selectClause._selectedVariables.size());
    ASSERT_EQ("?movie", pq._selectClause._selectedVariables[0]);
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
    ASSERT_EQ("?avg", pq._orderBy[0]._key);
    ASSERT_FALSE(pq._orderBy[0]._desc);
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
    ASSERT_EQ("?count", pq._orderBy[0]._key);
    ASSERT_FALSE(pq._orderBy[0]._desc);
  }

  {
    auto pq =
        SparqlParser(
            "SELECT ?r (GROUP_CONCAT(?r;SEPARATOR=\"Cake\") as ?concat) WHERE {"
            "?a <http://schema.org/name> ?b ."
            "?a ql:has-relation ?r }"
            "GROUP BY ?r "
            "ORDER BY ?count")
            .parse();
    ASSERT_EQ(1u, pq._selectClause._aliases.size());
    ASSERT_EQ("(group_concat(?r;SEPARATOR=\"Cake\") as ?concat)",
              pq._selectClause._aliases[0].getDescriptor());
  }

  {
    // Test for an alias in the order by statement
    auto pq = SparqlParser(
                  "SELECT DISTINCT ?x ?y WHERE \t {?x :myrel ?y}\n"
                  "ORDER BY DESC((COUNT(?x) as ?count)) LIMIT 10 OFFSET 15")
                  .parse();
    pq.expandPrefixes();
    ASSERT_EQ(1u, pq.children().size());
    const auto& c = pq.children()[0].getBasic();
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(2u, pq._selectClause._selectedVariables.size());
    ASSERT_EQ(1u, c._whereClauseTriples.size());
    ASSERT_EQ("10", pq._limit);
    ASSERT_EQ("15", pq._offset);
    ASSERT_EQ(1u, pq._orderBy.size());
    ASSERT_EQ("?count", pq._orderBy[0]._key);
    ASSERT_TRUE(pq._orderBy[0]._desc);
    ASSERT_EQ(1u, pq._selectClause._aliases.size());
    ASSERT_TRUE(pq._selectClause._aliases[0]._expression.isAggregate({}));
    ASSERT_EQ("(count(?x) as ?count)",
              pq._selectClause._aliases[0].getDescriptor());
    ASSERT_TRUE(pq._selectClause._distinct);
    ASSERT_FALSE(pq._selectClause._reduced);
  }
}

TEST(ParserTest, testGroupByAndAlias) {
  ParsedQuery pq =
      SparqlParser(
          "SELECT (COUNT(?a) as ?count) WHERE { ?b <rel> ?a } GROUP BY ?b")
          .parse();
  ASSERT_EQ(1u, pq._selectClause._selectedVariables.size());
  ASSERT_EQ("?count", pq._selectClause._selectedVariables[0]);
  ASSERT_EQ(1u, pq._selectClause._aliases.size());
  ASSERT_TRUE(pq._selectClause._aliases[0]._expression.isAggregate({}));
  ASSERT_EQ("(count(?a) as ?count)",
            pq._selectClause._aliases[0].getDescriptor());
  ASSERT_EQ(1u, pq._groupByVariables.size());
  ASSERT_EQ("?b", pq._groupByVariables[0]);
}

TEST(ParserTest, testParseLiteral) {
  using std::string;
  // Test a basic parse of a simple xsd string
  string inp = "   \"Astronaut\"^^xsd::string  \t";
  string ret = SparqlParser::parseLiteral(inp, true);
  ASSERT_EQ("\"Astronaut\"^^xsd::string", ret);

  // Test parsing without the isEntireString check and with escaped quotation
  // marks.
  inp = "?a ?b \"The \\\"Moon\\\"\"@en .";
  ret = SparqlParser::parseLiteral(inp, false);
  ASSERT_EQ("\"The \"Moon\"\"@en", ret);

  // Do a negative test for the isEntireString check
  inp = "?a ?b \"The \\\"Moon\\\"\"@en .";
  bool caught_exception = false;
  try {
    ret = SparqlParser::parseLiteral(inp, true);
  } catch (const ParseException& e) {
    caught_exception = true;
  }
  ASSERT_TRUE(caught_exception);

  // check if specifying the correct offset works
  inp = "?a ?b \"The \\\"Moon\\\"\"@en";
  ret = SparqlParser::parseLiteral(inp, true, 6);
  ASSERT_EQ("\"The \"Moon\"\"@en", ret);

  // Do not escape qutation marks with the isEntireString check
  inp = "?a ?b \"The \\\"Moon\"\"@en";
  caught_exception = false;
  try {
    ret = SparqlParser::parseLiteral(inp, true, 6);
  } catch (const ParseException& e) {
    caught_exception = true;
  }
  ASSERT_TRUE(caught_exception);
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
  bool failed = false;
  try {
    result = PropertyPathParser(inp).parse();
  } catch (const ParseException& e) {
    failed = true;
  }
  ASSERT_TRUE(failed);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// Copyright 2014, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>
#include "../src/parser/SparqlParser.h"
#include "../src/util/Exception.h"

TEST(ParserTest, testParse) {
  try {
    ParsedQuery pq = SparqlParser::parse("SELECT ?x WHERE {?x ?y ?z}");
    ASSERT_GT(pq.asString().size(), 0u);
    ASSERT_EQ(0u, pq._prefixes.size());
    ASSERT_EQ(1u, pq._selectedVariables.size());
    ASSERT_EQ(1u, pq._whereClauseTriples.size());

    pq = SparqlParser::parse(
        "PREFIX : <http://rdf.myprefix.com/>\n"
            "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
            "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
            "SELECT ?x ?z \n "
            "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y nsx:rel2 <http://abc.de>}");
    ASSERT_EQ(3u, pq._prefixes.size());
    ASSERT_EQ(2u, pq._selectedVariables.size());
    ASSERT_EQ(3u, pq._whereClauseTriples.size());

    ASSERT_EQ("", pq._prefixes[0]._prefix);
    ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
    ASSERT_EQ("ns", pq._prefixes[1]._prefix);
    ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
    ASSERT_EQ("?x", pq._selectedVariables[0]);
    ASSERT_EQ("?z", pq._selectedVariables[1]);
    ASSERT_EQ("?x", pq._whereClauseTriples[0]._s);
    ASSERT_EQ(":myrel", pq._whereClauseTriples[0]._p);
    ASSERT_EQ("?y", pq._whereClauseTriples[0]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[1]._s);
    ASSERT_EQ("ns:myrel", pq._whereClauseTriples[1]._p);
    ASSERT_EQ("?z", pq._whereClauseTriples[1]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[2]._s);
    ASSERT_EQ("nsx:rel2", pq._whereClauseTriples[2]._p);
    ASSERT_EQ("<http://abc.de>", pq._whereClauseTriples[2]._o);
    ASSERT_EQ("", pq._limit);
    ASSERT_EQ("", pq._offset);

    pq = SparqlParser::parse(
        "PREFIX : <http://rdf.myprefix.com/>\n"
            "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
            "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
            "SELECT ?x ?z \n "
            "WHERE \t {\n?x :myrel ?y. ?y ns:myrel ?z.\n?y nsx:rel2 <http://abc.de>\n}");
    ASSERT_EQ(3u, pq._prefixes.size());
    ASSERT_EQ(2u, pq._selectedVariables.size());
    ASSERT_EQ(3u, pq._whereClauseTriples.size());

    ASSERT_EQ("", pq._prefixes[0]._prefix);
    ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
    ASSERT_EQ("ns", pq._prefixes[1]._prefix);
    ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
    ASSERT_EQ("?x", pq._selectedVariables[0]);
    ASSERT_EQ("?z", pq._selectedVariables[1]);
    ASSERT_EQ("?x", pq._whereClauseTriples[0]._s);
    ASSERT_EQ(":myrel", pq._whereClauseTriples[0]._p);
    ASSERT_EQ("?y", pq._whereClauseTriples[0]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[1]._s);
    ASSERT_EQ("ns:myrel", pq._whereClauseTriples[1]._p);
    ASSERT_EQ("?z", pq._whereClauseTriples[1]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[2]._s);
    ASSERT_EQ("nsx:rel2", pq._whereClauseTriples[2]._p);
    ASSERT_EQ("<http://abc.de>", pq._whereClauseTriples[2]._o);
    ASSERT_EQ("", pq._limit);
    ASSERT_EQ("", pq._offset);

    pq = SparqlParser::parse(
        "PREFIX ns: <http://ns/>"
            "SELECT ?x ?z \n "
            "WHERE \t {\n?x <Directed_by> ?y. ?y ns:myrel.extend ?z.\n"
            "?y nsx:rel2 \"Hello... World\"}");
    ASSERT_EQ(1u, pq._prefixes.size());
    ASSERT_EQ(2u, pq._selectedVariables.size());
    ASSERT_EQ(3u, pq._whereClauseTriples.size());

    pq.expandPrefixes();

    ASSERT_EQ("?x", pq._selectedVariables[0]);
    ASSERT_EQ("?z", pq._selectedVariables[1]);
    ASSERT_EQ("?x", pq._whereClauseTriples[0]._s);
    ASSERT_EQ("<Directed_by>", pq._whereClauseTriples[0]._p);
    ASSERT_EQ("?y", pq._whereClauseTriples[0]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[1]._s);
    ASSERT_EQ("<http://ns/myrel.extend>", pq._whereClauseTriples[1]._p);
    ASSERT_EQ("?z", pq._whereClauseTriples[1]._o);
    ASSERT_EQ("?y", pq._whereClauseTriples[2]._s);
    ASSERT_EQ("nsx:rel2", pq._whereClauseTriples[2]._p);
    ASSERT_EQ("\"Hello... World\"", pq._whereClauseTriples[2]._o);
    ASSERT_EQ("", pq._limit);
    ASSERT_EQ("", pq._offset);


    pq = SparqlParser::parse(
        "SELECT ?x ?y WHERE {?x is-a Actor .  FILTER(?x != ?y)."
            "?y is-a Actor . FILTER(?y < ?x)} LIMIT 10");
    pq.expandPrefixes();
    ASSERT_EQ(2u, pq._filters.size());
    ASSERT_EQ("?x", pq._filters[0]._lhs);
    ASSERT_EQ("?y", pq._filters[0]._rhs);
    ASSERT_EQ(SparqlFilter::FilterType::NE, pq._filters[0]._type);
    ASSERT_EQ("?y", pq._filters[1]._lhs);
    ASSERT_EQ("?x", pq._filters[1]._rhs);
    ASSERT_EQ(SparqlFilter::FilterType::LT, pq._filters[1]._type);
    ASSERT_EQ(2u, pq._whereClauseTriples.size());

    pq = SparqlParser::parse(
        "SELECT ?x ?y WHERE {?x is-a Actor .  FILTER(?x != ?y)."
            "?y is-a Actor} LIMIT 10");
    pq.expandPrefixes();
    ASSERT_EQ(1u, pq._filters.size());
    ASSERT_EQ("?x", pq._filters[0]._lhs);
    ASSERT_EQ("?y", pq._filters[0]._rhs);
    ASSERT_EQ(SparqlFilter::FilterType::NE, pq._filters[0]._type);
    ASSERT_EQ(2u, pq._whereClauseTriples.size());


    pq = SparqlParser::parse(
        "SELECT ?x ?y WHERE {?x is-a Actor .  FILTER(?x != ?y)."
            "?y is-a Actor. ?x <in-text> ?c."
            "?c <in-text> \"coca* abuse\"} LIMIT 10");
    pq.expandPrefixes();
    ASSERT_EQ(1u, pq._filters.size());
    ASSERT_EQ("?x", pq._filters[0]._lhs);
    ASSERT_EQ("?y", pq._filters[0]._rhs);
    ASSERT_EQ(SparqlFilter::FilterType::NE, pq._filters[0]._type);
    ASSERT_EQ(4u, pq._whereClauseTriples.size());
    ASSERT_EQ("?x", pq._whereClauseTriples[2]._s);
    ASSERT_EQ("<in-text>", pq._whereClauseTriples[2]._p);
    ASSERT_EQ("?c", pq._whereClauseTriples[2]._o);
    ASSERT_EQ("?c", pq._whereClauseTriples[3]._s);
    ASSERT_EQ("<in-text>", pq._whereClauseTriples[3]._p);
    ASSERT_EQ("coca* abuse", pq._whereClauseTriples[3]._o);

    pq = SparqlParser::parse(
        "PREFIX : <>\n"
            "SELECT ?x ?y ?z TEXT(?c) SCORE(?c) ?c WHERE {\n"
            "?x :is-a :Politician .\n"
            "?x :in-text ?c .\n"
            "?c :in-text friend .\n"
            "?c :in-text ?y .\n"
            "?y :is-a :Scientist .\n"
            "FILTER(?x != ?y) .\n"
            "} ORDER BY ?c");
    pq.expandPrefixes();
    ASSERT_EQ(1u, pq._filters.size());
  }
  catch (const ad_semsearch::Exception& e) {
    FAIL() << e.getFullErrorMessage();
  }

};

TEST(ParserTest, testFilterWithoutDot) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX fb: <http://rdf.freebase.com/ns/>\n"
          "\n"
          "SELECT DISTINCT ?1 WHERE {\n"
          " fb:m.0fkvn fb:government.government_office_category.officeholders ?0 .\n"
          " ?0 fb:government.government_position_held.jurisdiction_of_office fb:m.0vmt .\n"
          " ?0 fb:government.government_position_held.office_holder ?1 .\n"
          " FILTER (?1 != fb:m.0fkvn)\n"
          " FILTER (?1 != fb:m.0vmt)\n"
          " FILTER (?1 != fb:m.018mts) \n"
          "} LIMIT 300");
  pq.expandPrefixes();
  ASSERT_EQ(1u, pq._prefixes.size());
  ASSERT_EQ(1u, pq._selectedVariables.size());
  ASSERT_EQ(3u, pq._whereClauseTriples.size());
  ASSERT_EQ(3u, pq._filters.size());
  ASSERT_EQ("?1", pq._filters[0]._lhs);
  ASSERT_EQ("<http://rdf.freebase.com/ns/m.0fkvn>", pq._filters[0]._rhs);
  ASSERT_EQ(SparqlFilter::FilterType::NE, pq._filters[0]._type);
  ASSERT_EQ("?1", pq._filters[1]._lhs);
  ASSERT_EQ("<http://rdf.freebase.com/ns/m.0vmt>", pq._filters[1]._rhs);
  ASSERT_EQ(SparqlFilter::FilterType::NE, pq._filters[1]._type);
  ASSERT_EQ("?1", pq._filters[2]._lhs);
  ASSERT_EQ("<http://rdf.freebase.com/ns/m.018mts>", pq._filters[2]._rhs);
  ASSERT_EQ(SparqlFilter::FilterType::NE, pq._filters[2]._type);
}



TEST(ParserTest, testExpandPrefixes) {
  ParsedQuery pq = SparqlParser::parse(
      "PREFIX : <http://rdf.myprefix.com/>\n"
          "PREFIX ns: <http://rdf.myprefix.com/ns/>\n"
          "PREFIX xxx: <http://rdf.myprefix.com/xxx/>\n"
          "SELECT ?x ?z \n "
          "WHERE \t {?x :myrel ?y. ?y ns:myrel ?z.?y nsx:rel2 <http://abc.de>}");
  pq.expandPrefixes();
  ASSERT_EQ(3u, pq._prefixes.size());
  ASSERT_EQ(2u, pq._selectedVariables.size());
  ASSERT_EQ(3u, pq._whereClauseTriples.size());
  ASSERT_EQ("", pq._prefixes[0]._prefix);
  ASSERT_EQ("<http://rdf.myprefix.com/>", pq._prefixes[0]._uri);
  ASSERT_EQ("ns", pq._prefixes[1]._prefix);
  ASSERT_EQ("<http://rdf.myprefix.com/ns/>", pq._prefixes[1]._uri);
  ASSERT_EQ("?x", pq._selectedVariables[0]);
  ASSERT_EQ("?z", pq._selectedVariables[1]);
  ASSERT_EQ("?x", pq._whereClauseTriples[0]._s);
  ASSERT_EQ("<http://rdf.myprefix.com/myrel>", pq._whereClauseTriples[0]._p);
  ASSERT_EQ("?y", pq._whereClauseTriples[0]._o);
  ASSERT_EQ("?y", pq._whereClauseTriples[1]._s);
  ASSERT_EQ("<http://rdf.myprefix.com/ns/myrel>", pq._whereClauseTriples[1]
      ._p);
  ASSERT_EQ("?z", pq._whereClauseTriples[1]._o);
  ASSERT_EQ("?y", pq._whereClauseTriples[2]._s);
  ASSERT_EQ("nsx:rel2", pq._whereClauseTriples[2]._p);
  ASSERT_EQ("<http://abc.de>", pq._whereClauseTriples[2]._o);
  ASSERT_EQ("", pq._limit);
  ASSERT_EQ("", pq._offset);
}

TEST(ParserTest, testSolutionModifiers) {
  ParsedQuery pq = SparqlParser::parse(
      "SELECT ?x WHERE \t {?x :myrel ?y}");
  pq.expandPrefixes();
  ASSERT_EQ(0u, pq._prefixes.size());
  ASSERT_EQ(1u, pq._selectedVariables.size());
  ASSERT_EQ(1u, pq._whereClauseTriples.size());
  ASSERT_EQ("", pq._limit);
  ASSERT_EQ("", pq._offset);
  ASSERT_EQ(size_t(0), pq._orderBy.size());
  ASSERT_FALSE(pq._distinct);
  ASSERT_FALSE(pq._reduced);


  pq = SparqlParser::parse(
      "SELECT ?x WHERE \t {?x :myrel ?y} LIMIT 10");
  pq.expandPrefixes();
  ASSERT_EQ(0u, pq._prefixes.size());
  ASSERT_EQ(1u, pq._selectedVariables.size());
  ASSERT_EQ(1u, pq._whereClauseTriples.size());
  ASSERT_EQ("10", pq._limit);
  ASSERT_EQ("", pq._offset);
  ASSERT_EQ(size_t(0), pq._orderBy.size());
  ASSERT_FALSE(pq._distinct);
  ASSERT_FALSE(pq._reduced);

  pq = SparqlParser::parse(
      "SELECT ?x WHERE \t {?x :myrel ?y}\n"
          "LIMIT 10 OFFSET 15");
  pq.expandPrefixes();
  ASSERT_EQ(0u, pq._prefixes.size());
  ASSERT_EQ(1u, pq._selectedVariables.size());
  ASSERT_EQ(1u, pq._whereClauseTriples.size());
  ASSERT_EQ("10", pq._limit);
  ASSERT_EQ("15", pq._offset);
  ASSERT_EQ(size_t(0), pq._orderBy.size());
  ASSERT_FALSE(pq._distinct);
  ASSERT_FALSE(pq._reduced);

  pq = SparqlParser::parse(
      "SELECT DISTINCT ?x ?y WHERE \t {?x :myrel ?y}\n"
          "ORDER BY ?y LIMIT 10 OFFSET 15");
  pq.expandPrefixes();
  ASSERT_EQ(0u, pq._prefixes.size());
  ASSERT_EQ(2u, pq._selectedVariables.size());
  ASSERT_EQ(1u, pq._whereClauseTriples.size());
  ASSERT_EQ("10", pq._limit);
  ASSERT_EQ("15", pq._offset);
  ASSERT_EQ(size_t(1), pq._orderBy.size());
  ASSERT_EQ("?y", pq._orderBy[0]._key);
  ASSERT_FALSE(pq._orderBy[0]._desc);
  ASSERT_TRUE(pq._distinct);
  ASSERT_FALSE(pq._reduced);

  pq = SparqlParser::parse(
      "SELECT DISTINCT ?x SCORE(?x|?c) ?y WHERE \t {?x :myrel ?y}\n"
          "ORDER BY ASC(?y) DESC(SCORE(?x|?c)) LIMIT 10 OFFSET 15");
  pq.expandPrefixes();
  ASSERT_EQ(0u, pq._prefixes.size());
  ASSERT_EQ(3u, pq._selectedVariables.size());
  ASSERT_EQ("SCORE(?x|?c)", pq._selectedVariables[1]);
  ASSERT_EQ(1u, pq._whereClauseTriples.size());
  ASSERT_EQ("10", pq._limit);
  ASSERT_EQ("15", pq._offset);
  ASSERT_EQ(size_t(2), pq._orderBy.size());
  ASSERT_EQ("?y", pq._orderBy[0]._key);
  ASSERT_FALSE(pq._orderBy[0]._desc);
  ASSERT_EQ("SCORE(?x|?c)", pq._orderBy[1]._key);
  ASSERT_TRUE(pq._orderBy[1]._desc);
  ASSERT_TRUE(pq._distinct);
  ASSERT_FALSE(pq._reduced);

  pq = SparqlParser::parse(
      "SELECT REDUCED ?x ?y WHERE \t {?x :myrel ?y}\n"
          "ORDER BY DESC(?x) ASC(?y) LIMIT 10 OFFSET 15");
  pq.expandPrefixes();
  ASSERT_EQ(0u, pq._prefixes.size());
  ASSERT_EQ(2u, pq._selectedVariables.size());
  ASSERT_EQ(1u, pq._whereClauseTriples.size());
  ASSERT_EQ("10", pq._limit);
  ASSERT_EQ("15", pq._offset);
  ASSERT_EQ(size_t(2), pq._orderBy.size());
  ASSERT_EQ("?x", pq._orderBy[0]._key);
  ASSERT_TRUE(pq._orderBy[0]._desc);
  ASSERT_EQ("?y", pq._orderBy[1]._key);
  ASSERT_FALSE(pq._orderBy[1]._desc);
  ASSERT_FALSE(pq._distinct);
  ASSERT_TRUE(pq._reduced);

  pq = SparqlParser::parse(
      "SELECT ?x ?y WHERE {?x is-a Actor} LIMIT 10");
  pq.expandPrefixes();
  ASSERT_EQ("10", pq._limit);

  pq = SparqlParser::parse(
      "SELECT DISTINCT ?movie WHERE { \n"
          "\n"
          "?movie <from-year> \"00-00-2000\"^^xsd:date .\n"
          "\n"
          "?movie <directed-by> <Scott%2C%20Ridley> .   }  LIMIT 50");
  pq.expandPrefixes();
  ASSERT_EQ(0u, pq._prefixes.size());
  ASSERT_EQ(1u, pq._selectedVariables.size());
  ASSERT_EQ("?movie", pq._selectedVariables[0]);
  ASSERT_EQ(2u, pq._whereClauseTriples.size());
  ASSERT_EQ("?movie", pq._whereClauseTriples[0]._s);
  ASSERT_EQ("<from-year>", pq._whereClauseTriples[0]._p);
  ASSERT_EQ("\"00-00-2000\"^^xsd:date", pq._whereClauseTriples[0]._o);
  ASSERT_EQ("?movie", pq._whereClauseTriples[1]._s);
  ASSERT_EQ("<directed-by>", pq._whereClauseTriples[1]._p);
  ASSERT_EQ("<Scott%2C%20Ridley>", pq._whereClauseTriples[1]._o);

  pq = SparqlParser::parse(
      "PREFIX xsd: <http://www.w3.org/2010/XMLSchema#>"
          "SELECT DISTINCT ?movie WHERE { \n"
          "\n"
          "?movie <from-year> \"00-00-2000\"^^xsd:date .\n"
          "\n"
          "?movie <directed-by> <Scott%2C%20Ridley> .   }  LIMIT 50");
  pq.expandPrefixes();
  ASSERT_EQ(1u, pq._prefixes.size());
  ASSERT_EQ(1u, pq._selectedVariables.size());
  ASSERT_EQ("?movie", pq._selectedVariables[0]);
  ASSERT_EQ(2u, pq._whereClauseTriples.size());
  ASSERT_EQ("?movie", pq._whereClauseTriples[0]._s);
  ASSERT_EQ("<from-year>", pq._whereClauseTriples[0]._p);
  ASSERT_EQ("\"00-00-2000\"^^<http://www.w3.org/2010/XMLSchema#date>",
            pq._whereClauseTriples[0]._o);
  ASSERT_EQ("?movie", pq._whereClauseTriples[1]._s);
  ASSERT_EQ("<directed-by>", pq._whereClauseTriples[1]._p);
  ASSERT_EQ("<Scott%2C%20Ridley>", pq._whereClauseTriples[1]._o);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "./util/GTestHelpers.h"
#include "parser/SparqlParser.h"
#include "parser/VariableCounter.h"

// _____________________________________________________________________________
namespace {

using namespace ::testing;
using parsedQuery::VariableCounter;
using V = Variable;

// Apply `VariableCounter` to the root graph pattern of the given SPARQL query.
VariableCounter parseAndCount(const std::string& sparql) {
  static EncodedIriManager encodedIriManager;
  auto pq = SparqlParser::parseQuery(&encodedIriManager, sparql);
  VariableCounter counter;
  counter(pq._rootGraphPattern);
  return counter;
}

// Shorthand matcher for `VariableCounter`.
Matcher<VariableCounter> counts(VariableCounter::Map expected) {
  return AD_PROPERTY(VariableCounter, counts,
                     UnorderedElementsAreArray(std::move(expected)));
}

// _____________________________________________________________________________
TEST(VariableCounterTest, VariableCounts) {
  // Basic graph patterns.
  EXPECT_THAT(parseAndCount("SELECT * { <s> <p> <o> }"), counts({}));
  EXPECT_THAT(parseAndCount("SELECT * { ?x <p> ?y }"),
              counts({{V{"?x"}, 1}, {V{"?y"}, 1}}));
  EXPECT_THAT(parseAndCount("SELECT * { ?x <p> ?x }"), counts({{V{"?x"}, 2}}));
  EXPECT_THAT(parseAndCount("SELECT * { ?x <p> ?y . ?y <q> ?z }"),
              counts({{V{"?x"}, 1}, {V{"?y"}, 2}, {V{"?z"}, 1}}));
  EXPECT_THAT(parseAndCount("SELECT * { ?x ?p ?y }"),
              counts({{V{"?x"}, 1}, {V{"?p"}, 1}, {V{"?y"}, 1}}));

  // Filter.
  EXPECT_THAT(parseAndCount("SELECT * { ?x <p> ?y . FILTER(?x < ?y) }"),
              counts({{V{"?x"}, 2}, {V{"?y"}, 2}}));
  EXPECT_THAT(parseAndCount("SELECT * { ?x <p> <o> . FILTER(BOUND(?z)) }"),
              counts({{V{"?x"}, 1}, {V{"?z"}, 1}}));

  // Optional.
  EXPECT_THAT(parseAndCount("SELECT * { ?x <p> ?y . OPTIONAL { ?y <q> ?z } }"),
              counts({{V{"?x"}, 1}, {V{"?y"}, 2}, {V{"?z"}, 1}}));
  EXPECT_THAT(
      parseAndCount(
          "SELECT * { ?x <p> ?y . OPTIONAL { ?y <q> ?z . FILTER(?z > 0) } "
          "}"),
      counts({{V{"?x"}, 1}, {V{"?y"}, 2}, {V{"?z"}, 2}}));
  EXPECT_THAT(parseAndCount("SELECT * { ?x <p> ?y . OPTIONAL { ?y <q> ?z . "
                            " } OPTIONAL { ?z <r> ?w } }"),
              counts({{V{"?x"}, 1}, {V{"?y"}, 2}, {V{"?z"}, 2}, {V{"?w"}, 1}}));

  // Union.
  EXPECT_THAT(parseAndCount("SELECT * { { ?x <p> ?y } UNION { ?a <q> ?b } }"),
              counts({{V{"?x"}, 1}, {V{"?y"}, 1}, {V{"?a"}, 1}, {V{"?b"}, 1}}));
  EXPECT_THAT(parseAndCount("SELECT * { { ?x <p> ?y } UNION { ?x <q> ?z } }"),
              counts({{V{"?x"}, 2}, {V{"?y"}, 1}, {V{"?z"}, 1}}));

  // Minus.
  EXPECT_THAT(parseAndCount("SELECT * { ?x <p> ?y . MINUS { ?x <q> ?z } }"),
              counts({{V{"?x"}, 2}, {V{"?y"}, 1}, {V{"?z"}, 1}}));

  // Bind.
  EXPECT_THAT(parseAndCount("SELECT * { ?x <p> ?y . BIND(?x + ?y AS ?z) }"),
              counts({{V{"?x"}, 2}, {V{"?y"}, 2}, {V{"?z"}, 1}}));
  EXPECT_THAT(parseAndCount("SELECT * { <s> <p> <o> . BIND(42 AS ?result) }"),
              counts({{V{"?result"}, 1}}));
  EXPECT_THAT(parseAndCount("SELECT * { BIND(42 AS ?o) . ?s ?p ?o }"),
              counts({{V{"?s"}, 1}, {V{"?p"}, 1}, {V{"?o"}, 2}}));

  // Values.
  EXPECT_THAT(
      parseAndCount("SELECT * { VALUES (?x ?y) { (<a> <b>) (<c> <d>) } }"),
      counts({{V{"?x"}, 1}, {V{"?y"}, 1}}));
  EXPECT_THAT(parseAndCount("SELECT * { VALUES ?x { <a> <b> <c> } }"),
              counts({{V{"?x"}, 1}}));
  EXPECT_THAT(
      parseAndCount("SELECT * { ?s ?p ?o . VALUES ?o { <a> <b> <c> } }"),
      counts({{V{"?s"}, 1}, {V{"?p"}, 1}, {V{"?o"}, 2}}));

  // Service.
  EXPECT_THAT(
      parseAndCount(
          "SELECT * { SERVICE <http://example.org/sparql> { ?x ?y ?z } }"),
      counts({{V{"?x"}, 1}, {V{"?y"}, 1}, {V{"?z"}, 1}}));

  // Graph.
  EXPECT_THAT(parseAndCount("SELECT * { GRAPH ?g { ?x <p> ?y } }"),
              counts({{V{"?g"}, 1}, {V{"?x"}, 1}, {V{"?y"}, 1}}));
  EXPECT_THAT(
      parseAndCount("SELECT * { GRAPH <http://example.org/g> { ?x <p> ?y } }"),
      counts({{V{"?x"}, 1}, {V{"?y"}, 1}}));

  // Transitive path.
  EXPECT_THAT(parseAndCount("SELECT ?x ?y WHERE { ?x <p>+ ?y }"),
              counts({{V{"?x"}, 1}, {V{"?y"}, 1}}));
}

}  // namespace

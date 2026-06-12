// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "./util/GTestHelpers.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlParser.h"
#include "parser/TripleComponent.h"
#include "parser/VariableCounter.h"

// _____________________________________________________________________________
namespace {

using namespace ::testing;
using parsedQuery::VariableCounter;
using V = Variable;

// Apply `VariableCounter` to the root graph pattern of the given SPARQL query.
VariableCounter parseAndCount(std::string sparql) {
  static EncodedIriManager encodedIriManager;
  auto pq = SparqlParser::parseQuery(&encodedIriManager, std::move(sparql));
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

  // Subquery.
  EXPECT_THAT(
      parseAndCount("SELECT * { ?x <p> ?y . { SELECT * { ?y <q> ?z } } }"),
      counts({{V{"?x"}, 1}, {V{"?y"}, 2}, {V{"?z"}, 1}}));

  // Describe.
  EXPECT_THAT(parseAndCount("DESCRIBE <x>"), counts({}));
  EXPECT_THAT(parseAndCount("DESCRIBE ?x"), counts({{V{"?x"}, 1}}));
  EXPECT_THAT(parseAndCount("DESCRIBE ?y WHERE { ?y <p> ?z }"),
              counts({{V{"?y"}, 2}, {V{"?z"}, 1}}));

  // Load.
  {
    VariableCounter count;
    count(parsedQuery::Load{
        TripleComponent::Iri::fromIriref("<https://example.org>"), false});
    EXPECT_THAT(count, counts({}));
  }

  // `TripleComponent`.
  {
    VariableCounter count;
    count(TripleComponent{V{"?x"}});
    EXPECT_THAT(count, counts({{V{"?x"}, 1}}));
  }
}

// _____________________________________________________________________________
TEST(VariableCounterTest, MagicServices) {
  // Path search.
  EXPECT_THAT(
      parseAndCount(
          "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
          "SELECT ?start ?end ?path ?edge WHERE {"
          "SERVICE pathSearch: {"
          "_:p pathSearch:algorithm pathSearch:allPaths ;"
          "pathSearch:source <x> ;"
          "pathSearch:target <z> ;"
          "pathSearch:pathColumn ?path ;"
          "pathSearch:edgeColumn ?edge ;"
          "pathSearch:start ?start ;"
          "pathSearch:end ?end ."
          "{ SELECT * WHERE { ?start <p> ?end } }"
          "}}"),
      counts({{V{"?start"}, 2},
              {V{"?end"}, 2},
              {V{"?path"}, 1},
              {V{"?edge"}, 1}}));
  EXPECT_THAT(
      parseAndCount(
          "PREFIX pathSearch: <https://qlever.cs.uni-freiburg.de/pathSearch/>"
          "SELECT * WHERE {"
          "SERVICE pathSearch: {"
          "_:p pathSearch:algorithm pathSearch:allPaths ;"
          "pathSearch:source ?src ;"
          "pathSearch:target <z> ;"
          "pathSearch:pathColumn ?path ;"
          "pathSearch:edgeColumn ?edge ;"
          "pathSearch:edgeProperty ?ep ;"
          "pathSearch:start ?start ;"
          "pathSearch:end ?end ."
          "{ SELECT * WHERE { ?start <p> ?end } }"
          "}}"),
      counts({{V{"?start"}, 2},
              {V{"?end"}, 2},
              {V{"?path"}, 1},
              {V{"?edge"}, 1},
              {V{"?src"}, 1},
              {V{"?ep"}, 1}}));

  // Spatial search.
  EXPECT_THAT(
      parseAndCount(
          "PREFIX qlss: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
          "SELECT * WHERE {"
          "SERVICE qlss: {"
          "_:config qlss:left ?e ;"
          "qlss:right ?z ;"
          "qlss:maxDistance 500 . } }"),
      counts({{V{"?e"}, 1}, {V{"?z"}, 1}}));
  EXPECT_THAT(
      parseAndCount(
          "PREFIX qlss: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
          "SELECT * WHERE {"
          "SERVICE qlss: {"
          "_:config qlss:left ?e ;"
          "qlss:right ?z ;"
          "qlss:maxDistance 500 ;"
          "qlss:bindDistance ?dist . } }"),
      counts({{V{"?e"}, 1}, {V{"?z"}, 1}, {V{"?dist"}, 1}}));
  EXPECT_THAT(
      parseAndCount(
          "PREFIX qlss: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
          "SELECT * WHERE {"
          "SERVICE qlss: {"
          "_:config qlss:left ?e ;"
          "qlss:right ?z ;"
          "qlss:numNearestNeighbors 3 ;"
          "qlss:payload ?z ;"
          "qlss:payload ?w ."
          "{ ?a <q> ?z . ?b <r> ?w } } }"),
      counts({{V{"?e"}, 1},
              {V{"?z"}, 3},
              {V{"?w"}, 2},
              {V{"?a"}, 1},
              {V{"?b"}, 1}}));

  // Text search.
  EXPECT_THAT(parseAndCount(
                  "PREFIX qlts: <https://qlever.cs.uni-freiburg.de/textSearch/>"
                  "SELECT * WHERE {"
                  "SERVICE qlts: {"
                  "?t qlts:contains ?conf ."
                  "?conf qlts:word \"test\" . } }"),
              counts({{V{"?conf"}, 1}, {V{"?t"}, 1}}));
  EXPECT_THAT(parseAndCount(
                  "PREFIX qlts: <https://qlever.cs.uni-freiburg.de/textSearch/>"
                  "SELECT * WHERE {"
                  "SERVICE qlts: {"
                  "?t qlts:contains ?conf ."
                  "?conf qlts:word \"test\" ."
                  "?conf qlts:score ?sc . } }"),
              counts({{V{"?conf"}, 1}, {V{"?t"}, 1}, {V{"?sc"}, 1}}));
  EXPECT_THAT(
      parseAndCount(
          "PREFIX qlts: <https://qlever.cs.uni-freiburg.de/textSearch/>"
          "SELECT * WHERE {"
          "SERVICE qlts: {"
          "?t qlts:contains ?c1 ."
          "?c1 qlts:word \"test\" ."
          "?t qlts:contains ?c2 ."
          "?c2 qlts:entity ?e . } }"),
      counts({{V{"?c1"}, 1}, {V{"?c2"}, 1}, {V{"?t"}, 2}, {V{"?e"}, 1}}));

  // Named cached result: no variables.
  EXPECT_THAT(parseAndCount(
                  "SELECT * { SERVICE ql:cached-result-with-name-myQuery {} }"),
              counts({}));

  // Materialized view.
  EXPECT_THAT(
      parseAndCount(
          "PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>"
          "SELECT * {"
          "SERVICE view:myView {"
          "_:config view:column-s ?s ;"
          "view:column-p ?p . } }"),
      counts({{V{"?s"}, 1}, {V{"?p"}, 1}}));

  // External values.
  EXPECT_THAT(
      parseAndCount(
          "PREFIX qlext: <https://qlever.cs.uni-freiburg.de/external-values/>"
          "SELECT * {"
          "SERVICE qlext: {"
          "_:b qlext:name \"myId\" ;"
          "qlext:variable ?x ;"
          "qlext:variable ?y . } }"),
      counts({{V{"?x"}, 1}, {V{"?y"}, 1}}));
}

}  // namespace

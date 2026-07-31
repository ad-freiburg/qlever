// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "global/Constants.h"
#include "parser/DatasetClauses.h"
#include "util/TripleComponentTestHelpers.h"

namespace {
using parsedQuery::DatasetClauses;
using Graphs = DatasetClauses::Graphs;
using Iri = ad_utility::triple_component::Iri;

// Shorthands for the graph IRIs that are used in the tests below.
auto iri = ad_utility::testing::iri;
Iri defaultGraph() { return iri(DEFAULT_GRAPH_IRI); }

// Return the `Graphs` that consist exactly of the given `iris`.
Graphs graphs(std::initializer_list<Iri> iris) {
  Graphs result{Graphs::value_type{}};
  for (const auto& singleIri : iris) {
    result.value().insert(TripleComponent{singleIri});
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(DatasetClauses, unconstrained) {
  DatasetClauses clauses;
  EXPECT_TRUE(clauses.isUnconstrainedOrWithClause());
  EXPECT_FALSE(clauses.defaultGraphsAreExplicitlyRestricted());
  EXPECT_EQ(clauses.activeDefaultGraphs(), std::nullopt);
  EXPECT_EQ(clauses.namedGraphs(), std::nullopt);
  EXPECT_TRUE(clauses.isCompatibleNamedGraph(iri("<foo>")));
}

// _____________________________________________________________________________
TEST(DatasetClauses, fromClauses) {
  // An explicit `FROM <foo>` restricts the default graph and, as required by
  // section 13.2 of the SPARQL 1.1 standard, leaves no named graphs.
  auto fromFoo =
      DatasetClauses::fromClauses({DatasetClause{iri("<foo>"), false}});
  EXPECT_FALSE(fromFoo.isUnconstrainedOrWithClause());
  EXPECT_TRUE(fromFoo.defaultGraphsAreExplicitlyRestricted());
  EXPECT_EQ(fromFoo.activeDefaultGraphs(), graphs({iri("<foo>")}));
  EXPECT_EQ(fromFoo.namedGraphs(), graphs({}));
  EXPECT_FALSE(fromFoo.isCompatibleNamedGraph(iri("<foo>")));

  // An explicit `FROM NAMED <foo>` restricts the named graphs and leaves an
  // empty default graph.
  auto fromNamedFoo =
      DatasetClauses::fromClauses({DatasetClause{iri("<foo>"), true}});
  EXPECT_FALSE(fromNamedFoo.isUnconstrainedOrWithClause());
  EXPECT_TRUE(fromNamedFoo.defaultGraphsAreExplicitlyRestricted());
  EXPECT_EQ(fromNamedFoo.activeDefaultGraphs(), graphs({}));
  EXPECT_EQ(fromNamedFoo.namedGraphs(), graphs({iri("<foo>")}));
  EXPECT_TRUE(fromNamedFoo.isCompatibleNamedGraph(iri("<foo>")));
  EXPECT_FALSE(fromNamedFoo.isCompatibleNamedGraph(iri("<bar>")));
}

// _____________________________________________________________________________
TEST(DatasetClauses, fromImplicitDefaultGraph) {
  auto clauses = DatasetClauses::fromImplicitDefaultGraph();

  // The default graph is restricted to `ql:default-graph`, but in contrast to
  // an explicit `FROM ql:default-graph` all named graphs stay available.
  EXPECT_EQ(clauses.activeDefaultGraphs(), graphs({defaultGraph()}));
  EXPECT_EQ(clauses.namedGraphs(), std::nullopt);
  EXPECT_TRUE(clauses.isCompatibleNamedGraph(iri("<foo>")));

  // The restriction is implicit, so features that cannot honor a graph
  // restriction (in particular the text index) must not reject such queries.
  EXPECT_FALSE(clauses.defaultGraphsAreExplicitlyRestricted());

  // The implicit default graph is not the same as an explicit
  // `FROM ql:default-graph`, even though both have the same active default
  // graphs.
  auto explicitDefaultGraph =
      DatasetClauses::fromClauses({DatasetClause{defaultGraph(), false}});
  EXPECT_EQ(clauses.activeDefaultGraphs(),
            explicitDefaultGraph.activeDefaultGraphs());
  EXPECT_NE(clauses, explicitDefaultGraph);
  EXPECT_EQ(explicitDefaultGraph.namedGraphs(), graphs({}));
}

// _____________________________________________________________________________
TEST(DatasetClauses, fromImplicitDefaultGraphInsideGraphClause) {
  auto clauses = DatasetClauses::fromImplicitDefaultGraph();

  // Inside `GRAPH <foo> {...}` the single active default graph is `<foo>` if
  // the outer query has no `FROM NAMED` and an implicit default graph.
  auto insideGraphFoo = clauses.getDatasetClauseForGraphClause(iri("<foo>"));
  EXPECT_EQ(insideGraphFoo.activeDefaultGraphs(), graphs({iri("<foo>")}));
  EXPECT_EQ(DatasetClauses{}.getDatasetClauseForGraphClause(iri("<foo>")),
            insideGraphFoo);

  // Inside `GRAPH ?var {...}` all named graphs are active (in the same scenario
  // as described above).
  auto insideGraphVar = clauses.getDatasetClauseForVariableGraphClause();
  EXPECT_EQ(insideGraphVar.activeDefaultGraphs(), std::nullopt);
  EXPECT_EQ(insideGraphVar.namedGraphs(), std::nullopt);
  EXPECT_EQ(DatasetClauses{}.getDatasetClauseForVariableGraphClause(),
            insideGraphVar);
}

// _____________________________________________________________________________
TEST(DatasetClauses, fromWithClause) {
  auto clauses = DatasetClauses::fromWithClause(iri("<foo>"));

  // A `WITH` clause only specifies the default graph; all named graphs stay
  // available.
  EXPECT_TRUE(clauses.isUnconstrainedOrWithClause());
  EXPECT_EQ(clauses.activeDefaultGraphs(), graphs({iri("<foo>")}));
  EXPECT_EQ(clauses.namedGraphs(), std::nullopt);
  EXPECT_TRUE(clauses.isCompatibleNamedGraph(iri("<bar>")));

  // In contrast to the implicit default graph, the restriction of a `WITH`
  // clause is explicitly requested by the query.
  EXPECT_TRUE(clauses.defaultGraphsAreExplicitlyRestricted());
  EXPECT_NE(clauses, DatasetClauses::fromImplicitDefaultGraph());
}

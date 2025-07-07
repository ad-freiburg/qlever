// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_DATASETCLAUSES_H
#define QLEVER_SRC_PARSER_DATASETCLAUSES_H

#include <vector>

#include "index/ScanSpecification.h"
#include "parser/sparqlParser/DatasetClause.h"

namespace parsedQuery {

// A struct for the FROM [NAMED] clause in queries, and the `USING [NAMED]` and
// `WITH` clauses from `SPARQL Update`.
struct DatasetClauses {
  using Graphs = ScanSpecificationAsTripleComponent::Graphs;

 private:
  // Store the default and named graphs.
  Graphs defaultGraphs_{};
  Graphs namedGraphs_{};

  // An empty set of graphs that sometimes has to be returned.
  Graphs emptyDummy_{Graphs::value_type{}};
  // True iff the `defaultGraph` is a single graph that originates from a `WITH`
  // clause, which slightly changes the semantics.
  bool defaultGraphSpecifiedUsingWith_ = false;

 public:
  // Divide the dataset clause from `clauses` into default and named graphs,
  // as needed for a `DatasetClauses` object.
  static DatasetClauses fromClauses(const std::vector<DatasetClause>& clauses);

  // Return the `DatasetClauses` that correspond to the `WITH <withGraph>`
  // clause in a SPARQL UPDATE.
  static DatasetClauses fromWithClause(const TripleComponent::Iri& withGraph);

  // Construct directly from two optional sets, mostly used in tests.
  DatasetClauses(Graphs defaultGraphs, Graphs namedGraphs);

  // Default constructor, leads to a completely unconstrained clause.
  DatasetClauses() = default;

  // Return true iff neither default nor named graphs were specified using a
  // FROM or USING graph. Note that this function also returns true for a WITH
  // clause, because those semantics are useful in the places where this
  // function is needed (WITH clauses are the weakest clauses and can easily be
  // overridden.
  bool isUnconstrainedOrWithClause() const;

  // Return the set of active default graphs (The set of graphs which will be
  // used evaluate all triples outside an explicit `GRAPH` clause.
  // `std::nullopt` means "use the implicit default graph", whereas an empty set
  // means "the active default graph is empty, because a named graph was
  // specified" (See the SPARQL 1.1 standard, section 13.2).
  const Graphs& activeDefaultGraphs() const;

  // Return the set of named graphs that can be used inside a `GRAPH` clause.
  // `std::nullopt` means "all named graphs can be used", whereas an empty set
  // means "no named graphs can be used, because a default graph was explicitly
  // specified" (See the SPARQL 1.1 standard, section 13.2).
  const Graphs& namedGraphs() const;

  // Get the DatasetClause that corresponds to a given `GRAPH <iri> {}` clause
  // when `this` is the dataset clause of the outer query. In particular,
  // if `<iri>` is a valid named graph in this dataset clause, then it will
  // become the single active default graph of the returned `DatasetClauses`.
  // Otherwise, the active default graph of the result will be empty.
  [[nodiscard]] DatasetClauses getDatasetClauseForGraphClause(
      const TripleComponent::Iri&) const;

  // Get the DatasetClause that corresponds to a given `GRAPH ?var {}` clause
  // when `*this` is the dataset clause of the outer query. In particular, the
  // named graphs of `*this` become the active default graphs of the return
  // value.
  [[nodiscard]] DatasetClauses getDatasetClauseForVariableGraphClause() const;

  // Return true iff the `graph` is a supported named graph, either because it
  // is explicitly part of the `namedGraphs()`, or because all named graphs are
  // implicitly allowed.
  bool isCompatibleNamedGraph(const TripleComponent::Iri& graph) const;

  bool operator==(const DatasetClauses& other) const = default;
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_DATASETCLAUSES_H

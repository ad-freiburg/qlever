// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_DATASETCLAUSES_H
#define QLEVER_SRC_PARSER_DATASETCLAUSES_H

#include <vector>

#include "backports/three_way_comparison.h"
#include "parser/sparqlParser/DatasetClause.h"

namespace parsedQuery {

// A struct for the FROM [NAMED] clause in queries, and the `USING [NAMED]` and
// `WITH` clauses from `SPARQL Update`.
struct DatasetClauses {
  // TODO<RobinTF> consider using the `GraphFilter` class for this, which has
  // similar semantics but provides a strong type.
  using Graphs = std::optional<ad_utility::HashSet<TripleComponent>>;

 private:
  // The way in which the `defaultGraphs_` below were specified, which slightly
  // changes the semantics (see `namedGraphsAreUnconstrained` below).
  enum class DefaultGraphKind {
    // The default graphs come from a `FROM` or `USING` clause (or were not
    // specified at all).
    FromOrUsing,
    // The default graph is the single graph of a `WITH` clause.
    With,
    // The default graph is the implicit unnamed default graph (internally
    // spelled as `ql:default-graph`) that is used for queries without a
    // dataset clause when the `union-graph-as-default-graph` runtime parameter
    // is disabled.
    Implicit
  };

  // Store the default and named graphs.
  Graphs defaultGraphs_{};
  Graphs namedGraphs_{};

  // An empty set of graphs that sometimes has to be returned.
  Graphs emptyDummy_{Graphs::value_type{}};
  // How the `defaultGraphs_` were specified.
  DefaultGraphKind defaultGraphKind_ = DefaultGraphKind::FromOrUsing;

 public:
  // Divide the dataset clause from `clauses` into default and named graphs,
  // as needed for a `DatasetClauses` object.
  static DatasetClauses fromClauses(const std::vector<DatasetClause>& clauses);

  // Return the `DatasetClauses` that correspond to the `WITH <withGraph>`
  // clause in a SPARQL UPDATE.
  static DatasetClauses fromWithClause(const TripleComponent::Iri& withGraph);

  // Return the `DatasetClauses` for a query without an explicit dataset clause
  // in the case that the union of all graphs is *not* used as the default
  // graph (see the `union-graph-as-default-graph` runtime parameter). The
  // active default graph then is the unnamed default graph (spelled
  // `ql:default-graph` inside QLever), while all named graphs stay available
  // inside `GRAPH` clauses. Note that this is deliberately different from an
  // explicit `FROM ql:default-graph`, which per section 13.2 of the SPARQL
  // 1.1 standard would leave the set of named graphs empty.
  static DatasetClauses fromImplicitDefaultGraph();

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

  // Return true iff the active default graphs are restricted by the query
  // itself, that is, by an explicit `FROM`/`FROM NAMED`/`USING`/`USING
  // NAMED`/`WITH` clause. In contrast to `activeDefaultGraphs().has_value()`
  // this returns false for the implicit default graph of
  // `fromImplicitDefaultGraph` above. Use this for features that silently
  // cannot honor a graph restriction (in particular the text index, which
  // stores no graph information) and therefore have to reject such queries.
  bool defaultGraphsAreExplicitlyRestricted() const;

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

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(DatasetClauses, defaultGraphs_,
                                              namedGraphs_, emptyDummy_,
                                              defaultGraphKind_)

 private:
  // Return true iff no explicit `FROM`/`USING` clause restricted the graphs,
  // such that all named graphs stay available inside `GRAPH` clauses. This is
  // the case for a completely unconstrained dataset, but also for a `WITH`
  // clause and for the implicit default graph, both of which only specify a
  // default graph without narrowing down the named graphs.
  bool namedGraphsAreUnconstrained() const;
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_DATASETCLAUSES_H

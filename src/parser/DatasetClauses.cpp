// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "parser/DatasetClauses.h"

#include "global/Constants.h"

namespace parsedQuery {

// _____________________________________________________________________________
DatasetClauses DatasetClauses::fromClauses(
    const std::vector<DatasetClause>& clauses) {
  DatasetClauses result;
  for (auto& [dataset, isNamed] : clauses) {
    auto& graphs = isNamed ? result.namedGraphs_ : result.defaultGraphs_;
    if (!graphs.has_value()) {
      graphs.emplace();
    }
    graphs.value().insert(dataset);
  }
  return result;
}

// _____________________________________________________________________________
DatasetClauses DatasetClauses::fromWithClause(
    const TripleComponent::Iri& withGraph) {
  DatasetClauses result;
  result.defaultGraphs_.emplace({withGraph});
  result.defaultGraphKind_ = DefaultGraphKind::With;
  return result;
}

// _____________________________________________________________________________
DatasetClauses DatasetClauses::fromImplicitDefaultGraph() {
  DatasetClauses result;
  result.defaultGraphs_.emplace(
      {TripleComponent::Iri::fromIriref(DEFAULT_GRAPH_IRI)});
  result.defaultGraphKind_ = DefaultGraphKind::Implicit;
  return result;
}

// _____________________________________________________________________________
DatasetClauses::DatasetClauses(Graphs defaultGraphs, Graphs namedGraphs)
    : defaultGraphs_{std::move(defaultGraphs)},
      namedGraphs_{std::move(namedGraphs)} {}

// _____________________________________________________________________________
bool DatasetClauses::isUnconstrainedOrWithClause() const {
  return (defaultGraphKind_ == DefaultGraphKind::With ||
          !defaultGraphs_.has_value()) &&
         !namedGraphs_.has_value();
}

// _____________________________________________________________________________
bool DatasetClauses::namedGraphsAreUnconstrained() const {
  return (defaultGraphKind_ != DefaultGraphKind::FromOrUsing ||
          !defaultGraphs_.has_value()) &&
         !namedGraphs_.has_value();
}

// _____________________________________________________________________________
bool DatasetClauses::defaultGraphsAreExplicitlyRestricted() const {
  return defaultGraphKind_ != DefaultGraphKind::Implicit &&
         activeDefaultGraphs().has_value();
}

// _____________________________________________________________________________
auto DatasetClauses::activeDefaultGraphs() const -> const Graphs& {
  return isUnconstrainedOrWithClause() || defaultGraphs_.has_value()
             ? defaultGraphs_
             : emptyDummy_;
}

// _____________________________________________________________________________
auto DatasetClauses::namedGraphs() const -> const Graphs& {
  return namedGraphsAreUnconstrained() || namedGraphs_.has_value()
             ? namedGraphs_
             : emptyDummy_;
}

// _____________________________________________________________________________
bool DatasetClauses::isCompatibleNamedGraph(
    const TripleComponent::Iri& graph) const {
  return namedGraphsAreUnconstrained() || namedGraphs().value().contains(graph);
}

// _____________________________________________________________________________
DatasetClauses DatasetClauses::getDatasetClauseForGraphClause(
    const TripleComponent::Iri& graphIri) const {
  DatasetClauses result;
  result.defaultGraphs_.emplace();
  if (isCompatibleNamedGraph(graphIri)) {
    result.defaultGraphs_.value().insert({graphIri});
  }
  return result;
}

// _____________________________________________________________________________
DatasetClauses DatasetClauses::getDatasetClauseForVariableGraphClause() const {
  DatasetClauses result;
  // Note: It is important that we use the member function `namedGraphs()` here,
  // because if default graphs were specified but no named graphs, then `GRAPH
  // ?var` clauses have to be empty according to the SPARQL 1.1 standard.
  result.defaultGraphs_ = namedGraphs();
  result.namedGraphs_ = namedGraphs_;
  return result;
}

}  // namespace parsedQuery

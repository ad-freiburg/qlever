// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "parser/DatasetClauses.h"

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
  result.defaultGraphSpecifiedUsingWith_ = true;
  return result;
}

// _____________________________________________________________________________
DatasetClauses::DatasetClauses(Graphs defaultGraphs, Graphs namedGraphs)
    : defaultGraphs_{std::move(defaultGraphs)},
      namedGraphs_{std::move(namedGraphs)} {}

// _____________________________________________________________________________
bool DatasetClauses::isUnconstrainedOrWithClause() const {
  return (defaultGraphSpecifiedUsingWith_ || !defaultGraphs_.has_value()) &&
         !namedGraphs_.has_value();
}

// _____________________________________________________________________________
auto DatasetClauses::activeDefaultGraphs() const -> const Graphs& {
  return isUnconstrainedOrWithClause() || defaultGraphs_.has_value()
             ? defaultGraphs_
             : emptyDummy_;
}

// _____________________________________________________________________________
auto DatasetClauses::namedGraphs() const -> const Graphs& {
  return isUnconstrainedOrWithClause() || namedGraphs_.has_value()
             ? namedGraphs_
             : emptyDummy_;
}

// _____________________________________________________________________________
bool DatasetClauses::isCompatibleNamedGraph(
    const TripleComponent::Iri& graph) const {
  return isUnconstrainedOrWithClause() || namedGraphs().value().contains(graph);
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

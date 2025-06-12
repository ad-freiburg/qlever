// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_DATASETCLAUSES_H
#define QLEVER_SRC_PARSER_DATASETCLAUSES_H

#include <vector>

#include "index/ScanSpecification.h"
#include "parser/sparqlParser/DatasetClause.h"

namespace parsedQuery {
// A struct for the FROM clause (default graphs) and FROM NAMED clauses (named
// graphs).
struct DatasetClauses {
  using Graphs = ScanSpecificationAsTripleComponent::Graphs;

 private:
  Graphs defaultGraphs_{};
  Graphs namedGraphs_{};
  Graphs emptyDummy_{Graphs::value_type{}};
  bool defaultGraphSpecifiedUsingWith_ = false;

 public:
  // Divide the dataset clause from `clauses` into default and named graphs,
  // as needed for a `DatasetClauses` object.
  static DatasetClauses fromClauses(const std::vector<DatasetClause>& clauses);

  DatasetClauses(Graphs defaultGraphs = std::nullopt,
                 Graphs namedGraphs = std::nullopt)
      : defaultGraphs_{std::move(defaultGraphs)},
        namedGraphs_{std::move(namedGraphs)} {}

  bool unspecified() const {
    return (defaultGraphSpecifiedUsingWith_ || !defaultGraphs_.has_value()) &&
           !namedGraphs_.has_value();
  }

  const auto& defaultGraphs() const {
    return unspecified() || defaultGraphs_.has_value() ? defaultGraphs_
                                                       : emptyDummy_;
  }

  const auto& namedGraphs() const {
    return unspecified() || namedGraphs_.has_value() ? namedGraphs_
                                                     : emptyDummy_;
  }

  // TODO<joka921> Can we get a safer interface here?
  auto& defaultGraphsMutable() { return defaultGraphs_; }

  bool isCompatibleNamedGraph(const TripleComponent::Iri& graph) const {
    return unspecified() || namedGraphs().value().contains(graph);
  }

  void setDefaultGraphIsSpecifiedUsingWith() {
    defaultGraphSpecifiedUsingWith_ = true;
  }

  bool operator==(const DatasetClauses& other) const = default;
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_DATASETCLAUSES_H

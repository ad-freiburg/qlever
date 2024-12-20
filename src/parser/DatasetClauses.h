// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once
#include <vector>

#include "index/ScanSpecification.h"
#include "parser/sparqlParser/DatasetClause.h"

namespace parsedQuery {
// A struct for the FROM clause (default graphs) and FROM NAMED clauses (named
// graphs).
struct DatasetClauses {
  ScanSpecificationAsTripleComponent::Graphs defaultGraphs_{};
  ScanSpecificationAsTripleComponent::Graphs namedGraphs_{};

  // Divide the dataset clause from `clauses` into default and named graphs, as
  // needed for a `DatasetClauses` object.
  static DatasetClauses fromClauses(const std::vector<DatasetClause>& clauses);

  bool operator==(const DatasetClauses& other) const = default;
};
}  // namespace parsedQuery

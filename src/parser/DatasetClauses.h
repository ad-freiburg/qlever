// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once
#include <vector>

#include "index/ScanSpecification.h"
#include "parser/sparqlParser/DatasetClause.h"

namespace parsedQuery {
// A struct for the FROM and FROM NAMED clauses;
struct DatasetClauses {
  // FROM clauses.
  ScanSpecificationAsTripleComponent::Graphs defaultGraphs_{};
  // FROM NAMED clauses.
  ScanSpecificationAsTripleComponent::Graphs namedGraphs_{};

  static DatasetClauses fromClauses(const std::vector<DatasetClause>& clauses);

  bool operator==(const DatasetClauses& other) const = default;
};
}  // namespace parsedQuery

// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "parser/DatasetClauses.h"

// _____________________________________________________________________________
parsedQuery::DatasetClauses parsedQuery::DatasetClauses::fromClauses(
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

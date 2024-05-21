//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include "parser/SelectClause.h"
#include "parser/Triples.h"
#include "parser/data/Types.h"

namespace parsedQuery {
struct UpdateClause : ClauseBase {
  // Triples with no variables.
  std::vector<SparqlTripleSimple> toInsert_;
  std::vector<SparqlTripleSimple> toDelete_;

  UpdateClause() = default;
  explicit UpdateClause(std::vector<SparqlTripleSimple> toInsert,
                        std::vector<SparqlTripleSimple> toDelete)
      : toInsert_{std::move(toInsert)}, toDelete_{std::move(toDelete)} {}
};
}  // namespace parsedQuery

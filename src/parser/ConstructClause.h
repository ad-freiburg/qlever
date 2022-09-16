//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include "parser/SelectClause.h"
#include "parser/data/Types.h"

namespace parsedQuery {
struct ConstructClause : ClauseBase {
  ad_utility::sparql_types::Triples triples_;

  ConstructClause() = default;
  explicit ConstructClause(ad_utility::sparql_types::Triples triples)
      : triples_(std::move(triples)) {}

  vector<const Variable*> containedVariables() {
    vector<const Variable*> out;
    for (const auto& triple : triples_) {
      for (const auto& varOrTerm : triple) {
        if (auto variable = std::get_if<Variable>(&varOrTerm)) {
          out.emplace_back(variable);
        }
      }
    }
    return out;
  }
};
}  // namespace parsedQuery

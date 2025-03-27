//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_CONSTRUCTCLAUSE_H
#define QLEVER_SRC_PARSER_CONSTRUCTCLAUSE_H

#include "parser/SelectClause.h"
#include "parser/data/Types.h"

namespace parsedQuery {
struct ConstructClause : ClauseBase {
  ad_utility::sparql_types::Triples triples_;

  ConstructClause() = default;
  explicit ConstructClause(ad_utility::sparql_types::Triples triples)
      : triples_(std::move(triples)) {}

  // Yields all variables that appear in this `ConstructClause`. Variables that
  // appear multiple times are also yielded multiple times.
  cppcoro::generator<const Variable> containedVariables() const {
    for (const auto& triple : triples_) {
      for (const auto& varOrTerm : triple) {
        if (auto variable = std::get_if<Variable>(&varOrTerm)) {
          co_yield *variable;
        }
      }
    }
  }
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_CONSTRUCTCLAUSE_H

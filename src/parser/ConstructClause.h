//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)
//  Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_PARSER_CONSTRUCTCLAUSE_H
#define QLEVER_SRC_PARSER_CONSTRUCTCLAUSE_H

#include "backports/algorithm.h"
#include "parser/SelectClause.h"
#include "parser/data/Types.h"
#include "parser/data/Variable.h"
#include "util/TransparentFunctors.h"

namespace parsedQuery {
struct ConstructClause : ClauseBase {
  ad_utility::sparql_types::Triples triples_;

  ConstructClause() = default;
  explicit ConstructClause(ad_utility::sparql_types::Triples triples)
      : triples_(std::move(triples)) {}

  // Lazily yields all variables that appear in this `ConstructClause`.
  // Variables that appear multiple times are also yielded multiple times.
  auto containedVariables() const {
    return triples_ | ql::views::join |
           ql::views::filter(ad_utility::holdsAlternative<Variable>) |
           ql::views::transform(ad_utility::get<Variable>);
  }
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_CONSTRUCTCLAUSE_H

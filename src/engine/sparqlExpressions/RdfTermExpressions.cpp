//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail::rdfExpressions {

inline auto getDatatype = [](OptIri input) -> IdOrLiteralOrIri {
  if (!input.has_value()) {
    return Id::makeUndefined();
  } else {
    return LiteralOrIri{std::move(input.value())};
  }
};

using GetDatatype = NARY<1, FV<decltype(getDatatype), DatatypeValueGetter>>;

}  //  namespace detail::rdfExpressions

using namespace detail::rdfExpressions;
using Expr = SparqlExpression::Ptr;

Expr makeDatatypeExpression(Expr child) {
  return std::make_unique<GetDatatype>(std::move(child));
}

}  // namespace sparqlExpression

//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./LangExpression.h"

#include "engine/sparqlExpressions/LiteralExpression.h"

namespace sparqlExpression {

namespace {
Variable getVariableOrThrow(const SparqlExpression::Ptr& ptr) {
  if (auto stringPtr = dynamic_cast<const VariableExpression*>(ptr.get())) {
    return stringPtr->value();
  } else {
    throw std::runtime_error{
        "The argument to the LANG function must be a single variable"};
  }
}
}  // namespace

// _____________________________________________________________________________
LangExpression::LangExpression(SparqlExpression::Ptr child)
    : variable_{getVariableOrThrow(child)} {}

}  // namespace sparqlExpression

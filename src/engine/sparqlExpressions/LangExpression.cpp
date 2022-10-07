//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./LangExpression.h"

#include "engine/sparqlExpressions/LiteralExpression.h"

namespace sparqlExpression {

// _____________________________________________________________________________
LangExpression::LangExpression(SparqlExpression::Ptr child) {
  if (auto stringPtr = dynamic_cast<const VariableExpression*>(child.get())) {
    variable_ = stringPtr->value();
  } else {
    throw std::runtime_error{
        "The argument to the LANG function must be a single variable"};
  }
}

}  // namespace sparqlExpression
//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BLANKNODEEXPRESSION_H_
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BLANKNODEEXPRESSION_H_

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {

SparqlExpression::Ptr makeBlankNodeExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeUniqueBlankNodeExpression(size_t label);
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BLANKNODEEXPRESSION_H_

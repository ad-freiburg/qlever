//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BLANKNODEEXPRESSION_H_
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BLANKNODEEXPRESSION_H_

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {

// Create a `SparqlExpression` representing the term `BNODE(?x)`.
SparqlExpression::Ptr makeBlankNodeExpression(SparqlExpression::Ptr child);

// Create a `SparqlExpression` representing the term `BNODE()`. You need to make
// sure that the passed label is unique across all other calls to `BNODE()`.
SparqlExpression::Ptr makeUniqueBlankNodeExpression();
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BLANKNODEEXPRESSION_H_

// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Inline implementation of performance-critical helpers from
// SparqlExpressionGenerators.h. Included unconditionally in
// SparqlExpressionGenerators.h when QLEVER_CHEAPER_COMPILATION is not set, so
// every TU gets the inline definition and the call can be inlined by the
// compiler. Under QLEVER_CHEAPER_COMPILATION this file is only included in the
// corresponding .cpp, providing a single out-of-line definition.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORSIMPL_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORSIMPL_H

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"

namespace sparqlExpression::detail {

#ifndef QLEVER_CHEAPER_COMPILATION
inline
#endif
    Id
    idOrLiteralOrIriToId(const IdOrLocalVocabEntry& value,
                         LocalVocab* localVocab) {
  return std::visit(
      ad_utility::OverloadCallOperator{[](ValueId id) { return id; },
                                       makeStringResultGetter(localVocab)},
      value);
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORSIMPL_H

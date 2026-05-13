//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"

namespace sparqlExpression::detail {

// _____________________________________________________________________________
Id idOrLiteralOrIriToId(const IdOrLocalVocabEntry& value,
                        LocalVocab* localVocab) {
  return std::visit(
      ad_utility::OverloadCallOperator{[](ValueId id) { return id; },
                                       makeStringResultGetter(localVocab)},
      value);
}

}  // namespace sparqlExpression::detail

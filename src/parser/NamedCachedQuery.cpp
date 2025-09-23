// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//                  Chair of Algorithms and Data Structures.

#include "parser/NamedCachedQuery.h"

namespace parsedQuery {

// _____________________________________________________________________________
void NamedCachedQuery::addParameter(
    [[maybe_unused]] const SparqlTriple& triple) {
  throwBecauseNotEmpty();
}

// _____________________________________________________________________________
const std::string& NamedCachedQuery::validateAndGetIdentifier() const {
  if (childGraphPattern_.has_value()) {
    throwBecauseNotEmpty();
  }
  return identifier_;
}

// _____________________________________________________________________________
void NamedCachedQuery::throwBecauseNotEmpty() {
  throw std::runtime_error{
      "The body of a named cache query request must be empty"};
}
}  // namespace parsedQuery

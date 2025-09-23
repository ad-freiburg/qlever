//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

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

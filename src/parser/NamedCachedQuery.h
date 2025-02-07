//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "parser/MagicServiceQuery.h"

namespace parsedQuery {
// A magic service for queries that are pinned with an explicit query name.
class NamedCachedQuery : public MagicServiceQuery {
  std::string identifier_;

 public:
  // Construct with the name of the named query.
  NamedCachedQuery(std::string identifier)
      : identifier_{std::move(identifier)} {}

  // Currently the body of the SERVICE clause must be empty.
  void addParameter([[maybe_unused]] const SparqlTriple& triple) override {
    throw std::runtime_error{
        "The body of a named cache query request must be empty"};
  }

  // Return the name of the named query, and check, that the configuration is
  // valid (which currently means, that the body of the SERVICE clause was
  // empty.
  const std::string& validateAndGetIdentifier() const {
    // TODO<joka921> Better error messages.
    AD_CORRECTNESS_CHECK(!childGraphPattern_.has_value());
    return identifier_;
  }
};
}  // namespace parsedQuery

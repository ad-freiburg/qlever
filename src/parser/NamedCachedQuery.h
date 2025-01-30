//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "parser/MagicServiceQuery.h"

namespace parsedQuery {
class NamedCachedQuery : public MagicServiceQuery {
  std::string identifier_;

 public:
  NamedCachedQuery(std::string identifier)
      : identifier_{std::move(identifier)} {}

  void addParameter([[maybe_unused]] const SparqlTriple& triple) override {
    throw std::runtime_error{
        "The body of a named cache query request must be empty"};
  }

  const std::string& validateAndGetIdentifier() const {
    // TODO<joka921> Better error messages.
    AD_CORRECTNESS_CHECK(!childGraphPattern_.has_value());
    return identifier_;
  }
};
}  // namespace parsedQuery

//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_NAMED_CACHED_QUERY_H
#define QLEVER_SRC_PARSER_NAMED_CACHED_QUERY_H

#include "parser/MagicServiceQuery.h"

namespace parsedQuery {
// A magic SERVICE for queries that are pinned with an explicit query name.
class NamedCachedQuery : public MagicServiceQuery {
  std::string identifier_;

 public:
  // Construct with the name of the named query.
  explicit NamedCachedQuery(std::string identifier)
      : identifier_{std::move(identifier)} {}

  // Currently the body of the SERVICE clause must be empty.
  void addParameter([[maybe_unused]] const SparqlTriple& triple) override {
    throwBecauseNotEmpty();
  }

  // Return the name of the named query, and check, that the configuration is
  // valid (which currently means, that the body of the SERVICE clause was
  // empty.
  const std::string& validateAndGetIdentifier() const {
    if (childGraphPattern_.has_value()) {
      throwBecauseNotEmpty();
    }
    return identifier_;
  }

 private:
  [[noreturn]] static void throwBecauseNotEmpty() {
    throw std::runtime_error{
        "The body of a named cache query request must be empty"};
  }
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_NAMED_CACHED_QUERY_H

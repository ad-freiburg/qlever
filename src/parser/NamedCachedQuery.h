// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//                  Chair of Algorithms and Data Structures.

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
  void addParameter([[maybe_unused]] const SparqlTriple& triple) override;

  // Return the name of the named query, and check, that the configuration is
  // valid (which currently means, that the body of the SERVICE clause was
  // empty.
  const std::string& validateAndGetIdentifier() const;

 private:
  [[noreturn]] static void throwBecauseNotEmpty();
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_NAMED_CACHED_QUERY_H

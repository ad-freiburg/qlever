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
class NamedCachedResult : public MagicServiceQuery {
  std::string identifier_;

 public:
  // Construct from an iri. The iri is required to have the form
  // `ql:cached-result-with-name-queryName`, else an exception is thrown.
  explicit NamedCachedResult(const TripleComponent::Iri& iri);

  // Currently the body of the SERVICE clause must be empty.
  void addParameter([[maybe_unused]] const SparqlTriple& triple) override;
  // Currently the body of the SERVICE clause must be empty.
  void addGraph(const GraphPatternOperation& childGraphPattern) override;

  // Return the name of the named query.
  const std::string& identifier() const;

 private:
  [[noreturn]] static void throwBecauseNotEmpty();
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_NAMED_CACHED_QUERY_H

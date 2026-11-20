// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_UPDATETRIPLES_H
#define QLEVER_SRC_PARSER_UPDATETRIPLES_H

#include <vector>

#include "index/LocalVocab.h"
#include "parser/SparqlTriple.h"

namespace updateClause {
// A class that combines a vector of triples with a `LocalVocab`. The local
// vocab is currently only used to store blank node IDs, all IRIs and literals
// are still encoded as strings in the triples.
struct UpdateTriples {
  using Vec = std::vector<SparqlTripleSimpleWithGraph>;
  Vec triples_{};
  // The parser has no access to the allocator of the query, but the size of
  // the local vocab is bounded by the size of the update.
  LocalVocab localVocab_{ad_utility::makeUnlimitedAllocator<Id>()};

  // Constructors
  UpdateTriples() = default;
  UpdateTriples(Vec triples, LocalVocab localVocab);

  // The copy operations have to be manually implemented because `LocalVocab` is
  // not copyable, but has an explicit `clone` function.
  UpdateTriples(const UpdateTriples& rhs);
  UpdateTriples& operator=(const UpdateTriples& rhs);

  UpdateTriples(UpdateTriples&& rhs) = default;
  UpdateTriples& operator=(UpdateTriples&& rhs) = default;

  // We have custom copy operations, so Sonarcloud insists on an explicitly
  // defined destructor.
  ~UpdateTriples() {}
};
}  // namespace updateClause

#endif  // QLEVER_SRC_PARSER_UPDATETRIPLES_H

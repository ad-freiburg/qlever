// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_UPDATETRIPLES_H
#define QLEVER_SRC_PARSER_UPDATETRIPLES_H

#include <vector>

#include "global/Id.h"
#include "index/LocalVocab.h"
#include "parser/SparqlTriple.h"
#include "util/Allocator.h"
#include "util/ContainersWithAllocator.h"

namespace updateClause {
// A class that combines a vector of triples with a `LocalVocab`. The local
// vocab is currently only used to store blank node IDs, all IRIs and literals
// are still encoded as strings in the triples.
struct UpdateTriples {
  using Vec = qlm::vector<SparqlTripleSimpleWithGraph>;
  Vec triples_;
  LocalVocab localVocab_{};

  // Constructors. The `allocator` is used for the `triples_`.
  explicit UpdateTriples(const qlever::Allocator<Id>& allocator);
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

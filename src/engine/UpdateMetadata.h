// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_UPDATEMETADATA_H
#define QLEVER_SRC_ENGINE_UPDATEMETADATA_H

#include <string>
#include <vector>

#include "backports/three_way_comparison.h"
#include "util/json.h"

// A class for keeping track of the number of triples of the `DeltaTriples`.
struct DeltaTriplesCount {
  int64_t triplesInserted_;
  int64_t triplesDeleted_;

  /// Output as json. The signature of this function is mandated by the json
  /// library to allow for implicit conversion.
  friend void to_json(nlohmann::json& j, const DeltaTriplesCount& count);

  friend DeltaTriplesCount operator-(const DeltaTriplesCount& lhs,
                                     const DeltaTriplesCount& rhs) {
    return {lhs.triplesInserted_ - rhs.triplesInserted_,
            lhs.triplesDeleted_ - rhs.triplesDeleted_};
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(DeltaTriplesCount,
                                              triplesInserted_, triplesDeleted_)
};

// The materialized delta for a single update operation: the N-Quads lines for
// triples that were genuinely inserted into or deleted from the `DeltaTriples`
// store (Stage-2 delta: after intra-request deduplication AND after dropping
// triples already present in / absent from `DeltaTriples`). Populated only
// when the client opts in via the `return-delta=true` URL parameter.
struct UpdateDelta {
  std::vector<std::string> inserted_;  // N-Quads lines for inserted triples
  std::vector<std::string> deleted_;   // N-Quads lines for deleted triples
};

// Metadata of a single update operation: number of inserted and deleted triples
// before the operation, of the operation, and after the operation, and
// optionally the materialized delta triples as N-Quads lines.
struct UpdateMetadata {
  std::optional<DeltaTriplesCount> countBefore_;
  std::optional<DeltaTriplesCount> inUpdate_;
  std::optional<DeltaTriplesCount> countAfter_;
  // Populated only when the client requests `return-delta=true`.
  std::optional<UpdateDelta> delta_;
};

#endif  // QLEVER_SRC_ENGINE_UPDATEMETADATA_H

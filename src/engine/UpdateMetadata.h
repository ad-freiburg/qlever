// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_UPDATEMETADATA_H
#define QLEVER_SRC_ENGINE_UPDATEMETADATA_H

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
                                     const DeltaTriplesCount& rhs);

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(DeltaTriplesCount,
                                              triplesInserted_, triplesDeleted_)
};

// Metadata of a single update operation: number of inserted and deleted triples
// before the operation, of the operation, and after the operation.
struct UpdateMetadata {
  std::optional<DeltaTriplesCount> countBefore_;
  std::optional<DeltaTriplesCount> inUpdate_;
  std::optional<DeltaTriplesCount> countAfter_;
};

#endif  // QLEVER_SRC_ENGINE_UPDATEMETADATA_H

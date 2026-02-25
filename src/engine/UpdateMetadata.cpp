// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/UpdateMetadata.h"

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const DeltaTriplesCount& count) {
  j = nlohmann::json{{"inserted", count.triplesInserted_},
                     {"deleted", count.triplesDeleted_},
                     {"total", count.triplesInserted_ + count.triplesDeleted_}};
}

// ____________________________________________________________________________
DeltaTriplesCount operator-(const DeltaTriplesCount& lhs,
                            const DeltaTriplesCount& rhs) {
  return {lhs.triplesInserted_ - rhs.triplesInserted_,
          lhs.triplesDeleted_ - rhs.triplesDeleted_};
}

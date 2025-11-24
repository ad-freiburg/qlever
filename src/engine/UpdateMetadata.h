// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures


#ifndef QLEVER_SRC_ENGINE_UPDATETYPES_H
#define QLEVER_SRC_ENGINE_UPDATETYPES_H

#include "index/DeltaTriples.h"

// Metadata of a single update operation: number of inserted and deleted triples
// before the operation, of the operation, and after the operation.
struct UpdateMetadata {
  std::optional<DeltaTriplesCount> countBefore_;
  std::optional<DeltaTriplesCount> inUpdate_;
  std::optional<DeltaTriplesCount> countAfter_;
};

#endif //QLEVER_SRC_ENGINE_UPDATETYPES_H

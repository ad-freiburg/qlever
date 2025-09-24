// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H
#define QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H

#include <memory>

#include "engine/idTable/IdTable.h"
#include "index/Index.h"
#include "rdfTypes/Variable.h"

// Forward declaration of s2 classes
class S2Polyline;
class MutableS2ShapeIndex;

// TODO explain
struct SpatialJoinCachedIndex {
  Variable geometryColumn_;
  std::shared_ptr<MutableS2ShapeIndex> s2index_;

  // TODO
  SpatialJoinCachedIndex(const Variable& geometryColumn,
                         const IdTable* restable, ColumnIndex col,
                         const Index& index);
};

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H

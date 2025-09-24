// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H
#define QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H

#include <memory>

#include "engine/idTable/IdTable.h"
#include "index/Index.h"
#include "rdfTypes/Variable.h"

// Forward declaration of s2 class
class MutableS2ShapeIndex;
class S2Polyline;

// This class holds a shape index that is created once by the named cached query
// mechanism and is then kept constant and persisted across queries.
class SpatialJoinCachedIndex {
 private:
  Variable geometryColumn_;
  std::shared_ptr<MutableS2ShapeIndex> s2index_;
  std::shared_ptr<std::vector<std::pair<S2Polyline, size_t>>> lines_;
  ad_utility::HashMap<size_t, size_t> shapeIndexToRow_{};

 public:
  // Constructor that builds an index from the geometries in the given column in
  // the `IdTable`. Currently only line strings are supported for the
  // experimental S2 point polyline algorithm.
  SpatialJoinCachedIndex(const Variable& geometryColumn, ColumnIndex col,
                         const IdTable* restable, const Index& index);

  // Getters
  const Variable& getGeometryColumn() const { return geometryColumn_; }
  std::shared_ptr<const MutableS2ShapeIndex> getIndex() const {
    return s2index_;
  }
  size_t getRow(size_t shapeIndex) { return shapeIndexToRow_.at(shapeIndex); }
};

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINCACHEDINDEX_H

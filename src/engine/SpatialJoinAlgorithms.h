//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: @Jonathan24680
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#pragma once

#include <memory>

#include "engine/Result.h"
#include "engine/SpatialJoin.h"

class SpatialJoinAlgorithms {
 public:
  // initialize the Algorithm with the needed parameters
  SpatialJoinAlgorithms(QueryExecutionContext* qec,
                        PreparedSpatialJoinParams params,
                        std::shared_ptr<SpatialJoinConfiguration> config);
  Result BaselineAlgorithm();
  Result S2geometryAlgorithm();
  Result BoundingBoxAlgorithm();

 private:
  // helper function which returns a GeoPoint if the element of the given table
  // represents a GeoPoint
  std::optional<GeoPoint> getPoint(const IdTable* restable, size_t row,
                                   ColumnIndex col) const;

  // helper function, which computes the distance of two points, where each
  // point comes from a different result table
  Id computeDist(const IdTable* resLeft, const IdTable* resRight,
                 size_t rowLeft, size_t rowRight, ColumnIndex leftPointCol,
                 ColumnIndex rightPointCol) const;

  // Helper function, which adds a row, which belongs to the result to the
  // result table. As inputs it uses a row of the left and a row of the right
  // child result table.
  void addResultTableEntry(IdTable* result, const IdTable* resultLeft,
                           const IdTable* resultRight, size_t rowLeft,
                           size_t rowRight, Id distance) const;

  QueryExecutionContext* qec_;
  PreparedSpatialJoinParams params_;
  std::shared_ptr<SpatialJoinConfiguration> config_;
};

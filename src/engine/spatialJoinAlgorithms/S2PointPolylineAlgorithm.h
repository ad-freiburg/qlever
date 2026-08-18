// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_S2POINTPOLYLINEALGORITHM_H
#define QLEVER_SRC_ENGINE_S2POINTPOLYLINEALGORITHM_H

#include "engine/spatialJoinAlgorithms/SpatialJoinAlgorithms.h"

// Spatial join between a set of `GeoPoint`s (left table) and the polylines
// cached in a named result (right table, see `NamedResultCache`), using an
// `S2ClosestEdgeQuery`. Only supports `maxDistance` tasks.
class S2PointPolylineAlgorithm : public SpatialJoinAlgorithms {
 public:
  using SpatialJoinAlgorithms::SpatialJoinAlgorithms;

  Result run() override;
};

#endif  // QLEVER_SRC_ENGINE_S2POINTPOLYLINEALGORITHM_H

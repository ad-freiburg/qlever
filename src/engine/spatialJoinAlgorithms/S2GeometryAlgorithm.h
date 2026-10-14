// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_S2GEOMETRYALGORITHM_H
#define QLEVER_SRC_ENGINE_S2GEOMETRYALGORITHM_H

#include "engine/spatialJoinAlgorithms/SpatialJoinAlgorithmBase.h"

// Spatial join between two sets of `GeoPoint`s using an S2 point index and a
// nearest-neighbor query. Supports `maxDistance` and `nearestNeighbors`
// tasks, but not areas/linestrings/....
class S2GeometryAlgorithm : public SpatialJoinAlgorithmBase {
 public:
  using SpatialJoinAlgorithmBase::SpatialJoinAlgorithmBase;

  Result run() override;

 private:
  // The maximum-results constraint, only meaningful for algorithms
  // supporting a `NearestNeighbors` task (this one and `BaselineAlgorithm`),
  // so it's self-computed from `config_` here rather than in the base class.
  std::optional<size_t> maxResults_ = config_.getMaxResults();
};

#endif  // QLEVER_SRC_ENGINE_S2GEOMETRYALGORITHM_H

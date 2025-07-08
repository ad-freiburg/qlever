// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINCONFIG_H
#define QLEVER_SRC_ENGINE_SPATIALJOINCONFIG_H

#include <cstddef>
#include <optional>
#include <variant>

#include "parser/PayloadVariables.h"
#include "parser/data/Variable.h"

// This header contains enums and configuration structs for the spatial join
// operation. It allows including these types without also including the whole
// class declaration of the spatial join operation.

// The supported spatial join types (geometry predicates). When updating this
// enum, also add a case in `getGeoFunctionExpressionParameters` in
// `GeoExpression.cpp`.
enum class SpatialJoinType {
  INTERSECTS,
  CONTAINS,
  COVERS,
  CROSSES,
  TOUCHES,
  EQUALS,
  OVERLAPS,
  WITHIN_DIST
};

// A nearest neighbor search with optionally a maximum distance.
struct NearestNeighborsConfig {
  size_t maxResults_;
  std::optional<double> maxDist_ = std::nullopt;
};

// A spatial search limited only by a maximum distance.
struct MaxDistanceConfig {
  double maxDist_;
};

// Spatial join using one of the join types above. The maximal distance is
// relevant only for the `WITHIN_DIST` join type.
struct SpatialJoinConfig {
  SpatialJoinType joinType_;
  std::optional<double> maxDist_ = std::nullopt;
};

// Configuration to restrict the results provided by the SpatialJoin
using SpatialJoinTask =
    std::variant<NearestNeighborsConfig, MaxDistanceConfig, SpatialJoinConfig>;

// Selection of a SpatialJoin algorithm
enum class SpatialJoinAlgorithm {
  BASELINE,
  S2_GEOMETRY,
  BOUNDING_BOX,
  LIBSPATIALJOIN
};
const SpatialJoinAlgorithm SPATIAL_JOIN_DEFAULT_ALGORITHM =
    SpatialJoinAlgorithm::S2_GEOMETRY;

// The configuration object that will be provided by the special SERVICE.
struct SpatialJoinConfiguration {
  // The task defines search parameters
  SpatialJoinTask task_;

  // The variables for the two tables to be joined
  Variable left_;
  Variable right_;

  // If given, the distance will be added to the result and be bound to this
  // variable.
  std::optional<Variable> distanceVariable_ = std::nullopt;

  // If given a vector of variables, the selected variables will be part of the
  // result table - the join column will automatically be part of the result.
  // You may use PayloadAllVariables to select all columns of the right table.
  PayloadVariables payloadVariables_ = PayloadVariables::all();

  // Choice of algorithm.
  SpatialJoinAlgorithm algo_ = SPATIAL_JOIN_DEFAULT_ALGORITHM;

  std::optional<SpatialJoinType> joinType_ = std::nullopt;
};

// The spatial join operation without a limit on the maximum number of results
// can, in the worst case have a square number of results, but usually this is
// not the case. 1 divided by this constant is the damping factor for the
// estimated number of results.
static const size_t SPATIAL_JOIN_MAX_DIST_SIZE_ESTIMATE = 1000;

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINCONFIG_H

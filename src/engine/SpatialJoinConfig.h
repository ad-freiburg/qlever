// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINCONFIG_H
#define QLEVER_SRC_ENGINE_SPATIALJOINCONFIG_H

#include <array>
#include <cstddef>
#include <optional>
#include <string_view>
#include <variant>

#include "parser/PayloadVariables.h"
#include "rdfTypes/Variable.h"

// This header contains enums and configuration structs for the spatial join
// operation. It allows including these types without also including the whole
// class declaration of the spatial join operation.

// The supported spatial join types (geometry predicates). When updating this
// enum with a new two-argument boolean relation, also add a case in
// `getGeoFunctionExpressionParameters` in `GeoExpression.cpp`. This does not
// apply to `WITHIN_DIST` and `DE9IM`, which take an additional parameter
// (the distance resp. the filter pattern) and are therefore not exposed as
// `geof:sf...`-style two-argument relations.
enum class SpatialJoinType {
  INTERSECTS,
  CONTAINS,
  COVERS,
  CROSSES,
  TOUCHES,
  EQUALS,
  OVERLAPS,
  WITHIN,
  WITHIN_DIST,
  DE9IM
};

// String representation of the `SpatialJoinType` values.
inline constexpr std::array<std::string_view, 10> SpatialJoinTypeString{
    "intersects", "contains", "covers", "crosses",     "touches",
    "equals",     "overlaps", "within", "within-dist", "de9im"};

// A DE-9IM filter pattern: a fixed-size string of exactly 9 characters, each
// one of `0`-`2`, `T`/`t`, `F`/`f`, or `*` (see
// https://en.wikipedia.org/wiki/DE-9IM). Stored without a null terminator.
using De9imFilterString = std::array<char, 9>;

// If `filter` is a syntactically valid DE-9IM filter pattern (as described for
// `De9imFilterString` above), return it as a `De9imFilterString`, else
// `std::nullopt`. Implemented as a simple character-class check instead of a
// regex library to keep this frequently-included header cheap to compile.
constexpr std::optional<De9imFilterString> validateDe9imFilterString(
    std::string_view filter) {
  if (filter.size() != 9) {
    return std::nullopt;
  }
  De9imFilterString result{};
  for (size_t i = 0; i < result.size(); ++i) {
    char c = filter[i];
    bool isValidChar = (c >= '0' && c <= '2') || c == 'T' || c == 't' ||
                       c == 'F' || c == 'f' || c == '*';
    if (!isValidChar) {
      return std::nullopt;
    }
    result[i] = c;
  }
  return result;
}

// A nearest neighbor search with optionally a maximum distance.
struct NearestNeighborsConfig {
  size_t maxResults_;
  std::optional<double> maxDist_ = std::nullopt;
};

// A spatial search limited only by a maximum distance.
struct MaxDistanceConfig {
  double maxDist_;
};

// Spatial join with libspatialjoin using one of the join types above. The
// maximal distance is relevant only for the `WITHIN_DIST` join type, and the
// DE-9IM filter pattern only for the `DE9IM` join type.
struct LibSpatialJoinConfig {
  SpatialJoinType joinType_;
  std::optional<double> maxDist_ = std::nullopt;
  std::optional<De9imFilterString> de9imFilter_ = std::nullopt;
};

// Configuration to restrict the results provided by the SpatialJoin
using SpatialJoinTask = std::variant<NearestNeighborsConfig, MaxDistanceConfig,
                                     LibSpatialJoinConfig>;

// Selection of a SpatialJoin algorithm
enum class SpatialJoinAlgorithm {
  BASELINE,
  S2_GEOMETRY,
  BOUNDING_BOX,
  LIBSPATIALJOIN,
  S2_POINT_POLYLINE
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

  // Join type for `libspatialjoin` algorithm.
  std::optional<SpatialJoinType> joinType_ = std::nullopt;

  // Cache name for precomputed right child with s2 index (only for
  // s2-point-polyline algorithm)
  std::optional<std::string> rightCacheName_ = std::nullopt;
};

// The spatial join operation without a limit on the maximum number of results
// can, in the worst case have a square number of results, but usually this is
// not the case. 1 divided by this constant is the damping factor for the
// estimated number of results.
static const size_t SPATIAL_JOIN_MAX_DIST_SIZE_ESTIMATE = 1000;

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINCONFIG_H

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
#include <type_traits>
#include <utility>
#include <variant>

#include "engine/SpatialJoinType.h"
#include "parser/PayloadVariables.h"
#include "rdfTypes/Variable.h"
#include "util/EnumWithStrings.h"
#include "util/Exception.h"

// This header contains enums and configuration structs for the spatial join
// operation. It allows including these types without also including the whole
// class declaration of the spatial join operation.

// A DE-9IM filter pattern: a fixed-size string of exactly 9 characters, each
// one of `0`-`2`, `T`/`t`, `F`/`f`, or `*` (see
// https://en.wikipedia.org/wiki/DE-9IM). Stored without a null terminator.
using De9imFilterString = std::array<char, 9>;

// Parsing and validation of `De9imFilterString`s, see
// `rdfTypes/GeoSparqlHelpers.h`.

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
  std::optional<double> maxDist_;
  std::optional<De9imFilterString> de9imFilter_;

  // For join types other than `WITHIN_DIST`/`DE9IM`, where neither
  // `maxDist_` nor `de9imFilter_` apply.
  explicit LibSpatialJoinConfig(SpatialJoinType joinType)
      : LibSpatialJoinConfig(joinType, std::nullopt, std::nullopt) {}

  // The constructor checks that `maxDist_` is set if and only if the join
  // type is `WITHIN_DIST`, and that `de9imFilter_` is set if and only if the
  // join type is `DE9IM`, because these fields are not independent of each
  // other.
  LibSpatialJoinConfig(SpatialJoinType joinType, std::optional<double> maxDist,
                       std::optional<De9imFilterString> de9imFilter)
      : joinType_{joinType}, maxDist_{maxDist}, de9imFilter_{de9imFilter} {
    AD_CORRECTNESS_CHECK(maxDist_.has_value() ==
                         (joinType_ == SpatialJoinType::WITHIN_DIST));
    AD_CORRECTNESS_CHECK(de9imFilter_.has_value() ==
                         (joinType_ == SpatialJoinType::DE9IM));
  }
};

// Configuration to restrict the results provided by the SpatialJoin
using SpatialJoinTask = std::variant<NearestNeighborsConfig, MaxDistanceConfig,
                                     LibSpatialJoinConfig>;

// Selection of a SpatialJoin algorithm. When adding an algorithm here, also
// add its name to the `descriptions_` of `SpatialJoinAlgorithm` below.
enum class SpatialJoinAlgorithmEnum {
  BASELINE,
  S2_GEOMETRY,
  BOUNDING_BOX,
  LIBSPATIALJOIN,
  S2_POINT_POLYLINE
};

// Wrapper around `SpatialJoinAlgorithmEnum` that provides conversion to and
// from the string representation used in the SPARQL syntax (the value of the
// `<algorithm>` parameter) and in error messages, see `SpatialQuery.cpp`.
class SpatialJoinAlgorithm
    : public ad_utility::EnumWithStrings<SpatialJoinAlgorithm,
                                         SpatialJoinAlgorithmEnum> {
 public:
  using Enum = SpatialJoinAlgorithmEnum;

  static constexpr std::array<std::pair<Enum, std::string_view>, 5>
      descriptions_{{{Enum::BASELINE, "baseline"},
                     {Enum::S2_GEOMETRY, "s2"},
                     {Enum::BOUNDING_BOX, "boundingBox"},
                     {Enum::LIBSPATIALJOIN, "libspatialjoin"},
                     {Enum::S2_POINT_POLYLINE, "experimentalPointPolyline"}}};
  static const SpatialJoinAlgorithm BASELINE;
  static const SpatialJoinAlgorithm S2_GEOMETRY;
  static const SpatialJoinAlgorithm BOUNDING_BOX;
  static const SpatialJoinAlgorithm LIBSPATIALJOIN;
  static const SpatialJoinAlgorithm S2_POINT_POLYLINE;

  static constexpr std::string_view typeName() {
    return "spatial join algorithm";
  }

  using EnumWithStrings::EnumWithStrings;
};

const inline SpatialJoinAlgorithm SpatialJoinAlgorithm::BASELINE{
    Enum::BASELINE};
const inline SpatialJoinAlgorithm SpatialJoinAlgorithm::S2_GEOMETRY{
    Enum::S2_GEOMETRY};
const inline SpatialJoinAlgorithm SpatialJoinAlgorithm::BOUNDING_BOX{
    Enum::BOUNDING_BOX};
const inline SpatialJoinAlgorithm SpatialJoinAlgorithm::LIBSPATIALJOIN{
    Enum::LIBSPATIALJOIN};
const inline SpatialJoinAlgorithm SpatialJoinAlgorithm::S2_POINT_POLYLINE{
    Enum::S2_POINT_POLYLINE};

// Default algorithm used where an explicit choice is not required: the
// deprecated magic-predicate spatial search syntax (which has no way to
// specify an algorithm) and internal/test construction of a
// `SpatialJoinConfiguration` that doesn't care about the algorithm.
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

  // Cache name for precomputed right child with s2 index (only for
  // s2-point-polyline algorithm)
  std::optional<std::string> rightCacheName_ = std::nullopt;

  // Extract the maximum distance constraint from `task_`, if the task type
  // specifies one. Every task type has a (possibly always-empty) `maxDist_`
  // field, so this is defined for all of them.
  std::optional<double> getMaxDist() const {
    auto visitor = [](const auto& config) -> std::optional<double> {
      return config.maxDist_;
    };
    return std::visit(visitor, task_);
  }

  // Extract the maximum-results constraint from `task_`. Only
  // `NearestNeighborsConfig` tasks specify one; for `MaxDistanceConfig` and
  // `LibSpatialJoinConfig` tasks this is always `std::nullopt`.
  std::optional<size_t> getMaxResults() const {
    auto visitor = [](const auto& config) -> std::optional<size_t> {
      using T = std::decay_t<decltype(config)>;
      if constexpr (std::is_same_v<T, MaxDistanceConfig>) {
        return std::nullopt;
      } else if constexpr (std::is_same_v<T, LibSpatialJoinConfig>) {
        return std::nullopt;
      } else {
        static_assert(std::is_same_v<T, NearestNeighborsConfig>);
        return config.maxResults_;
      }
    };
    return std::visit(visitor, task_);
  }

  // Extract the join type from `task_`. Only `LibSpatialJoinConfig` tasks
  // specify one; for the other task types this is always `std::nullopt`.
  std::optional<SpatialJoinType> getJoinType() const {
    auto visitor = [](const auto& config) -> std::optional<SpatialJoinType> {
      using T = std::decay_t<decltype(config)>;
      if constexpr (std::is_same_v<T, LibSpatialJoinConfig>) {
        return config.joinType_;
      } else {
        return std::nullopt;
      }
    };
    return std::visit(visitor, task_);
  }

  // Extract the DE-9IM filter pattern from `task_`, if the task is a
  // `LibSpatialJoinConfig` with one set (only relevant for the `DE9IM` join
  // type).
  std::optional<De9imFilterString> getDe9imFilter() const {
    auto visitor = [](const auto& config) -> std::optional<De9imFilterString> {
      using T = std::decay_t<decltype(config)>;
      if constexpr (std::is_same_v<T, LibSpatialJoinConfig>) {
        return config.de9imFilter_;
      } else {
        return std::nullopt;
      }
    };
    return std::visit(visitor, task_);
  }
};

// The spatial join operation without a limit on the maximum number of results
// can, in the worst case have a square number of results, but usually this is
// not the case. 1 divided by this constant is the damping factor for the
// estimated number of results.
static const size_t SPATIAL_JOIN_MAX_DIST_SIZE_ESTIMATE = 1000;

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINCONFIG_H

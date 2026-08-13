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

#include "engine/SpatialJoinType.h"
#include "parser/PayloadVariables.h"
#include "rdfTypes/Variable.h"

// This header contains enums and configuration structs for the spatial join
// operation. It allows including these types without also including the whole
// class declaration of the spatial join operation.

// A DE-9IM filter pattern: a fixed-size string of exactly 9 characters, each
// one of `0`-`2`, `T`/`t`, `F`/`f`, or `*` (see
// https://en.wikipedia.org/wiki/DE-9IM). Stored without a null terminator.
using De9imFilterString = std::array<char, 9>;

// If `filter` is a syntactically valid DE-9IM filter pattern (i.e. exactly 9
// characters, each one of `0`-`2`, `T`/`t`, `F`/`f`, or `*`, see
// `De9imFilterString` above), return it as a `De9imFilterString`, else
// `std::nullopt`. Implemented as a simple character-class check instead of a
// regex library to keep this frequently-included header cheap to compile.
// Note: this does not check whether the pattern can match disjoint
// geometries, see `de9imFilterCanMatchDisjoint` below for that.
constexpr std::optional<De9imFilterString> parseDe9imFilterString(
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

// Whether the given (syntactically valid) DE-9IM `filter` could match a
// disjoint pair of geometries. Patterns for which this holds (e.g.
// `*********` or the literal disjoint pattern `FF*FF****`) are unsupported:
// the pinned `libspatialjoin` never enumerates disjoint candidate pairs to
// its callback (see `Sweeper::doDE9IMCheck`), regardless of the configured
// filter, so accepting such a pattern would silently omit matching disjoint
// pairs from the result.
//
// The DE-9IM matrix entries are ordered II, IB, IE, BI, BB, BE, EI, EB, EE. A
// pair of geometries is disjoint iff II, IB, BI, and BB (indices 0, 1, 3, 4)
// are all `F`. A filter character only excludes `F` if it is a digit, `T`, or
// `t`; `*` and `F`/`f` both admit it. If all four of these positions admit
// `F`, the pattern could match a disjoint pair.
constexpr bool de9imFilterCanMatchDisjoint(const De9imFilterString& filter) {
  auto admitsF = [](char c) { return c == '*' || c == 'F' || c == 'f'; };
  return admitsF(filter[0]) && admitsF(filter[1]) && admitsF(filter[3]) &&
         admitsF(filter[4]);
}

// If `filter` is a syntactically valid DE-9IM filter pattern that cannot
// match a disjoint pair of geometries, return it as a `De9imFilterString`,
// else `std::nullopt`. See `parseDe9imFilterString` and
// `de9imFilterCanMatchDisjoint` above, which this combines and which should
// be used directly if the two failure cases need to be reported separately
// (as in `SpatialQuery.cpp`).
constexpr std::optional<De9imFilterString> validateDe9imFilterString(
    std::string_view filter) {
  auto result = parseDe9imFilterString(filter);
  if (!result.has_value() || de9imFilterCanMatchDisjoint(result.value())) {
    return std::nullopt;
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

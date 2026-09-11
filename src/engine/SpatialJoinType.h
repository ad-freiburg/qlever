// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINTYPE_H
#define QLEVER_SRC_ENGINE_SPATIALJOINTYPE_H

#include <array>
#include <string_view>
#include <utility>

#include "util/EnumWithStrings.h"

namespace detail {
// The supported spatial join types (geometry predicates). When updating this
// enum with a new two-argument boolean relation, also add a case in
// `getGeoFunctionExpressionParameters` in `GeoExpression.cpp`. This does not
// apply to `WITHIN_DIST` and `DE9IM`, which take an additional parameter
// (the distance resp. the filter pattern) and are therefore not exposed as
// `geof:sf...`-style two-argument relations.
enum class SpatialJoinTypeEnum {
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
}  // namespace detail

// Wrapper around `detail::SpatialJoinTypeEnum` that provides conversion to
// and from the string representation used in the SPARQL syntax (e.g. for
// error messages, see `SpatialQuery.cpp`). Note: the underlying
// `SpatialJoinTypeEnum` (accessible as `SpatialJoinType::Enum`), and not
// `SpatialJoinType` itself, has to be used as a template parameter wherever
// the join type is required at compile time (e.g. `WktGeometricRelation`),
// because `SpatialJoinType` is not a structural type.
class SpatialJoinType
    : public ad_utility::EnumWithStrings<SpatialJoinType,
                                         detail::SpatialJoinTypeEnum> {
 public:
  using Enum = detail::SpatialJoinTypeEnum;

  static constexpr std::array<std::pair<Enum, std::string_view>, 10>
      descriptions_{{{Enum::INTERSECTS, "intersects"},
                     {Enum::CONTAINS, "contains"},
                     {Enum::COVERS, "covers"},
                     {Enum::CROSSES, "crosses"},
                     {Enum::TOUCHES, "touches"},
                     {Enum::EQUALS, "equals"},
                     {Enum::OVERLAPS, "overlaps"},
                     {Enum::WITHIN, "within"},
                     {Enum::WITHIN_DIST, "within-dist"},
                     {Enum::DE9IM, "de9im"}}};
  static const SpatialJoinType INTERSECTS;
  static const SpatialJoinType CONTAINS;
  static const SpatialJoinType COVERS;
  static const SpatialJoinType CROSSES;
  static const SpatialJoinType TOUCHES;
  static const SpatialJoinType EQUALS;
  static const SpatialJoinType OVERLAPS;
  static const SpatialJoinType WITHIN;
  static const SpatialJoinType WITHIN_DIST;
  static const SpatialJoinType DE9IM;

  static constexpr std::string_view typeName() { return "spatial join type"; }

  using EnumWithStrings::EnumWithStrings;
};

const inline SpatialJoinType SpatialJoinType::INTERSECTS{Enum::INTERSECTS};
const inline SpatialJoinType SpatialJoinType::CONTAINS{Enum::CONTAINS};
const inline SpatialJoinType SpatialJoinType::COVERS{Enum::COVERS};
const inline SpatialJoinType SpatialJoinType::CROSSES{Enum::CROSSES};
const inline SpatialJoinType SpatialJoinType::TOUCHES{Enum::TOUCHES};
const inline SpatialJoinType SpatialJoinType::EQUALS{Enum::EQUALS};
const inline SpatialJoinType SpatialJoinType::OVERLAPS{Enum::OVERLAPS};
const inline SpatialJoinType SpatialJoinType::WITHIN{Enum::WITHIN};
const inline SpatialJoinType SpatialJoinType::WITHIN_DIST{Enum::WITHIN_DIST};
const inline SpatialJoinType SpatialJoinType::DE9IM{Enum::DE9IM};

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINTYPE_H

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "rdfTypes/GeoCellGrid.h"

#include <cmath>

#include "backports/algorithm.h"
#include "util/Exception.h"

namespace ad_utility {

// ____________________________________________________________________________
const GeoCellGridScheme GeoCellGridScheme::Flat{Enum::Flat};

// ____________________________________________________________________________
GeoCellGrid::GeoCellGrid(uint8_t level, GeoCellGridScheme scheme)
    : level_{level}, scheme_{scheme} {
  // The `+ 2` accounts for the marker bit of the `SplitVocabulary` and at
  // least one bit for the position of a word; see `numPositionBits()`.
  AD_CONTRACT_CHECK(level >= 1 && numCellBits() + 2 <= ValueId::numDataBits,
                    "Invalid level for a geo cell grid");
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::sentinelCell() const {
  return (uint64_t{1} << numCellBits()) - 1;
}

// ____________________________________________________________________________
uint64_t GeoCellGrid::gridCoordinate(double normalized) const {
  AD_CONTRACT_CHECK(!std::isnan(normalized));

  // The number of cells per dimension is at most 2^31, so the conversion to
  // `double` (exact up to 2^53) is lossless.
  auto numCells = static_cast<double>(numCellsPerDimension());
  double raw = std::floor(normalized * numCells);
  return static_cast<uint64_t>(std::clamp(raw, 0.0, numCells - 1.0));
}

// ____________________________________________________________________________
GeoCellGrid::GridBox GeoCellGrid::gridBox(double u1, double v1, double u2,
                                          double v2) const {
  return {gridCoordinate(u1), gridCoordinate(v1), gridCoordinate(u2),
          gridCoordinate(v2)};
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::cellIndexFromPoint(double lng,
                                                       double lat) const {
  auto [u, v] = normalize(lng, lat);
  switch (scheme_) {
    case GeoCellGridScheme::Enum::Flat:
      return (gridCoordinate(v) << level_) | gridCoordinate(u);
  }
  AD_FAIL();
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::flatCell(double u1, double v1, double u2,
                                             double v2) const {
  auto [x1, y1, x2, y2] = gridBox(u1, v1, u2, v2);
  if (x1 != x2 || y1 != y2) {
    return sentinelCell();
  }
  return (y1 << level_) | x1;
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::cellIndexFromBoundingBox(
    const BoundingBox& box) const {
  // Normalize the corners through the `GeoPoint` bit encoding (idempotent):
  // the precomputed bounding boxes of the `GeoVocabulary` have gone through
  // this quantization, freshly parsed ones have not, and the cell assignment
  // must be identical for both.
  auto ll =
      GeoPoint::fromBitRepresentation(box.lowerLeft().toBitRepresentation());
  auto ur =
      GeoPoint::fromBitRepresentation(box.upperRight().toBitRepresentation());
  auto [u1, v1] = normalize(ll.getLng(), ll.getLat());
  auto [u2, v2] = normalize(ur.getLng(), ur.getLat());
  switch (scheme_) {
    case GeoCellGridScheme::Enum::Flat:
      return flatCell(u1, v1, u2, v2);
  }
  AD_FAIL();
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::cellIndexFromWktLiteral(
    std::string_view wktLiteral) const {
  auto box = GeometryInfo::getBoundingBox(wktLiteral);
  if (!box.has_value()) {
    return sentinelCell();
  }
  return cellIndexFromBoundingBox(box.value());
}

// ____________________________________________________________________________
void GeoCellGrid::flatCover(double u1, double v1, double u2, double v2,
                            CellRanges& ranges) const {
  auto [x1, y1, x2, y2] = gridBox(u1, v1, u2, v2);
  // One range per row, plus one for the sentinel appended by the caller.
  ranges.reserve(ranges.size() + (y2 - y1 + 1) + 1);
  for (uint64_t y = y1; y <= y2; ++y) {
    ranges.emplace_back((y << level_) | x1, (y << level_) | x2);
  }
}

// ____________________________________________________________________________
GeoCellGrid::CellRanges GeoCellGrid::coveringCellRanges(double minLng,
                                                        double minLat,
                                                        double maxLng,
                                                        double maxLat) const {
  AD_CONTRACT_CHECK(minLng <= maxLng && minLat <= maxLat);
  auto [u1, v1] = normalize(minLng, minLat);
  auto [u2, v2] = normalize(maxLng, maxLat);
  CellRanges ranges;
  switch (scheme_) {
    case GeoCellGridScheme::Enum::Flat:
      flatCover(u1, v1, u2, v2, ranges);
      break;
    default:
      AD_FAIL();
  }
  // The sentinel cell can hold a geometry that intersects any rectangle, so
  // its range is part of every cover, independently of the scheme.
  ranges.emplace_back(sentinelCell(), sentinelCell());
  return mergeRanges(ranges);
}

// ____________________________________________________________________________
GeoCellGrid::CellRanges GeoCellGrid::mergeRanges(const CellRanges& ranges) {
  AD_EXPENSIVE_CHECK(ql::ranges::is_sorted(ranges));
  CellRanges merged;
  merged.reserve(ranges.size());
  for (const auto& range : ranges) {
    if (!merged.empty() && range.first <= merged.back().second + 1) {
      merged.back().second = std::max(merged.back().second, range.second);
    } else {
      merged.push_back(range);
    }
  }
  return merged;
}

}  // namespace ad_utility

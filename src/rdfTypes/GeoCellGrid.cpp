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
std::string_view toString(GeoCellGridScheme scheme) {
  switch (scheme) {
    case GeoCellGridScheme::Flat:
      return "flat";
  }
  AD_FAIL();
}

// ____________________________________________________________________________
std::optional<GeoCellGridScheme> geoCellGridSchemeFromString(
    std::string_view name) {
  for (auto scheme : allGeoCellGridSchemes) {
    if (name == toString(scheme)) {
      return scheme;
    }
  }
  return std::nullopt;
}

// ____________________________________________________________________________
GeoCellGrid::GeoCellGrid(uint8_t level, GeoCellGridScheme scheme)
    : level_{level}, scheme_{scheme} {
  AD_CONTRACT_CHECK(level >= 1 && numCellBits() + 1 <= ValueId::numDataBits,
                    "Invalid level for a geo cell grid");
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::sentinelCell() const {
  return (uint64_t{1} << numCellBits()) - 1;
}

// ____________________________________________________________________________
uint64_t GeoCellGrid::gridCoordinate(double normalized) const {
  // The number of cells per dimension is at most 2^31, so the conversion to
  // `double` (exact up to 2^53) is lossless.
  double numCells = static_cast<double>(numCellsPerDimension());
  double raw = std::floor(normalized * numCells);
  return static_cast<uint64_t>(std::clamp(raw, 0.0, numCells - 1.0));
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::cellIndexFromPoint(double lng,
                                                       double lat) const {
  AD_CONTRACT_CHECK(scheme_ == GeoCellGridScheme::Flat);
  return (gridCoordinate((lat + 90.0) / 180.0) << level_) |
         gridCoordinate((lng + 180.0) / 360.0);
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::flatCell(double u1, double v1, double u2,
                                             double v2) const {
  uint64_t x1 = gridCoordinate(u1);
  uint64_t x2 = gridCoordinate(u2);
  uint64_t y1 = gridCoordinate(v1);
  uint64_t y2 = gridCoordinate(v2);
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
  uint64_t llBits = box.lowerLeft().toBitRepresentation();
  uint64_t urBits = box.upperRight().toBitRepresentation();
  auto ll = GeoPoint::fromBitRepresentation(llBits);
  auto ur = GeoPoint::fromBitRepresentation(urBits);
  double u1 = (ll.getLng() + 180.0) / 360.0;
  double u2 = (ur.getLng() + 180.0) / 360.0;
  double v1 = (ll.getLat() + 90.0) / 180.0;
  double v2 = (ur.getLat() + 90.0) / 180.0;
  switch (scheme_) {
    case GeoCellGridScheme::Flat:
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
  uint64_t x1 = gridCoordinate(u1);
  uint64_t x2 = gridCoordinate(u2);
  uint64_t y1 = gridCoordinate(v1);
  uint64_t y2 = gridCoordinate(v2);
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
  double u1 = (minLng + 180.0) / 360.0;
  double u2 = (maxLng + 180.0) / 360.0;
  double v1 = (minLat + 90.0) / 180.0;
  double v2 = (maxLat + 90.0) / 180.0;
  CellRanges ranges;
  switch (scheme_) {
    case GeoCellGridScheme::Flat:
      flatCover(u1, v1, u2, v2, ranges);
      ranges.emplace_back(sentinelCell(), sentinelCell());
      break;
  }
  // Sort and merge into ascending, non-overlapping ranges (adjacent ranges
  // are merged as well).
  ql::ranges::sort(ranges);
  CellRanges merged;
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

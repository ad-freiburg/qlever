// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "rdfTypes/GeoCellGrid.h"

#include <absl/numeric/bits.h>

#include <cmath>

#include "backports/algorithm.h"
#include "util/Exception.h"

namespace ad_utility {

// ____________________________________________________________________________
GeoRectangle padGeoRectangle(const GeoRectangle& rectangle,
                             double distanceMeters) {
  AD_CONTRACT_CHECK(distanceMeters >= 0);
  // One degree of latitude is at least 110'567 m everywhere, so dividing by
  // 110'000 overestimates the padding.
  double dLat = distanceMeters / 110'000.0;
  double minLat = std::max(rectangle.minLat_ - dLat, -90.0);
  double maxLat = std::min(rectangle.maxLat_ + dLat, 90.0);
  // One degree of longitude at latitude x is at least 110'000 m * cos(x), so
  // using the largest latitude of the padded band overestimates the padding.
  double maxAbsLat = std::max(std::abs(minLat), std::abs(maxLat));
  double minLng = -180.0;
  double maxLng = 180.0;
  if (maxAbsLat < 89.0) {
    double dLng =
        distanceMeters / (110'000.0 * std::cos(maxAbsLat * M_PI / 180.0));
    minLng = rectangle.minLng_ - dLng;
    maxLng = rectangle.maxLng_ + dLng;
    if (minLng < -180.0 || maxLng > 180.0) {
      // The padded rectangle wraps around the antimeridian, which the grid
      // cannot represent; degrade to the full longitude range.
      minLng = -180.0;
      maxLng = 180.0;
    }
  }
  return {minLng, minLat, maxLng, maxLat};
}

// ____________________________________________________________________________
const GeoCellGridScheme GeoCellGridScheme::Flat{Enum::Flat};
const GeoCellGridScheme GeoCellGridScheme::Flat4Shifts{Enum::Flat4Shifts};
const GeoCellGridScheme GeoCellGridScheme::Hierarchical{Enum::Hierarchical};
const GeoCellGridScheme GeoCellGridScheme::Hierarchical3Shifts{
    Enum::Hierarchical3Shifts};

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
  if (isHierarchical()) {
    // The root cell of the first quadtree copy.
    return uint64_t{1} << (2 * uint64_t{level_});
  }
  return maxCellIndex();
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

namespace {

// Deterministic 64-bit mixer (splitmix64). Used to spread the grid copy
// assignment of the `Flat4Shifts` scheme. NOTE: This function is part of the
// index format; changing it changes the cell assignment.
uint64_t mixHash(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

// The hash that `Flat4Shifts` derives from the two corners of a bounding box
// (given as `GeoPoint` bit representations).
uint64_t cornerHash(uint64_t lowerLeftBits, uint64_t upperRightBits) {
  return mixHash(lowerLeftBits ^ (upperRightBits << 1));
}

// The fractional part of `x` in [0, 1).
double fractional(double x) { return x - std::floor(x); }

// Interleave the lowest `numBits` bits of `x` and `y` into a Morton code
// (bit i of y above bit i of x).
uint64_t mortonInterleave(uint64_t x, uint64_t y, uint64_t numBits) {
  uint64_t code = 0;
  for (uint64_t i = 0; i < numBits; ++i) {
    code |= ((x >> i) & 1) << (2 * i);
    code |= ((y >> i) & 1) << (2 * i + 1);
  }
  return code;
}

}  // namespace

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::cellIndexFromPoint(double lng,
                                                       double lat) const {
  auto [u, v] = normalize(lng, lat);
  switch (scheme_.value()) {
    case GeoCellGridScheme::Enum::Flat:
      return (gridCoordinate(v) << level_) | gridCoordinate(u);
    case GeoCellGridScheme::Enum::Flat4Shifts: {
      // Same hash as `cellIndexFromBoundingBox` for the degenerate box.
      auto bits =
          GeoPoint{std::clamp(lat, -90.0, 90.0), std::clamp(lng, -180.0, 180.0)}
              .toBitRepresentation();
      return flat4ShiftsCell(u, v, u, v, cornerHash(bits, bits));
    }
    case GeoCellGridScheme::Enum::Hierarchical:
      return hierarchicalCell(u, v, u, v);
    case GeoCellGridScheme::Enum::Hierarchical3Shifts:
      return hierarchical3ShiftsCell(u, v, u, v);
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
GeoCellGrid::CellIndex GeoCellGrid::flat4ShiftsCell(double u1, double v1,
                                                    double u2, double v2,
                                                    uint64_t hash) const {
  // Grid copy s = (sy << 1) | sx is shifted by half a cell in the dimensions
  // with a set bit. The shifted grid coordinate: subtract half a cell before
  // flooring, clamp at the edges (the border cells of a shifted copy are
  // 1.5 cells wide, which is fine for conservative containment).
  auto coordinate = [numCells = static_cast<double>(numCellsPerDimension())](
                        double normalized, bool shifted) {
    double raw = std::floor(normalized * numCells - (shifted ? 0.5 : 0.0));
    return static_cast<uint64_t>(std::clamp(raw, 0.0, numCells - 1.0));
  };
  // Try the four copies in a hash-dependent order and take the first one
  // whose cell contains the box: this spreads small geometries (which fit
  // several copies) evenly, so that no copy accumulates most of the data.
  for (uint64_t i = 0; i < 4; ++i) {
    uint64_t s = (hash + i) & 3;
    bool sx = (s & 1) != 0;
    bool sy = (s & 2) != 0;
    uint64_t x1 = coordinate(u1, sx);
    uint64_t x2 = coordinate(u2, sx);
    uint64_t y1 = coordinate(v1, sy);
    uint64_t y2 = coordinate(v2, sy);
    if (x1 == x2 && y1 == y2) {
      return (s << (2 * uint64_t{level_})) | (y1 << level_) | x1;
    }
  }
  return sentinelCell();
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::hierarchicalCellOfLeafBox(
    uint64_t x1, uint64_t y1, uint64_t x2, uint64_t y2) const {
  // The smallest enclosing cell is the lowest common ancestor of the two
  // corner leaves: its depth is determined by the highest differing bit of
  // the leaf coordinates.
  uint64_t diff = (x1 ^ x2) | (y1 ^ y2);
  uint64_t depthBelow = absl::bit_width(diff);  // = level_ - cell level
  uint64_t path =
      mortonInterleave(x1 >> depthBelow, y1 >> depthBelow, level_ - depthBelow);
  // S2-style encoding: the Morton path, followed by a single 1 bit, padded
  // with zeros to 2 * level_ + 1 bits. The descendants (and only they) of a
  // cell with encoding c at depth d below the leaves then form the interval
  // [c - (4^d - 1), c + (4^d - 1)].
  return (path << (2 * depthBelow + 1)) | (uint64_t{1} << (2 * depthBelow));
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::hierarchicalCell(double u1, double v1,
                                                     double u2,
                                                     double v2) const {
  auto [x1, y1, x2, y2] = gridBox(u1, v1, u2, v2);
  return hierarchicalCellOfLeafBox(x1, y1, x2, y2);
}

// ____________________________________________________________________________
GeoCellGrid::CellIndex GeoCellGrid::hierarchical3ShiftsCell(double u1,
                                                            double v1,
                                                            double u2,
                                                            double v2) const {
  // Chan's shifted quadtrees: copy s is translated by s/3 of the domain in
  // both dimensions (with wrap-around; a box that wraps in a copy simply
  // does not fit a regular cell there and falls back to that copy's root).
  // Every box of size w fits a cell of size <= 6w in at least one copy. Take
  // the deepest cell over the three copies, ties broken by the smaller copy
  // index.
  CellIndex bestCell = 0;
  uint64_t bestDepthBelow = 0;
  bool haveBest = false;
  for (uint64_t s = 0; s < 3; ++s) {
    double offset = static_cast<double>(s) / 3.0;
    double a1 = fractional(u1 + offset);
    double a2 = fractional(u2 + offset);
    double b1 = fractional(v1 + offset);
    double b2 = fractional(v2 + offset);
    CellIndex cell;
    if (a2 < a1 || b2 < b1) {
      // Wrapped around in this copy: only the root contains the box.
      cell = uint64_t{1} << (2 * uint64_t{level_});
    } else {
      auto [x1, y1, x2, y2] = gridBox(a1, b1, a2, b2);
      cell = hierarchicalCellOfLeafBox(x1, y1, x2, y2);
    }
    // The depth below the leaves is encoded in the number of trailing zeros.
    uint64_t depthBelow = absl::countr_zero(cell);
    if (!haveBest || depthBelow < bestDepthBelow) {
      bestCell = (s << (2 * uint64_t{level_} + 1)) | cell;
      bestDepthBelow = depthBelow;
      haveBest = true;
    }
  }
  return bestCell;
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
  auto [u1, v1] = normalize(ll.getLng(), ll.getLat());
  auto [u2, v2] = normalize(ur.getLng(), ur.getLat());
  switch (scheme_.value()) {
    case GeoCellGridScheme::Enum::Flat:
      return flatCell(u1, v1, u2, v2);
    case GeoCellGridScheme::Enum::Flat4Shifts:
      return flat4ShiftsCell(u1, v1, u2, v2, cornerHash(llBits, urBits));
    case GeoCellGridScheme::Enum::Hierarchical:
      return hierarchicalCell(u1, v1, u2, v2);
    case GeoCellGridScheme::Enum::Hierarchical3Shifts:
      return hierarchical3ShiftsCell(u1, v1, u2, v2);
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
void GeoCellGrid::flat4ShiftsCover(double u1, double v1, double u2, double v2,
                                   CellRanges& ranges) const {
  auto coordinate = [numCells = static_cast<double>(numCellsPerDimension())](
                        double normalized, bool shifted) {
    double raw = std::floor(normalized * numCells - (shifted ? 0.5 : 0.0));
    return static_cast<uint64_t>(std::clamp(raw, 0.0, numCells - 1.0));
  };
  for (uint64_t s = 0; s < 4; ++s) {
    bool sx = (s & 1) != 0;
    bool sy = (s & 2) != 0;
    uint64_t x1 = coordinate(u1, sx);
    uint64_t x2 = coordinate(u2, sx);
    uint64_t y1 = coordinate(v1, sy);
    uint64_t y2 = coordinate(v2, sy);
    uint64_t prefix = s << (2 * uint64_t{level_});
    for (uint64_t y = y1; y <= y2; ++y) {
      ranges.emplace_back(prefix | (y << level_) | x1,
                          prefix | (y << level_) | x2);
    }
  }
}

// ____________________________________________________________________________
void GeoCellGrid::hierarchicalCoverRecurse(uint64_t queryX1, uint64_t queryY1,
                                           uint64_t queryX2, uint64_t queryY2,
                                           uint64_t levelK, uint64_t cellX,
                                           uint64_t cellY,
                                           CellIndex shiftPrefix,
                                           CellRanges& ranges) const {
  uint64_t depthBelow = level_ - levelK;
  uint64_t firstX = cellX << depthBelow;
  uint64_t lastX = ((cellX + 1) << depthBelow) - 1;
  uint64_t firstY = cellY << depthBelow;
  uint64_t lastY = ((cellY + 1) << depthBelow) - 1;
  if (lastX < queryX1 || firstX > queryX2 || lastY < queryY1 ||
      firstY > queryY2) {
    return;
  }
  uint64_t path = mortonInterleave(cellX, cellY, levelK);
  CellIndex id = shiftPrefix | (path << (2 * depthBelow + 1)) |
                 (uint64_t{1} << (2 * depthBelow));
  uint64_t subtreeOffset = (uint64_t{1} << (2 * depthBelow)) - 1;
  if (firstX >= queryX1 && lastX <= queryX2 && firstY >= queryY1 &&
      lastY <= queryY2) {
    // Fully inside: this cell and all its descendants match.
    ranges.emplace_back(id - subtreeOffset, id + subtreeOffset);
    return;
  }
  // Partially inside: this cell matches (geometries stored here can
  // intersect the query rectangle); descend for the descendants.
  ranges.emplace_back(id, id);
  if (levelK < level_) {
    for (uint64_t child = 0; child < 4; ++child) {
      hierarchicalCoverRecurse(queryX1, queryY1, queryX2, queryY2, levelK + 1,
                               (cellX << 1) | (child & 1),
                               (cellY << 1) | (child >> 1), shiftPrefix,
                               ranges);
    }
  }
}

// ____________________________________________________________________________
void GeoCellGrid::hierarchicalCover(double u1, double v1, double u2, double v2,
                                    CellIndex shiftPrefix,
                                    CellRanges& ranges) const {
  auto [x1, y1, x2, y2] = gridBox(u1, v1, u2, v2);
  hierarchicalCoverRecurse(x1, y1, x2, y2, 0, 0, 0, shiftPrefix, ranges);
}

// ____________________________________________________________________________
void GeoCellGrid::hierarchical3ShiftsCover(double u1, double v1, double u2,
                                           double v2,
                                           CellRanges& ranges) const {
  for (uint64_t s = 0; s < 3; ++s) {
    double offset = static_cast<double>(s) / 3.0;
    CellIndex prefix = s << (2 * uint64_t{level_} + 1);
    double a1 = fractional(u1 + offset);
    double a2 = fractional(u2 + offset);
    double b1 = fractional(v1 + offset);
    double b2 = fractional(v2 + offset);
    // A rectangle that wraps around in the translated coordinates splits
    // into up to four pieces.
    std::vector<std::pair<double, double>> uIntervals =
        a2 < a1 ? std::vector<std::pair<double, double>>{{a1, 1.0}, {0.0, a2}}
                : std::vector<std::pair<double, double>>{{a1, a2}};
    std::vector<std::pair<double, double>> vIntervals =
        b2 < b1 ? std::vector<std::pair<double, double>>{{b1, 1.0}, {0.0, b2}}
                : std::vector<std::pair<double, double>>{{b1, b2}};
    for (const auto& [ua, ub] : uIntervals) {
      for (const auto& [va, vb] : vIntervals) {
        hierarchicalCover(ua, va, ub, vb, prefix, ranges);
      }
    }
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
  switch (scheme_.value()) {
    case GeoCellGridScheme::Enum::Flat:
      flatCover(u1, v1, u2, v2, ranges);
      break;
    case GeoCellGridScheme::Enum::Flat4Shifts:
      flat4ShiftsCover(u1, v1, u2, v2, ranges);
      break;
    case GeoCellGridScheme::Enum::Hierarchical:
      hierarchicalCover(u1, v1, u2, v2, 0, ranges);
      break;
    case GeoCellGridScheme::Enum::Hierarchical3Shifts:
      hierarchical3ShiftsCover(u1, v1, u2, v2, ranges);
      break;
    default:
      AD_FAIL();
  }
  // The sentinel cell can hold a geometry that intersects any rectangle, so
  // its range is part of every cover, independently of the scheme (for the
  // hierarchical schemes, the root cells are produced by the cover anyway).
  ranges.emplace_back(sentinelCell(), sentinelCell());
  // Only the `Flat` scheme produces its ranges in ascending order.
  if (scheme_ != GeoCellGridScheme::Enum::Flat) {
    ql::ranges::sort(ranges);
  }
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

// ____________________________________________________________________________
GeoCellIdPrefilter::GeoCellIdPrefilter(const GeoCellGrid& grid, double minLng,
                                       double minLat, double maxLng,
                                       double maxLat) {
  for (auto [first, last] :
       grid.coveringCellRanges(minLng, minLat, maxLng, maxLat)) {
    keepRanges_.push_back(grid.vocabIndexRangeForCells(first, last));
  }
}

// ____________________________________________________________________________
bool GeoCellIdPrefilter::canBeSkipped(uint64_t vocabIndexBits) const {
  if (!GeoCellGrid::isGeoVocabIndex(vocabIndexBits)) {
    // Not a WKT literal of the geo vocabulary, so we cannot decide anything.
    return false;
  }
  // Find the first keep-range that ends after the index; the index is kept
  // iff that range also starts at or before it.
  auto it = ql::ranges::upper_bound(keepRanges_, vocabIndexBits, {},
                                    &std::pair<uint64_t, uint64_t>::second);
  return it == keepRanges_.end() || vocabIndexBits < it->first;
}

}  // namespace ad_utility

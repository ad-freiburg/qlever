// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include "rdfTypes/GeoCellGrid.h"

#include <algorithm>
#include <cmath>

#include "util/Exception.h"

namespace ad_utility {

// ____________________________________________________________________________
GeoRectangle padGeoRectangle(GeoRectangle rectangle, double distanceMeters) {
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
    constexpr double pi = 3.14159265358979323846;
    double dLng =
        distanceMeters / (110'000.0 * std::cos(maxAbsLat * pi / 180.0));
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
GeoCellGrid::GeoCellGrid(uint8_t level) : level_{level} {
  AD_CONTRACT_CHECK(level >= 1 && numCellBits() + 1 <= ValueId::numDataBits,
                    "Invalid level for a geo cell grid");
}

// ____________________________________________________________________________
uint64_t GeoCellGrid::gridX(double lng) const {
  double raw = std::floor((lng + 180.0) / 360.0 * numCellsPerDimension());
  return static_cast<uint64_t>(
      std::clamp(raw, 0.0, static_cast<double>(numCellsPerDimension() - 1)));
}

// ____________________________________________________________________________
uint64_t GeoCellGrid::gridY(double lat) const {
  double raw = std::floor((lat + 90.0) / 180.0 * numCellsPerDimension());
  return static_cast<uint64_t>(
      std::clamp(raw, 0.0, static_cast<double>(numCellsPerDimension() - 1)));
}

// ____________________________________________________________________________
GeoCellGrid::Cell GeoCellGrid::cellFromPoint(double lng, double lat) const {
  return (gridY(lat) << level_) | gridX(lng);
}

// ____________________________________________________________________________
GeoCellGrid::Cell GeoCellGrid::cellFromBoundingBox(
    const BoundingBox& box) const {
  // Normalize the corners through the `GeoPoint` bit encoding (idempotent):
  // the precomputed bounding boxes of the `GeoVocabulary` have gone through
  // this quantization, freshly parsed ones have not, and the cell assignment
  // must be identical for both.
  auto quantize = [](const GeoPoint& p) {
    return GeoPoint::fromBitRepresentation(p.toBitRepresentation());
  };
  auto ll = quantize(box.lowerLeft());
  auto ur = quantize(box.upperRight());
  if (gridX(ll.getLng()) != gridX(ur.getLng()) ||
      gridY(ll.getLat()) != gridY(ur.getLat())) {
    return sentinelCell();
  }
  return cellFromPoint(ll.getLng(), ll.getLat());
}

// ____________________________________________________________________________
GeoCellGrid::Cell GeoCellGrid::cellFromWktLiteral(
    std::string_view wktLiteral) const {
  auto box = GeometryInfo::getBoundingBox(wktLiteral);
  if (!box.has_value()) {
    return sentinelCell();
  }
  return cellFromBoundingBox(box.value());
}

// ____________________________________________________________________________
std::vector<std::pair<GeoCellGrid::Cell, GeoCellGrid::Cell>>
GeoCellGrid::coveringCellRanges(double minLng, double minLat, double maxLng,
                                double maxLat) const {
  AD_CONTRACT_CHECK(minLng <= maxLng && minLat <= maxLat);
  uint64_t x1 = gridX(minLng);
  uint64_t x2 = gridX(maxLng);
  uint64_t y1 = gridY(minLat);
  uint64_t y2 = gridY(maxLat);
  std::vector<std::pair<Cell, Cell>> result;
  result.reserve(y2 - y1 + 2);
  for (uint64_t y = y1; y <= y2; ++y) {
    result.emplace_back((y << level_) | x1, (y << level_) | x2);
  }
  result.emplace_back(sentinelCell(), sentinelCell());
  return result;
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
  auto it = std::upper_bound(
      keepRanges_.begin(), keepRanges_.end(), vocabIndexBits,
      [](uint64_t bits, const std::pair<uint64_t, uint64_t>& range) {
        return bits < range.second;
      });
  return it == keepRanges_.end() || vocabIndexBits < it->first;
}

}  // namespace ad_utility

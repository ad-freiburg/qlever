// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_RDFTYPES_GEORECTANGLE_H
#define QLEVER_SRC_RDFTYPES_GEORECTANGLE_H

#include <optional>
#include <utility>
#include <vector>

#include "global/ValueId.h"

namespace ad_utility {

class GeoCellGrid;

// A geographic rectangle in plain degrees. In contrast to `BoundingBox` it
// is a simple aggregate without invariants, suitable for query rectangles
// that may cover the whole world.
struct GeoRectangle {
  double minLng_;
  double minLat_;
  double maxLng_;
  double maxLat_;
  bool operator==(const GeoRectangle&) const = default;
};

// Grow `rectangle` on all sides by at least `distanceMeters` (measured on the
// earth's surface) and clamp it to the valid coordinate ranges. The result is
// conservative: every point within `distanceMeters` of the input rectangle is
// contained in the result. Near the poles and across the antimeridian the
// longitude range degrades to [-180, 180].
GeoRectangle padGeoRectangle(const GeoRectangle& rectangle,
                             double distanceMeters);

// The estimated fraction of the rows that the block prefilter
// `GeoRectangleExpression` keeps for `rectangle` which actually lie inside
// the rectangle. The block prefilter keeps the whole latitude band of the
// rectangle, so with geometries spread uniformly in longitude this is the
// share of the band that the rectangle covers. The query planner uses it as
// the selectivity of a spatial join whose geometry side was prefiltered.
double geoRectangleSelectivity(const GeoRectangle& rectangle);

// The share of the area of the cells of `grid` that `rectangle` touches which
// is covered by `rectangle` itself, in [0, 1]. A prefilter with `rectangle`
// keeps all geometries of these cells. Assuming that the geometries are spread
// uniformly within a cell, this is the fraction of them that actually lie in
// `rectangle`, which makes it a size estimate for the prefiltered candidates.
double fractionOfCoveringCells(const GeoRectangle& rectangle,
                               const GeoCellGrid& grid);

// The selectivity for an index with the given grid: the share of the covering
// cells with a grid (the block prefilter then keeps exactly those cells), the
// share of the latitude band without one.
double geoRectangleSelectivity(const GeoRectangle& rectangle,
                               const std::optional<GeoCellGrid>& grid);

// Decide for a single `ValueId`, without any disk access, whether the
// geometry it stands for is certainly outside a query rectangle. This is the
// row-level counterpart of the block prefilter `GeoRectangleExpression`. A
// `GeoPoint` is decided by the coordinates encoded in the ID; a WKT literal of
// a vocabulary with a geo cell grid by the cell bits of its ID (a bit
// extraction and a binary search over a handful of cell ranges). Conservative:
// literals without cell information, indices outside the WKT region, and all
// other datatypes are never skipped.
class GeoRectangleIdPrefilter {
  GeoRectangle rectangle_;
  // Half-open, ascending ranges of vocabulary index payloads that must be
  // kept (covering cells plus the "no information" cells); empty without a
  // grid.
  std::vector<std::pair<uint64_t, uint64_t>> keepRanges_;

 public:
  explicit GeoRectangleIdPrefilter(const GeoRectangle& rectangle)
      : rectangle_{rectangle} {}
  GeoRectangleIdPrefilter(const std::optional<GeoCellGrid>& grid,
                          const GeoRectangle& rectangle);

  // Return true iff the word with the given vocabulary index payload is
  // certainly outside the query rectangle (only decidable with a grid).
  bool canBeSkipped(uint64_t vocabIndexBits) const;

  // Return true iff the geometry with the given ID is certainly outside the
  // query rectangle (see above).
  bool canBeSkipped(ValueId id) const;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_RDFTYPES_GEORECTANGLE_H

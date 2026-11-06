// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_FIXEDGEOMETRYALGORITHM_H
#define QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_FIXEDGEOMETRYALGORITHM_H

#include <spatialjoin/BoxIds.h>
#include <util/geo/Geo.h>

#include <optional>
#include <variant>

#include "engine/spatialJoinAlgorithms/SpatialJoinAlgorithmBase.h"

// A spatial join where one side is a single geometry, a point or a polygon,
// and the other side consists of points. This is the shape of a `FILTER` with
// a constant geometry that the query planner rewrites to a spatial join, for
// example all places within a given polygon, or within a distance of a given
// point. The one geometry is prepared once, and every point of the other side
// is checked against it directly, with the predicate functions of
// `libspatialjoin`, in memory and without its sweep. The results are the same
// as those of the `LibspatialjoinAlgorithm`, which writes the geometries to an
// event file on disk, sorts that file, and runs several thread pools; for
// small inputs it spends almost all of its time in this machinery.
//
// Supported are the join types `INTERSECTS`, `CONTAINS`, `COVERS`, `WITHIN`,
// and, for a fixed point, `WITHIN_DIST`. For other join types, for a fixed
// geometry of another type (a line or a multipolygon), or when the other side
// contains WKT literals, `runIfApplicable` returns `std::nullopt` and the
// caller uses the `LibspatialjoinAlgorithm`.
class FixedGeometryAlgorithm : public SpatialJoinAlgorithmBase {
 public:
  using SpatialJoinAlgorithmBase::SpatialJoinAlgorithmBase;

  // Run the algorithm if it applies to the inputs (see above), otherwise
  // return `std::nullopt` after no more than a pass over the IDs of the
  // other side.
  std::optional<Result> runIfApplicable();

  // Run the algorithm. The inputs must be such that it applies.
  Result run() override;

  // A polygon prepared for the checks against many points, in the integer
  // Web Mercator coordinates of `libspatialjoin`: with its bounding box and
  // its box ids (the grid cells it covers fully or partially), which decide
  // most points without a look at the polygon itself.
  struct PreparedPolygon {
    util::geo::I32XSortedPolygon polygon_;
    util::geo::I32Box boundingBox_;
    sj::boxids::BoxIdList boxIds_;
  };
  using FixedGeometry = std::variant<util::geo::I32Point, PreparedPolygon>;

  // Parse the fixed geometry from a `ValueId`: a `GeoPoint`, or a WKT literal
  // from the vocabulary of `index` or from the local vocabulary that is a
  // point or a polygon. Returns `std::nullopt` for everything else.
  static std::optional<FixedGeometry> parseFixedGeometry(ValueId id,
                                                         const Index& index);

  // The relation of a point to a prepared polygon, computed as
  // `libspatialjoin` computes it in its sweep (bounding box and box ids
  // first, the exact check only if they do not decide).
  static util::geo::DE9IMatrix relate(const util::geo::I32Point& point,
                                      const PreparedPolygon& polygon);

  // The distance in meters between two points in integer Web Mercator
  // coordinates, computed as `libspatialjoin` computes it for a distance
  // join. Points that are certainly further apart than `maxDist` get
  // `std::numeric_limits<double>::max()`.
  static double meterDist(const util::geo::I32Point& a,
                          const util::geo::I32Point& b, double maxDist);

  // Convert a `GeoPoint` to the integer Web Mercator coordinates of
  // `libspatialjoin`, exactly as the `SpatialJoinParser` does.
  static util::geo::I32Point toWebMercator(const GeoPoint& point);

 private:
  // The actual join, after `runIfApplicable` has checked the inputs.
  Result runWith(bool fixedIsLeft, const FixedGeometry& fixed);
};

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_FIXEDGEOMETRYALGORITHM_H

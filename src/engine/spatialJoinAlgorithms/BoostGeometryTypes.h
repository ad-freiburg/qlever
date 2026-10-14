// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_BOOSTGEOMETRYTYPES_H
#define QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_BOOSTGEOMETRYTYPES_H

// NOTE: Only include the parts of Boost.Geometry that are actually needed and
// in particular NOT `<boost/geometry.hpp>`. Starting with Boost 1.90, some of
// the algorithms (e.g. `closest_points`, `covered_by`, `within`) transitively
// include `<boost/range/algorithm/sort.hpp>`, which contains the
// using-declaration `boost::range::sort` in namespace `boost`. That
// using-declaration is ill-formed in every translation unit that also sees the
// namespace `boost::sort` (from Boost.Sort, used by `util/blockSort`, which is
// part of the precompiled header of the `engine` library). Those algorithms are
// therefore only used in `BoostGeometryAlgorithms.cpp`, see the functions
// declared below.
#include <boost/geometry/algorithms/envelope.hpp>
#include <boost/geometry/core/cs.hpp>
#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <boost/geometry/io/wkt/read.hpp>
#include <boost/geometry/strategies/strategies.hpp>
#include <boost/variant.hpp>
#include <optional>

#include "rdfTypes/GeoPoint.h"

// Geometry types (and helpers operating on them) used by the algorithms that
// parse arbitrary geometries (points and areas) into `RtreeEntry`s, i.e.
// `RtreeEntryAlgorithm` and its subclasses. Not used by the S2-based or
// `libspatialjoin`-based algorithms, which parse geometries differently.
namespace BoostGeometryNamespace {
namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

using Point = bg::model::point<double, 2, bg::cs::cartesian>;
using Box = bg::model::box<Point>;
using Polygon = boost::geometry::model::polygon<
    boost::geometry::model::d2::point_xy<double>>;
using Linestring = bg::model::linestring<Point>;
using MultiPoint = bg::model::multi_point<Point>;
using MultiLinestring = bg::model::multi_linestring<Linestring>;
using MultiPolygon = bg::model::multi_polygon<Polygon>;
using AnyGeometry = boost::variant<Point, Linestring, Polygon, MultiPoint,
                                   MultiLinestring, MultiPolygon>;
using Segment = boost::geometry::model::segment<Point>;

// this struct is used to get the bounding box of an arbitrary geometry type.
struct BoundingBoxVisitor : public boost::static_visitor<Box> {
  template <typename Geometry>
  Box operator()(const Geometry& geometry) const {
    Box box;
    boost::geometry::envelope(geometry, box);
    return box;
  }
};

// Return the two closest points (in euclidean geometry) of two arbitrary
// geometries as a segment. Defined in `BoostGeometryAlgorithms.cpp` (see the
// note at the top).
Segment closestPoints(const AnyGeometry& geo1, const AnyGeometry& geo2);

// Return true iff `point` is inside of or on the boundary of `box`. Defined in
// `BoostGeometryAlgorithms.cpp` (see the note at the top).
bool coveredBy(const Point& point, const Box& box);

struct RtreeEntry {
  size_t row_;
  std::optional<size_t> geometryIndex_;
  std::optional<GeoPoint> geoPoint_;
  std::optional<Box> boundingBox_;
};

using Value = std::pair<Box, RtreeEntry>;

}  // namespace BoostGeometryNamespace

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_BOOSTGEOMETRYTYPES_H

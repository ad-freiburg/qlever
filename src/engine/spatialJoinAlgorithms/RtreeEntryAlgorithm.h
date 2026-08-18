// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_RTREEENTRYALGORITHM_H
#define QLEVER_SRC_ENGINE_RTREEENTRYALGORITHM_H

#include <boost/foreach.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <variant>

#include "engine/spatialJoinAlgorithms/SpatialJoinAlgorithms.h"
#include "rdfTypes/GeoSparqlHelpers.h"
#include "util/VectorWithMemoryLimit.h"

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

// this struct is used to calculate the distance between two arbitrary
// geometries. It calculates the two closest points (in euclidean geometry),
// transforms the two closest points, to a GeoPoint and then calculates the
// distance of the two points on the earth. As the closest points are calculated
// using euclidean geometry, this is only an approximation. On the sphere two
// other points might be closer.
struct ClosestPointVisitor : public boost::static_visitor<double> {
  template <typename Geometry1, typename Geometry2>
  double operator()(const Geometry1& geo1, const Geometry2& geo2) const {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
    throw std::runtime_error(
        "ClosestPointVisitor not implemented for C++17, please use a different "
        "spatial join implementation");
    (void)geo1;
    (void)geo2;
#else
    Segment seg;
    bg::closest_points(geo1, geo2, seg);
    GeoPoint closestPoint1(bg::get<0, 1>(seg), bg::get<0, 0>(seg));
    GeoPoint closestPoint2(bg::get<1, 1>(seg), bg::get<1, 0>(seg));
    return ad_utility::detail::wktDistImpl(closestPoint1, closestPoint2);
#endif
  }
};

struct RtreeEntry {
  size_t row_;
  std::optional<size_t> geometryIndex_;
  std::optional<GeoPoint> geoPoint_;
  std::optional<Box> boundingBox_;
};

using Value = std::pair<Box, RtreeEntry>;

}  // namespace BoostGeometryNamespace

// Intermediate base for the two algorithms that support arbitrary geometries
// (points and areas) by parsing each of them into an `RtreeEntry` and
// approximating the distance between two such entries: `BaselineAlgorithm`
// (checks every pair) and `BoundingBoxAlgorithm` (prunes pairs using an
// r-tree over these entries). The S2-based algorithms only support points and
// `LibspatialjoinAlgorithm` parses geometries completely differently, so none
// of them need (or get) this shared state.
class RtreeEntryAlgorithm : public SpatialJoinAlgorithms {
 protected:
  using Point = BoostGeometryNamespace::Point;
  using Box = BoostGeometryNamespace::Box;
  using AnyGeometry = BoostGeometryNamespace::AnyGeometry;
  using RtreeEntry = BoostGeometryNamespace::RtreeEntry;

 public:
  using SpatialJoinAlgorithms::SpatialJoinAlgorithms;

  // calculates the midpoint of the given Box
  Point calculateMidpointOfBox(const Box& box) const;

  void setUseMidpointForAreas_(bool useMidpointForAreas) {
    useMidpointForAreas_ = useMidpointForAreas;
  }

  // Helper function, which computes the distance of two geometries, where each
  // geometry has already been parsed and is available as an RtreeEntry
  Id computeDist(RtreeEntry& geo1, RtreeEntry& geo2);

  // wrapper to access non const private function for testing
  std::optional<RtreeEntry> onlyForTestingGetRtreeEntry(
      const IdTableView<0>* idTable, const size_t row, const ColumnIndex col) {
    return getRtreeEntry(idTable, row, col);
  }

 protected:
  // this helper function takes an idtable, a row and a column. It then tries
  // to parse a geometry or a geoPoint of that cell in the idtable. If it
  // succeeds, it returns an rtree entry of that geometry/geopoint
  std::optional<RtreeEntry> getRtreeEntry(const IdTableView<0>* idTable,
                                          const size_t row,
                                          const ColumnIndex col);

  // if the distance calculation should be approximated, by the midpoint of
  // the area
  bool useMidpointForAreas_ = true;

  // this vector stores the geometries, which have already been parsed
  ad_utility::VectorWithMemoryLimit<AnyGeometry> geometries_{
      qec_->getAllocator()};

 private:
  // returns everything between the first two quotes. If the string does not
  // contain two quotes, the string is returned as a whole
  std::string_view betweenQuotes(std::string_view extractFrom) const;

  // this helper function gets the string which represents the area from the
  // idtable.
  std::optional<size_t> getAnyGeometry(const IdTableView<0>* idtable,
                                       size_t row, size_t col);

  // this helper function approximates a conversion of the distance between two
  // objects from degrees to meters. Here we assume, that the conversion from
  // degrees to meters is constant, which is however only true for the latitude
  // values. For the longitude values this is not true. Therefore a value which
  // works very good for almost all longitudes and latitudes has been chosen.
  // Only for the poles, the conversion will be way to large (for the longitude
  // difference). Note, that this function is expensive and should only be
  // called when needed
  double computeDist(const size_t geometryIndex1,
                     const size_t geometryIndex2) const;

  // this helper function converts a GeoPoint into a boost geometry Point
  size_t convertGeoPointToPoint(GeoPoint point);

  // number of times the parsing of a geometry failed. For now this is only used
  // to print the warning once, but it could also be used to print how many
  // geometries failed. It is mutable to let parsing function which are const
  // still modify the the nr of failed parsings.
  size_t numFailedParsedGeometries_ = 0;
};

#endif  // QLEVER_SRC_ENGINE_RTREEENTRYALGORITHM_H

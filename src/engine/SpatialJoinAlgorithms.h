//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: @Jonathan24680
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#pragma once

#include <boost/foreach.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <memory>
#include <variant>

#include "engine/Result.h"
#include "engine/SpatialJoin.h"

namespace BoostGeometryNamespace {
namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

using Point = bg::model::point<double, 2, bg::cs::cartesian>;
using Box = bg::model::box<Point>;
using Value = std::pair<Box, size_t>;
using Polygon = boost::geometry::model::polygon<
    boost::geometry::model::d2::point_xy<double>>;
using Linestring = bg::model::linestring<Point>;
using MultiPoint = bg::model::multi_point<Point>;
using MultiLinestring = bg::model::multi_linestring<Linestring>;
using MultiPolygon = bg::model::multi_polygon<Polygon>;
using AnyGeometry = boost::variant<Point, Linestring, Polygon, MultiPoint,
                                   MultiLinestring, MultiPolygon>;
// this struct is used to get the bounding box of an arbitrary geometry type.
struct BoundingBoxVisitor : public boost::static_visitor<Box> {
  template <typename Geometry>
  Box operator()(const Geometry& geometry) const {
    Box box;
    boost::geometry::envelope(geometry, box);
    return box;
  }
};

// this struct is used to get the distance between two arbitrary geometry types
struct DistanceVisitor : public boost::static_visitor<double> {
  template <typename Geometry1, typename Geometry2>
  double operator()(const Geometry1& geom1, const Geometry2& geom2) const {
    return bg::distance(geom1, geom2);
  }
};

}  // namespace BoostGeometryNamespace

class SpatialJoinAlgorithms {
  using Point = BoostGeometryNamespace::Point;
  using Box = BoostGeometryNamespace::Box;
  using AnyGeometry = BoostGeometryNamespace::AnyGeometry;
 public:
  // initialize the Algorithm with the needed parameters
  SpatialJoinAlgorithms(QueryExecutionContext* qec,
                        PreparedSpatialJoinParams params,
                        SpatialJoinConfiguration config,
                        std::optional<SpatialJoin*> spatialJoin = std::nullopt);
  Result BaselineAlgorithm();
  Result S2geometryAlgorithm();
  Result BoundingBoxAlgorithm();

  // This function computes the bounding box(es) which represent all points,
  // which are in reach of the starting point with a distance of at most
  // 'maxDistanceInMeters'. In theory there is always only one bounding box, but
  // when mapping the spherical surface on a cartesian plane there are borders.
  // So when the "single true" bounding box crosses the left or right (+/-180
  // longitude line) or the poles (+/- 90 latitude, which on the cartesian
  // mapping is the top and bottom edge of the rectangular mapping) then the
  // single box gets split into multiple boxes (i.e. one on the left and one on
  // the right, which when seen on the sphere look like a single box, but on the
  // map and in the internal representation it looks like two/more boxes). The
  // additionalDist gets added on the max distance to compensate for areas being
  // bigger than points. AdditionalDist must be the max distance from the
  // midpoint of the bounding box of the area to any point inside the area.
  // The function getMaxDistFromMidpointToAnyPointInsideTheBox() can be used to
  // calculate it.
  std::vector<Box> computeBoundingBox(const Point& startPoint,
      double additionalDist = 0) const;

  // This function returns true, iff the given point is contained in any of the
  // bounding boxes
  bool isContainedInBoundingBoxes(const std::vector<Box>& boundingBox,
      Point point) const;

  // calculates the midpoint of the given Box
  Point calculateMidpointOfBox(const Box& box) const;

  void setUseMidpointForAreas_(bool useMidpointForAreas) {
    useMidpointForAreas_ = useMidpointForAreas;
  }

  // Helper function, which computes the distance of two points, where each
  // point comes from a different result table
  Id computeDist(const IdTable* resLeft, const IdTable* resRight,
                 size_t rowLeft, size_t rowRight, ColumnIndex leftPointCol,
                 ColumnIndex rightPointCol) const;

  // this function calculates the maximum distance from the midpoint of the box
  // to any other point, which is contained in the box. If the midpoint has
  // already been calculated, because it is needed in other places as well, it
  // can be given to the function, otherwise the function calculates the
  // midpoint itself
  double getMaxDistFromMidpointToAnyPointInsideTheBox(
      const Box& box, std::optional<Point> midpoint = std::nullopt) const;

  // this function gets the string which represents the area from the idtable.
  bool getAnyGeometry(const IdTable* idtable, size_t row, size_t col, AnyGeometry& geometry) const;

 private:
  // Helper function which returns a GeoPoint if the element of the given table
  // represents a GeoPoint
  std::optional<GeoPoint> getPoint(const IdTable* restable, size_t row,
                                   ColumnIndex col) const;

  // returns everything between the first two quotes. If the string does not
  // contain two quotes, the string is returned as a whole
  std::string_view betweenQuotes(std::string_view extractFrom) const;

  // Helper function, which adds a row, which belongs to the result to the
  // result table. As inputs it uses a row of the left and a row of the right
  // child result table.
  void addResultTableEntry(IdTable* result, const IdTable* resultLeft,
                           const IdTable* resultRight, size_t rowLeft,
                           size_t rowRight, Id distance) const;

  // This helper function calculates the bounding boxes based on a box, where
  // definitely no match can occur. This means every element in the anti
  // bounding box is guaranteed to be more than 'maxDistanceInMeters' away from
  // the startPoint. The function is then returning the set of boxes, which
  // cover everything on earth, except for the anti bounding box. This function
  // gets used, when the usual procedure, would just result in taking a big
  // bounding box, which covers the whole planet (so for extremely large max
  // distances)
  std::vector<Box> computeBoundingBoxForLargeDistances(
      const Point& startPoint) const;
  
  QueryExecutionContext* qec_;
  PreparedSpatialJoinParams params_;
  SpatialJoinConfiguration config_;
  std::optional<SpatialJoin*> spatialJoin_;

  // if the distance calculation should be approximated, by the midpoint of
  // the area
  bool useMidpointForAreas_ = true;

  // circumference in meters at the equator (max) and the pole (min) (as the
  // earth is not exactly a sphere the circumference is different. Note that
  // the values are given in meters)
  static constexpr double circumferenceMax_ = 40'075'000;
  static constexpr double circumferenceMin_ = 40'007'000;

  // radius of the earth in meters (as the earth is not exactly a sphere the
  // radius at the equator has been taken)
  static constexpr double radius_ = 6'378'000;

  // convert coordinates to the usual ranges (-180 to 180 and -90 to 90)
  void convertToNormalCoordinates(Point& point) const;

  // return whether one of the poles is being touched
  std::array<bool, 2> isAPoleTouched(const double& latitude) const;
};

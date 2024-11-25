#pragma once

#include <boost/foreach.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point.hpp>

#include "engine/Result.h"
#include "engine/SpatialJoin.h"

namespace BoostGeometryNamespace {
namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

using Point = bg::model::point<double, 2, bg::cs::cartesian>;
using Box = bg::model::box<Point>;
using Value = std::pair<Point, size_t>;
}  // namespace BoostGeometryNamespace

class SpatialJoinAlgorithms {
 public:
  // Initialize the Algorithm with the needed parameters
  SpatialJoinAlgorithms(
      QueryExecutionContext* qec, PreparedSpatialJoinParams params,
      bool addDistToResult,
      std::variant<NearestNeighborsConfig, MaxDistanceConfig> config,
      std::optional<SpatialJoin*> spatialJoin = std::nullopt);
  Result BaselineAlgorithm();
  Result S2geometryAlgorithm();
  Result BoundingBoxAlgorithm();

  std::vector<BoostGeometryNamespace::Box>
  OnlyForTestingWrapperComputeBoundingBox(
      const BoostGeometryNamespace::Point& startPoint) const {
    return computeBoundingBox(startPoint);
  }

  bool OnlyForTestingWrapperContainedInBoundingBoxes(
      const std::vector<BoostGeometryNamespace::Box>& boundingBox,
      const BoostGeometryNamespace::Point& point) const {
    return isContainedInBoundingBoxes(boundingBox, point);
  }

 private:
  // Helper function which returns a GeoPoint if the element of the given table
  // represents a GeoPoint
  std::optional<GeoPoint> getPoint(const IdTable* restable, size_t row,
                                   ColumnIndex col) const;

  // Helper function, which computes the distance of two points, where each
  // point comes from a different result table
  Id computeDist(const IdTable* resLeft, const IdTable* resRight,
                 size_t rowLeft, size_t rowRight, ColumnIndex leftPointCol,
                 ColumnIndex rightPointCol) const;

  // Helper function, which adds a row, which belongs to the result to the
  // result table. As inputs it uses a row of the left and a row of the right
  // child result table.
  void addResultTableEntry(IdTable* result, const IdTable* resultLeft,
                           const IdTable* resultRight, size_t rowLeft,
                           size_t rowRight, Id distance) const;

  // This function computes the bounding box(es) which represent all points,
  // which are in reach of the starting point with a distance of at most
  // 'maxDistanceInMeters'. In theory there is always only one bounding box, but
  // when mapping the spherical surface on a cartesian plane there are borders.
  // So when the "single true" bounding box crosses the left or right (+/-180
  // longitude line) or the poles (+/- 90 latitude, which on the cartesian
  // mapping is the top and bottom edge of the rectangular mapping) then the
  // single box gets split into multiple boxes (i.e. one on the left and one on
  // the right, which when seen on the sphere look like a single box, but on the
  // map and in the internal representation it looks like two/more boxes)
  std::vector<BoostGeometryNamespace::Box> computeBoundingBox(
      const BoostGeometryNamespace::Point& startPoint) const;

  // This helper function calculates the bounding boxes based on a box, where
  // definitely no match can occur. This means every element in the anti
  // bounding box is guaranteed to be more than 'maxDistanceInMeters' away from
  // the startPoint. The function is then returning the set of boxes, which
  // cover everything on earth, except for the anti bounding box. This function
  // gets used, when the usual procedure, would just result in taking a big
  // bounding box, which covers the whole planet (so for extremely large max
  // distances)
  std::vector<BoostGeometryNamespace::Box> computeBoundingBoxForLargeDistances(
      const BoostGeometryNamespace::Point& startPoint) const;

  // This function returns true, iff the given point is contained in any of the
  // bounding boxes
  bool isContainedInBoundingBoxes(
      const std::vector<BoostGeometryNamespace::Box>& boundingBox,
      BoostGeometryNamespace::Point point) const;

  QueryExecutionContext* qec_;
  PreparedSpatialJoinParams params_;
  bool addDistToResult_;
  std::variant<NearestNeighborsConfig, MaxDistanceConfig> config_;
  std::optional<SpatialJoin*> spatialJoin_;

  // circumference in meters at the equator (max) and the pole (min) (as the
  // earth is not exactly a sphere the circumference is different. Note that
  // the values are given in meters)
  static constexpr double circumferenceMax_ = 40'075'000;
  static constexpr double circumferenceMin_ = 40'007'000;

  // radius of the earth in meters (as the earth is not exactly a sphere the
  // radius at the equator has been taken)
  static constexpr double radius_ = 6'378'000;

  // convert coordinates to the usual ranges (-180 to 180 and -90 to 90)
  void convertToNormalCoordinates(BoostGeometryNamespace::Point& point) const;

  // return whether one of the poles is being touched
  std::array<bool, 2> isAPoleTouched(const double& latitude) const;
};

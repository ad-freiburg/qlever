#pragma once

#include <boost/foreach.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point.hpp>

#include "engine/Result.h"
#include "engine/SpatialJoin.h"

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

typedef bg::model::point<double, 2, bg::cs::cartesian> point;
typedef bg::model::box<point> box;
typedef std::pair<point, size_t> value;

class SpatialJoinAlgorithms {
 public:
  // initialize the Algorithm with the needed parameters
  SpatialJoinAlgorithms(
      QueryExecutionContext* qec, PreparedSpatialJoinParams params,
      bool addDistToResult,
      std::variant<NearestNeighborsConfig, MaxDistanceConfig> config);
  Result BaselineAlgorithm();
  Result S2geometryAlgorithm();
  Result BoundingBoxAlgorithm();

  std::vector<box> OnlyForTestingWrapperComputeBoundingBox(
      const point& startPoint) {
    return computeBoundingBox(startPoint);
  }

  bool OnlyForTestingWrapperContainedInBoundingBoxes(
      const std::vector<box>& bbox, point point1) {
    return containedInBoundingBoxes(bbox, point1);
  }

 private:
  // helper function which returns a GeoPoint if the element of the given table
  // represents a GeoPoint
  std::optional<GeoPoint> getPoint(const IdTable* restable, size_t row,
                                   ColumnIndex col) const;

  // helper function, which computes the distance of two points, where each
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

  // this function computes the bounding box(es), which represent all points,
  // which are in reach of the starting point with a distance of at most
  // maxDistanceInMeters
  std::vector<box> computeBoundingBox(const point& startPoint);

  // this helper function calculates the bounding boxes based on a box, where
  // definetly no match can occur. This function gets used, when the usual
  // procedure, would just result in taking a big bounding box, which covers
  // the whole planet (so for large max distances)
  std::vector<box> computeAntiBoundingBox(const point& startPoint);

  // this function returns true, when the given point is contained in any of the
  // bounding boxes
  bool containedInBoundingBoxes(const std::vector<box>& bbox, point point1);

  QueryExecutionContext* qec_;
  PreparedSpatialJoinParams params_;
  bool addDistToResult_;
  std::variant<NearestNeighborsConfig, MaxDistanceConfig> config_;

  // circumference in meters at the equator (max) and the pole (min) (as the
  // earth is not exactly a sphere the circumference is different. Note the
  // factor of 1000 to convert to meters)
  static constexpr double circumferenceMax_ = 40075 * 1000;
  static constexpr double circumferenceMin_ = 40007 * 1000;

  // radius of the earth in meters (as the earth is not exactly a sphere the
  // radius at the equator has been taken)
  static constexpr double radius_ = 6378 * 1000;  // * 1000 to convert to meters
};

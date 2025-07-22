// Copyright 2024 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Jonathan Zeller github@Jonathan24680
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>
//          Patrick Brosi <brosi@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_H
#define QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_H

#include <spatialjoin/Sweeper.h>
#include <spatialjoin/WKTParse.h>

#include <boost/foreach.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <memory>
#include <variant>

#include "engine/Result.h"
#include "engine/SpatialJoin.h"
#include "util/GeoSparqlHelpers.h"

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
    Segment seg;
    bg::closest_points(geo1, geo2, seg);
    GeoPoint closestPoint1(bg::get<0, 1>(seg), bg::get<0, 0>(seg));
    GeoPoint closestPoint2(bg::get<1, 1>(seg), bg::get<1, 0>(seg));
    return ad_utility::detail::wktDistImpl(closestPoint1, closestPoint2);
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

class SpatialJoinAlgorithms {
  using Point = BoostGeometryNamespace::Point;
  using Box = BoostGeometryNamespace::Box;
  using AnyGeometry = BoostGeometryNamespace::AnyGeometry;
  using RtreeEntry = BoostGeometryNamespace::RtreeEntry;

 public:
  // initialize the Algorithm with the needed parameters
  SpatialJoinAlgorithms(QueryExecutionContext* qec,
                        PreparedSpatialJoinParams params,
                        SpatialJoinConfiguration config,
                        std::optional<SpatialJoin*> spatialJoin = std::nullopt);
  Result BaselineAlgorithm();
  Result S2geometryAlgorithm();
  Result BoundingBoxAlgorithm();
  Result LibspatialjoinAlgorithm();

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
  std::vector<Box> computeQueryBox(const Point& startPoint,
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

  // Helper function, which computes the distance of two geometries, where each
  // geometry has already been parsed and is available as an RtreeEntry
  Id computeDist(RtreeEntry& geo1, RtreeEntry& geo2);

  // this function calculates the maximum distance from the midpoint of the box
  // to any other point, which is contained in the box. If the midpoint has
  // already been calculated, because it is needed in other places as well, it
  // can be given to the function, otherwise the function calculates the
  // midpoint itself
  double getMaxDistFromMidpointToAnyPointInsideTheBox(
      const Box& box, std::optional<Point> midpoint = std::nullopt) const;

  // this function gets the string which represents the area from the idtable.
  std::optional<size_t> getAnyGeometry(const IdTable* idtable, size_t row,
                                       size_t col);

  // wrapper to access non const private function for testing
  std::optional<RtreeEntry> onlyForTestingGetRtreeEntry(const IdTable* idTable,
                                                        const size_t row,
                                                        const ColumnIndex col) {
    return getRtreeEntry(idTable, row, col);
  }

  // This helper functions parses WKT geometries from the given `column` in
  // `idTable` and adds them to `sweeper` (which will be used to perform the
  // spatial join). The Boolean `leftOrRightSide` specifies whether these
  // geometries are from the left or right side of the spatial join. The parsing
  // is multithreaded, using up to `numThreads` threads. If a `prefilterBox` is
  // given, geometries not intersecting this box will neither be parsed nor
  // added to `sweeper`. The function returns the aggregated bounding box of all
  // added geometries, which may be used as a prefilter at next call and the
  // number of geometries added. This function is only `public` for testing
  // purposes and should otherwise not be used outside of this class.
  using IdTableAndJoinColumn = std::pair<const IdTable*, const ColumnIndex>;
  std::pair<util::geo::I32Box, size_t> libspatialjoinParse(
      bool leftOrRightSide, IdTableAndJoinColumn idTableAndCol,
      sj::Sweeper& sweeper, size_t numThreads,
      std::optional<util::geo::I32Box> prefilterBox) const;

  // Helper for `libspatialjoinParse` to check the bounding box (only if
  // available from a `GeoVocabulary`) of a given vocabulary entry against the
  // `prefilterLatLngBox`. Returns `true` if the geometry can be discarded just
  // by the bounding box. Should only be applied if the index is known to be
  // built on a `GeoVocabulary`.
  static bool prefilterGeoByBoundingBox(
      const std::optional<util::geo::DBox>& prefilterLatLngBox,
      const Index& index, VocabIndex vocabIndex);

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
  std::vector<Box> computeQueryBoxForLargeDistances(
      const Point& startPoint) const;

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

  // this helper function takes an idtable, a row and a column. It then tries
  // to parse a geometry or a geoPoint of that cell in the idtable. If it
  // succeeds, it returns an rtree entry of that geometry/geopoint
  std::optional<RtreeEntry> getRtreeEntry(const IdTable* idTable,
                                          const size_t row,
                                          const ColumnIndex col);

  // this helper function converts a GeoPoint into a boost geometry Point
  size_t convertGeoPointToPoint(GeoPoint point);

  // This helper function calculates the query box. The query box is a box
  // that is guaranteed to contain all possible candidates of a `WITHIN_DIST`
  // query. It returns a `std::vector` because if the box crosses the poles or
  // the -180/180 longitude line, we have to cut them into multiple boxes.
  // If there is more than one box, the boxes are disjoint.
  std::vector<Box> getQueryBox(const std::optional<RtreeEntry>& entry) const;

  // Calls the `cancellationWrapper` which throws if the query has been
  // cancelled.
  void throwIfCancelled() const;

  QueryExecutionContext* qec_;
  PreparedSpatialJoinParams params_;
  SpatialJoinConfiguration config_;
  std::optional<SpatialJoin*> spatialJoin_;

  // Maximum area of bounding box in square coordinates for prefiltering
  // libspatialjoin input by bounding box. If exceeded, prefiltering is
  // disabled. See `libspatialjoinParse`.
  static constexpr double maxAreaPrefilterBox_ = 2500.0;

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

  // number of times the parsing of a geometry failed. For now this is only used
  // to print the warning once, but it could also be used to print how many
  // geometries failed. It is mutable to let parsing function which are const
  // still modify the the nr of failed parsings.
  size_t numFailedParsedGeometries_ = 0;

  // this vector stores the geometries, which have already been parsed
  std::vector<AnyGeometry, ad_utility::AllocatorWithLimit<AnyGeometry>>
      geometries_;
};

#endif  // QLEVER_SRC_ENGINE_SPATIALJOINALGORITHMS_H

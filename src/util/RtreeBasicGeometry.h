//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#ifndef QLEVER_RTREEBASICGEOMETRY_H
#define QLEVER_RTREEBASICGEOMETRY_H

#include <boost/geometry.hpp>
#include <boost/serialization/split_free.hpp>
#include <ctre-unicode.hpp>

#include "./Rtree.h"

class BasicGeometry {
 public:
  typedef boost::geometry::model::point<
      double, 2,
      boost::geometry::cs::spherical_equatorial<boost::geometry::degree>>
      Point;
  typedef boost::geometry::model::box<Point> BoundingBox;

  static double GetMinX(BoundingBox boundingBox) {
    return boundingBox.min_corner().get<0>();
  }
  static double GetMinY(BoundingBox boundingBox) {
    return boundingBox.min_corner().get<1>();
  }
  static double GetMaxX(BoundingBox boundingBox) {
    return boundingBox.max_corner().get<0>();
  }
  static double GetMaxY(BoundingBox boundingBox) {
    return boundingBox.max_corner().get<1>();
  }

  // ___________________________________________________________________________
  // Create a bounding box, based on the corner coordinates
  static BasicGeometry::BoundingBox CreateBoundingBox(double pointOneX,
                                                      double pointOneY,
                                                      double pointTwoX,
                                                      double pointTwoY) {
    return {{pointOneX, pointOneY}, {pointTwoX, pointTwoY}};
  }

  // ___________________________________________________________________________
  // Take two bounding boxes and combine them into one bounding box containing
  // both
  static BasicGeometry::BoundingBox CombineBoundingBoxes(
      const BasicGeometry::BoundingBox& b1,
      const BasicGeometry::BoundingBox& b2) {
    auto minX = [](BasicGeometry::BoundingBox b) -> double {
      return b.min_corner().get<0>();
    };
    auto minY = [](BasicGeometry::BoundingBox b) -> double {
      return b.min_corner().get<1>();
    };
    auto maxX = [](BasicGeometry::BoundingBox b) -> double {
      return b.max_corner().get<0>();
    };
    auto maxY = [](BasicGeometry::BoundingBox b) -> double {
      return b.max_corner().get<1>();
    };

    double globalMinX = minX(b1) < minX(b2) ? minX(b1) : minX(b2);
    double globalMinY = minY(b1) < minY(b2) ? minY(b1) : minY(b2);
    double globalMaxX = maxX(b1) > maxX(b2) ? maxX(b1) : maxX(b2);
    double globalMaxY = maxY(b1) > maxY(b2) ? maxY(b1) : maxY(b2);

    return {{globalMinX, globalMinY}, {globalMaxX, globalMaxY}};
  }

  static bool BoundingBoxesAreEqual(BasicGeometry::BoundingBox b1,
                                    BasicGeometry::BoundingBox b2) {
    if (BasicGeometry::GetMinX(b1) != BasicGeometry::GetMinX(b2)) return false;
    if (BasicGeometry::GetMinY(b1) != BasicGeometry::GetMinY(b2)) return false;
    if (BasicGeometry::GetMaxX(b1) != BasicGeometry::GetMaxX(b2)) return false;
    if (BasicGeometry::GetMaxY(b1) != BasicGeometry::GetMaxY(b2)) return false;
    return true;
  }

  static bool IsBorderOfSplitCandidate(uint64_t current, uint64_t splitSize,
                                       uint64_t M) {
    if (((current + 1) % splitSize == 0 && (current + 1) / splitSize < M) ||
        (current % splitSize == 0 && current / splitSize >= 1))
      return true;
    return false;
  }

  // ___________________________________________________________________________
  // Convert a single wkt literal to a datapoint in the format suitable for the
  // Rtree
  static std::optional<BoundingBox> ConvertWordToRtreeEntry(
      std::string_view wkt) {
    /**
     * Convert a single wkt literal to a boundingbox.
     * Get the bounding box of either a multipolygon, polygon or a linestring
     */
    if (!wkt.starts_with("\"MULTIPOLYGON") && !wkt.starts_with("\"POLYGON") &&
        !wkt.starts_with("\"LINESTRING")) {
      return {};
    }

    double maxDouble = std::numeric_limits<double>::max();

    double minX = maxDouble;
    double maxX = -maxDouble;
    double minY = maxDouble;
    double maxY = -maxDouble;

    // Iterate over matches and capture x and y coordinates
    for (
        auto match : ctre::range<
            R"( *([\-|\+]?[0-9]+(?:[.][0-9]+)?) +([\-|\+]?[0-9]+(?:[.][0-9]+)?))">(
            wkt)) {
      double x = std::stod(std::string(match.get<1>()));
      double y = std::stod(std::string(match.get<2>()));

      if (x < minX) minX = x;
      if (x > maxX) maxX = x;
      if (y < minY) minY = y;
      if (y > maxY) maxY = y;
    }

    return {BasicGeometry::CreateBoundingBox(minX, minY, maxX, maxY)};
  }
};

// ___________________________________________________________________________
// Data type for a value of the Rtree, which contains the id of the object and
// its bounding box.
struct RTreeValue {
  BasicGeometry::BoundingBox box{};
  uint64_t id = 0;
  [[nodiscard]] double MinX() const { return box.min_corner().get<0>(); }
  [[nodiscard]] double MaxX() const { return box.max_corner().get<0>(); }
  [[nodiscard]] double MinY() const { return box.min_corner().get<1>(); }
  [[nodiscard]] double MaxY() const { return box.max_corner().get<1>(); }

  bool operator==(const RTreeValue& other) const {
    if (id != other.id) return false;
    if (!BasicGeometry::BoundingBoxesAreEqual(box, other.box)) return false;
    return true;
  }

  template <class Archive>
  void serialize(Archive& a, [[maybe_unused]] const unsigned int version) {
    a& box;
    a& id;
  }
};

// ___________________________________________________________________________
// Data type for a value of the Rtree (id and boundingbox), with the additional
// information of its position in the x- and y-sorting. This is only used to
// create the Rtree in a more efficient way
struct RTreeValueWithOrderIndex : RTreeValue {
  uint64_t orderX = 0;
  uint64_t orderY = 0;

  bool operator==(const RTreeValueWithOrderIndex& other) const {
    if (id != other.id) return false;
    if (!BasicGeometry::BoundingBoxesAreEqual(box, other.box)) return false;
    if (orderX != other.orderX) return false;
    if (orderY != other.orderY) return false;
    return true;
  }
};

namespace boost::serialization {
template <class Archive>
void save(Archive& a, const BasicGeometry::BoundingBox& b,
          [[maybe_unused]] unsigned int version) {
  a << b.min_corner().get<0>();
  a << b.min_corner().get<1>();
  a << b.max_corner().get<0>();
  a << b.max_corner().get<1>();
}
template <class Archive>
void load(Archive& a, BasicGeometry::BoundingBox& b,
          [[maybe_unused]] unsigned int version) {
  double minX = 0;
  a >> minX;
  double minY = 0;
  a >> minY;
  double maxX = 0;
  a >> maxX;
  double maxY = 0;
  a >> maxY;
  b = BasicGeometry::BoundingBox(BasicGeometry::Point(minX, minY),
                                 BasicGeometry::Point(maxX, maxY));
}
}  // namespace boost::serialization
BOOST_SERIALIZATION_SPLIT_FREE(BasicGeometry::BoundingBox);

#endif  // QLEVER_RTREEBASICGEOMETRY_H

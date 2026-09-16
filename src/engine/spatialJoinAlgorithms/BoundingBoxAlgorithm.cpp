// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/spatialJoinAlgorithms/BoundingBoxAlgorithm.h"

#include <cmath>
#include <set>

#include "backports/three_way_comparison.h"
#include "util/Exception.h"
#include "util/VectorWithMemoryLimit.h"

using namespace BoostGeometryNamespace;

// ____________________________________________________________________________
bool BoundingBoxAlgorithm::isContainedInBoundingBoxes(
    const std::vector<Box>& boundingBox, Point point) const {
  // correct lon and lat bounds if necessary
  while (point.get<0>() < -180) {
    point.set<0>(point.get<0>() + 360);
  }
  while (point.get<0>() > 180) {
    point.set<0>(point.get<0>() - 360);
  }
  if (point.get<1>() < -90) {
    point.set<1>(-90);
  } else if (point.get<1>() > 90) {
    point.set<1>(90);
  }

  return ql::ranges::any_of(boundingBox, [point](const Box& aBox) {
    return boost::geometry::covered_by(point, aBox);
  });
}

// ____________________________________________________________________________
std::vector<Box> BoundingBoxAlgorithm::computeQueryBox(
    const Point& startPoint, double additionalDist) const {
  const auto& maxDist = maxDist_;
  AD_CORRECTNESS_CHECK(maxDist.has_value(),
                       "Max distance must have a value for this operation");
  // haversine function
  auto haversine = [](double theta) { return (1 - std::cos(theta)) / 2; };

  // inverse haversine function
  auto archaversine = [](double theta) { return std::acos(1 - 2 * theta); };

  // safety buffer for numerical inaccuracies
  double maxDistInMetersBuffer = maxDist.value() + additionalDist;
  if (maxDistInMetersBuffer < 10) {
    maxDistInMetersBuffer = 10;
  } else if (maxDist.value() < std::numeric_limits<double>::max() / 1.02) {
    maxDistInMetersBuffer = 1.01 * maxDistInMetersBuffer;
  } else {
    maxDistInMetersBuffer = std::numeric_limits<double>::max();
  }

  // for large distances, where the lower calculation would just result in
  // a single bounding box for the whole planet, do an optimized version
  if (maxDist.value() > circumferenceMax_ / 4.0 &&
      maxDist.value() < circumferenceMax_ / 2.01) {
    return computeQueryBoxForLargeDistances(startPoint);
  }

  // compute latitude bound
  double maxDistInDegrees = maxDistInMetersBuffer * (360 / circumferenceMax_);
  double upperLatBound = startPoint.get<1>() + maxDistInDegrees;
  double lowerLatBound = startPoint.get<1>() - maxDistInDegrees;

  auto southPoleReached = isAPoleTouched(lowerLatBound).at(1);
  auto northPoleReached = isAPoleTouched(upperLatBound).at(0);

  if (southPoleReached || northPoleReached) {
    return {Box(Point(-180.0f, lowerLatBound), Point(180.0f, upperLatBound))};
  }

  // compute longitude bound. For an explanation of the calculation and the
  // naming convention see my master thesis
  double alpha = maxDistInMetersBuffer / radius_;
  double gamma = (90 - std::abs(startPoint.get<1>())) * (2 * M_PI / 360);
  double beta = std::acos(std::cos(gamma) / std::cos(alpha));
  double delta = 0;
  if (maxDistInMetersBuffer > circumferenceMax_ / 20) {
    // use law of cosines
    delta = std::acos((std::cos(alpha) - std::cos(gamma) * std::cos(beta)) /
                      (std::sin(gamma) * std::sin(beta)));
  } else {
    // use law of haversines for numerical stability
    delta = archaversine((haversine(alpha - haversine(gamma - beta))) /
                         (std::sin(gamma) * std::sin(beta)));
  }
  double lonRange = delta * 360 / (2 * M_PI);
  double leftLonBound = startPoint.get<0>() - lonRange;
  double rightLonBound = startPoint.get<0>() + lonRange;
  // test for "overflows" and create two bounding boxes if necessary
  if (leftLonBound < -180) {
    auto box1 =
        Box(Point(-180, lowerLatBound), Point(rightLonBound, upperLatBound));
    auto box2 = Box(Point(leftLonBound + 360, lowerLatBound),
                    Point(180, upperLatBound));
    return {box1, box2};
  } else if (rightLonBound > 180) {
    auto box1 =
        Box(Point(leftLonBound, lowerLatBound), Point(180, upperLatBound));
    auto box2 = Box(Point(-180, lowerLatBound),
                    Point(rightLonBound - 360, upperLatBound));
    return {box1, box2};
  }
  // default case, when no bound has an "overflow"
  return {Box(Point(leftLonBound, lowerLatBound),
              Point(rightLonBound, upperLatBound))};
}

// ____________________________________________________________________________
std::vector<Box> BoundingBoxAlgorithm::computeQueryBoxForLargeDistances(
    const Point& startPoint) const {
  const auto& maxDist = maxDist_;
  AD_CORRECTNESS_CHECK(maxDist.has_value(),
                       "Max distance must have a value for this operation");

  // point on the opposite side of the globe
  Point antiPoint(startPoint.get<0>() + 180, startPoint.get<1>() * -1);
  if (antiPoint.get<0>() > 180) {
    antiPoint.set<0>(antiPoint.get<0>() - 360);
  }
  // for an explanation of the formula see the master thesis. Divide by two two
  // only consider the distance from the point to the antiPoint, subtract
  // maxDist and a safety margine from that
  double antiDist = (circumferenceMin_ / 2.0) - maxDist.value() * 1.01;
  // use the bigger circumference as an additional safety margin, use 2.01
  // instead of 2.0 because of rounding inaccuracies in floating point
  // operations
  double distToAntiPoint = (360 / circumferenceMax_) * (antiDist / 2.01);
  double upperBound = antiPoint.get<1>() + distToAntiPoint;
  double lowerBound = antiPoint.get<1>() - distToAntiPoint;
  double leftBound = antiPoint.get<0>() - distToAntiPoint;
  double rightBound = antiPoint.get<0>() + distToAntiPoint;
  bool northPoleTouched = false;
  bool southPoleTouched = false;
  bool boxCrosses180Longitude = false;  // if the 180 to -180 line is touched
  // if a pole is crossed, ignore the part after the crossing
  if (upperBound > 90) {
    upperBound = 90;
    northPoleTouched = true;
  }
  if (lowerBound < -90) {
    lowerBound = -90;
    southPoleTouched = true;
  }
  if (leftBound < -180) {
    leftBound += 360;
  }
  if (rightBound > 180) {
    rightBound -= 360;
  }
  if (rightBound < leftBound) {
    boxCrosses180Longitude = true;
  }
  // compute bounding boxes using the anti bounding box from above
  std::vector<Box> boxes;
  if (!northPoleTouched) {
    // add upper bounding box(es)
    if (boxCrosses180Longitude) {
      boxes.emplace_back(Point(leftBound, upperBound), Point(180, 90));
      boxes.emplace_back(Point(-180, upperBound), Point(rightBound, 90));
    } else {
      boxes.emplace_back(Point(leftBound, upperBound), Point(rightBound, 90));
    }
  }
  if (!southPoleTouched) {
    // add lower bounding box(es)
    if (boxCrosses180Longitude) {
      boxes.emplace_back(Point(leftBound, -90), Point(180, lowerBound));
      boxes.emplace_back(Point(-180, -90), Point(rightBound, lowerBound));
    } else {
      boxes.emplace_back(Point(leftBound, -90), Point(rightBound, lowerBound));
    }
  }
  // add the box(es) inbetween the longitude lines
  if (boxCrosses180Longitude) {
    // only one box needed to cover the longitudes
    boxes.emplace_back(Point(rightBound, -90), Point(leftBound, 90));
  } else {
    // two boxes needed, one left and one right of the anti bounding box
    boxes.emplace_back(Point(-180, -90), Point(leftBound, 90));
    boxes.emplace_back(Point(rightBound, -90), Point(180, 90));
  }
  return boxes;
}

// ____________________________________________________________________________
std::array<bool, 2> BoundingBoxAlgorithm::isAPoleTouched(
    const double& latitude) const {
  bool northPoleReached = false;
  bool southPoleReached = false;
  if (latitude >= 90) {
    northPoleReached = true;
  }
  if (latitude <= -90) {
    southPoleReached = true;
  }
  return std::array{northPoleReached, southPoleReached};
}

// ____________________________________________________________________________
double BoundingBoxAlgorithm::getMaxDistFromMidpointToAnyPointInsideTheBox(
    const Box& box, std::optional<Point> midpoint) const {
  if (!midpoint) {
    midpoint = calculateMidpointOfBox(box);
  }
  double distLng =
      std::abs(box.min_corner().get<0>() - midpoint.value().get<0>());
  double distLat =
      std::abs(box.min_corner().get<1>() - midpoint.value().get<1>());
  // convert to meters and return
  return (distLng + distLat) * 40075000 / 360;
}

// ____________________________________________________________________________
std::vector<Box> BoundingBoxAlgorithm::getQueryBox(
    const std::optional<RtreeEntry>& entry) const {
  if (!entry.value().geoPoint_) {
    auto midpoint = calculateMidpointOfBox(entry.value().boundingBox_.value());
    return computeQueryBox(midpoint,
                           getMaxDistFromMidpointToAnyPointInsideTheBox(
                               entry.value().boundingBox_.value(), midpoint));
  } else {
    return computeQueryBox(Point(entry.value().geoPoint_.value().getLng(),
                                 entry.value().geoPoint_.value().getLat()));
  }
}

// ____________________________________________________________________________
Result BoundingBoxAlgorithm::run() {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  throw std::runtime_error(
      "BoundingBoxAlgorithm is not supported in this build");
#else
  // helper struct to avoid duplicate entries for areas
  struct AddedPair {
    size_t rowLeft_;
    size_t rowRight_;

    auto compareThreeWay(const AddedPair& other) const {
      return (rowLeft_ == other.rowLeft_)
                 ? ql::compareThreeWay(rowRight_, other.rowRight_)
                 : ql::compareThreeWay(rowLeft_, other.rowLeft_);
    }

    QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(AddedPair)
  };

  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, leftSelectedCols, rightSelectedCols, numColumns] =
      params_;
  IdTable result{numColumns, qec_->getAllocator()};

  // create r-tree for smaller result table
  auto smallerResult = idTableLeft;
  auto otherResult = idTableRight;
  bool leftResSmaller = true;
  auto smallerResJoinCol = leftJoinCol;
  auto otherResJoinCol = rightJoinCol;
  if (idTableLeft->numRows() > idTableRight->numRows()) {
    std::swap(smallerResult, otherResult);
    leftResSmaller = false;
    std::swap(smallerResJoinCol, otherResJoinCol);
  }

  // build rtree with one child
  bgi::rtree<Value, bgi::quadratic<16>, bgi::indexable<Value>,
             bgi::equal_to<Value>, ad_utility::AllocatorWithLimit<Value> >
      rtree(bgi::quadratic<16>{}, bgi::indexable<Value>{},
            bgi::equal_to<Value>{}, qec_->getAllocator());
  for (size_t i = 0; i < smallerResult->numRows(); i++) {
    throwIfCancelled();

    // add every box together with the additional information into the rtree
    std::optional<RtreeEntry> entry =
        getRtreeEntry(smallerResult, i, smallerResJoinCol);
    if (!entry) {
      // nothing to do. When parsing a point or an area fails, a warning
      // message gets printed at another place and the point/area just gets
      // skipped
      continue;
    }
    rtree.insert(std::pair(entry.value().boundingBox_.value(),
                           std::move(entry.value())));
  }

  // query rtree with the other child
  ad_utility::VectorWithMemoryLimit<Value> results{qec_->getAllocator()};
  for (size_t i = 0; i < otherResult->numRows(); i++) {
    throwIfCancelled();

    std::optional<RtreeEntry> entry =
        getRtreeEntry(otherResult, i, otherResJoinCol);
    if (!entry) {
      // nothing to do. When parsing a point or an area fails, a warning
      // message gets printed at another place and the point/area just gets
      // skipped
      continue;
    }
    std::vector<Box> queryBox = getQueryBox(entry);

    results.clear();

    ql::ranges::for_each(queryBox, [&](const Box& bbox) {
      rtree.query(bgi::intersects(bbox), std::back_inserter(results));
    });

    std::set<AddedPair> pairs;
    ql::ranges::for_each(results, [&](Value& res) {
      size_t rowLeft = res.second.row_;
      size_t rowRight = i;
      if (!leftResSmaller) {
        std::swap(rowLeft, rowRight);
      }
      auto distance = computeDist(res.second, entry.value());
      AD_CORRECTNESS_CHECK(distance.getDatatype() == Datatype::Double);
      if (distance.getDouble() * 1000 <= maxDist_.value()) {
        // make sure, that no duplicate elements are inserted in the result
        // table. As duplicates can only occur, when areas are not approximated
        // as midpoints, the additional runtime can be saved in that case
        if (useMidpointForAreas_) {
          addResultTableEntry(&result, idTableLeft, idTableRight, rowLeft,
                              rowRight, distance);
        } else if (pairs.insert(AddedPair{rowLeft, rowRight}).second) {
          addResultTableEntry(&result, idTableLeft, idTableRight, rowLeft,
                              rowRight, distance);
        }
      }
    });
  }
  auto resTable =
      Result(std::move(result), std::vector<ColumnIndex>{},
             Result::getMergedLocalVocab(*resultLeft, *resultRight));
  return resTable;
#endif
}

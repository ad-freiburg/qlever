// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BOUNDINGBOXALGORITHM_H
#define QLEVER_SRC_ENGINE_BOUNDINGBOXALGORITHM_H

#include "engine/spatialJoinAlgorithms/RtreeEntryAlgorithm.h"

// Spatial join for `maxDistance` tasks using an r-tree built over the
// bounding boxes of the smaller input table: for each row of the larger
// table, a padded query box (or, for large distances, its complement) is
// used to find candidate matches, which are then checked exactly.
class BoundingBoxAlgorithm : public RtreeEntryAlgorithm {
 public:
  using RtreeEntryAlgorithm::RtreeEntryAlgorithm;

  Result run() override;

  // This function returns true, iff the given point is contained in any of
  // the bounding boxes
  bool isContainedInBoundingBoxes(const std::vector<Box>& boundingBox,
                                  Point point) const;

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

  // this function calculates the maximum distance from the midpoint of the box
  // to any other point, which is contained in the box. If the midpoint has
  // already been calculated, because it is needed in other places as well, it
  // can be given to the function, otherwise the function calculates the
  // midpoint itself
  double getMaxDistFromMidpointToAnyPointInsideTheBox(
      const Box& box, std::optional<Point> midpoint = std::nullopt) const;

 private:
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

  // return whether one of the poles is being touched
  std::array<bool, 2> isAPoleTouched(const double& latitude) const;

  // This helper function calculates the query box. The query box is a box
  // that is guaranteed to contain all possible candidates of a `WITHIN_DIST`
  // query. It returns a `std::vector` because if the box crosses the poles or
  // the -180/180 longitude line, we have to cut them into multiple boxes.
  // If there is more than one box, the boxes are disjoint.
  std::vector<Box> getQueryBox(const std::optional<RtreeEntry>& entry) const;

  // circumference in meters at the equator (max) and the pole (min) (as the
  // earth is not exactly a sphere the circumference is different. Note that
  // the values are given in meters)
  static constexpr double circumferenceMax_ = 40'075'000;
  static constexpr double circumferenceMin_ = 40'007'000;

  // radius of the earth in meters (as the earth is not exactly a sphere the
  // radius at the equator has been taken)
  static constexpr double radius_ = 6'378'000;
};

#endif  // QLEVER_SRC_ENGINE_BOUNDINGBOXALGORITHM_H

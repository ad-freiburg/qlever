//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: @Jonathan24680
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "engine/SpatialJoinAlgorithms.h"

#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#include <s2/util/units/length-units.h>

#include <cmath>

#include "engine/SpatialJoin.h"
#include "util/GeoSparqlHelpers.h"

using namespace BoostGeometryNamespace;

// ____________________________________________________________________________
SpatialJoinAlgorithms::SpatialJoinAlgorithms(
    QueryExecutionContext* qec, PreparedSpatialJoinParams params,
    SpatialJoinConfiguration config, std::optional<SpatialJoin*> spatialJoin)
    : qec_{qec},
      params_{std::move(params)},
      config_{std::move(config)},
      spatialJoin_{spatialJoin} {}

// ____________________________________________________________________________
std::optional<GeoPoint> SpatialJoinAlgorithms::getPoint(const IdTable* restable,
                                                        size_t row,
                                                        ColumnIndex col) const {
  auto id = restable->at(row, col);
  return id.getDatatype() == Datatype::GeoPoint
             ? std::optional{id.getGeoPoint()}
             : std::nullopt;
};

// ____________________________________________________________________________
Id SpatialJoinAlgorithms::computeDist(const IdTable* idTableLeft,
                                      const IdTable* idTableRight,
                                      size_t rowLeft, size_t rowRight,
                                      ColumnIndex leftPointCol,
                                      ColumnIndex rightPointCol) const {
  auto point1 = getPoint(idTableLeft, rowLeft, leftPointCol);
  auto point2 = getPoint(idTableRight, rowRight, rightPointCol);
  if (!point1.has_value() || !point2.has_value()) {
    return Id::makeUndefined();
  }
  return Id::makeFromDouble(
      ad_utility::detail::wktDistImpl(point1.value(), point2.value()));
}

// ____________________________________________________________________________
void SpatialJoinAlgorithms::addResultTableEntry(IdTable* result,
                                                const IdTable* idTableLeft,
                                                const IdTable* idTableRight,
                                                size_t rowLeft, size_t rowRight,
                                                Id distance) const {
  // this lambda function copies values from copyFrom into the table res only if
  // the column of the value is specified in sourceColumns. If sourceColumns is
  // nullopt, all columns are added. It copies them into the row rowIndRes and
  // column column colIndRes. It returns the column number until which elements
  // were copied
  auto addColumns = [](IdTable* res, const IdTable* copyFrom, size_t rowIndRes,
                       size_t colIndRes, size_t rowIndCopy,
                       std::optional<std::vector<ColumnIndex>> sourceColumns =
                           std::nullopt) {
    size_t nCols = sourceColumns.has_value() ? sourceColumns.value().size()
                                             : copyFrom->numColumns();
    for (size_t i = 0; i < nCols; i++) {
      auto col = sourceColumns.has_value() ? sourceColumns.value()[i] : i;
      res->at(rowIndRes, colIndRes + i) = (*copyFrom).at(rowIndCopy, col);
    }
    return colIndRes + nCols;
  };

  auto resrow = result->numRows();
  result->emplace_back();
  // add columns to result table
  size_t rescol = 0;
  rescol = addColumns(result, idTableLeft, resrow, rescol, rowLeft);
  rescol = addColumns(result, idTableRight, resrow, rescol, rowRight,
                      params_.rightSelectedCols_);

  if (config_.distanceVariable_.has_value()) {
    result->at(resrow, rescol) = distance;
    // rescol isn't used after that in this function, but future updates,
    // which add additional columns, would need to remember to increase
    // rescol at this place otherwise. If they forget to do this, the
    // distance column will be overwritten, the variableToColumnMap will
    // not work and so on
    // rescol += 1;
  }
}

// ____________________________________________________________________________
Result SpatialJoinAlgorithms::BaselineAlgorithm() {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, rightSelectedCols, numColumns, maxDist,
              maxResults] = params_;
  IdTable result{numColumns, qec_->getAllocator()};

  // cartesian product between the two tables, pairs are restricted according to
  // `maxDistance_` and `maxResults_`
  for (size_t rowLeft = 0; rowLeft < idTableLeft->size(); rowLeft++) {
    // This priority queue stores the intermediate best results if `maxResults_`
    // is used. Each intermediate result is stored as a pair of its `rowRight`
    // and distance. Since the queue will hold at most `maxResults_ + 1` items,
    // it is not a memory concern.
    auto compare = [](std::pair<size_t, double> a,
                      std::pair<size_t, double> b) {
      return a.second < b.second;
    };
    std::priority_queue<std::pair<size_t, double>,
                        std::vector<std::pair<size_t, double>>,
                        decltype(compare)>
        intermediate(compare);

    // Inner loop of cartesian product
    for (size_t rowRight = 0; rowRight < idTableRight->size(); rowRight++) {
      Id dist = computeDist(idTableLeft, idTableRight, rowLeft, rowRight,
                            leftJoinCol, rightJoinCol);

      // Ensure `maxDist_` constraint
      if (dist.getDatatype() != Datatype::Double ||
          (maxDist.has_value() &&
           (dist.getDouble() * 1000) > static_cast<double>(maxDist.value()))) {
        continue;
      }

      // If there is no `maxResults_` we can add the result row immediately
      if (!maxResults.has_value()) {
        addResultTableEntry(&result, idTableLeft, idTableRight, rowLeft,
                            rowRight, dist);
        continue;
      }

      // Ensure `maxResults_` constraint using priority queue
      intermediate.push(std::pair{rowRight, dist.getDouble()});
      // Too many results? Drop the worst one
      if (intermediate.size() > maxResults.value()) {
        intermediate.pop();
      }
    }

    // If we are using the priority queue, we didn't add the results in the
    // inner loop, so we do it now.
    if (maxResults.has_value()) {
      size_t numResults = intermediate.size();
      for (size_t item = 0; item < numResults; item++) {
        // Get and remove largest item from priority queue
        auto [rowRight, dist] = intermediate.top();
        intermediate.pop();

        addResultTableEntry(&result, idTableLeft, idTableRight, rowLeft,
                            rowRight, Id::makeFromDouble(dist));
      }
    }
  }
  return Result(std::move(result), std::vector<ColumnIndex>{},
                Result::getMergedLocalVocab(*resultLeft, *resultRight));
}

// ____________________________________________________________________________
Result SpatialJoinAlgorithms::S2geometryAlgorithm() {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, rightSelectedCols, numColumns, maxDist,
              maxResults] = params_;
  IdTable result{numColumns, qec_->getAllocator()};

  // Helper function to convert `GeoPoint` to `S2Point`
  auto constexpr toS2Point = [](const GeoPoint& p) {
    auto lat = p.getLat();
    auto lng = p.getLng();
    auto latlng = S2LatLng::FromDegrees(lat, lng);
    return S2Point{latlng};
  };

  S2PointIndex<size_t> s2index;

  // Optimization: If we only search by maximum distance, the operation is
  // symmetric, so the larger table can be used for the index
  bool indexOfRight =
      (maxResults.has_value() || (idTableLeft->size() > idTableRight->size()));
  auto indexTable = indexOfRight ? idTableRight : idTableLeft;
  auto indexJoinCol = indexOfRight ? rightJoinCol : leftJoinCol;

  // Populate the index
  for (size_t row = 0; row < indexTable->size(); row++) {
    auto p = getPoint(indexTable, row, indexJoinCol);
    if (p.has_value()) {
      s2index.Add(toS2Point(p.value()), row);
    }
  }

  // Performs a nearest neighbor search on the index and returns the closest
  // points that satisfy the criteria given by `maxDist_` and `maxResults_`.

  // Construct a query object with the given constraints
  auto s2query = S2ClosestPointQuery<size_t>{&s2index};

  if (maxResults.has_value()) {
    s2query.mutable_options()->set_max_results(
        static_cast<int>(maxResults.value()));
  }
  if (maxDist.has_value()) {
    s2query.mutable_options()->set_inclusive_max_distance(S2Earth::ToAngle(
        util::units::Meters(static_cast<float>(maxDist.value()))));
  }

  auto searchTable = indexOfRight ? idTableLeft : idTableRight;
  auto searchJoinCol = indexOfRight ? leftJoinCol : rightJoinCol;

  // Use the index to lookup the points of the other table
  for (size_t searchRow = 0; searchRow < searchTable->size(); searchRow++) {
    auto p = getPoint(searchTable, searchRow, searchJoinCol);
    if (!p.has_value()) {
      continue;
    }
    auto s2target =
        S2ClosestPointQuery<size_t>::PointTarget{toS2Point(p.value())};

    for (const auto& neighbor : s2query.FindClosestPoints(&s2target)) {
      // In this loop we only receive points that already satisfy the given
      // criteria
      auto indexRow = neighbor.data();
      auto dist = S2Earth::ToKm(neighbor.distance());

      auto rowLeft = indexOfRight ? searchRow : indexRow;
      auto rowRight = indexOfRight ? indexRow : searchRow;
      addResultTableEntry(&result, idTableLeft, idTableRight, rowLeft, rowRight,
                          Id::makeFromDouble(dist));
    }
  }

  return Result(std::move(result), std::vector<ColumnIndex>{},
                Result::getMergedLocalVocab(*resultLeft, *resultRight));
}

// ____________________________________________________________________________
std::vector<Box> SpatialJoinAlgorithms::computeBoundingBox(
    const Point& startPoint) const {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, rightSelectedCols, numColumns, maxDist,
              maxResults] = params_;
  AD_CORRECTNESS_CHECK(maxDist.has_value(),
                       "Max distance must have a value for this operation");
  // haversine function
  auto haversine = [](double theta) { return (1 - std::cos(theta)) / 2; };

  // inverse haversine function
  auto archaversine = [](double theta) { return std::acos(1 - 2 * theta); };

  // safety buffer for numerical inaccuracies
  double maxDistInMetersBuffer;
  if (maxDist.value() < 10) {
    maxDistInMetersBuffer = 10;
  } else if (static_cast<double>(maxDist.value()) <
             static_cast<double>(std::numeric_limits<long long>::max()) /
                 1.02) {
    maxDistInMetersBuffer = 1.01 * static_cast<double>(maxDist.value());
  } else {
    maxDistInMetersBuffer =
        static_cast<double>(std::numeric_limits<long long>::max());
  }

  // for large distances, where the lower calculation would just result in
  // a single bounding box for the whole planet, do an optimized version
  if (static_cast<double>(maxDist.value()) > circumferenceMax_ / 4.0 &&
      static_cast<double>(maxDist.value()) < circumferenceMax_ / 2.01) {
    return computeBoundingBoxForLargeDistances(startPoint);
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
std::vector<Box> SpatialJoinAlgorithms::computeBoundingBoxForLargeDistances(
    const Point& startPoint) const {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, rightSelectedCols, numColumns, maxDist,
              maxResults] = params_;
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
  double antiDist =
      (circumferenceMin_ / 2.0) - static_cast<double>(maxDist.value()) * 1.01;
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
bool SpatialJoinAlgorithms::isContainedInBoundingBoxes(
    const std::vector<Box>& boundingBox, Point point) const {
  convertToNormalCoordinates(point);

  return ql::ranges::any_of(boundingBox, [point](const Box& aBox) {
    return boost::geometry::covered_by(point, aBox);
  });
}

// ____________________________________________________________________________
void SpatialJoinAlgorithms::convertToNormalCoordinates(Point& point) const {
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
}

// ____________________________________________________________________________
std::array<bool, 2> SpatialJoinAlgorithms::isAPoleTouched(
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
Result SpatialJoinAlgorithms::BoundingBoxAlgorithm() {
  auto printWarning = [alreadyWarned = false,
                       &spatialJoin = spatialJoin_]() mutable {
    if (!alreadyWarned) {
      std::string warning =
          "The input to a spatial join contained at least one element, "
          "that is not a point geometry and is thus skipped. Note that "
          "QLever currently only accepts point geometries for the "
          "spatial joins";
      AD_LOG_WARN << warning << std::endl;
      alreadyWarned = true;
      if (spatialJoin.has_value()) {
        AD_CORRECTNESS_CHECK(spatialJoin.value() != nullptr);
        spatialJoin.value()->addWarning(warning);
      }
    }
  };

  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, rightSelectedCols, numColumns, maxDist,
              maxResults] = params_;
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

  bgi::rtree<Value, bgi::quadratic<16>, bgi::indexable<Value>,
             bgi::equal_to<Value>, ad_utility::AllocatorWithLimit<Value>>
      rtree(bgi::quadratic<16>{}, bgi::indexable<Value>{},
            bgi::equal_to<Value>{}, qec_->getAllocator());
  for (size_t i = 0; i < smallerResult->numRows(); i++) {
    // get point of row i
    auto geopoint = getPoint(smallerResult, i, smallerResJoinCol);

    if (!geopoint) {
      printWarning();
      continue;
    }

    Point p(geopoint.value().getLng(), geopoint.value().getLat());

    // add every point together with the row number into the rtree
    rtree.insert(std::make_pair(std::move(p), i));
  }
  std::vector<Value, ad_utility::AllocatorWithLimit<Value>> results{
      qec_->getAllocator()};
  for (size_t i = 0; i < otherResult->numRows(); i++) {
    auto geopoint1 = getPoint(otherResult, i, otherResJoinCol);

    if (!geopoint1) {
      printWarning();
      continue;
    }

    Point p(geopoint1.value().getLng(), geopoint1.value().getLat());

    // query the other rtree for every point using the following bounding box
    std::vector<Box> bbox = computeBoundingBox(p);
    results.clear();

    ql::ranges::for_each(bbox, [&](const Box& bbox) {
      rtree.query(bgi::intersects(bbox), std::back_inserter(results));
    });

    ql::ranges::for_each(results, [&](const Value& res) {
      size_t rowLeft = res.second;
      size_t rowRight = i;
      if (!leftResSmaller) {
        std::swap(rowLeft, rowRight);
      }
      auto distance = computeDist(idTableLeft, idTableRight, rowLeft, rowRight,
                                  leftJoinCol, rightJoinCol);
      AD_CORRECTNESS_CHECK(distance.getDatatype() == Datatype::Double);
      if (distance.getDouble() * 1000 <= static_cast<double>(maxDist.value())) {
        addResultTableEntry(&result, idTableLeft, idTableRight, rowLeft,
                            rowRight, distance);
      }
    });
  }
  auto resTable =
      Result(std::move(result), std::vector<ColumnIndex>{},
             Result::getMergedLocalVocab(*resultLeft, *resultRight));
  return resTable;
}

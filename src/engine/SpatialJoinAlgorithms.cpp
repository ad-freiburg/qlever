// Copyright 2024 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Jonathan Zeller github@Jonathan24680
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>
//          Patrick Brosi <brosi@cs.uni-freiburg.de>

#include "engine/SpatialJoinAlgorithms.h"

#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#include <s2/util/units/length-units.h>
#include <spatialjoin/BoxIds.h>
#include <spatialjoin/Sweeper.h>
#include <spatialjoin/WKTParse.h>
#include <util/geo/Geo.h>

#include <cmath>
#include <set>

#include "engine/ExportQueryExecutionTrees.h"
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
      spatialJoin_{spatialJoin},
      geometries_{qec->getAllocator()} {}

// ____________________________________________________________________________
util::geo::I32Box SpatialJoinAlgorithms::libspatialjoinParse(
    bool leftOrRightSide, const IdTable* idTable, ColumnIndex column,
    sj::Sweeper& sweeper, size_t numThreads) const {
  // Initialize the parser.
  sj::WKTParser parser(&sweeper, numThreads);

  // Iterate over all rows in `idTable` and parse the geometries from `column`.
  for (size_t row = 0; row < idTable->size(); row++) {
    auto id = idTable->at(row, column);
    if (id.getDatatype() == Datatype::VocabIndex) {
      const auto& wkt = qec_->getIndex().indexToString(id.getVocabIndex());
      parser.parseWKT(wkt.c_str(), row, leftOrRightSide);
    } else if (id.getDatatype() == Datatype::GeoPoint) {
      const auto& p = id.getGeoPoint();
      parser.parsePoint(util::geo::DPoint(p.getLng(), p.getLat()), row,
                        leftOrRightSide);
    }
  }

  // Wait for all parser threads to finish, then return the bounding box of all
  // the geometries parsed so far.
  parser.done();
  return parser.getBoundingBox();
}

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
std::string_view SpatialJoinAlgorithms::betweenQuotes(
    std::string_view extractFrom) const {
  size_t pos1 = extractFrom.find("\"", 0);
  size_t pos2 = extractFrom.find("\"", pos1 + 1);
  if (pos1 != std::string::npos && pos2 != std::string::npos) {
    return extractFrom.substr(pos1 + 1, pos2 - pos1 - 1);
  } else {
    return extractFrom;
  }
}

std::optional<size_t> SpatialJoinAlgorithms::getAnyGeometry(
    const IdTable* idtable, size_t row, size_t col) {
  auto printWarning = [this, &spatialJoin = spatialJoin_]() {
    if (this->numFailedParsedGeometries_ == 0) {
      std::string warning =
          "The input to a spatial join contained at least one element, "
          "that is not a Point, Linestring, Polygon, MultiPoint, "
          "MultiLinestring or MultiPolygon geometry and is thus skipped. Note "
          "that QLever currently only accepts those geometries for "
          "the spatial joins";
      AD_LOG_WARN << warning << std::endl;
      this->numFailedParsedGeometries_ += 1;
      if (spatialJoin.has_value()) {
        AD_CORRECTNESS_CHECK(spatialJoin.value() != nullptr);
        spatialJoin.value()->addWarning(warning);
      }
    }
  };

  // unfortunately, the current implementation requires the fully materialized
  // string. In the future this might get changed. When only the bounding box
  // is needed, one could store it in an ID similar to GeoPoint (but with less
  // precision), and then the full geometry would only need to be read, when
  // the exact distance is wanted
  std::string str(betweenQuotes(ExportQueryExecutionTrees::idToStringAndType(
                                    qec_->getIndex(), idtable->at(row, col), {})
                                    .value()
                                    .first));
  AnyGeometry geometry;
  try {
    bg::read_wkt(str, geometry);
    geometries_.push_back(std::move(geometry));
  } catch (...) {
    printWarning();
    return std::nullopt;
  }
  return geometries_.size() - 1;  // index of the last element
}

// ____________________________________________________________________________
double SpatialJoinAlgorithms::computeDist(const size_t geometryIndex1,
                                          const size_t geometryIndex2) const {
  return boost::apply_visitor(ClosestPointVisitor(),
                              geometries_.at(geometryIndex1),
                              geometries_.at(geometryIndex2));
};

// ____________________________________________________________________________
size_t SpatialJoinAlgorithms::convertGeoPointToPoint(GeoPoint point) {
  geometries_.emplace_back(Point(point.getLng(), point.getLat()));
  return geometries_.size() - 1;  // index of the last element
};

// ____________________________________________________________________________
Id SpatialJoinAlgorithms::computeDist(RtreeEntry& geo1, RtreeEntry& geo2) {
  auto convertPoint = [&](RtreeEntry& entry) {
    if (entry.geoPoint_) {
      return entry.geoPoint_.value();
    }
    if (!entry.boundingBox_.has_value()) {
      entry.boundingBox_ = boost::apply_visitor(
          BoundingBoxVisitor(), geometries_.at(entry.geometryIndex_.value()));
    }
    Point p = calculateMidpointOfBox(entry.boundingBox_.value());
    return GeoPoint(p.get<1>(), p.get<0>());
  };

  auto getIndex = [&](RtreeEntry& entry) {
    if (!entry.geometryIndex_) {
      entry.geometryIndex_ = convertGeoPointToPoint(entry.geoPoint_.value());
    }
    return entry.geometryIndex_.value();
  };

  // use the already parsed geometries to calculate the distance
  if (useMidpointForAreas_ ||
      (geo1.geoPoint_.has_value() && geo2.geoPoint_.has_value())) {
    return Id::makeFromDouble(ad_utility::detail::wktDistImpl(
        convertPoint(geo1), convertPoint(geo2)));
  } else {
    // at least one area
    return Id::makeFromDouble(computeDist(getIndex(geo1), getIndex(geo2)));
  }
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
              rightJoinCol, rightSelectedCols, numColumns, maxDist, maxResults,
              joinType] = params_;
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

    auto entryLeft = getRtreeEntry(idTableLeft, rowLeft, leftJoinCol);

    // Inner loop of cartesian product
    for (size_t rowRight = 0; rowRight < idTableRight->size(); rowRight++) {
      auto entryRight = getRtreeEntry(idTableRight, rowRight, rightJoinCol);

      if (!entryLeft || !entryRight) {
        continue;
      }

      Id dist = computeDist(entryLeft.value(), entryRight.value());

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
Result SpatialJoinAlgorithms::LibspatialjoinAlgorithm() {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, rightSelectedCols, numColumns, maxDist, maxResults,
              joinType] = params_;
  // Setup.
  IdTable result{numColumns, qec_->getAllocator()};
  size_t NUM_THREADS = std::thread::hardware_concurrency();
  std::vector<std::vector<std::pair<size_t, size_t>>> results(NUM_THREADS);
  std::vector<std::vector<double>> resultDists(NUM_THREADS);
  auto joinTypeVal = joinType.value_or(SpatialJoinType::INTERSECTS);

  // Add number of threads to runtime information.
  spatialJoin_.value()->runtimeInfo().addDetail("spatialjoin num threads",
                                                NUM_THREADS);

  // Set the distance for the `WITHIN_DIST` join type. This has to be set to
  // a value < 0 to disable the `WITHIN_DIST` calculation in `libspatialjoin`.
  double withinDist = -1;
  if (joinTypeVal == SpatialJoinType::WITHIN_DIST) {
    withinDist = maxDist.value_or(0);
    spatialJoin_.value()->runtimeInfo().addDetail("within-dist", withinDist);
  }

  // Configure the sweeper.
  const sj::SweeperCfg sweeperCfg = [&] {
    sj::SweeperCfg cfg;
    cfg.numThreads = NUM_THREADS;
    cfg.numCacheThreads = NUM_THREADS;
    cfg.geomCacheMaxSize = 10000;
    cfg.pairStart = "";
    cfg.sepIsect = std::string{static_cast<char>(SpatialJoinType::INTERSECTS)};
    cfg.sepContains = std::string{static_cast<char>(SpatialJoinType::CONTAINS)};
    cfg.sepCovers = std::string{static_cast<char>(SpatialJoinType::COVERS)};
    cfg.sepTouches = std::string{static_cast<char>(SpatialJoinType::TOUCHES)};
    cfg.sepEquals = std::string{static_cast<char>(SpatialJoinType::EQUALS)};
    cfg.sepOverlaps = std::string{static_cast<char>(SpatialJoinType::OVERLAPS)};
    cfg.sepCrosses = std::string{static_cast<char>(SpatialJoinType::CROSSES)};
    cfg.pairEnd = "";
    cfg.useBoxIds = true;
    cfg.useArea = true;
    cfg.useOBB = false;
    cfg.useCutouts = true;
    cfg.useDiagBox = true;
    cfg.useFastSweepSkip = true;
    cfg.useInnerOuter = false;
    cfg.noGeometryChecks = false;
    cfg.withinDist = withinDist;
    cfg.writeRelCb = [&results, &resultDists, joinTypeVal](
                         size_t t, const char* a, const char* b,
                         const char* pred) {
      if (joinTypeVal == SpatialJoinType::WITHIN_DIST) {
        results[t].push_back({std::atoi(a), std::atoi(b)});
        resultDists[t].push_back(atof(pred));
      } else if (pred[0] == static_cast<char>(joinTypeVal)) {
        results[t].push_back({std::atoi(a), std::atoi(b)});
      }
    };
    cfg.logCb = {};
    cfg.statsCb = {};
    cfg.sweepProgressCb = {};
    return cfg;
  }();

  std::string sweeperPath = qec_->getIndex().getOnDiskBase() + ".spatialjoin";
  sj::Sweeper sweeper(sweeperCfg, ".", "", sweeperPath.c_str());
  ad_utility::Timer tParse{ad_utility::Timer::Started};

  // Parse the geometries from the left and right input table, starting with the
  // smaller one. Compute the bounding box of the smaller table (appropriately
  // inflated for `WITHIN_DIST` joins) and only add those geometries from the
  // larger table that intersect this bounding box.
  if (idTableLeft->size() < idTableRight->size()) {
    auto box = libspatialjoinParse(false, idTableLeft, leftJoinCol, sweeper,
                                   NUM_THREADS);
    sweeper.setFilterBox(box);
    libspatialjoinParse(true, idTableRight, rightJoinCol, sweeper, NUM_THREADS);
  } else {
    auto box = libspatialjoinParse(true, idTableRight, rightJoinCol, sweeper,
                                   NUM_THREADS);
    sweeper.setFilterBox(box);
    libspatialjoinParse(false, idTableLeft, leftJoinCol, sweeper, NUM_THREADS);
  }

  // Flush the geometry caches and the sweepline event list cache to disk and
  // add the time for parsing and processing the geometries to the runtime
  // information.
  sweeper.flush();
  spatialJoin_.value()->runtimeInfo().addDetail("time for reading geometries",
                                                tParse.msecs().count());

  // Now do the sweep, which performs the actual spatial join.
  ad_utility::Timer tSweep{ad_utility::Timer::Started};
  sweeper.sweep();
  spatialJoin_.value()->runtimeInfo().addDetail("time for spatialjoin sweep",
                                                tSweep.msecs().count());
  ad_utility::Timer tCollect{ad_utility::Timer::Started};

  // Collect the results and add them to the result table. For `WITHIN_DIST`,
  // also add the distance for each pair of objects in the result.
  for (size_t t = 0; t < NUM_THREADS; t++) {
    for (size_t i = 0; i < results[t].size(); i++) {
      const auto& res = results[t][i];
      double dist = 0;
      if (joinTypeVal == SpatialJoinType::WITHIN_DIST) {
        dist = resultDists[t][i];
      }
      addResultTableEntry(&result, idTableLeft, idTableRight, res.first,
                          res.second, Id::makeFromDouble(dist));
    }
  }
  spatialJoin_.value()->runtimeInfo().addDetail(
      "time for collecting results from threads", tCollect.msecs().count());

  // Return the result.
  return Result(std::move(result), std::vector<ColumnIndex>{},
                Result::getMergedLocalVocab(*resultLeft, *resultRight));
}

// ____________________________________________________________________________
Result SpatialJoinAlgorithms::S2geometryAlgorithm() {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, rightSelectedCols, numColumns, maxDist, maxResults,
              joinType] = params_;
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
std::vector<Box> SpatialJoinAlgorithms::computeQueryBox(
    const Point& startPoint, double additionalDist) const {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, rightSelectedCols, numColumns, maxDist, maxResults,
              joinType] = params_;
  AD_CORRECTNESS_CHECK(maxDist.has_value(),
                       "Max distance must have a value for this operation");
  // haversine function
  auto haversine = [](double theta) { return (1 - std::cos(theta)) / 2; };

  // inverse haversine function
  auto archaversine = [](double theta) { return std::acos(1 - 2 * theta); };

  // safety buffer for numerical inaccuracies
  double maxDistInMetersBuffer =
      static_cast<double>(maxDist.value()) + additionalDist;
  if (maxDistInMetersBuffer < 10) {
    maxDistInMetersBuffer = 10;
  } else if (static_cast<double>(maxDist.value()) <
             static_cast<double>(std::numeric_limits<long long>::max()) /
                 1.02) {
    maxDistInMetersBuffer = 1.01 * maxDistInMetersBuffer;
  } else {
    maxDistInMetersBuffer =
        static_cast<double>(std::numeric_limits<long long>::max());
  }

  // for large distances, where the lower calculation would just result in
  // a single bounding box for the whole planet, do an optimized version
  if (static_cast<double>(maxDist.value()) > circumferenceMax_ / 4.0 &&
      static_cast<double>(maxDist.value()) < circumferenceMax_ / 2.01) {
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
std::vector<Box> SpatialJoinAlgorithms::computeQueryBoxForLargeDistances(
    const Point& startPoint) const {
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, rightSelectedCols, numColumns, maxDist, maxResults,
              joinType] = params_;
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
Point SpatialJoinAlgorithms::calculateMidpointOfBox(const Box& box) const {
  double lng = (box.min_corner().get<0>() + box.max_corner().get<0>()) / 2.0;
  double lat = (box.min_corner().get<1>() + box.max_corner().get<1>()) / 2.0;
  return Point(lng, lat);
}

// ____________________________________________________________________________
double SpatialJoinAlgorithms::getMaxDistFromMidpointToAnyPointInsideTheBox(
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
std::optional<RtreeEntry> SpatialJoinAlgorithms::getRtreeEntry(
    const IdTable* idTable, const size_t row, const ColumnIndex col) {
  RtreeEntry entry{row, std::nullopt, std::nullopt, std::nullopt};
  entry.geoPoint_ = getPoint(idTable, row, col);

  if (!entry.geoPoint_) {
    entry.geometryIndex_ = getAnyGeometry(idTable, row, col);
    if (!entry.geometryIndex_) {
      return std::nullopt;
    }
    entry.boundingBox_ = boost::apply_visitor(
        BoundingBoxVisitor(), geometries_.at(entry.geometryIndex_.value()));
  } else {
    entry.boundingBox_ =
        Box(Point(entry.geoPoint_.value().getLng(),
                  entry.geoPoint_.value().getLat()),
            Point(entry.geoPoint_.value().getLng() + 0.00000001,
                  entry.geoPoint_.value().getLat() + 0.00000001));
  }
  return entry;
}

// ____________________________________________________________________________
std::vector<Box> SpatialJoinAlgorithms::getQueryBox(
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
Result SpatialJoinAlgorithms::BoundingBoxAlgorithm() {
  // helper struct to avoid duplicate entries for areas
  struct AddedPair {
    size_t rowLeft_;
    size_t rowRight_;

    auto operator<=>(const AddedPair& other) const {
      return (rowLeft_ == other.rowLeft_) ? (rowRight_ <=> other.rowRight_)
                                          : (rowLeft_ <=> other.rowLeft_);
    }
  };

  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, rightSelectedCols, numColumns, maxDist, maxResults,
              joinType] = params_;
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
             bgi::equal_to<Value>, ad_utility::AllocatorWithLimit<Value>>
      rtree(bgi::quadratic<16>{}, bgi::indexable<Value>{},
            bgi::equal_to<Value>{}, qec_->getAllocator());
  for (size_t i = 0; i < smallerResult->numRows(); i++) {
    if (spatialJoin_.has_value()) {
      spatialJoin_.value()->checkCancellationWrapperForSpatialJoinAlgorithms();
    }
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
  std::vector<Value, ad_utility::AllocatorWithLimit<Value>> results{
      qec_->getAllocator()};
  for (size_t i = 0; i < otherResult->numRows(); i++) {
    if (spatialJoin_.has_value()) {
      spatialJoin_.value()->checkCancellationWrapperForSpatialJoinAlgorithms();
    }
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
      if (distance.getDouble() * 1000 <= static_cast<double>(maxDist.value())) {
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
}

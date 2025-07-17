// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <absl/strings/str_join.h>
#include <gmock/gmock.h>
#include <spatialjoin/Sweeper.h>

#include <cstdlib>
#include <stdexcept>

#include "../util/GTestHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/SpatialJoinAlgorithms.h"
#include "engine/SpatialJoinConfig.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "rdfTypes/Literal.h"
#include "rdfTypes/Variable.h"
#include "util/GeoSparqlHelpers.h"
#include "util/SourceLocation.h"
#include "util/geo/Geo.h"

// _____________________________________________________________________________
namespace libSJPrefilterTestHelpers {

using namespace ad_utility::testing;
using namespace SpatialJoinTestHelpers;

struct SweeperSingleResult {
  SpatialJoinType sjType_;
  int indexInLeftTable_;
  int indexInRightTable_;
};
using SweeperResult = std::vector<std::vector<SweeperSingleResult>>;
using SweeperDistResult = std::vector<std::vector<double>>;

using QET = std::shared_ptr<QueryExecutionTree>;
using QEC = QueryExecutionContext*;
using Loc = ad_utility::source_location;

using ValIdToGeomName = ad_utility::HashMap<ValueId, std::string>;
using GeomNameToValId = ad_utility::HashMap<std::string, ValueId>;

struct SweeperSingleResultWithIds {
  SpatialJoinType sjType_;
  ValueId left_;
  ValueId right_;
  double meterDistance_;
};
using SweeperResultWithIds = std::vector<SweeperSingleResultWithIds>;
using GeoRelationWithIds = std::tuple<SpatialJoinType, ValueId, ValueId>;

struct SweeperTestResult {
  SweeperResultWithIds results_;
  util::geo::DBox boxAfterAddingLeft_;
  util::geo::DBox boxAfterAddingRight_;
  size_t numElementsInSweeper_;
  size_t numElementsSkippedByPrefilter_;
  size_t numElementsAddedLeft_;
  size_t numElementsAddedRight_;

  void printResults(const ValIdToGeomName& vMap) const {
    for (const auto& [sjType, valIdLeft, valIdRight, dist] : results_) {
      std::cout << "RESULTS: type="
                << SpatialJoinTypeString.at(static_cast<int>(sjType))
                << " left=" << vMap.at(valIdLeft)
                << " right=" << vMap.at(valIdRight) << " dist=" << dist
                << std::endl;
    }
  }
};

struct TestGeometry {
  std::string name_;
  std::string wkt_;
  bool isInGermany_;
};

const std::vector<TestGeometry> geometries{
    {"uni", areaUniFreiburg, true},
    {"minster", areaMuenster, true},
    {"gk-allee", lineSegmentGeorgesKoehlerAllee, true},
    {"london", areaLondonEye, false},
    {"lib", areaStatueOfLiberty, false},
    {"eiffel", areaEiffelTower, false},
    // Special
    {"approx-de", approximatedAreaGermany, true},
    {"uni-separate", areaTFCampus, true},
};

util::geo::I32Point webMercProjFunc(const util::geo::DPoint& p) {
  auto projPoint = latLngToWebMerc(p);
  return {static_cast<int>(projPoint.getX() * PREC),
          static_cast<int>(projPoint.getY() * PREC)};
}

util::geo::I32Box boxToWebMerc(const util::geo::DBox& b) {
  return {webMercProjFunc(b.getLowerLeft()),
          webMercProjFunc(b.getUpperRight())};
}

// Helpers for testing the `LibspatialjoinAlgorithm`
// TODO configurable for different tests
std::string buildLibSJTestDataset(bool addApproxGermany = false,
                                  bool germanyDifferentPredicate = true,
                                  bool addSeparateUni = false) {
  std::string kg;
  for (const auto& [name, wkt, isInGermany] : geometries) {
    if (addApproxGermany && name == "approx-de") {
      kg = absl::StrCat(kg, "<approx-de> <wkt-approx-de> ", wkt, " .\n");
    }
    if (addSeparateUni && name == "uni-separate") {
      kg = absl::StrCat(kg, "<uni-separate> <wkt-uni-separate> ", wkt, " .\n");
    }
    if (name != "uni-separate" && name != "approx-de") {
      kg = absl::StrCat(
          kg, "<", name, "> <wkt-",
          (isInGermany && germanyDifferentPredicate ? "de" : "other"), "> ",
          wkt, " .\n");
    }
  }
  return kg;
}

struct ValIdTable {
  ValIdToGeomName vMap_;
  GeomNameToValId nMap_;

  void print() const {
    for (const auto& [a, b] : vMap_) {
      std::cout << " VMAP " << a << " " << b << std::endl;
    }
    for (const auto& [a, b] : nMap_) {
      std::cout << " NMAP " << a << " " << b << std::endl;
    }
  }
};

//
ValIdTable resolveValIdTable(QueryExecutionContext* qec) {
  ValIdToGeomName vMap;
  GeomNameToValId nMap;
  for (const auto& [name, wkt, isInGermany] : geometries) {
    VocabIndex idx;
    if (!qec->getIndex().getVocab().getId(wkt, &idx)) {
      // This literal is not contained in this index
      continue;
    }
    auto vId = ValueId::makeFromVocabIndex(idx);
    vMap[vId] = name;
    nMap[name] = vId;
  }
  return {std::move(vMap), std::move(nMap)};
}

// TODO
sj::SweeperCfg makeSweeperCfg(const LibSpatialJoinConfig& libSJConfig,
                              SweeperResult& results,
                              SweeperDistResult& resultDists,
                              double withinDist) {
  using enum SpatialJoinType;
  sj::SweeperCfg cfg;
  cfg.numThreads = 1;
  cfg.numCacheThreads = 1;
  cfg.geomCacheMaxSize = 10'000;
  cfg.pairStart = "";
  cfg.sepIsect = std::string{static_cast<char>(INTERSECTS)};
  cfg.sepContains = std::string{static_cast<char>(CONTAINS)};
  cfg.sepCovers = std::string{static_cast<char>(COVERS)};
  cfg.sepTouches = std::string{static_cast<char>(TOUCHES)};
  cfg.sepEquals = std::string{static_cast<char>(EQUALS)};
  cfg.sepOverlaps = std::string{static_cast<char>(OVERLAPS)};
  cfg.sepCrosses = std::string{static_cast<char>(CROSSES)};
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
  auto joinTypeVal = libSJConfig.joinType_;
  cfg.writeRelCb = [&results, &resultDists, joinTypeVal](
                       size_t t, const char* a, const char* b,
                       const char* pred) {
    if (joinTypeVal == WITHIN_DIST) {
      results[t].push_back({WITHIN_DIST, std::atoi(a), std::atoi(b)});
      resultDists[t].push_back(atof(pred));
    } else {
      results[t].push_back(
          {static_cast<SpatialJoinType>(pred[0]), std::atoi(a), std::atoi(b)});
    }
  };
  cfg.logCb = {};
  cfg.statsCb = {};
  cfg.sweepProgressCb = {};
  cfg.sweepCancellationCb = {};
  return cfg;
}

//
void runParsingAndSweeper(QueryExecutionContext* qec, QET leftChild,
                          QET rightChild,
                          const LibSpatialJoinConfig& libSJConfig,
                          SweeperTestResult& testResult,
                          bool usePrefilter = true, Loc loc = Loc::current()) {
  using V = Variable;
  auto l = generateLocationTrace(loc);

  SpatialJoinConfiguration config{libSJConfig, V{"?geom1"}, V{"?geom2"}};
  config.algo_ = SpatialJoinAlgorithm::LIBSPATIALJOIN;

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, config, leftChild,
                                                 rightChild);
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  auto prepared = spatialJoin->onlyForTestingGetPrepareJoin();
  SpatialJoinAlgorithms sjAlgo{qec, prepared, config, spatialJoin};

  SweeperResult results{{}};
  SweeperDistResult resultDists{{}};

  double withinDist = libSJConfig.maxDist_.value_or(-1);
  auto sweeperCfg =
      makeSweeperCfg(libSJConfig, results, resultDists, withinDist);
  std::string sweeperPath = qec->getIndex().getOnDiskBase() + ".spatialjoin";
  sj::Sweeper sweeper(sweeperCfg, ".", "", sweeperPath.c_str());

  ASSERT_EQ(sweeper.numElements(), 0);
  auto [box, countLeft] = sjAlgo.libspatialjoinParse(
      false, {prepared.idTableLeft_, prepared.leftJoinCol_}, sweeper, 1,
      std::nullopt);
  // sweeper.setFilterBox(box);
  std::optional<util::geo::I32Box> prefilterBox = std::nullopt;
  if (usePrefilter) {
    prefilterBox = sweeper.getPaddedBoundingBox(box);
  }
  auto [boxRight, countRight] = sjAlgo.libspatialjoinParse(
      true, {prepared.idTableRight_, prepared.rightJoinCol_}, sweeper, 1,
      prefilterBox);
  sweeper.flush();
  ASSERT_EQ(spatialJoin->runtimeInfo().details_.contains(
                "num-geoms-dropped-by-prefilter"),
            usePrefilter);
  size_t numSkipped =
      usePrefilter
          ? static_cast<size_t>(spatialJoin->runtimeInfo()
                                    .details_["num-geoms-dropped-by-prefilter"])
          : 0;
  size_t numEl = sweeper.numElements();
  if (countLeft && countRight) {
    sweeper.sweep();
  }
  // TODO check countleft countright

  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(resultDists.size(), 1);

  if (libSJConfig.maxDist_.has_value()) {
    ASSERT_EQ(results.at(0).size(), resultDists.at(0).size());
  }

  SweeperResultWithIds resultMatched;

  for (size_t row = 0; row < results.at(0).size(); row++) {
    double dist = 0;
    if (libSJConfig.maxDist_.has_value()) {
      dist = resultDists.at(0).at(row);
    }

    const auto [sjType, leftIdx, rightIdx] = results.at(0).at(row);
    auto valIdLeft = prepared.idTableLeft_->at(leftIdx, prepared.leftJoinCol_);
    auto valIdRight =
        prepared.idTableRight_->at(rightIdx, prepared.rightJoinCol_);

    resultMatched.emplace_back(sjType, valIdLeft, valIdRight, dist);
  }

  auto boxLatLng = SpatialJoinAlgorithms::prefilterBoxToLatLng(box);
  ASSERT_TRUE(boxLatLng.has_value());

  auto boxRightLatLng = SpatialJoinAlgorithms::prefilterBoxToLatLng(boxRight);
  ASSERT_TRUE(boxRightLatLng.has_value());

  testResult = {resultMatched, boxLatLng.value(), boxRightLatLng.value(),
                numEl,         numSkipped,        countLeft,
                countRight};
}

//
void checkPrefilterBox(const util::geo::DBox& actualLatLng,
                       const util::geo::DBox& expectedLatLng,
                       Loc loc = Loc::current()) {
  auto l = generateLocationTrace(loc);

  auto lowerLeftActual = actualLatLng.getLowerLeft();
  auto lowerLeftExpected = expectedLatLng.getLowerLeft();
  ASSERT_NEAR(lowerLeftActual.getX(), lowerLeftExpected.getX(), 0.0001);
  ASSERT_NEAR(lowerLeftActual.getY(), lowerLeftExpected.getY(), 0.0001);

  auto upperRightActual = actualLatLng.getUpperRight();
  auto upperRightExpected = expectedLatLng.getUpperRight();
  ASSERT_NEAR(upperRightActual.getX(), upperRightExpected.getX(), 0.0001);
  ASSERT_NEAR(upperRightActual.getY(), upperRightExpected.getY(), 0.0001);
}

//
void checkSweeperTestResult(
    const ValIdToGeomName& vMap, const SweeperTestResult& actual,
    const SweeperTestResult& expected,
    std::optional<SpatialJoinType> checkOnlySjType = std::nullopt,
    bool checkPrefilterBoxes = false, Loc loc = Loc::current()) {
  auto l = generateLocationTrace(loc);

  ad_utility::HashMap<GeoRelationWithIds, double> expectedResultsAndDist;

  auto checkValId = [&](ValueId valId, Loc loc = Loc::current()) {
    auto l = generateLocationTrace(loc);
    ASSERT_EQ(valId.getDatatype(), Datatype::VocabIndex);
    ASSERT_TRUE(vMap.contains(valId));
  };

  for (const auto& [sjType, valIdLeft, valIdRight, dist] : expected.results_) {
    if (checkOnlySjType.has_value() && sjType != checkOnlySjType.value()) {
      continue;
    }
    checkValId(valIdLeft);
    checkValId(valIdRight);

    expectedResultsAndDist[{sjType, valIdLeft, valIdRight}] = dist;
  }

  size_t numActualResults = 0;
  for (const auto& [sjType, valIdLeft, valIdRight, dist] : actual.results_) {
    if (checkOnlySjType.has_value() && sjType != checkOnlySjType.value()) {
      continue;
    }
    checkValId(valIdLeft);
    checkValId(valIdRight);

    GeoRelationWithIds key{sjType, valIdLeft, valIdRight};
    ASSERT_TRUE(expectedResultsAndDist.contains(key));
    ASSERT_NEAR(expectedResultsAndDist[key], dist, 0.01);
    ++numActualResults;
  }

  ASSERT_EQ(numActualResults, expectedResultsAndDist.size());

  ASSERT_EQ(actual.numElementsInSweeper_, expected.numElementsInSweeper_);
  ASSERT_EQ(actual.numElementsSkippedByPrefilter_,
            expected.numElementsSkippedByPrefilter_);
  ASSERT_EQ(actual.numElementsAddedLeft_, expected.numElementsAddedLeft_);
  ASSERT_EQ(actual.numElementsAddedRight_, expected.numElementsAddedRight_);

  if (checkPrefilterBoxes) {
    if (actual.numElementsAddedLeft_ > 0) {
      checkPrefilterBox(actual.boxAfterAddingLeft_,
                        expected.boxAfterAddingLeft_);
    }
    if (actual.numElementsAddedRight_ > 0) {
      checkPrefilterBox(actual.boxAfterAddingRight_,
                        expected.boxAfterAddingRight_);
    }
  }
}

QET makeLeftChild(QEC qec, std::string pred) {
  return buildIndexScan(qec,
                        {"?obj1", absl::StrCat("<wkt-", pred, ">"), "?geom1"});
}

QET makeRightChild(QEC qec, std::string pred) {
  return buildIndexScan(qec,
                        {"?obj2", absl::StrCat("<wkt-", pred, ">"), "?geom2"});
}

util::geo::DBox makeAggregatedBoundingBox(
    const std::vector<std::string>& wktGeometries) {
  auto aggregatedWkt =
      absl::StrCat("\"GEOMETRYCOLLECTION(", absl::StrJoin(wktGeometries, ", "),
                   ")\"^^<", GEO_WKT_LITERAL, ">");
  auto boundingBox = ad_utility::GeometryInfo::getBoundingBox(aggregatedWkt);
  if (!boundingBox.has_value()) {
    throw std::runtime_error("Could not compute expected bounding box.");
  }
  return ad_utility::detail::boundingBoxToUtilBox(boundingBox.value());
}

}  // namespace libSJPrefilterTestHelpers

// _____________________________________________________________________________
namespace {

using namespace libSJPrefilterTestHelpers;

// _____________________________________________________________________________
TEST(SpatialJoinTest, prefilterBoxToLatLng) {
  // TODO SpatialJoinAlgorithms::prefilterBoxToLatLng;
}

// _____________________________________________________________________________
TEST(SpatialJoinTest, prefilterGeoByBoundingBox) {
  // TODO SpatialJoinAlgorithms::prefilterGeoByBoundingBox;
}

// _____________________________________________________________________________
TEST(SpatialJoinTest, BoundingBoxPrefilter) {
  // Create a `QueryExecutionContext` on a `GeoVocabulary` which holds various
  // literals. In particular, prefiltering can be applied using the Germany
  // bounding box.
  using enum SpatialJoinType;

  // No intersections
  auto kg = buildLibSJTestDataset();
  // std::cout << kg << std::endl;
  auto qec = buildQec(kg, true);
  auto leftChild = makeLeftChild(qec, "de");
  auto rightChild = makeRightChild(qec, "other");

  auto boundingBoxOnlyGermany = makeAggregatedBoundingBox(
      {areaMuenster, areaUniFreiburg, lineSegmentGeorgesKoehlerAllee});

  auto [vMap, nMap] = resolveValIdTable(qec);
  ASSERT_EQ(vMap.size(), 6);
  ASSERT_EQ(nMap.size(), 6);
  SweeperTestResult testResult;
  runParsingAndSweeper(qec, leftChild, rightChild, {INTERSECTS}, testResult,
                       true);
  SweeperTestResult expected{{}, boundingBoxOnlyGermany, {}, 3, 3, 3, 0};
  checkSweeperTestResult(vMap, testResult, expected, INTERSECTS, true);

  // Repeat without prefilter
  runParsingAndSweeper(qec, leftChild, rightChild, {INTERSECTS}, testResult,
                       false);
  expected = {{}, {}, {}, 6, 0, 3, 3};
  checkSweeperTestResult(vMap, testResult, expected, INTERSECTS);

  // Intersects de

  auto kgWithOnlyUni = buildLibSJTestDataset(false, true, true);
  auto qecWithOnlyUni = buildQec(kgWithOnlyUni, true);
  auto leftChildOnlyUni = makeLeftChild(qecWithOnlyUni, "uni-separate");
  auto rightChildDEForOnlyUni = makeRightChild(qecWithOnlyUni, "de");

  auto [vMapOnlyU, nMapOnlyU] = resolveValIdTable(qecWithOnlyUni);
  ASSERT_EQ(vMapOnlyU.size(), nMapOnlyU.size());
  runParsingAndSweeper(qecWithOnlyUni, leftChildOnlyUni, rightChildDEForOnlyUni,
                       {INTERSECTS}, testResult, true);

  auto x = nMapOnlyU["uni-separate"];
  auto y = nMapOnlyU["gk-allee"];
  auto y2 = nMapOnlyU["uni"];
  ASSERT_EQ(x.getDatatype(), Datatype::VocabIndex);
  ASSERT_EQ(y.getDatatype(), Datatype::VocabIndex);
  ASSERT_EQ(y2.getDatatype(), Datatype::VocabIndex);

  expected = {
      {{INTERSECTS, x, y, 0}, {INTERSECTS, x, y2, 0}}, {}, {}, 3, 1, 1, 2};
  checkSweeperTestResult(vMapOnlyU, testResult, expected, INTERSECTS);

  runParsingAndSweeper(qecWithOnlyUni, leftChildOnlyUni, rightChildDEForOnlyUni,
                       {WITHIN_DIST, 5000}, testResult, true);
  auto y3 = nMapOnlyU["minster"];
  expected = {{{WITHIN_DIST, x, y, 0},
               {WITHIN_DIST, x, y2, 0},
               {WITHIN_DIST, x, y3, 2225.01}},
              {},
              {},
              4,
              0,
              1,
              3};
  checkSweeperTestResult(vMapOnlyU, testResult, expected, WITHIN_DIST);

  //

  auto kgAll = buildLibSJTestDataset(true, false, false);
  auto qecAll = buildQec(kgAll, true);
  auto leftChildAll = makeLeftChild(qecAll, "approx-de");
  auto rightChildAll = makeRightChild(qecAll, "other");

  auto [vMapA, nMapA] = resolveValIdTable(qecAll);
  runParsingAndSweeper(qecAll, leftChildAll, rightChildAll, {INTERSECTS},
                       testResult, true);
  auto xA = nMapA["approx-de"];
  auto yA = nMapA["minster"];
  auto yA2 = nMapA["uni"];
  auto yA3 = nMapA["gk-allee"];
  auto bb = ad_utility::GeometryInfo::getBoundingBox(approximatedAreaGermany);
  ASSERT_TRUE(bb.has_value());
  auto bbU = ad_utility::detail::boundingBoxToUtilBox(bb.value());

  expected = {{{INTERSECTS, xA, yA, 0},
               {INTERSECTS, xA, yA2, 0},
               {INTERSECTS, xA, yA3, 0}},
              bbU,
              boundingBoxOnlyGermany,
              4,
              3,
              1,
              3};
  checkSweeperTestResult(vMapA, testResult, expected, INTERSECTS, true);
  runParsingAndSweeper(qecAll, leftChildAll, rightChildAll, {INTERSECTS},
                       testResult, false);
  expected.numElementsInSweeper_ = 7;
  expected.numElementsSkippedByPrefilter_ = 0;
  expected.numElementsAddedLeft_ = 1;
  expected.numElementsAddedRight_ = 6;
  auto expectedBoundingBoxRightNoFilter = makeAggregatedBoundingBox(
      {areaMuenster, areaUniFreiburg, lineSegmentGeorgesKoehlerAllee,
       areaLondonEye, areaStatueOfLiberty, areaEiffelTower});
  expected.boxAfterAddingRight_ = expectedBoundingBoxRightNoFilter;
  checkSweeperTestResult(vMapA, testResult, expected, INTERSECTS, true);

  //
  auto yA4 = nMapA["london"];
  auto yA5 = nMapA["eiffel"];
  runParsingAndSweeper(qecAll, leftChildAll, rightChildAll,
                       {WITHIN_DIST, 1'000'000}, testResult, true);
  // TODO project
  expected = {{{WITHIN_DIST, xA, yA, 0},
               {WITHIN_DIST, xA, yA2, 0},
               {WITHIN_DIST, xA, yA3, 0},
               {WITHIN_DIST, xA, yA4, 426521.1497},
               {WITHIN_DIST, xA, yA5, 314975.6311}},
              {},
              {},
              6,
              1,
              1,
              5};
  checkSweeperTestResult(vMapA, testResult, expected, WITHIN_DIST);

  // TODO check boxes

  // also test maxdist because of extension of bounding box (e.g. uni-tf in
  // first run, minster in second, maxdist 50km)

  // all of them also without prefiltering , should be more to parse but same
  // result
}

}  // namespace

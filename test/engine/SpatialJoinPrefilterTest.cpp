// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <spatialjoin/Sweeper.h>

#include <cstdlib>

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

using SweeperResult =
    std::vector<std::vector<std::tuple<SpatialJoinType, int, int>>>;
using SweeperDistResult = std::vector<std::vector<double>>;

using QET = std::shared_ptr<QueryExecutionTree>;
using Loc = ad_utility::source_location;

using ValIdToGeomName = ad_utility::HashMap<ValueId, std::string>;
using GeomNameToValId = ad_utility::HashMap<std::string, ValueId>;

using SweeperResultWithIds =
    std::vector<std::tuple<SpatialJoinType, ValueId, ValueId, double>>;
using GeoRelationWithIds = std::tuple<SpatialJoinType, ValueId, ValueId>;

struct SweeperTestResult {
  SweeperResultWithIds results_;
  util::geo::DBox boxAfterAddingLeft_;
  util::geo::DBox boxAfterAddingRight_;
  size_t numElementsInSweeper_;
  size_t numElementsSkippedByPrefilter_;
  size_t numElementsAddedLeft_;
  size_t numElementsAddedRight_;
};

using TestGeometry = std::tuple<std::string, std::string, bool>;

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

//
std::pair<ValIdToGeomName, GeomNameToValId> resolveValIdTable(
    QueryExecutionContext* qec) {
  ValIdToGeomName vMap;
  GeomNameToValId nMap;
  std::cout << "***********************************" << std::endl;
  for (const auto& [name, wkt, isInGermany] : geometries) {
    VocabIndex idx;
    if (!qec->getIndex().getVocab().getId(wkt, &idx)) {
      // This literal is not contained in this index
      continue;
    }
    auto vId = ValueId::makeFromVocabIndex(idx);
    // Make explicit copy of strings otherwise we get
    vMap[ValueId{vId}] = std::string{name};
    nMap[std::string{name}] = ValueId{vId};
  }
  for (const auto& [name, wkt, isInGermany] : geometries) {
    auto p = GeoPoint::parseFromLiteral(
        ad_utility::triple_component::Literal::literalWithoutQuotes(wkt));
    if (!p.has_value()) {
      continue;
    }
    auto b = ValueId::makeFromGeoPoint(p.value());
    vMap[b] = name;
    nMap[name] = b;
  }
  // TODO add geo points
  for (const auto& [a, b] : vMap) {
    std::cout << " VMAP " << a << " " << b << std::endl;
  }
  for (const auto& [a, b] : nMap) {
    std::cout << " NMAP " << a << " " << b << std::endl;
  }
  return {vMap, nMap};
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
    } else if (pred[0] == static_cast<char>(joinTypeVal)) {
      results[t].push_back(
          {static_cast<SpatialJoinType>(pred[0]), std::atoi(a), std::atoi(b)});
    }
    // if (pred[0] == static_cast<char>(libSJConfig.joinType_) ||
    //     withinDist >= 0) {
    //   // TODO ??
    //   results[t].push_back(
    //       {static_cast<SpatialJoinType>(pred[0]), std::atoi(a),
    //       std::atoi(b)});
    // }
    // if (withinDist >= 0) {
    //   resultDists[t].push_back(atof(pred));
    // }
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

  auto lla = actualLatLng.getLowerLeft();
  auto lle = expectedLatLng.getLowerLeft();
  ASSERT_NEAR(lla.getX(), lle.getX(), 0.0001);
  ASSERT_NEAR(lla.getY(), lle.getY(), 0.0001);

  auto ura = actualLatLng.getUpperRight();
  auto ure = expectedLatLng.getUpperRight();
  ASSERT_NEAR(ura.getX(), ure.getX(), 0.0001);
  ASSERT_NEAR(ura.getY(), ure.getY(), 0.0001);
}

//
void checkSweeperTestResult(
    const ValIdToGeomName& vMap, const SweeperTestResult& actual,
    const SweeperTestResult& expected,
    std::optional<SpatialJoinType> checkOnlySjType = std::nullopt,
    bool checkPrefilterBoxes = false, Loc loc = Loc::current()) {
  auto l = generateLocationTrace(loc);

  ad_utility::HashMap<GeoRelationWithIds, double> expectedResultsAndDist;

  for (const auto& [sjType, valIdLeft, valIdRight, dist] : expected.results_) {
    if (checkOnlySjType.has_value() && sjType != checkOnlySjType.value()) {
      continue;
    }
    ASSERT_EQ(valIdLeft.getDatatype(), Datatype::VocabIndex);
    ASSERT_EQ(valIdRight.getDatatype(), Datatype::VocabIndex);
    ASSERT_TRUE(vMap.contains(valIdLeft));
    ASSERT_TRUE(vMap.contains(valIdRight));

    std::cout << " EXPECTED " << SpatialJoinTypeString[(int)sjType] << " "
              << vMap.at(valIdLeft) << " " << vMap.at(valIdRight) << " " << dist
              << std::endl;
    expectedResultsAndDist[{sjType, valIdLeft, valIdRight}] = dist;
  }
  size_t countActual = 0;
  for (const auto& [sjType, valIdLeft, valIdRight, dist] : actual.results_) {
    if (checkOnlySjType.has_value() && sjType != checkOnlySjType.value()) {
      continue;
    }
    ASSERT_EQ(valIdLeft.getDatatype(), Datatype::VocabIndex);
    ASSERT_EQ(valIdRight.getDatatype(), Datatype::VocabIndex);
    ASSERT_TRUE(vMap.contains(valIdLeft));
    ASSERT_TRUE(vMap.contains(valIdRight));

    std::cout << " ACTUAL " << SpatialJoinTypeString[(int)sjType] << " "
              << vMap.at(valIdLeft) << " " << vMap.at(valIdRight) << " " << dist
              << std::endl;
    GeoRelationWithIds key{sjType, valIdLeft, valIdRight};
    ASSERT_TRUE(expectedResultsAndDist.contains(key));
    ASSERT_NEAR(expectedResultsAndDist[key], dist, 0.01);
    ++countActual;
  }
  ASSERT_EQ(countActual, expectedResultsAndDist.size());

  if (checkPrefilterBoxes) {
    checkPrefilterBox(actual.boxAfterAddingLeft_, expected.boxAfterAddingLeft_);
    checkPrefilterBox(actual.boxAfterAddingRight_,
                      expected.boxAfterAddingRight_);
  }

  ASSERT_EQ(actual.numElementsInSweeper_, expected.numElementsInSweeper_);
  ASSERT_EQ(actual.numElementsSkippedByPrefilter_,
            expected.numElementsSkippedByPrefilter_);
  ASSERT_EQ(actual.numElementsAddedLeft_, expected.numElementsAddedLeft_);
  ASSERT_EQ(actual.numElementsAddedRight_, expected.numElementsAddedRight_);
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
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<wkt-de>"}, "?geom1"});
  auto kgWithOnlyUni = buildLibSJTestDataset(false, true, true);
  auto qecWithOnlyUni = buildQec(kgWithOnlyUni, true);
  auto leftChildOnlyUni = buildIndexScan(
      qecWithOnlyUni, {"?obj1", std::string{"<wkt-uni-separate>"}, "?geom1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<wkt-other>"}, "?geom2"});
  auto rightChildDEForOnlyUni = buildIndexScan(
      qecWithOnlyUni, {"?obj2", std::string{"<wkt-de>"}, "?geom2"});
  auto kgAll = buildLibSJTestDataset(true, false, false);
  auto qecAll = buildQec(kgAll, true);
  auto leftChildAll = buildIndexScan(
      qecAll, {"?obj1", std::string{"<wkt-approx-de>"}, "?geom1"});
  auto rightChildAll =
      buildIndexScan(qecAll, {"?obj2", std::string{"<wkt-other>"}, "?geom2"});

  auto [vMap, nMap] = resolveValIdTable(qec);
  SweeperTestResult testResult;
  runParsingAndSweeper(qec, leftChild, rightChild, {INTERSECTS}, testResult,
                       true);
  SweeperTestResult expected{{}, {}, {}, 3, 3, 3, 0};
  checkSweeperTestResult(vMap, testResult, expected);

  // Repeat without prefilter
  runParsingAndSweeper(qec, leftChild, rightChild, {INTERSECTS}, testResult,
                       false);
  expected = {{}, {}, {}, 6, 0, 3, 3};
  checkSweeperTestResult(vMap, testResult, expected);

  // Intersects de

  auto [vMapOnlyU, nMapOnlyU] = resolveValIdTable(qecWithOnlyUni);
  ASSERT_EQ(vMapOnlyU.size(), nMapOnlyU.size());
  runParsingAndSweeper(qecWithOnlyUni, leftChildOnlyUni, rightChildDEForOnlyUni,
                       {INTERSECTS}, testResult, true);

  auto x = nMapOnlyU["uni-separate"];
  auto y = nMapOnlyU["gk-allee"];
  auto y2 = nMapOnlyU["uni"];
  ASSERT_TRUE(x.getDatatype() == Datatype::VocabIndex);
  ASSERT_TRUE(y.getDatatype() == Datatype::VocabIndex);
  ASSERT_TRUE(y2.getDatatype() == Datatype::VocabIndex);

  expected = {
      {{INTERSECTS, x, y, 0}, {INTERSECTS, x, y2, 0}}, {}, {}, 3, 1, 1, 2};
  checkSweeperTestResult(vMapOnlyU, testResult, expected);

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
  checkSweeperTestResult(vMapOnlyU, testResult, expected);

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

  auto expectedBoundingBoxRight = absl::StrCat(
      "\"GEOMETRYCOLLECTION(", areaMuenster, ", ", areaUniFreiburg, ", ",
      lineSegmentGeorgesKoehlerAllee, ")\"^^<", GEO_WKT_LITERAL, ">");
  auto bb2 = ad_utility::GeometryInfo::getBoundingBox(expectedBoundingBoxRight);
  ASSERT_TRUE(bb2.has_value());
  auto bbA = ad_utility::detail::boundingBoxToUtilBox(bb2.value());

  expected = {{{INTERSECTS, xA, yA, 0},
               {INTERSECTS, xA, yA2, 0},
               {INTERSECTS, xA, yA3, 0}},
              bbU,
              bbA,
              4,
              3,
              1,
              3};
  checkSweeperTestResult(vMapA, testResult, expected, std::nullopt, true);
  runParsingAndSweeper(qecAll, leftChildAll, rightChildAll, {INTERSECTS},
                       testResult, false);
  expected.numElementsInSweeper_ = 7;
  expected.numElementsSkippedByPrefilter_ = 0;
  expected.numElementsAddedLeft_ = 1;
  expected.numElementsAddedRight_ = 6;
  auto expectedBoundingBoxRightNoFilter =
      absl::StrCat("\"GEOMETRYCOLLECTION(", areaMuenster, ", ", areaUniFreiburg,
                   ", ", lineSegmentGeorgesKoehlerAllee, ", ", areaLondonEye,
                   ", ", areaStatueOfLiberty, ", ", areaEiffelTower, ")\"^^<",
                   GEO_WKT_LITERAL, ">");
  auto bb2NoFilter = ad_utility::GeometryInfo::getBoundingBox(
      expectedBoundingBoxRightNoFilter);
  ASSERT_TRUE(bb2NoFilter.has_value());
  auto bbANoFilter =
      ad_utility::detail::boundingBoxToUtilBox(bb2NoFilter.value());
  // TODO also check boxes?
  expected.boxAfterAddingRight_ = bbANoFilter;
  checkSweeperTestResult(vMapA, testResult, expected, std::nullopt, true);

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
  checkSweeperTestResult(vMapA, testResult, expected);

  // TODO check boxes

  // {{{INTERSECTS, nMap["uni"], nMap["gk-allee"], 0}}, {}, {}, 5, 4};

  // Now check results / resultDists

  // LibSpatialJoinConfig libSJConfig{INTERSECTS, std::nullopt};
  // if (withinDist >= 0) {
  //   libSJConfig = {WITHIN_DIST, withinDist};
  // }

  // sjAlgo.libspatialjoinParse(bool leftOrRightSide, IdTableAndJoinColumn
  // idTableAndCol, sj::Sweeper &sweeper, size_t numThreads,
  // std::optional<util::geo::I32Box> prefilterBox)

  // also test maxdist because of extension of bounding box (e.g. uni-tf in
  // first run, minster in second, maxdist 50km)

  //  intersects wkt-de/wkt-other , wkt intersects all against approx-de , wkt
  //  all against uni-tf with maxdist
  // all of them also without prefiltering , should be more to parse but same
  // result
}

}  // namespace

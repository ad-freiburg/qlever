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
#include "rdfTypes/Variable.h"
#include "util/GeoSparqlHelpers.h"
#include "util/SourceLocation.h"

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
  util::geo::I32Box boxAfterAddingLeft_;
  util::geo::I32Box boxAfterAddingRight_;
  size_t numElementsInSweeper_;
  size_t numElementsSkippedByPrefilter_;
};

using TestGeometry = std::tuple<std::string, std::string, bool>;

const std::vector<TestGeometry> geometries{
    {"uni", areaUniFreiburg, true},
    {"uni-p", pointUniFreiburg, true},
    {"minster", areaMuenster, true},
    {"minster-p", pointMinster, true},
    {"gk-allee", lineSegmentGeorgesKoehlerAllee, true},
    {"london", areaLondonEye, false},
    {"lib", areaStatueOfLiberty, false},
    {"eiffel", areaEiffelTower, false},
    {"eiffel-p", pointEiffelTower, false},
    // Special
    {"approx-de", approximatedAreaGermany, true},
    {"uni-separate", areaUniFreiburg, true},
};

// Helpers for testing the `LibspatialjoinAlgorithm`
// TODO configurable for different tests
std::string buildLibSJTestDataset(bool addApproxGermany = false,
                                  bool germanyDifferentPredicate = true,
                                  bool addSeparateUni = false) {
  std::string kg;
  for (const auto& [name, wkt, isInGermany] : geometries) {
    if (addApproxGermany) {
      if (name == "approx-de") {
        kg = absl::StrCat(kg, "<approx-de> <wkt-approx-de> ", wkt, " .\n");
      }
    } else if (addSeparateUni) {
      if (name == "uni-separate") {
        kg =
            absl::StrCat(kg, "<uni-separate> <wkt-uni-separate> ", wkt, " .\n");
      }
    } else {
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
  cfg.writeRelCb = [&results, &resultDists, withinDist, &libSJConfig](
                       size_t t, const char* a, const char* b,
                       const char* pred) {
    if (pred[0] == static_cast<char>(libSJConfig.joinType_)) {
      // TODO ??
    }
    results[t].push_back(
        {static_cast<SpatialJoinType>(pred[0]), std::atoi(a), std::atoi(b)});
    if (withinDist >= 0) {
      resultDists[t].push_back(atof(pred));
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
  sweeper.setFilterBox(box);
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

  testResult = {resultMatched, box, boxRight, numEl, numSkipped};
}

//
void checkPrefilterBox(const util::geo::I32Box& actual,
                       const util::geo::I32Box& expected,
                       Loc loc = Loc::current()) {
  auto l = generateLocationTrace(loc);

  auto actualLatLng = SpatialJoinAlgorithms::prefilterBoxToLatLng(actual);
  ASSERT_TRUE(actualLatLng.has_value());
  auto expectedLatLng = SpatialJoinAlgorithms::prefilterBoxToLatLng(expected);
  ASSERT_TRUE(expectedLatLng.has_value());

  auto lla = actualLatLng.value().getLowerLeft();
  auto lle = expectedLatLng.value().getLowerLeft();
  ASSERT_NEAR(lla.getX(), lle.getX(), 0.0001);
  ASSERT_NEAR(lla.getY(), lle.getY(), 0.0001);

  auto ura = actualLatLng.value().getUpperRight();
  auto ure = expectedLatLng.value().getUpperRight();
  ASSERT_NEAR(ura.getX(), ure.getX(), 0.0001);
  ASSERT_NEAR(ura.getY(), ure.getY(), 0.0001);
}

//
void checkSweeperTestResult(QueryExecutionContext* qec,
                            const SweeperTestResult& actual,
                            const SweeperTestResult& expected,
                            bool checkPrefilterBoxes = false,
                            Loc loc = Loc::current()) {
  auto l = generateLocationTrace(loc);

  ASSERT_EQ(actual.results_.size(), expected.results_.size());
  auto [vMap, nMap] = resolveValIdTable(qec);
  ad_utility::HashMap<GeoRelationWithIds, double> map;
  for (const auto& [sjType, valIdLeft, valIdRight, dist] : expected.results_) {
    std::cout << " EXPECTED " << (int)(sjType) << " " << vMap.at(valIdLeft)
              << " " << vMap.at(valIdRight) << " " << dist << std::endl;
    map[{sjType, valIdLeft, valIdRight}] = dist;
  }
  for (const auto& [sjType, valIdLeft, valIdRight, dist] : actual.results_) {
    std::cout << " ACTUAL " << (int)(sjType) << " " << vMap.at(valIdLeft) << " "
              << vMap.at(valIdRight) << " " << dist << std::endl;
    GeoRelationWithIds key{sjType, valIdLeft, valIdRight};
    ASSERT_TRUE(map.contains(key));
    ASSERT_NEAR(map[key], dist, 0.01);
  }
  if (checkPrefilterBoxes) {
    checkPrefilterBox(actual.boxAfterAddingLeft_, expected.boxAfterAddingLeft_);
    checkPrefilterBox(actual.boxAfterAddingRight_,
                      expected.boxAfterAddingRight_);
  }

  ASSERT_EQ(actual.numElementsInSweeper_, expected.numElementsInSweeper_);
  ASSERT_EQ(actual.numElementsSkippedByPrefilter_,
            expected.numElementsSkippedByPrefilter_);
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

  auto kg = buildLibSJTestDataset();
  auto qec = buildQec(kg, true);
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<wkt-de>"}, "?geom1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<wkt-other>"}, "?geom2"});
  auto [vMap, nMap] = resolveValIdTable(qec);
  SweeperTestResult testResult;
  runParsingAndSweeper(qec, leftChild, rightChild, {INTERSECTS}, testResult,
                       true);

  SweeperTestResult expected{{}, {}, {}, 7, 4};
  // {{{INTERSECTS, nMap["uni"], nMap["gk-allee"], 0}}, {}, {}, 5, 4};
  checkSweeperTestResult(qec, testResult, expected);

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

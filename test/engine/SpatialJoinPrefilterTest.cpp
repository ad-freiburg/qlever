// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <spatialjoin/Sweeper.h>

#include <cstdlib>
#include <fstream>
#include <regex>
#include <variant>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/IndexScan.h"
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

// Helpers for testing the `LibspatialjoinAlgorithm`
// TODO configurable for different tests
std::string buildLibSJTestDataset() {
  auto kg = absl::StrCat(
      "<uni> <wkt-de> ", areaUniFreiburg, " . \n <uni-p> <wkt-de> ",
      pointUniFreiburg, " .\n <minster> <wkt-de> ", areaMuenster,
      " .\n <minster-p> <wkt-de> ", pointMinster, " .\n <gk-allee> <wkt-de> ",
      lineSegmentGeorgesKoehlerAllee, " .\n <london> <wkt-other> ",
      areaLondonEye, " .\n <lib> <wkt-other> ", areaStatueOfLiberty,
      " .\n <eiffel> <wkt-other> ", areaEiffelTower,
      " .\n <eiffel-p> <wkt-other> ", pointEiffelTower, " .\n");
  // auto approxDe =
  //     absl::StrCat("<approx-de> <wkt-de> ", approximatedAreaGermany, " .\n");
  return kg;
}

// TODO
sj::SweeperCfg makeSweeperCfg(SweeperResult& results,
                              SweeperDistResult& resultDists,
                              double withinDist) {
  sj::SweeperCfg cfg;
  cfg.numThreads = 1;
  cfg.numCacheThreads = 1;
  cfg.geomCacheMaxSize = 10'000;
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
  cfg.writeRelCb = [&results, &resultDists, withinDist](size_t t, const char* a,
                                                        const char* b,
                                                        const char* pred) {
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
struct SweeperTestResult {
  SweeperResult results_;
  SweeperDistResult resultDists_;
  util::geo::I32Box boxAfterAddingLeft_;
  util::geo::I32Box boxAfterAddingRight_;
  size_t numElementsInSweeper_;
  size_t numElementsSkippedByPrefilter_;
};

//
void runParsingAndSweeper(QueryExecutionContext* qec, QET leftChild,
                          QET rightChild,
                          const LibSpatialJoinConfig& libSJConfig,
                          SweeperTestResult& testResult,
                          bool usePrefilter = true, Loc loc = Loc::current()) {
  using V = Variable;
  using enum SpatialJoinType;
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
  auto sweeperCfg = makeSweeperCfg(results, resultDists, withinDist);
  std::string sweeperPath = qec->getIndex().getOnDiskBase() + ".spatialjoin";
  sj::Sweeper sweeper(sweeperCfg, ".", "", sweeperPath.c_str());

  ASSERT_EQ(sweeper.numElements(), 0);
  auto box = sjAlgo.libspatialjoinParse(
      false, {prepared.idTableLeft_, prepared.leftJoinCol_}, sweeper, 1,
      std::nullopt);
  sweeper.setFilterBox(box);
  std::optional<util::geo::I32Box> prefilterBox = std::nullopt;
  if (usePrefilter) {
    prefilterBox = sweeper.getPaddedBoundingBox(box);
  }
  auto boxRight = sjAlgo.libspatialjoinParse(
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
  sweeper.sweep();
  testResult = {results, resultDists, box, boxRight, numEl, numSkipped};
}

//
void x() { SweeperTestResult result; }

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
  // bouding box.

  auto kg = buildLibSJTestDataset();
  auto qec = buildQec(kg, true);
  auto leftChild =
      buildIndexScan(qec, {"?obj1", std::string{"<wkt-de>"}, "?geom1"});
  auto rightChild =
      buildIndexScan(qec, {"?obj2", std::string{"<wkt-other>"}, "?geom2"});
  SweeperTestResult testResult;
  runParsingAndSweeper(qec, leftChild, rightChild,
                       {SpatialJoinType::INTERSECTS, std::nullopt}, testResult,
                       true);

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
}

}  // namespace

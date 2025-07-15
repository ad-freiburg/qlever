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

// Helpers for testing the `LibspatialjoinAlgorithm`
// TODO configurable for different tests
std::string buildLibSJTestDataset(bool addApproxGermany = false,
                                  bool germanyDifferentPredicate = true,
                                  bool addSeparateUni = false) {
  std::vector<std::tuple<std::string, std::string, bool>> geometries{
      {"uni", areaUniFreiburg, true},
      {"uni-p", pointUniFreiburg, true},
      {"minster", areaMuenster, true},
      {"minster-p", pointMinster, true},
      {"gk-allee", lineSegmentGeorgesKoehlerAllee, true},
      {"london", areaLondonEye, false},
      {"lib", areaStatueOfLiberty, false},
      {"eiffel", areaEiffelTower, false},
      {"eiffel-p", pointEiffelTower, false},
  };

  std::string kg;
  for (const auto& [name, wkt, isInGermany] : geometries) {
    kg = absl::StrCat(
        kg, "<", name, "> <wkt-",
        (isInGermany && germanyDifferentPredicate ? "de" : "other"), "> ", wkt,
        " .\n");
  }

  if (addApproxGermany) {
    kg = absl::StrCat(kg, "<approx-de> <wkt-approx-de> ",
                      approximatedAreaGermany, " .\n");
  }

  if (addSeparateUni) {
    kg = absl::StrCat(kg, "<uni-separate> <wkt-uni-separate> ", areaUniFreiburg,
                      " .\n");
  }

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

  if (libSJConfig.maxDist_.has_value()) {
    ASSERT_EQ(results.size(), resultDists.size());
  }

  testResult = {results, resultDists, box, boxRight, numEl, numSkipped};
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
void checkSweeperTestResult(const SweeperTestResult& actual,
                            const SweeperTestResult& expected,
                            Loc loc = Loc::current()) {
  auto l = generateLocationTrace(loc);

  ASSERT_EQ(actual.results_.size(), expected.results_.size());
  ASSERT_EQ(actual.resultDists_.size(), expected.resultDists_.size());
  // TODO actually compare results_ and resultDists_

  checkPrefilterBox(actual.boxAfterAddingLeft_, expected.boxAfterAddingLeft_);
  checkPrefilterBox(actual.boxAfterAddingRight_, expected.boxAfterAddingRight_);

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
  SweeperTestResult testResult;
  runParsingAndSweeper(qec, leftChild, rightChild, {INTERSECTS}, testResult,
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

  //  intersects wkt-de/wkt-other , wkt intersects all against approx-de , wkt
  //  all against uni-tf with maxdist
  // all of them also without prefiltering , should be more to parse but same
  // result
}

}  // namespace

// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_ENGINE_SPATIALJOINPREFILTERTESTHELPERS_H
#define QLEVER_TEST_ENGINE_SPATIALJOINPREFILTERTESTHELPERS_H

#include <absl/strings/str_join.h>
#include <gmock/gmock.h>
#include <spatialjoin/Sweeper.h>
#include <util/geo/Geo.h>

#include <cstdlib>
#include <stdexcept>

#include "../util/GTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/SpatialJoinAlgorithms.h"
#include "engine/SpatialJoinConfig.h"
#include "global/RuntimeParameters.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "rdfTypes/Literal.h"
#include "rdfTypes/Variable.h"
#include "util/GeoSparqlHelpers.h"
#include "util/SourceLocation.h"

// _____________________________________________________________________________
namespace SpatialJoinPrefilterTestHelpers {

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

// Struct for the output of `runParsingAndSweeper`
struct SweeperTestResult {
  SweeperResultWithIds results_;
  util::geo::DBox boxAfterAddingLeft_;
  util::geo::DBox boxAfterAddingRight_;
  size_t numElementsInSweeper_;
  size_t numElementsSkippedByPrefilter_;
  size_t numElementsAddedLeft_;
  size_t numElementsAddedRight_;

  // ___________________________________________________________________________
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

// Example data for the tests below
struct TestGeometry {
  std::string name_;
  std::string wkt_;
  bool isInGermany_;
};
const std::vector<TestGeometry> testGeometries{
    {"uni", areaUniFreiburg, true},
    {"minster", areaMuenster, true},
    {"gk-allee", lineSegmentGeorgesKoehlerAllee, true},
    {"london", areaLondonEye, false},
    {"lib", areaStatueOfLiberty, false},
    {"eiffel", areaEiffelTower, false},
    {"approx-de", approximatedAreaGermany, true},
    {"uni-separate", areaTFCampus, true},
    {"invalid", invalidWkt, false},
    {"cape-town", areaCapeTownStation, false},
};
constexpr std::string_view VAR_LEFT{"?geom1"};
constexpr std::string_view VAR_RIGHT{"?geom2"};

// Helper to generate different test datasets
inline std::string buildLibSJTestDataset(bool addApproxGermany = false,
                                         bool germanyDifferentPredicate = true,
                                         bool addSeparateUni = false,
                                         bool addInvalid = false,
                                         bool addCapeTown = false) {
  std::string kg;
  for (const auto& [name, wkt, isInGermany] : testGeometries) {
    if (addApproxGermany && name == "approx-de") {
      kg = absl::StrCat(kg, "<approx-de> <wkt-approx-de> ", wkt, " .\n");
    }
    if (addSeparateUni && name == "uni-separate") {
      kg = absl::StrCat(kg, "<uni-separate> <wkt-uni-separate> ", wkt, " .\n");
    }
    if (addInvalid && name == "invalid") {
      kg = absl::StrCat(kg, "<invalid> <wkt-invalid> ", wkt, " .\n");
    }
    if (name != "uni-separate" && name != "approx-de" && name != "invalid" &&
        (name != "cape-town" || addCapeTown)) {
      kg = absl::StrCat(
          kg, "<", name, "> <wkt-",
          (isInGermany && germanyDifferentPredicate ? "de" : "other"), "> ",
          wkt, " .\n");
    }
  }
  return kg;
}

// Holds the mappings produced by `resolveValIdTable`
struct ValIdTable {
  ValIdToGeomName vMap_;
  GeomNameToValId nMap_;

  // ___________________________________________________________________________
  void print() const {
    for (const auto& [a, b] : vMap_) {
      std::cout << " VMAP " << a << " " << b << std::endl;
    }
    for (const auto& [a, b] : nMap_) {
      std::cout << " NMAP " << a << " " << b << std::endl;
    }
  }
};

// Retrieve the `ValueId` for a given `name` from a `GeomNameToValId`.
inline ValueId getValId(const GeomNameToValId& nMap, std::string_view name) {
  auto valId = nMap.at(name);
  EXPECT_EQ(valId.getDatatype(), Datatype::VocabIndex);
  return valId;
}

// Helper to create a `ValIdTable` struct which maps `ValueId`s to names and
// names to `ValueId`s for the geometries in `testGeometries`.
inline ValIdTable resolveValIdTable(QueryExecutionContext* qec,
                                    size_t expectedSize,
                                    Loc loc = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(loc);
  ValIdToGeomName vMap;
  GeomNameToValId nMap;

  for (const auto& [name, wkt, isInGermany] : testGeometries) {
    VocabIndex idx;
    if (!qec->getIndex().getVocab().getId(wkt, &idx)) {
      // This literal is not contained in the index of the current `qec`
      continue;
    }

    auto vId = ValueId::makeFromVocabIndex(idx);
    vMap[vId] = name;
    nMap[name] = vId;
  }

  EXPECT_EQ(vMap.size(), expectedSize);
  EXPECT_EQ(nMap.size(), expectedSize);
  return {std::move(vMap), std::move(nMap)};
}

// Helper to construct the `SweeperCfg` configuration struct for
// `runParsingAndSweeper`
inline sj::SweeperCfg makeSweeperCfg(const LibSpatialJoinConfig& libSJConfig,
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

// Helpers to build index scans for `runParsingAndSweeper`
inline QET makeLeftChild(QEC qec, std::string_view pred) {
  return buildIndexScan(
      qec, {"?a", absl::StrCat("<wkt-", pred, ">"), std::string{VAR_LEFT}});
}
inline QET makeRightChild(QEC qec, std::string_view pred) {
  return buildIndexScan(
      qec, {"?b", absl::StrCat("<wkt-", pred, ">"), std::string{VAR_RIGHT}});
}

// Run a complete spatial join and record information into a `SweeperTestResult`
// struct, which can be compared against an expected result in the next step.
inline void runParsingAndSweeper(
    QEC qec, std::string_view leftPred, std::string_view rightPred,
    const LibSpatialJoinConfig& sjTask, SweeperTestResult& testResult,
    bool usePrefilter = true, bool checkPrefilterDeactivate = false,
    bool useRegularImplementation = false, Loc loc = AD_CURRENT_SOURCE_LOC()) {
  using V = Variable;
  auto l = generateLocationTrace(loc);

  // Children of spatial join
  auto leftChild = makeLeftChild(qec, leftPred);
  auto rightChild = makeRightChild(qec, rightPred);

  // Build spatial join operation
  V varLeft{std::string{VAR_LEFT}};
  V varRight{std::string{VAR_RIGHT}};
  SpatialJoinConfiguration config{sjTask, varLeft, varRight};
  config.algo_ = SpatialJoinAlgorithm::LIBSPATIALJOIN;
  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, config, leftChild,
                                                 rightChild);
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  // Build `SpatialJoinAlgorithms` instance from spatial join operation
  auto prepared = spatialJoin->onlyForTestingGetPrepareJoin();
  SpatialJoinAlgorithms sjAlgo{qec, prepared, config, spatialJoin};

  // The regular implementation can also be tested instead of this mock version,
  // but then only limited information is available.
  if (useRegularImplementation) {
    auto result = sjAlgo.LibspatialjoinAlgorithm();
    auto varToCol = spatialJoin->computeVariableToColumnMap();
    auto leftCol = varToCol.at(varLeft).columnIndex_;
    auto rightCol = varToCol.at(varRight).columnIndex_;
    auto resultNumRows = result.idTable().numRows();
    testResult = SweeperTestResult{};
    testResult.results_.reserve(resultNumRows);
    for (size_t i = 0; i < result.idTable().numRows(); i++) {
      testResult.results_.emplace_back(sjTask.joinType_,
                                       result.idTable().at(i, leftCol),
                                       result.idTable().at(i, rightCol), 0);
    }
    if (spatialJoin->runtimeInfo().details_.contains(
            "num-geoms-dropped-by-prefilter")) {
      testResult.numElementsSkippedByPrefilter_ =
          static_cast<size_t>(spatialJoin->runtimeInfo()
                                  .details_["num-geoms-dropped-by-prefilter"]);
    }
    return;
  }

  // Instantiate a libspatialjoin `Sweeper`
  SweeperResult results{{}};
  SweeperDistResult resultDists{{}};
  double withinDist = sjTask.maxDist_.value_or(-1);
  auto sweeperCfg = makeSweeperCfg(sjTask, results, resultDists, withinDist);
  std::string sweeperPath = qec->getIndex().getOnDiskBase() + ".spatialjoin";
  sj::Sweeper sweeper(sweeperCfg, ".", "", sweeperPath.c_str());

  ASSERT_EQ(sweeper.numElements(), 0);

  // Run first parsing step (left side)
  auto [aggBoundingBoxLeft, numGeomAddedLeft, numGeomDroppedLeft,
        numThreadsLeft] =
      sjAlgo.libspatialjoinParse(false,
                                 {prepared.idTableLeft_, prepared.leftJoinCol_},
                                 sweeper, 1, std::nullopt);
  // Due to problems in `Sweeper` when a side is empty, we don't use
  // `sweeper.setFilterBox(box);` here.

  // Run second parsing step (right side)
  std::optional<util::geo::I32Box> prefilterBox = std::nullopt;
  if (usePrefilter) {
    prefilterBox = sweeper.getPaddedBoundingBox(aggBoundingBoxLeft);
  }
  auto [aggBoundingBoxRight, numGeomAddedRight, numGeomDroppedRight,
        numThreadsRight] =
      sjAlgo.libspatialjoinParse(
          true, {prepared.idTableRight_, prepared.rightJoinCol_}, sweeper, 1,
          prefilterBox);

  sweeper.flush();

  // Check counters
  size_t numSkipped = numGeomDroppedLeft + numGeomDroppedRight;

  size_t numEl = sweeper.numElements();
  if (numGeomAddedLeft && numGeomAddedRight) {
    sweeper.sweep();
  }

  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(resultDists.size(), 1);

  if (sjTask.maxDist_.has_value()) {
    ASSERT_EQ(results.at(0).size(), resultDists.at(0).size());
  }

  // If the bounding box is very large, the prefiltering should be deactivated
  if (checkPrefilterDeactivate) {
    ASSERT_TRUE(spatialJoin->runtimeInfo().details_.contains(
        "prefilter-disabled-by-bounding-box-area"));
    ASSERT_TRUE(spatialJoin->runtimeInfo()
                    .details_["prefilter-disabled-by-bounding-box-area"]);
  }

  // Convert result from row numbers in `IdTable`s to `ValueId`s
  SweeperResultWithIds resultMatched;
  for (size_t row = 0; row < results.at(0).size(); row++) {
    double dist = 0;
    if (sjTask.maxDist_.has_value()) {
      dist = resultDists.at(0).at(row);
    }

    const auto [sjType, leftIdx, rightIdx] = results.at(0).at(row);
    auto valIdLeft = prepared.idTableLeft_->at(leftIdx, prepared.leftJoinCol_);
    auto valIdRight =
        prepared.idTableRight_->at(rightIdx, prepared.rightJoinCol_);

    resultMatched.emplace_back(sjType, valIdLeft, valIdRight, dist);
  }

  // Convert aggregated bounding boxes from web mercator int32 to lat/lng double
  auto boxLeftLatLng =
      ad_utility::detail::projectInt32WebMercToDoubleLatLng(aggBoundingBoxLeft);
  auto boxRightLatLng = ad_utility::detail::projectInt32WebMercToDoubleLatLng(
      aggBoundingBoxRight);

  // Write struct with all results of the test run
  testResult = {resultMatched, boxLeftLatLng,    boxRightLatLng,   numEl,
                numSkipped,    numGeomAddedLeft, numGeomAddedRight};
}

// Helper to approximately compare two prefilter boxes from
// `runParsingAndSweeper`
inline void checkPrefilterBox(const util::geo::DBox& actualLatLng,
                              const util::geo::DBox& expectedLatLng,
                              Loc loc = AD_CURRENT_SOURCE_LOC()) {
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

// Helper to approximately compare the results of `runParsingAndSweeper` with an
// expected result, both as `SweeperTestResult`
inline void checkSweeperTestResult(
    const ValIdToGeomName& vMap, const SweeperTestResult& actual,
    const SweeperTestResult& expected,
    std::optional<SpatialJoinType> checkOnlySjType = std::nullopt,
    bool checkPrefilterBoxes = false, Loc loc = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(loc);

  ad_utility::HashMap<GeoRelationWithIds, double> expectedResultsAndDist;

  auto checkValId = [&](ValueId valId, Loc loc = AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(loc);
    ASSERT_EQ(valId.getDatatype(), Datatype::VocabIndex);
    ASSERT_TRUE(vMap.contains(valId));
  };

  // Build a hash table of the expected rows
  for (const auto& [sjType, valIdLeft, valIdRight, dist] : expected.results_) {
    if (checkOnlySjType.has_value() && sjType != checkOnlySjType.value()) {
      continue;
    }
    checkValId(valIdLeft);
    checkValId(valIdRight);

    expectedResultsAndDist[{sjType, valIdLeft, valIdRight}] = dist;
  }

  // For every result row, lookup if it is contained in the hash table of
  // expected rows. Afterwards the number of rows is also compared, thus
  // achieving equivalence.
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

  // Compare counters
  ASSERT_EQ(actual.numElementsInSweeper_, expected.numElementsInSweeper_);
  ASSERT_EQ(actual.numElementsSkippedByPrefilter_,
            expected.numElementsSkippedByPrefilter_);
  ASSERT_EQ(actual.numElementsAddedLeft_, expected.numElementsAddedLeft_);
  ASSERT_EQ(actual.numElementsAddedRight_, expected.numElementsAddedRight_);

  // Compare aggregated bounding boxes
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

// Construct a bounding box for a list of geometries simply by computing the
// bounding box of a geometry collection with all geometries. Use to compute the
// expected bounding box after adding the geometries to `Sweeper`.
inline util::geo::DBox makeAggregatedBoundingBox(
    const std::vector<std::string>& wktGeometries) {
  std::vector<std::string> wktWithoutDatatype;
  for (const auto& geom : wktGeometries) {
    wktWithoutDatatype.push_back(ad_utility::detail::removeDatatype(geom));
  }
  auto aggregatedWkt = absl::StrCat("\"GEOMETRYCOLLECTION(",
                                    absl::StrJoin(wktWithoutDatatype, ", "),
                                    ")\"^^<", GEO_WKT_LITERAL, ">");
  auto boundingBox = ad_utility::GeometryInfo::getBoundingBox(aggregatedWkt);
  if (!boundingBox.has_value()) {
    throw std::runtime_error("Could not compute expected bounding box.");
  }
  return ad_utility::detail::boundingBoxToUtilBox(boundingBox.value());
}

const auto boundingBoxGermanPlaces = makeAggregatedBoundingBox(
    {areaMuenster, areaUniFreiburg, lineSegmentGeorgesKoehlerAllee});
const auto boundingBoxOtherPlaces = makeAggregatedBoundingBox(
    {areaLondonEye, areaEiffelTower, areaStatueOfLiberty});
const auto boundingBoxAllPlaces = makeAggregatedBoundingBox(
    {areaMuenster, areaUniFreiburg, lineSegmentGeorgesKoehlerAllee,
     areaLondonEye, areaStatueOfLiberty, areaEiffelTower});
const auto boundingBoxVeryLarge = makeAggregatedBoundingBox(
    {areaMuenster, areaUniFreiburg, lineSegmentGeorgesKoehlerAllee,
     areaLondonEye, areaStatueOfLiberty, areaEiffelTower, areaCapeTownStation});
const auto boundingBoxGermany =
    makeAggregatedBoundingBox({approximatedAreaGermany});
const auto boundingBoxUniAndLondon =
    makeAggregatedBoundingBox({areaUniFreiburg, areaLondonEye});
const auto boundingBoxUniSeparate = makeAggregatedBoundingBox({areaTFCampus});

}  // namespace SpatialJoinPrefilterTestHelpers

#endif  // QLEVER_TEST_ENGINE_SPATIALJOINPREFILTERTESTHELPERS_H

// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <absl/strings/str_join.h>
#include <gmock/gmock.h>
#include <spatialjoin/Sweeper.h>

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
#include "util/geo/Geo.h"

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
std::string buildLibSJTestDataset(bool addApproxGermany = false,
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
ValueId getValId(const GeomNameToValId& nMap, std::string_view name) {
  auto valId = nMap.at(name);
  EXPECT_EQ(valId.getDatatype(), Datatype::VocabIndex);
  return valId;
}

// Helper to create a `ValIdTable` struct which maps `ValueId`s to names and
// names to `ValueId`s for the geometries in `testGeometries`.
ValIdTable resolveValIdTable(QueryExecutionContext* qec, size_t expectedSize,
                             Loc loc = Loc::current()) {
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

// Helpers to build index scans for `runParsingAndSweeper`
QET makeLeftChild(QEC qec, std::string_view pred) {
  return buildIndexScan(
      qec, {"?a", absl::StrCat("<wkt-", pred, ">"), std::string{VAR_LEFT}});
}
QET makeRightChild(QEC qec, std::string_view pred) {
  return buildIndexScan(
      qec, {"?b", absl::StrCat("<wkt-", pred, ">"), std::string{VAR_RIGHT}});
}

// Run a complete spatial join and record information into a `SweeperTestResult`
// struct, which can be compared against an expected result in the next step.
void runParsingAndSweeper(
    QEC qec, std::string_view leftPred, std::string_view rightPred,
    const LibSpatialJoinConfig& sjTask, SweeperTestResult& testResult,
    bool usePrefilter = true, bool checkPrefilterDeactivate = false,
    bool useRegularImplementation = false, Loc loc = Loc::current()) {
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
  auto [aggBoundingBoxLeft, numGeomAddedLeft] = sjAlgo.libspatialjoinParse(
      false, {prepared.idTableLeft_, prepared.leftJoinCol_}, sweeper, 1,
      std::nullopt);
  // Due to problems in `Sweeper` when a side is empty, we don't use
  // `sweeper.setFilterBox(box);` here.

  // Run second parsing step (right side)
  std::optional<util::geo::I32Box> prefilterBox = std::nullopt;
  if (usePrefilter) {
    prefilterBox = sweeper.getPaddedBoundingBox(aggBoundingBoxLeft);
  }
  auto [aggBoundingBoxRight, numGeomAddedRight] = sjAlgo.libspatialjoinParse(
      true, {prepared.idTableRight_, prepared.rightJoinCol_}, sweeper, 1,
      prefilterBox);

  sweeper.flush();

  // Check counters
  size_t numSkipped = 0;
  ASSERT_EQ(spatialJoin->runtimeInfo().details_.contains(
                "num-geoms-dropped-by-prefilter"),
            usePrefilter);
  if (usePrefilter) {
    numSkipped = static_cast<size_t>(
        spatialJoin->runtimeInfo().details_["num-geoms-dropped-by-prefilter"]);
  }

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

// Helper to approximately compare the results of `runParsingAndSweeper` with an
// expected result, both as `SweeperTestResult`
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
util::geo::DBox makeAggregatedBoundingBox(
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

// _____________________________________________________________________________
namespace {

using namespace SpatialJoinPrefilterTestHelpers;
using enum SpatialJoinType;

// Each of the following tests creates a `QueryExecutionContext` on a
// `GeoVocabulary` which holds various literals carefully selected literals. It
// then performs a spatial join and examines the result as well as the
// prefiltering during the geometry parsing.

// _____________________________________________________________________________
TEST(SpatialJoinTest, BoundingBoxPrefilterNoIntersections) {
  // Case 1: No intersections
  // Left: Three geometries in Germany (3x Freiburg)
  // Right: Three geometries outside of Germany (London, Paris, New York)
  auto kg = buildLibSJTestDataset();
  auto qec = buildQec(kg, true);
  auto [vMap, nMap] = resolveValIdTable(qec, 6);

  // With prefilter: No results but one entire side gets filtered out by
  // bounding box
  SweeperTestResult testResult;
  runParsingAndSweeper(qec, "de", "other", {INTERSECTS}, testResult, true);
  checkSweeperTestResult(vMap, testResult,
                         {{}, boundingBoxGermanPlaces, {}, 3, 3, 3, 0},
                         INTERSECTS, true);

  // Without prefilter: No results but everything has to be checked
  SweeperTestResult testResultNoFilter;
  runParsingAndSweeper(qec, "de", "other", {INTERSECTS}, testResultNoFilter,
                       false);
  checkSweeperTestResult(
      vMap, testResultNoFilter,
      {{}, boundingBoxGermanPlaces, boundingBoxOtherPlaces, 6, 0, 3, 3},
      INTERSECTS, true);
}

// _____________________________________________________________________________
TEST(SpatialJoinTest, BoundingBoxPrefilterIntersectsCoversAndNonIntersects) {
  // Case 2: Intersections, coverage and non-intersection
  // Left: University Freiburg, Campus Faculty of Engineering
  // Right: Three geometries in Freiburg: Road through the campus (intersection,
  // but not contained), main building on campus (contained) and Freiburg
  // Minster (no intersection)
  auto kg = buildLibSJTestDataset(false, true, true);
  auto qec = buildQec(kg, true);

  auto [vMap, nMap] = resolveValIdTable(qec, 7);

  auto vIdCampus = getValId(nMap, "uni-separate");
  auto vIdGkAllee = getValId(nMap, "gk-allee");
  auto vIdUni = getValId(nMap, "uni");
  auto vIdMinster = getValId(nMap, "minster");

  // Intersects: Campus intersects road and main building
  SweeperTestResult testResultIntersects;
  runParsingAndSweeper(qec, "uni-separate", "de", {INTERSECTS},
                       testResultIntersects, true);
  checkSweeperTestResult(vMap, testResultIntersects,
                         {{{INTERSECTS, vIdCampus, vIdGkAllee, 0},
                           {INTERSECTS, vIdCampus, vIdUni, 0}},
                          {},
                          {},
                          3,
                          1,
                          1,
                          2},
                         INTERSECTS);

  // Contains: Campus contains main building
  SweeperTestResult testResultContains;
  runParsingAndSweeper(qec, "uni-separate", "de", {CONTAINS},
                       testResultContains, true);
  checkSweeperTestResult(vMap, testResultContains,
                         {{
                              {CONTAINS, vIdCampus, vIdUni, 0},
                          },
                          {},
                          {},
                          3,
                          1,
                          1,
                          2},
                         CONTAINS);

  // Within distance 5km: Minster satisfies this, s.t. all three geometries
  // from the right are expected to be returned.
  SweeperTestResult testResultWithinDist;
  runParsingAndSweeper(qec, "uni-separate", "de", {WITHIN_DIST, 5000},
                       testResultWithinDist, true);
  checkSweeperTestResult(vMap, testResultWithinDist,
                         {{{WITHIN_DIST, vIdCampus, vIdGkAllee, 0},
                           {WITHIN_DIST, vIdCampus, vIdUni, 0},
                           {WITHIN_DIST, vIdCampus, vIdMinster, 2225.01}},
                          {},
                          {},
                          4,
                          0,
                          1,
                          3},
                         WITHIN_DIST);
}

TEST(SpatialJoinTest, BoundingBoxPrefilterLargeContainsNotContains) {
  // Case 3: Large bounding box which contains half of the geometries
  // Left: Approximate boundary of Germany
  // Right: All other test geometries (3x in Freiburg, 1x in London, Paris and
  // New York)
  auto kg = buildLibSJTestDataset(true, false, false);
  auto qec = buildQec(kg, true);

  auto [vMap, nMap] = resolveValIdTable(qec, 7);

  auto vIdGermany = getValId(nMap, "approx-de");
  auto vIdMinster = getValId(nMap, "minster");
  auto vIdUni = getValId(nMap, "uni");
  auto vIdGkAllee = getValId(nMap, "gk-allee");
  auto vIdLondon = getValId(nMap, "london");
  auto vIdParis = getValId(nMap, "eiffel");

  // Intersects with prefiltering: Three geometries in Germany intersect, the
  // other three don't and can be excluded by prefiltering
  SweeperTestResult testResultIntersects;
  runParsingAndSweeper(qec, "approx-de", "other", {INTERSECTS},
                       testResultIntersects, true);
  const SweeperResultWithIds expectedResultIntersects{
      {INTERSECTS, vIdGermany, vIdMinster, 0},
      {INTERSECTS, vIdGermany, vIdUni, 0},
      {INTERSECTS, vIdGermany, vIdGkAllee, 0}};
  checkSweeperTestResult(vMap, testResultIntersects,
                         {expectedResultIntersects, boundingBoxGermany,
                          boundingBoxGermanPlaces, 4, 3, 1, 3},
                         INTERSECTS, true);

  // Intersects without prefiltering: Same result but all geometries are checked
  SweeperTestResult testResultNoFilter;
  runParsingAndSweeper(qec, "approx-de", "other", {INTERSECTS},
                       testResultNoFilter, false);
  checkSweeperTestResult(vMap, testResultNoFilter,
                         {expectedResultIntersects, boundingBoxGermany,
                          boundingBoxAllPlaces, 7, 0, 1, 6},
                         INTERSECTS, true);

  // Within distance of 1 000 km: London and Paris are outside of the bounding
  // box of the left side (Germany) but within the distance range, New York is
  // outside
  SweeperTestResult testResultWithinDist;
  runParsingAndSweeper(qec, "approx-de", "other", {WITHIN_DIST, 1'000'000},
                       testResultWithinDist, true);
  checkSweeperTestResult(vMap, testResultWithinDist,
                         {{{WITHIN_DIST, vIdGermany, vIdUni, 0},
                           {WITHIN_DIST, vIdGermany, vIdMinster, 0},
                           {WITHIN_DIST, vIdGermany, vIdGkAllee, 0},
                           {WITHIN_DIST, vIdGermany, vIdLondon, 426521.1497},
                           {WITHIN_DIST, vIdGermany, vIdParis, 314975.6311}},
                          {},
                          {},
                          6,
                          1,
                          1,
                          5},
                         WITHIN_DIST);
}

// _____________________________________________________________________________
TEST(SpatialJoinTest, BoundingBoxPrefilterDeactivatedTooLargeBox) {
  // Case 4: Very large bounding box, such that prefiltering is deactivated
  // automatically because it will likely not provide a performance gain
  // Left + Right: All geometries in Germany, France, UK, USA and South Africa
  auto kg = buildLibSJTestDataset(false, false, true, false, true);
  auto qec = buildQec(kg, true);
  auto [vMap, nMap] = resolveValIdTable(qec, 8);

  auto vIdUniSep = getValId(nMap, "uni-separate");
  auto vIdUni = getValId(nMap, "uni");
  auto vIdGkAllee = getValId(nMap, "gk-allee");

  {
    auto cleanUp =
        setRuntimeParameterForTest<"spatial-join-prefilter-max-size">(2'500);

    // Intersects with prefiltering, but prefiltering is not used due to too
    // large bounding box
    SweeperTestResult testResult;
    runParsingAndSweeper(qec, "other", "uni-separate", {INTERSECTS}, testResult,
                         true, true);
    checkSweeperTestResult(vMap, testResult,
                           {{{INTERSECTS, vIdUni, vIdUniSep, 0},
                             {INTERSECTS, vIdGkAllee, vIdUniSep, 0}},
                            boundingBoxVeryLarge,
                            boundingBoxUniSeparate,
                            8,
                            0,
                            7,
                            1},
                           INTERSECTS, true);
  }

  // Update runtime parameter for second test
  double bbSize = util::geo::area(boundingBoxVeryLarge);
  EXPECT_GT(bbSize, 2'500);
  EXPECT_LT(bbSize, 10'000);

  {
    auto cleanUp =
        setRuntimeParameterForTest<"spatial-join-prefilter-max-size">(10'000);

    // Using the custom max size of the prefilter box, prefiltering should now
    // be used again.
    SweeperTestResult testResultCustomMax;
    runParsingAndSweeper(qec, "other", "uni-separate", {INTERSECTS},
                         testResultCustomMax, true, false);
    checkSweeperTestResult(vMap, testResultCustomMax,
                           {{{INTERSECTS, vIdUni, vIdUniSep, 0},
                             {INTERSECTS, vIdGkAllee, vIdUniSep, 0}},
                            boundingBoxVeryLarge,
                            boundingBoxUniSeparate,
                            8,
                            0,
                            7,
                            1},
                           INTERSECTS, true);
  }
}

// _____________________________________________________________________________
TEST(SpatialJoinTest, BoundingBoxPrefilterRegularImplementation) {
  // Case 5: Test that the regular implementation `LibspatialjoinAlgorithm()`
  // instead of the test mock version calls the parsing and prefiltering
  // correctly
  // Left: All other test geometries (3x in Freiburg, 1x in London, Paris and
  // New York)
  // Right: Approximate boundary of Germany
  auto kg = buildLibSJTestDataset(true, false, false);
  auto qec = buildQec(kg, true);

  auto [vMap, nMap] = resolveValIdTable(qec, 7);
  auto vIdGermany = getValId(nMap, "approx-de");
  auto vIdUni = getValId(nMap, "uni");
  auto vIdGkAllee = getValId(nMap, "gk-allee");
  auto vIdMinster = getValId(nMap, "minster");

  // Within search: Geometry inside of Germany
  SweeperTestResult testResultRegularImpl;
  runParsingAndSweeper(qec, "other", "approx-de", {WITHIN},
                       testResultRegularImpl, true, false, true);
  // Here we can only check the results and the number of geometries skipped by
  // prefilter, because we are not using the mock algorithm which captures the
  // other information.
  checkSweeperTestResult(vMap, testResultRegularImpl,
                         {{
                              {WITHIN, vIdUni, vIdGermany, 0},
                              {WITHIN, vIdGkAllee, vIdGermany, 0},
                              {WITHIN, vIdMinster, vIdGermany, 0},
                          },
                          {},
                          {},
                          0,
                          3,
                          0,
                          0});

  // One child is an empty index scan
  SweeperTestResult testResultEmpty;
  runParsingAndSweeper(qec, "does-not-exist", "approx-de", {INTERSECTS},
                       testResultEmpty, true, false, true);
  checkSweeperTestResult(vMap, testResultEmpty, {{}, {}, {}, 0, 0, 0, 0});
}

// Test for other utility functions related to geometry prefiltering

// _____________________________________________________________________________
TEST(SpatialJoinTest, prefilterGeoByBoundingBox) {
  auto kg = buildLibSJTestDataset(true, false, false, true);
  auto qec = buildQec(kg, true);
  const auto& index = qec->getIndex();

  auto [vMap, nMap] = resolveValIdTable(qec, 8);

  auto idxUni = getValId(nMap, "uni").getVocabIndex();
  auto idxLondon = getValId(nMap, "london").getVocabIndex();
  auto idxNewYork = getValId(nMap, "lib").getVocabIndex();
  auto idxInvalid = getValId(nMap, "invalid").getVocabIndex();

  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxGermany, index, idxUni));
  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxGermany, index, idxLondon));
  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxGermany, index, idxNewYork));

  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxUniAndLondon, index, idxUni));
  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxUniAndLondon, index, idxLondon));
  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxUniAndLondon, index, idxNewYork));

  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxOtherPlaces, index, idxUni));

  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxUniAndLondon, index, idxInvalid));
  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxGermany, index, idxInvalid));
  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxOtherPlaces, index, idxInvalid));

  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(std::nullopt,
                                                                index, idxUni));
  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      std::nullopt, index, idxLondon));
  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      std::nullopt, index, idxNewYork));
  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      std::nullopt, index, idxInvalid));
}

}  // namespace

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>

#include <cstdlib>
#include <fstream>
#include <regex>
#include <variant>

#include "../util/IndexTestHelpers.h"
#include "./../../src/util/GeoSparqlHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/IndexScan.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/SpatialJoinAlgorithms.h"
#include "parser/data/Variable.h"

namespace {  // anonymous namespace to avoid linker problems

using namespace ad_utility::testing;
using namespace SpatialJoinTestHelpers;

// Shortcut for SpatialJoin task parameters
using SJ =
    std::variant<NearestNeighborsConfig, MaxDistanceConfig, SpatialJoinConfig>;

namespace computeResultTest {

// Represents from left to right: the algorithm, addLeftChildFirst,
// bigChildLeft, a spatial join task and if areas (=true) or points (=false)
// should be used
using SpatialJoinTestParam =
    std::tuple<SpatialJoinAlgorithm, bool, bool, SpatialJoinTask, bool>;

using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

class SpatialJoinParamTest
    : public ::testing::TestWithParam<SpatialJoinTestParam> {
 public:
  void createAndTestSpatialJoin(QueryExecutionContext* qec, Variable left,
                                SJ task, Variable right,
                                std::shared_ptr<QueryExecutionTree> leftChild,
                                std::shared_ptr<QueryExecutionTree> rightChild,
                                bool addLeftChildFirst,
                                Rows expectedOutputUnorderedRows,
                                Row columnNames,
                                bool isWrongPointInputTest = false) {
    // this function is like transposing a matrix. An entry which has been
    // stored at (i, k) is now stored at (k, i). The reason this is needed is
    // the following: this function receives the input as a vector of vector of
    // strings. Each vector contains a vector, which contains a row of the
    // result. This row contains all columns. After swapping, each vector
    // contains a vector, which contains all entries of one column. As now each
    // of the vectors contain only one column, we can later order them according
    // to the variable to column map and then compare the result.
    auto swapColumns = [&](Rows toBeSwapped) {
      Rows result;
      bool firstIteration = true;
      for (size_t i = 0; i < toBeSwapped.size(); i++) {
        for (size_t k = 0; k < toBeSwapped.at(i).size(); k++) {
          if (firstIteration) {
            result.push_back(Row{toBeSwapped.at(i).at(k)});
          } else {
            result.at(k).push_back(toBeSwapped.at(i).at(k));
          }
        }
        firstIteration = false;
      }
      return result;
    };

    std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(
            qec, SpatialJoinConfiguration{task, left, right}, std::nullopt,
            std::nullopt);

    // add first child
    std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
    SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
    auto firstChild = addLeftChildFirst ? leftChild : rightChild;
    auto secondChild = addLeftChildFirst ? rightChild : leftChild;
    Variable firstVariable = addLeftChildFirst ? left : right;
    Variable secondVariable = addLeftChildFirst ? right : left;

    auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
    spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());

    // add second child
    auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
    spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

    // prepare expected output
    // swap rows and columns to use the function orderColAccordingToVarColMap
    auto expectedMaxDistCols = swapColumns(expectedOutputUnorderedRows);
    auto expectedOutputOrdered =
        orderColAccordingToVarColMap(spatialJoin->computeVariableToColumnMap(),
                                     expectedMaxDistCols, columnNames);
    auto expectedOutput =
        createRowVectorFromColumnVector(expectedOutputOrdered);

    // Select algorithm
    spatialJoin->selectAlgorithm(std::get<0>(GetParam()));

    // At worst quadratic time
    ASSERT_LE(spatialJoin->getCostEstimate(),
              (firstChild->getSizeEstimate() * secondChild->getSizeEstimate()) +
                  firstChild->getCostEstimate() +
                  secondChild->getCostEstimate() + 1);

    // The cost of the child operations needs to be included in the cost
    // estimate of the spatial join (which is based on the childrens' size not
    // cost estimate)
    ASSERT_GE(spatialJoin->getCostEstimate(),
              firstChild->getCostEstimate() + secondChild->getCostEstimate());

    auto res = spatialJoin->computeResult(false);
    auto vec = printTable(qec, &res);
    /*
    for (size_t i = 0; i < vec.size(); ++i) {
      EXPECT_STREQ(vec.at(i).c_str(), expectedOutput.at(i).c_str());
    }*/
    EXPECT_THAT(vec, ::testing::UnorderedElementsAreArray(expectedOutput));

    if (isWrongPointInputTest &&
        std::get<0>(GetParam()) == SpatialJoinAlgorithm::BOUNDING_BOX) {
      auto warnings = spatialJoin->collectWarnings();
      bool containsWrongPointWarning = false;
      std::string warningMessage =
          "The input to a spatial join contained at least one element, "
          "that is not a Point, Linestring, Polygon, MultiPoint, "
          "MultiLinestring or MultiPolygon geometry and is thus skipped. Note "
          "that QLever currently only accepts those geometries for "
          "the spatial joins";
      for (const auto& warning : warnings) {
        if (warning == warningMessage) {
          containsWrongPointWarning = true;
        }
      }
      ASSERT_TRUE(containsWrongPointWarning);
    }
  }

  // build the test using the small dataset. Let the SpatialJoin operation be
  // the last one (the left and right child are maximally large for this test
  // query) the following Query will be simulated, the max distance will be
  // different depending on the test: Select * where {
  //   ?obj1 <name> ?name1 .
  //   ?obj1 <hasGeometry> ?geo1 .
  //   ?geo1 <asWKT> ?point1
  //   ?obj2 <name> ?name2 .
  //   ?obj2 <hasGeometry> ?geo2 .
  //   ?geo2 <asWKT> ?point2
  //   ?point1 <max-distance-in-meters:XXXX> ?point2 .
  // }
  void buildAndTestSmallTestSetLargeChildren(SJ task, bool addLeftChildFirst,
                                             Rows expectedOutput,
                                             Row columnNames) {
    auto qec = buildTestQEC(std::get<4>(GetParam()));
    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);
    // ===================== build the first child
    // ===============================
    auto leftChild = buildMediumChild(
        qec, {"?obj1", std::string{"<name>"}, "?name1"},
        {"?obj1", std::string{"<hasGeometry>"}, "?geo1"},
        {"?geo1", std::string{"<asWKT>"}, "?point1"}, "?obj1", "?geo1");

    // ======================= build the second child
    // ============================
    auto rightChild = buildMediumChild(
        qec, {"?obj2", std::string{"<name>"}, "?name2"},
        {"?obj2", std::string{"<hasGeometry>"}, "?geo2"},
        {"?geo2", std::string{"<asWKT>"}, "?point2"}, "?obj2", "?geo2");

    createAndTestSpatialJoin(qec, Variable{"?point1"}, task,
                             Variable{"?point2"}, leftChild, rightChild,
                             addLeftChildFirst, expectedOutput, columnNames);
  }

  // build the test using the small dataset. Let the SpatialJoin operation.
  // The following Query will be simulated, the max distance will be different
  // depending on the test:
  // Select * where {
  //   ?geo1 <asWKT> ?point1
  //   ?geo2 <asWKT> ?point2
  //   ?point1 <max-distance-in-meters:XXXX> ?point2 .
  // }
  void buildAndTestSmallTestSetSmallChildren(SJ task, bool addLeftChildFirst,
                                             Rows expectedOutput,
                                             Row columnNames) {
    auto qec = buildTestQEC(std::get<4>(GetParam()));
    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);
    // ====================== build inputs ===================================
    auto leftChild =
        buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
    auto rightChild =
        buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

    createAndTestSpatialJoin(qec, Variable{"?point1"}, task,
                             Variable{"?point2"}, leftChild, rightChild,
                             addLeftChildFirst, expectedOutput, columnNames);
  }

  // build the test using the small dataset. Let the SpatialJoin operation be
  // the last one. the following Query will be simulated, the max distance will
  // be different depending on the test: Select * where {
  //   ?obj1 <name> ?name1 .
  //   ?obj1 <hasGeometry> ?geo1 .
  //   ?geo1 <asWKT> ?point1
  //   ?geo2 <asWKT> ?point2
  //   ?point1 <max-distance-in-meters:XXXX> ?point2 .
  // }
  void buildAndTestSmallTestSetDiffSizeChildren(SJ task, bool addLeftChildFirst,
                                                Rows expectedOutput,
                                                Row columnNames,
                                                bool bigChildLeft) {
    auto qec = buildTestQEC(std::get<4>(GetParam()));
    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);
    // ========================= build big child =============================
    auto bigChild = buildMediumChild(
        qec, {"?obj1", std::string{"<name>"}, "?name1"},
        {"?obj1", std::string{"<hasGeometry>"}, "?geo1"},
        {"?geo1", std::string{"<asWKT>"}, "?point1"}, "?obj1", "?geo1");

    // ========================= build small child
    // ===============================
    Variable point2{"?point2"};
    auto smallChild =
        buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

    auto firstChild = bigChildLeft ? bigChild : smallChild;
    auto secondChild = bigChildLeft ? smallChild : bigChild;
    auto firstVariable = bigChildLeft ? Variable{"?point1"} : point2;
    auto secondVariable = bigChildLeft ? point2 : Variable{"?point1"};

    createAndTestSpatialJoin(qec, firstVariable, task, secondVariable,
                             firstChild, secondChild, addLeftChildFirst,
                             expectedOutput, columnNames);
  }

  void testDiffSizeIdTables(SJ task, bool addLeftChildFirst,
                            Rows expectedOutput, Row columnNames,
                            bool bigChildLeft) {
    auto qec = buildTestQEC(std::get<4>(GetParam()));
    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);
    // ====================== build small input ==============================
    std::string geometry =
        std::get<4>(GetParam()) ? "<geometryArea1>" : "<geometry1>";
    TripleComponent point1{Variable{"?point1"}};
    TripleComponent subject{
        ad_utility::triple_component::Iri::fromIriref(geometry)};
    auto smallChild = ad_utility::makeExecutionTree<IndexScan>(
        qec, Permutation::Enum::PSO,
        SparqlTripleSimple{subject, TripleComponent::Iri::fromIriref("<asWKT>"),
                           point1});
    // ====================== build big input ================================
    auto bigChild =
        buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

    auto firstChild = bigChildLeft ? bigChild : smallChild;
    auto secondChild = bigChildLeft ? smallChild : bigChild;
    auto firstVariable =
        bigChildLeft ? Variable{"?point2"} : point1.getVariable();
    auto secondVariable =
        bigChildLeft ? point1.getVariable() : Variable{"?point2"};

    createAndTestSpatialJoin(qec, firstVariable, task, secondVariable,
                             firstChild, secondChild, addLeftChildFirst,
                             expectedOutput, columnNames);
  }

  void testWrongPointInInput(SJ task, bool addLeftChildFirst,
                             Rows expectedOutput, Row columnNames) {
    auto kg = createSmallDataset();
    // make first point wrong:
    auto pos = kg.find("POINT(");
    kg = kg.insert(pos + 7, "wrongStuff");

    auto qec = buildQec(kg);
    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);
    // ====================== build inputs ================================
    Variable point1{"?point1"};
    Variable point2{"?point2"};
    auto leftChild =
        buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
    auto rightChild =
        buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

    createAndTestSpatialJoin(qec, point1, task, point2, leftChild, rightChild,
                             addLeftChildFirst, expectedOutput, columnNames,
                             true);
  }

  std::optional<MaxDistanceConfig> getMaxDist() {
    auto task = std::get<3>(GetParam());

    if (std::holds_alternative<MaxDistanceConfig>(task)) {
      return std::get<MaxDistanceConfig>(task);
    }
    return std::nullopt;
  }

  std::optional<NearestNeighborsConfig> getNearestNeighbors() {
    auto task = std::get<3>(GetParam());

    if (std::holds_alternative<NearestNeighborsConfig>(task)) {
      return std::get<NearestNeighborsConfig>(task);
    }
    return std::nullopt;
  }

  Row mergeToRow(Row part1, Row part2, Row part3) {
    Row result = part1;
    for (size_t i = 0; i < part2.size(); i++) {
      result.push_back(part2.at(i));
    }
    for (size_t i = 0; i < part3.size(); i++) {
      result.push_back(part3.at(i));
    }
    return result;
  };

  std::string name1 = (std::get<4>(GetParam())) ? "\"Uni Freiburg TF Area\""
                                                : "\"Uni Freiburg TF\"";
  std::string name2 = (std::get<4>(GetParam())) ? "\"Minster Freiburg Area\""
                                                : "\"Minster Freiburg\"";
  std::string name3 =
      (std::get<4>(GetParam())) ? "\"London Eye Area\"" : "\"London Eye\"";
  std::string name4 = (std::get<4>(GetParam())) ? "\"Statue of liberty Area\""
                                                : "\"Statue of liberty\"";
  std::string name5 =
      (std::get<4>(GetParam())) ? "\"eiffel tower Area\"" : "\"eiffel tower\"";

  std::string node1 = (std::get<4>(GetParam())) ? "<nodeArea_1>" : "<node_1>";
  std::string node2 = (std::get<4>(GetParam())) ? "<nodeArea_2>" : "<node_2>";
  std::string node3 = (std::get<4>(GetParam())) ? "<nodeArea_3>" : "<node_3>";
  std::string node4 = (std::get<4>(GetParam())) ? "<nodeArea_4>" : "<node_4>";
  std::string node5 = (std::get<4>(GetParam())) ? "<nodeArea_5>" : "<node_5>";

  std::string geometry1 =
      (std::get<4>(GetParam())) ? "<geometryArea1>" : "<geometry1>";
  std::string geometry2 =
      (std::get<4>(GetParam())) ? "<geometryArea2>" : "<geometry2>";
  std::string geometry3 =
      (std::get<4>(GetParam())) ? "<geometryArea3>" : "<geometry3>";
  std::string geometry4 =
      (std::get<4>(GetParam())) ? "<geometryArea4>" : "<geometry4>";
  std::string geometry5 =
      (std::get<4>(GetParam())) ? "<geometryArea5>" : "<geometry5>";

  std::string wktString1 = (std::get<4>(GetParam()))
                               ? SpatialJoinTestHelpers::areaUniFreiburg
                               : "POINT(7.835050 48.012670)";
  std::string wktString2 = (std::get<4>(GetParam()))
                               ? SpatialJoinTestHelpers::areaMuenster
                               : "POINT(7.852980 47.995570)";
  std::string wktString3 = (std::get<4>(GetParam()))
                               ? SpatialJoinTestHelpers::areaLondonEye
                               : "POINT(-0.119570 51.503330)";
  std::string wktString4 = (std::get<4>(GetParam()))
                               ? SpatialJoinTestHelpers::areaStatueOfLiberty
                               : "POINT(-74.044540 40.689250)";
  std::string wktString5 = (std::get<4>(GetParam()))
                               ? SpatialJoinTestHelpers::areaEiffelTower
                               : "POINT(2.294510 48.858250)";

  Rows unordered_rows{{name1, node1, geometry1, wktString1},
                      {name2, node2, geometry2, wktString2},
                      {name3, node3, geometry3, wktString3},
                      {name4, node4, geometry4, wktString4},
                      {name5, node5, geometry5, wktString5}};

  // Shortcuts
  Row TF = unordered_rows.at(0);
  Row Mun = unordered_rows.at(1);
  Row Eye = unordered_rows.at(2);
  Row Lib = unordered_rows.at(3);
  Row Eif = unordered_rows.at(4);

  Rows unordered_rows_small{{geometry1, wktString1},
                            {geometry2, wktString2},
                            {geometry3, wktString3},
                            {geometry4, wktString4},
                            {geometry5, wktString5}};

  // Shortcuts
  Row sTF = unordered_rows_small.at(0);
  Row sMun = unordered_rows_small.at(1);
  Row sEye = unordered_rows_small.at(2);
  Row sLib = unordered_rows_small.at(3);
  Row sEif = unordered_rows_small.at(4);

  // in all calculations below, the factor 1000 is used to convert from km to m

  // distance from the object to itself should be zero
  Row expectedDistSelf{"0"};

  // helper functions
  GeoPoint P(double x, double y) { return GeoPoint(y, x); }

  std::string expectedDist(const GeoPoint& p1, const GeoPoint& p2) {
    auto p1_ = S2Point{S2LatLng::FromDegrees(p1.getLat(), p1.getLng())};
    auto p2_ = S2Point{S2LatLng::FromDegrees(p2.getLat(), p2.getLng())};

    return std::to_string(S2Earth::ToKm(S1Angle(p1_, p2_)));
  }

  // Places for testing
  GeoPoint PUni = P(7.83505, 48.01267);
  GeoPoint PMun = P(7.85298, 47.99557);
  GeoPoint PEif = P(2.29451, 48.85825);
  GeoPoint PEye = P(-0.11957, 51.50333);
  GeoPoint PLib = P(-74.04454, 40.68925);
  std::vector<GeoPoint> testPlaces = std::vector{PUni, PMun, PEif, PEye, PLib};

  // distance from Uni Freiburg to Freiburger Münster is 2,33 km according to
  // google maps
  Row expectedDistUniMun{expectedDist(PUni, PMun)};

  // distance from Uni Freiburg to Eiffel Tower is 419,32 km according to
  // google maps
  Row expectedDistUniEif{expectedDist(PUni, PEif)};

  // distance from Minster Freiburg to eiffel tower is 421,09 km according to
  // google maps
  Row expectedDistMunEif{expectedDist(PMun, PEif)};

  // distance from london eye to eiffel tower is 340,62 km according to
  // google maps
  Row expectedDistEyeEif{expectedDist(PEye, PEif)};

  // distance from Uni Freiburg to London Eye is 690,18 km according to
  // google maps
  Row expectedDistUniEye{expectedDist(PUni, PEye)};

  // distance from Minster Freiburg to London Eye is 692,39 km according to
  // google maps
  Row expectedDistMunEye{expectedDist(PMun, PEye)};

  // distance from Uni Freiburg to Statue of Liberty is 6249,55 km according to
  // google maps
  Row expectedDistUniLib{expectedDist(PUni, PLib)};

  // distance from Minster Freiburg to Statue of Liberty is 6251,58 km
  // according to google maps
  Row expectedDistMunLib{expectedDist(PMun, PLib)};

  // distance from london eye to statue of liberty is 5575,08 km according to
  // google maps
  Row expectedDistEyeLib{expectedDist(PEye, PLib)};

  // distance from eiffel tower to Statue of liberty is 5837,42 km according to
  // google maps
  Row expectedDistEifLib{expectedDist(PEif, PLib)};

  using ExpectedRowsMaxDist = std::unordered_map<size_t, Rows>;

  ExpectedRowsMaxDist expectedMaxDistRows = {
      {1,
       {mergeToRow(TF, TF, expectedDistSelf),
        mergeToRow(Mun, Mun, expectedDistSelf),
        mergeToRow(Eye, Eye, expectedDistSelf),
        mergeToRow(Lib, Lib, expectedDistSelf),
        mergeToRow(Eif, Eif, expectedDistSelf)}},
      {5000,
       {mergeToRow(TF, TF, expectedDistSelf),
        mergeToRow(TF, Mun, expectedDistUniMun),
        mergeToRow(Mun, Mun, expectedDistSelf),
        mergeToRow(Mun, TF, expectedDistUniMun),
        mergeToRow(Eye, Eye, expectedDistSelf),
        mergeToRow(Lib, Lib, expectedDistSelf),
        mergeToRow(Eif, Eif, expectedDistSelf)}},
      {500000,
       {mergeToRow(TF, TF, expectedDistSelf),
        mergeToRow(TF, Mun, expectedDistUniMun),
        mergeToRow(TF, Eif, expectedDistUniEif),
        mergeToRow(Mun, Mun, expectedDistSelf),
        mergeToRow(Mun, TF, expectedDistUniMun),
        mergeToRow(Mun, Eif, expectedDistMunEif),
        mergeToRow(Eye, Eye, expectedDistSelf),
        mergeToRow(Eye, Eif, expectedDistEyeEif),
        mergeToRow(Lib, Lib, expectedDistSelf),
        mergeToRow(Eif, Eif, expectedDistSelf),
        mergeToRow(Eif, TF, expectedDistUniEif),
        mergeToRow(Eif, Mun, expectedDistMunEif),
        mergeToRow(Eif, Eye, expectedDistEyeEif)}},
      {1000000,
       {mergeToRow(TF, TF, expectedDistSelf),
        mergeToRow(TF, Mun, expectedDistUniMun),
        mergeToRow(TF, Eif, expectedDistUniEif),
        mergeToRow(TF, Eye, expectedDistUniEye),
        mergeToRow(Mun, Mun, expectedDistSelf),
        mergeToRow(Mun, TF, expectedDistUniMun),
        mergeToRow(Mun, Eif, expectedDistMunEif),
        mergeToRow(Mun, Eye, expectedDistMunEye),
        mergeToRow(Eye, Eye, expectedDistSelf),
        mergeToRow(Eye, Eif, expectedDistEyeEif),
        mergeToRow(Eye, TF, expectedDistUniEye),
        mergeToRow(Eye, Mun, expectedDistMunEye),
        mergeToRow(Lib, Lib, expectedDistSelf),
        mergeToRow(Eif, Eif, expectedDistSelf),
        mergeToRow(Eif, TF, expectedDistUniEif),
        mergeToRow(Eif, Mun, expectedDistMunEif),
        mergeToRow(Eif, Eye, expectedDistEyeEif)}},
      {10000000,
       {mergeToRow(TF, TF, expectedDistSelf),
        mergeToRow(TF, Mun, expectedDistUniMun),
        mergeToRow(TF, Eif, expectedDistUniEif),
        mergeToRow(TF, Eye, expectedDistUniEye),
        mergeToRow(TF, Lib, expectedDistUniLib),
        mergeToRow(Mun, Mun, expectedDistSelf),
        mergeToRow(Mun, TF, expectedDistUniMun),
        mergeToRow(Mun, Eif, expectedDistMunEif),
        mergeToRow(Mun, Eye, expectedDistMunEye),
        mergeToRow(Mun, Lib, expectedDistMunLib),
        mergeToRow(Eye, Eye, expectedDistSelf),
        mergeToRow(Eye, Eif, expectedDistEyeEif),
        mergeToRow(Eye, TF, expectedDistUniEye),
        mergeToRow(Eye, Mun, expectedDistMunEye),
        mergeToRow(Eye, Lib, expectedDistEyeLib),
        mergeToRow(Lib, Lib, expectedDistSelf),
        mergeToRow(Lib, TF, expectedDistUniLib),
        mergeToRow(Lib, Mun, expectedDistMunLib),
        mergeToRow(Lib, Eye, expectedDistEyeLib),
        mergeToRow(Lib, Eif, expectedDistEifLib),
        mergeToRow(Eif, Eif, expectedDistSelf),
        mergeToRow(Eif, TF, expectedDistUniEif),
        mergeToRow(Eif, Mun, expectedDistMunEif),
        mergeToRow(Eif, Eye, expectedDistEyeEif),
        mergeToRow(Eif, Lib, expectedDistEifLib)}}};

  ExpectedRowsMaxDist expectedMaxDistRowsSmall = {
      {1,
       {
           mergeToRow(sTF, sTF, expectedDistSelf),
           mergeToRow(sMun, sMun, expectedDistSelf),
           mergeToRow(sEye, sEye, expectedDistSelf),
           mergeToRow(sLib, sLib, expectedDistSelf),
           mergeToRow(sEif, sEif, expectedDistSelf),
       }},
      {5000,
       {mergeToRow(sTF, sTF, expectedDistSelf),
        mergeToRow(sTF, sMun, expectedDistUniMun),
        mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sMun, sTF, expectedDistUniMun),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sEif, sEif, expectedDistSelf)}},
      {500000,
       {mergeToRow(sTF, sTF, expectedDistSelf),
        mergeToRow(sTF, sMun, expectedDistUniMun),
        mergeToRow(sTF, sEif, expectedDistUniEif),
        mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sMun, sTF, expectedDistUniMun),
        mergeToRow(sMun, sEif, expectedDistMunEif),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sEye, sEif, expectedDistEyeEif),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sEif, sEif, expectedDistSelf),
        mergeToRow(sEif, sTF, expectedDistUniEif),
        mergeToRow(sEif, sMun, expectedDistMunEif),
        mergeToRow(sEif, sEye, expectedDistEyeEif)}},
      {1000000,
       {mergeToRow(sTF, sTF, expectedDistSelf),
        mergeToRow(sTF, sMun, expectedDistUniMun),
        mergeToRow(sTF, sEif, expectedDistUniEif),
        mergeToRow(sTF, sEye, expectedDistUniEye),
        mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sMun, sTF, expectedDistUniMun),
        mergeToRow(sMun, sEif, expectedDistMunEif),
        mergeToRow(sMun, sEye, expectedDistMunEye),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sEye, sEif, expectedDistEyeEif),
        mergeToRow(sEye, sTF, expectedDistUniEye),
        mergeToRow(sEye, sMun, expectedDistMunEye),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sEif, sEif, expectedDistSelf),
        mergeToRow(sEif, sTF, expectedDistUniEif),
        mergeToRow(sEif, sMun, expectedDistMunEif),
        mergeToRow(sEif, sEye, expectedDistEyeEif)}},
      {10000000,
       {mergeToRow(sTF, sTF, expectedDistSelf),
        mergeToRow(sTF, sMun, expectedDistUniMun),
        mergeToRow(sTF, sEif, expectedDistUniEif),
        mergeToRow(sTF, sEye, expectedDistUniEye),
        mergeToRow(sTF, sLib, expectedDistUniLib),
        mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sMun, sTF, expectedDistUniMun),
        mergeToRow(sMun, sEif, expectedDistMunEif),
        mergeToRow(sMun, sEye, expectedDistMunEye),
        mergeToRow(sMun, sLib, expectedDistMunLib),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sEye, sEif, expectedDistEyeEif),
        mergeToRow(sEye, sTF, expectedDistUniEye),
        mergeToRow(sEye, sMun, expectedDistMunEye),
        mergeToRow(sEye, sLib, expectedDistEyeLib),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sLib, sTF, expectedDistUniLib),
        mergeToRow(sLib, sMun, expectedDistMunLib),
        mergeToRow(sLib, sEye, expectedDistEyeLib),
        mergeToRow(sLib, sEif, expectedDistEifLib),
        mergeToRow(sEif, sEif, expectedDistSelf),
        mergeToRow(sEif, sTF, expectedDistUniEif),
        mergeToRow(sEif, sMun, expectedDistMunEif),
        mergeToRow(sEif, sEye, expectedDistEyeEif),
        mergeToRow(sEif, sLib, expectedDistEifLib)}}};

  ExpectedRowsMaxDist expectedMaxDistRowsSmallWrongPoint = {
      {1,
       {
           mergeToRow(sMun, sMun, expectedDistSelf),
           mergeToRow(sEye, sEye, expectedDistSelf),
           mergeToRow(sLib, sLib, expectedDistSelf),
           mergeToRow(sEif, sEif, expectedDistSelf),
       }},
      {5000,
       {mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sEif, sEif, expectedDistSelf)}},
      {500000,
       {mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sMun, sEif, expectedDistMunEif),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sEye, sEif, expectedDistEyeEif),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sEif, sEif, expectedDistSelf),
        mergeToRow(sEif, sMun, expectedDistMunEif),
        mergeToRow(sEif, sEye, expectedDistEyeEif)}},
      {1000000,
       {mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sMun, sEif, expectedDistMunEif),
        mergeToRow(sMun, sEye, expectedDistMunEye),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sEye, sEif, expectedDistEyeEif),
        mergeToRow(sEye, sMun, expectedDistMunEye),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sEif, sEif, expectedDistSelf),
        mergeToRow(sEif, sMun, expectedDistMunEif),
        mergeToRow(sEif, sEye, expectedDistEyeEif)}},
      {10000000,
       {mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sMun, sEif, expectedDistMunEif),
        mergeToRow(sMun, sEye, expectedDistMunEye),
        mergeToRow(sMun, sLib, expectedDistMunLib),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sEye, sEif, expectedDistEyeEif),
        mergeToRow(sEye, sMun, expectedDistMunEye),
        mergeToRow(sEye, sLib, expectedDistEyeLib),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sLib, sMun, expectedDistMunLib),
        mergeToRow(sLib, sEye, expectedDistEyeLib),
        mergeToRow(sLib, sEif, expectedDistEifLib),
        mergeToRow(sEif, sEif, expectedDistSelf),
        mergeToRow(sEif, sMun, expectedDistMunEif),
        mergeToRow(sEif, sEye, expectedDistEyeEif),
        mergeToRow(sEif, sLib, expectedDistEifLib)}}};

  ExpectedRowsMaxDist expectedMaxDistRowsDiff = {
      {1,
       {mergeToRow(TF, sTF, expectedDistSelf),
        mergeToRow(Mun, sMun, expectedDistSelf),
        mergeToRow(Eye, sEye, expectedDistSelf),
        mergeToRow(Lib, sLib, expectedDistSelf),
        mergeToRow(Eif, sEif, expectedDistSelf)}},
      {5000,
       {mergeToRow(TF, sTF, expectedDistSelf),
        mergeToRow(TF, sMun, expectedDistUniMun),
        mergeToRow(Mun, sMun, expectedDistSelf),
        mergeToRow(Mun, sTF, expectedDistUniMun),
        mergeToRow(Eye, sEye, expectedDistSelf),
        mergeToRow(Lib, sLib, expectedDistSelf),
        mergeToRow(Eif, sEif, expectedDistSelf)}},
      {500000,
       {mergeToRow(TF, sTF, expectedDistSelf),
        mergeToRow(TF, sMun, expectedDistUniMun),
        mergeToRow(TF, sEif, expectedDistUniEif),
        mergeToRow(Mun, sMun, expectedDistSelf),
        mergeToRow(Mun, sTF, expectedDistUniMun),
        mergeToRow(Mun, sEif, expectedDistMunEif),
        mergeToRow(Eye, sEye, expectedDistSelf),
        mergeToRow(Eye, sEif, expectedDistEyeEif),
        mergeToRow(Lib, sLib, expectedDistSelf),
        mergeToRow(Eif, sEif, expectedDistSelf),
        mergeToRow(Eif, sTF, expectedDistUniEif),
        mergeToRow(Eif, sMun, expectedDistMunEif),
        mergeToRow(Eif, sEye, expectedDistEyeEif)}},
      {1000000,
       {mergeToRow(TF, sTF, expectedDistSelf),
        mergeToRow(TF, sMun, expectedDistUniMun),
        mergeToRow(TF, sEif, expectedDistUniEif),
        mergeToRow(TF, sEye, expectedDistUniEye),
        mergeToRow(Mun, sMun, expectedDistSelf),
        mergeToRow(Mun, sTF, expectedDistUniMun),
        mergeToRow(Mun, sEif, expectedDistMunEif),
        mergeToRow(Mun, sEye, expectedDistMunEye),
        mergeToRow(Eye, sEye, expectedDistSelf),
        mergeToRow(Eye, sEif, expectedDistEyeEif),
        mergeToRow(Eye, sTF, expectedDistUniEye),
        mergeToRow(Eye, sMun, expectedDistMunEye),
        mergeToRow(Lib, sLib, expectedDistSelf),
        mergeToRow(Eif, sEif, expectedDistSelf),
        mergeToRow(Eif, sTF, expectedDistUniEif),
        mergeToRow(Eif, sMun, expectedDistMunEif),
        mergeToRow(Eif, sEye, expectedDistEyeEif)}},
      {10000000,
       {mergeToRow(TF, sTF, expectedDistSelf),
        mergeToRow(TF, sMun, expectedDistUniMun),
        mergeToRow(TF, sEif, expectedDistUniEif),
        mergeToRow(TF, sEye, expectedDistUniEye),
        mergeToRow(TF, sLib, expectedDistUniLib),
        mergeToRow(Mun, sMun, expectedDistSelf),
        mergeToRow(Mun, sTF, expectedDistUniMun),
        mergeToRow(Mun, sEif, expectedDistMunEif),
        mergeToRow(Mun, sEye, expectedDistMunEye),
        mergeToRow(Mun, sLib, expectedDistMunLib),
        mergeToRow(Eye, sEye, expectedDistSelf),
        mergeToRow(Eye, sEif, expectedDistEyeEif),
        mergeToRow(Eye, sTF, expectedDistUniEye),
        mergeToRow(Eye, sMun, expectedDistMunEye),
        mergeToRow(Eye, sLib, expectedDistEyeLib),
        mergeToRow(Lib, sLib, expectedDistSelf),
        mergeToRow(Lib, sTF, expectedDistUniLib),
        mergeToRow(Lib, sMun, expectedDistMunLib),
        mergeToRow(Lib, sEye, expectedDistEyeLib),
        mergeToRow(Lib, sEif, expectedDistEifLib),
        mergeToRow(Eif, sEif, expectedDistSelf),
        mergeToRow(Eif, sTF, expectedDistUniEif),
        mergeToRow(Eif, sMun, expectedDistMunEif),
        mergeToRow(Eif, sEye, expectedDistEyeEif),
        mergeToRow(Eif, sLib, expectedDistEifLib)}}};

  ExpectedRowsMaxDist expectedMaxDistRowsDiffIDTable = {
      {1, {mergeToRow({sTF.at(1)}, sTF, expectedDistSelf)}},
      {5000,
       {mergeToRow({sTF.at(1)}, sTF, expectedDistSelf),
        mergeToRow({sTF.at(1)}, sMun, expectedDistUniMun)}},
      {500000,
       {mergeToRow({sTF.at(1)}, sTF, expectedDistSelf),
        mergeToRow({sTF.at(1)}, sMun, expectedDistUniMun),
        mergeToRow({sTF.at(1)}, sEif, expectedDistUniEif)}},
      {1000000,
       {mergeToRow({sTF.at(1)}, sTF, expectedDistSelf),
        mergeToRow({sTF.at(1)}, sMun, expectedDistUniMun),
        mergeToRow({sTF.at(1)}, sEif, expectedDistUniEif),
        mergeToRow({sTF.at(1)}, sEye, expectedDistUniEye)}},
      {10000000,
       {mergeToRow({sTF.at(1)}, sTF, expectedDistSelf),
        mergeToRow({sTF.at(1)}, sMun, expectedDistUniMun),
        mergeToRow({sTF.at(1)}, sEif, expectedDistUniEif),
        mergeToRow({sTF.at(1)}, sEye, expectedDistUniEye),
        mergeToRow({sTF.at(1)}, sLib, expectedDistUniLib)}}};

  // The expected values for nearest nneighbors are stored in a nested map. The
  // key in the outer map is the maximum number of results and the key in the
  // inner map is the maximum distance or std::nullopt.
  using ExpectedRowsNearestNeighborsMaxDist =
      std::unordered_map<std::optional<size_t>, Rows>;
  using ExpectedRowsNearestNeighbors =
      std::unordered_map<size_t, ExpectedRowsNearestNeighborsMaxDist>;

  ExpectedRowsNearestNeighbors expectedNearestNeighbors = {
      {1,
       {{std::nullopt,
         {mergeToRow(TF, TF, expectedDistSelf),
          mergeToRow(Mun, Mun, expectedDistSelf),
          mergeToRow(Eye, Eye, expectedDistSelf),
          mergeToRow(Lib, Lib, expectedDistSelf),
          mergeToRow(Eif, Eif, expectedDistSelf)}}}},
      {2,
       {{std::nullopt,
         {mergeToRow(TF, TF, expectedDistSelf),
          mergeToRow(Mun, Mun, expectedDistSelf),
          mergeToRow(Eye, Eye, expectedDistSelf),
          mergeToRow(Lib, Lib, expectedDistSelf),
          mergeToRow(Eif, Eif, expectedDistSelf),
          mergeToRow(TF, Mun, expectedDistUniMun),
          mergeToRow(Mun, TF, expectedDistUniMun),
          mergeToRow(Eye, Eif, expectedDistEyeEif),
          mergeToRow(Lib, Eye, expectedDistEyeLib),
          mergeToRow(Eif, Eye, expectedDistEyeEif)}},
        {40,
         {mergeToRow(TF, TF, expectedDistSelf),
          mergeToRow(Mun, Mun, expectedDistSelf),
          mergeToRow(Eye, Eye, expectedDistSelf),
          mergeToRow(Lib, Lib, expectedDistSelf),
          mergeToRow(Eif, Eif, expectedDistSelf)}},
        {4000,
         {mergeToRow(TF, TF, expectedDistSelf),
          mergeToRow(Mun, Mun, expectedDistSelf),
          mergeToRow(Eye, Eye, expectedDistSelf),
          mergeToRow(Lib, Lib, expectedDistSelf),
          mergeToRow(Eif, Eif, expectedDistSelf),
          mergeToRow(TF, Mun, expectedDistUniMun),
          mergeToRow(Mun, TF, expectedDistUniMun)}},
        {400000,
         {mergeToRow(TF, TF, expectedDistSelf),
          mergeToRow(Mun, Mun, expectedDistSelf),
          mergeToRow(Eye, Eye, expectedDistSelf),
          mergeToRow(Lib, Lib, expectedDistSelf),
          mergeToRow(Eif, Eif, expectedDistSelf),
          mergeToRow(TF, Mun, expectedDistUniMun),
          mergeToRow(Mun, TF, expectedDistUniMun),
          mergeToRow(Eye, Eif, expectedDistEyeEif),
          mergeToRow(Eif, Eye, expectedDistEyeEif)}}}},
      {3,
       {{500000,
         {mergeToRow(TF, TF, expectedDistSelf),
          mergeToRow(Mun, Mun, expectedDistSelf),
          mergeToRow(Eye, Eye, expectedDistSelf),
          mergeToRow(Lib, Lib, expectedDistSelf),
          mergeToRow(Eif, Eif, expectedDistSelf),
          mergeToRow(TF, Mun, expectedDistUniMun),
          mergeToRow(Mun, TF, expectedDistUniMun),
          mergeToRow(Mun, Eif, expectedDistMunEif),
          mergeToRow(TF, Eif, expectedDistUniEif),
          mergeToRow(Eye, Eif, expectedDistEyeEif),
          mergeToRow(Eif, Eye, expectedDistEyeEif),
          mergeToRow(Eif, TF, expectedDistUniEif)}}}}};

  // some combinations of the gtest parameters are invalid. Those cases should
  // not be tested and are therefore excluded
  bool isInvalidAreaTestConfig(std::optional<MaxDistanceConfig> maxDistConfig) {
    bool isAreaDataset = std::get<4>(GetParam());
    bool isS2geoAlg =
        std::get<0>(GetParam()) == SpatialJoinAlgorithm::S2_GEOMETRY;
    return isAreaDataset && (!maxDistConfig.has_value() ||
                             (maxDistConfig.has_value() && isS2geoAlg));
  }
};

// test the compute result method on small examples
TEST_P(SpatialJoinParamTest, computeResultSmallDatasetLargeChildren) {
  Row columnNames = {
      "?name1",  "?obj1",   "?geo1",
      "?point1", "?name2",  "?obj2",
      "?geo2",   "?point2", "?distOfTheTwoObjectsAddedInternally"};
  bool addLeftChildFirst = std::get<1>(GetParam());

  auto nearestNeighborsTask = getNearestNeighbors();
  auto maxDistTask = getMaxDist();
  if (isInvalidAreaTestConfig(maxDistTask)) {
    return;
  }

  if (maxDistTask.has_value()) {
    buildAndTestSmallTestSetLargeChildren(
        maxDistTask.value(), addLeftChildFirst,
        expectedMaxDistRows[maxDistTask.value().maxDist_], columnNames);
  } else if (nearestNeighborsTask.has_value()) {
    buildAndTestSmallTestSetLargeChildren(
        nearestNeighborsTask.value(), addLeftChildFirst,
        expectedNearestNeighbors[nearestNeighborsTask.value().maxResults_]
                                [nearestNeighborsTask.value().maxDist_],
        columnNames);
  } else {
    AD_THROW("Invalid config");
  }
}

TEST_P(SpatialJoinParamTest, computeResultSmallDatasetSmallChildren) {
  Row columnNames{"?obj1", "?point1", "?obj2", "?point2",
                  "?distOfTheTwoObjectsAddedInternally"};
  bool addLeftChildFirst = std::get<1>(GetParam());

  auto maxDistTask = getMaxDist();
  if (isInvalidAreaTestConfig(maxDistTask)) {
    return;
  }
  if (maxDistTask.has_value()) {
    buildAndTestSmallTestSetSmallChildren(
        maxDistTask.value(), addLeftChildFirst,
        expectedMaxDistRowsSmall[maxDistTask.value().maxDist_], columnNames);
  }
}

TEST_P(SpatialJoinParamTest, computeResultSmallDatasetDifferentSizeChildren) {
  Row columnNames{"?name1",
                  "?obj1",
                  "?geo1",
                  "?point1",
                  "?obj2",
                  "?point2",
                  "?distOfTheTwoObjectsAddedInternally"};
  bool addLeftChildFirst = std::get<1>(GetParam());
  bool bigChildLeft = std::get<2>(GetParam());

  auto maxDistTask = getMaxDist();
  if (isInvalidAreaTestConfig(maxDistTask)) {
    return;
  }
  if (maxDistTask.has_value()) {
    buildAndTestSmallTestSetDiffSizeChildren(
        maxDistTask.value(), addLeftChildFirst,
        expectedMaxDistRowsDiff[maxDistTask.value().maxDist_], columnNames,
        bigChildLeft);
  }
}

TEST_P(SpatialJoinParamTest, maxSizeMaxDistanceTest) {
  auto maxDist = std::numeric_limits<double>::max();
  MaxDistanceConfig maxDistConf{maxDist};
  bool addLeftChildFirst = std::get<1>(GetParam());

  if (isInvalidAreaTestConfig(maxDistConf)) {
    return;
  }

  // test small children
  Row columnNames{"?obj1", "?point1", "?obj2", "?point2",
                  "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetSmallChildren(maxDistConf, addLeftChildFirst,
                                        expectedMaxDistRowsSmall[10000000],
                                        columnNames);

  // test diff size children
  columnNames = {"?name1",
                 "?obj1",
                 "?geo1",
                 "?point1",
                 "?obj2",
                 "?point2",
                 "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetDiffSizeChildren(maxDistConf, addLeftChildFirst,
                                           expectedMaxDistRowsDiff[10000000],
                                           columnNames, false);

  // test large size children
  columnNames = {"?name1",  "?obj1",   "?geo1",
                 "?point1", "?name2",  "?obj2",
                 "?geo2",   "?point2", "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetLargeChildren(maxDistConf, addLeftChildFirst,
                                        expectedMaxDistRows[10000000],
                                        columnNames);
}

TEST_P(SpatialJoinParamTest, diffSizeIdTables) {
  Row columnNames{"?point1", "?obj2", "?point2",
                  "?distOfTheTwoObjectsAddedInternally"};
  bool addLeftChildFirst = std::get<1>(GetParam());
  bool bigChildLeft = std::get<2>(GetParam());

  auto maxDistTask = getMaxDist();
  if (isInvalidAreaTestConfig(maxDistTask)) {
    return;
  }
  if (maxDistTask.has_value()) {
    testDiffSizeIdTables(
        maxDistTask.value(), addLeftChildFirst,
        expectedMaxDistRowsDiffIDTable[maxDistTask.value().maxDist_],
        columnNames, bigChildLeft);
  }
}

TEST_P(SpatialJoinParamTest, wrongPointInInput) {
  // expected behavior: point is skipped
  Row columnNames{"?obj1", "?point1", "?obj2", "?point2",
                  "?distOfTheTwoObjectsAddedInternally"};
  bool addLeftChildFirst = std::get<1>(GetParam());

  auto maxDistTask = getMaxDist();
  if (isInvalidAreaTestConfig(maxDistTask)) {
    return;
  }
  if (maxDistTask.has_value() and !std::get<4>(GetParam())) {
    testWrongPointInInput(
        maxDistTask.value(), addLeftChildFirst,
        expectedMaxDistRowsSmallWrongPoint[maxDistTask.value().maxDist_],
        columnNames);
  }
}

INSTANTIATE_TEST_SUITE_P(
    SpatialJoin, SpatialJoinParamTest,
    ::testing::Combine(
        ::testing::Values(SpatialJoinAlgorithm::BASELINE,
                          SpatialJoinAlgorithm::S2_GEOMETRY,
                          SpatialJoinAlgorithm::BOUNDING_BOX),
        ::testing::Bool(), ::testing::Bool(),
        ::testing::Values(MaxDistanceConfig{1}, MaxDistanceConfig{5000},
                          MaxDistanceConfig{500000}, MaxDistanceConfig{1000000},
                          MaxDistanceConfig{10000000},
                          NearestNeighborsConfig{1}, NearestNeighborsConfig{2},
                          NearestNeighborsConfig{2, 400000},
                          NearestNeighborsConfig{2, 4000},
                          NearestNeighborsConfig{2, 40},
                          NearestNeighborsConfig{3, 500000}),
        ::testing::Bool()));

}  // end of Namespace computeResultTest

namespace boundingBox {

using BoostGeometryNamespace::Box;
using BoostGeometryNamespace::Point;
using BoostGeometryNamespace::Value;

// usage of this helper function for the bounding box to test:
// Iterate over every edge of the bounding box. Furthermore iterate over each
// edge (i.e. use 100 points, which are on the edge of the bounding box). Then
// call this function for each of those points twice. Once slightly move the
// point inside the bounding box and give 'shouldBeWithin' = true to this
// function, the other time move it slightly outside of the bounding box and
// give 'shouldBeTrue' = false to the function. Do this for all edges. Note
// that this function is not taking a set of boxes, as neighboring boxes would
// not work with this approach (slightly outside of one box can be inside the
// neighboring box. For a set of boxes, check each box separately)
void testBounds(double x, double y, const Box& bbox, bool shouldBeWithin) {
  // correct lon bounds if necessary
  if (x < -180) {
    x += 360;
  } else if (x > 180) {
    x -= 360;
  }

  // testing only possible, if lat bounds are correct and the lon bounds
  // don't cover everything (as then left or right of the box is again
  // inside the box because of the spherical geometry). If we have a bounding
  // box, which goes from -180 to 180 longitude, then left of the bounding box
  // is just in the bounding box again. (i.e. -180.00001 is the same as
  // +179.99999). As all longitudes are covered, a left or right bound does
  // not exist (on the sphere this makes intuitive sense). A test in that case
  // is not necessary, because this test is about testing the edges and if
  // everything is covered an edge doesn't exist there is no need for testing
  // in that case
  double minLonBox = bbox.min_corner().get<0>();
  double maxLonBox = bbox.max_corner().get<0>();
  if (y < 90 && y > -90 && !(minLonBox < 179.9999 && maxLonBox > 179.9999)) {
    bool within = boost::geometry::covered_by(Point(x, y), bbox);
    ASSERT_TRUE(within == shouldBeWithin);
  }
};

// This function performs multiple tests on the bounding box. First it asserts,
// that a point, which is not contained in any bounding box is more than
// 'maxDistInMeters' away from 'startPoint'. Second it iterates over the edges
// of the bounding box and checks, that points which are slightly inside or
// outside of the bounding box are correctly identified.
void testBoundingBox(const size_t& maxDistInMeters, const Point& startPoint) {
  // this helper function checks, that a point, which is not contained in any
  // bounding box, is really more than 'maxDistanceInMeters' away from
  // 'startPoint'
  auto checkOutside = [&](const Point& point1, const Point& startPoint,
                          const std::vector<Box>& bbox,
                          SpatialJoinAlgorithms* spatialJoinAlg) {
    // check if the point is contained in any bounding box
    bool within = spatialJoinAlg->isContainedInBoundingBoxes(bbox, point1);
    if (!within) {
      GeoPoint geo1{point1.get<1>(), point1.get<0>()};
      GeoPoint geo2{startPoint.get<1>(), startPoint.get<0>()};
      double dist = ad_utility::detail::wktDistImpl(geo1, geo2) * 1000;
      ASSERT_GT(static_cast<long long>(dist), maxDistInMeters);
    }
  };

  SpatialJoinAlgorithms spatialJoinAlgs =
      getDummySpatialJoinAlgsForWrapperTesting(maxDistInMeters);

  std::vector<Box> bbox = spatialJoinAlgs.computeQueryBox(startPoint);
  // broad grid test
  for (int lon = -180; lon < 180; lon += 20) {
    for (int lat = -90; lat < 90; lat += 20) {
      checkOutside(Point(lon, lat), startPoint, bbox, &spatialJoinAlgs);
    }
  }

  // do tests at the border of the box. The exact usage of this is described in
  // the function comment of the helper function 'testBounds'
  for (size_t k = 0; k < bbox.size(); k++) {
    // use a small delta for testing because of floating point inaccuracies
    const double delta = 0.00000001;
    const Point minPoint = bbox.at(k).min_corner();
    const Point maxPoint = bbox.at(k).max_corner();
    const double lowX = minPoint.get<0>();
    const double lowY = minPoint.get<1>();
    const double highX = maxPoint.get<0>();
    const double highY = maxPoint.get<1>();
    const double xRange = highX - lowX - 2 * delta;
    const double yRange = highY - lowY - 2 * delta;
    for (size_t i = 0; i <= 100; i++) {
      // barely in or out at the left edge
      testBounds(lowX + delta, lowY + delta + (yRange / 100) * i, bbox.at(k),
                 true);
      testBounds(lowX - delta, lowY + delta + (yRange / 100) * i, bbox.at(k),
                 false);
      checkOutside(Point(lowX - delta, lowY + (yRange / 100) * i), startPoint,
                   bbox, &spatialJoinAlgs);
      // barely in or out at the bottom edge
      testBounds(lowX + delta + (xRange / 100) * i, lowY + delta, bbox.at(k),
                 true);
      testBounds(lowX + delta + (xRange / 100) * i, lowY - delta, bbox.at(k),
                 false);
      checkOutside(Point(lowX + (xRange / 100) * i, lowY - delta), startPoint,
                   bbox, &spatialJoinAlgs);
      // barely in or out at the right edge
      testBounds(highX - delta, lowY + delta + (yRange / 100) * i, bbox.at(k),
                 true);
      testBounds(highX + delta, lowY + delta + (yRange / 100) * i, bbox.at(k),
                 false);
      checkOutside(Point(highX + delta, lowY + (yRange / 100) * i), startPoint,
                   bbox, &spatialJoinAlgs);
      // barely in or out at the top edge
      testBounds(lowX + delta + (xRange / 100) * i, highY - delta, bbox.at(k),
                 true);
      testBounds(lowX + delta + (xRange / 100) * i, highY + delta, bbox.at(k),
                 false);
      checkOutside(Point(lowX + (xRange / 100) * i, highY + delta), startPoint,
                   bbox, &spatialJoinAlgs);
    }
  }
}

TEST(SpatialJoin, computeBoundingBox) {
  // ASSERT_EQ("", "uncomment the part below again");
  double circ = 40075 * 1000;  // circumference of the earth (at the equator)
  // 180.0001 in case 180 is represented internally as 180.0000000001
  for (double lon = -180; lon <= 180.0001; lon += 15) {
    // 90.0001 in case 90 is represented internally as 90.000000001
    for (double lat = -90; lat <= 90.0001; lat += 15) {
      // circ / 2 means, that all points on earth are within maxDist km of any
      // starting point
      for (int maxDist = 0; maxDist <= circ / 2.0; maxDist += circ / 36.0) {
        testBoundingBox(maxDist, Point(lon, lat));
      }
    }
  }
}

TEST(SpatialJoin, isContainedInBoundingBoxes) {
  SpatialJoinAlgorithms spatialJoinAlgs =
      getDummySpatialJoinAlgsForWrapperTesting();

  // note that none of the boxes is overlapping, therefore we can check, that
  // none of the points which should be contained in one box are contained in
  // another box
  std::vector<Box> boxes = {
      Box(Point(20, 40), Point(40, 60)),
      Box(Point(-180, -20), Point(-150, 30)),  // touching left border
      Box(Point(50, -30), Point(180, 10)),     // touching right border
      Box(Point(-30, 50), Point(10, 90)),      // touching north pole
      Box(Point(-45, -90), Point(0, -45))      // touching south pole
  };

  // the first entry in this vector is a vector of points, which is contained
  // in the first box, the second entry contains points, which are contained in
  // the second box and so on
  std::vector<std::vector<Point>> containedInBox = {
      {Point(20, 40), Point(40, 40), Point(40, 60), Point(20, 60),
       Point(30, 50)},
      {Point(-180, -20), Point(-150, -20), Point(-150, 30), Point(-180, 30),
       Point(-150, 0)},
      {Point(50, -30), Point(180, -30), Point(180, 10), Point(50, 10),
       Point(70, -10)},
      {Point(-30, 50), Point(10, 50), Point(10, 90), Point(-30, 90),
       Point(-20, 60)},
      {Point(-45, -90), Point(0, -90), Point(0, -45), Point(-45, -45),
       Point(-10, -60)}};

  // all combinations of box is contained in bounding boxes and is not
  // contained. a one encodes, the bounding box is contained in the set of
  // bounding boxes, a zero encodes, it isn't. If a box is contained, it's
  // checked, that the points which should be contained in the box are
  // contained. If the box is not contained, it's checked, that the points which
  // are contained in that box are not contained in the box set (because the
  // boxes don't overlap)
  for (size_t a = 0; a <= 1; a++) {
    for (size_t b = 0; a <= 1; a++) {
      for (size_t c = 0; a <= 1; a++) {
        for (size_t d = 0; a <= 1; a++) {
          for (size_t e = 0; a <= 1; a++) {
            std::vector<Box> toTest;  // the set of bounding boxes
            std::vector<std::vector<Point>> shouldBeContained;
            std::vector<std::vector<Point>> shouldNotBeContained;
            if (a == 1) {  // box nr. 0 is contained in the set of boxes
              toTest.push_back(boxes.at(0));
              shouldBeContained.push_back(containedInBox.at(0));
            } else {  // box nr. 0 is not contained in the set of boxes
              shouldNotBeContained.push_back(containedInBox.at(0));
            }
            if (b == 1) {  // box nr. 1 is contained in the set of boxes
              toTest.push_back(boxes.at(1));
              shouldBeContained.push_back(containedInBox.at(1));
            } else {  // box nr. 1 is not contained in the set of boxes
              shouldNotBeContained.push_back(containedInBox.at(1));
            }
            if (c == 1) {
              toTest.push_back(boxes.at(2));
              shouldBeContained.push_back(containedInBox.at(2));
            } else {
              shouldNotBeContained.push_back(containedInBox.at(2));
            }
            if (d == 1) {
              toTest.push_back(boxes.at(3));
              shouldBeContained.push_back(containedInBox.at(3));
            } else {
              shouldNotBeContained.push_back(containedInBox.at(3));
            }
            if (e == 1) {
              toTest.push_back(boxes.at(4));
              shouldBeContained.push_back(containedInBox.at(4));
            } else {
              shouldNotBeContained.push_back(containedInBox.at(4));
            }
            if (toTest.size() > 0) {
              // test all points, which should be contained in the bounding
              // boxes
              for (size_t i = 0; i < shouldBeContained.size(); i++) {
                for (size_t k = 0; k < shouldBeContained.at(i).size(); k++) {
                  ASSERT_TRUE(spatialJoinAlgs.isContainedInBoundingBoxes(
                      toTest, shouldBeContained.at(i).at(k)));
                }
              }
              // test all points, which shouldn't be contained in the bounding
              // boxes
              for (size_t i = 0; i < shouldNotBeContained.size(); i++) {
                for (size_t k = 0; k < shouldNotBeContained.at(i).size(); k++) {
                  ASSERT_FALSE(spatialJoinAlgs.isContainedInBoundingBoxes(
                      toTest, shouldNotBeContained.at(i).at(k)));
                }
              }
            }
          }
        }
      }
    }
  }
}

void testBoundingBoxOfAreaOrMidpointOfBox(bool testArea = true) {
  auto checkBoundingBox = [](Box box, double minLng, double minLat,
                             double maxLng, double maxLat) {
    ASSERT_DOUBLE_EQ(minLng, box.min_corner().get<0>());
    ASSERT_DOUBLE_EQ(minLat, box.min_corner().get<1>());
    ASSERT_DOUBLE_EQ(maxLng, box.max_corner().get<0>());
    ASSERT_DOUBLE_EQ(maxLat, box.max_corner().get<1>());
  };

  auto checkMidpoint = [](const Point& point, double lng, double lat) {
    ASSERT_DOUBLE_EQ(point.get<0>(), lng);
    ASSERT_DOUBLE_EQ(point.get<1>(), lat);
  };

  SpatialJoinAlgorithms sja = getDummySpatialJoinAlgsForWrapperTesting();

  BoostGeometryNamespace::AnyGeometry geometryA;
  std::string wktA =
      "POLYGON((9.33 47.41, 9.31 47.45, 9.32 47.48, 9.35 47.42, 9.33 "
      "47.41))";  // closed polygon
  boost::geometry::read_wkt(wktA, geometryA);
  Box a = boost::apply_visitor(BoostGeometryNamespace::BoundingBoxVisitor(),
                               geometryA);

  BoostGeometryNamespace::AnyGeometry geometryB;
  std::string wktB =
      "POLYGON((-4.1 10.0, -9.9 10.0, -9.9 -1.0, -4.1 -1.0))";  // not closed
                                                                // polygon
  boost::geometry::read_wkt(wktB, geometryB);
  Box b = boost::apply_visitor(BoostGeometryNamespace::BoundingBoxVisitor(),
                               geometryB);

  BoostGeometryNamespace::AnyGeometry geometryC;
  std::string wktC =
      "POLYGON((0.0 0.0, 1.1 0.0, 1.1 1.1, 0.0 1.1, 0.0 0.0))";  // closed
                                                                 // polygon
  boost::geometry::read_wkt(wktC, geometryC);
  Box c = boost::apply_visitor(BoostGeometryNamespace::BoundingBoxVisitor(),
                               geometryC);

  if (testArea) {
    checkBoundingBox(a, 9.31, 47.41, 9.35, 47.48);
    checkBoundingBox(b, -9.9, -1.0, -4.1, 10.0);
    checkBoundingBox(c, 0.0, 0.0, 1.1, 1.1);
  } else {
    checkMidpoint(sja.calculateMidpointOfBox(a), 9.33, 47.445);
    checkMidpoint(sja.calculateMidpointOfBox(b), -7.0, 4.5);
    checkMidpoint(sja.calculateMidpointOfBox(c), 0.55, 0.55);
  }
}

TEST(SpatialJoin, BoundingBoxOfArea) { testBoundingBoxOfAreaOrMidpointOfBox(); }

TEST(SpatialJoin, MidpointOfBoundingBox) {
  testBoundingBoxOfAreaOrMidpointOfBox(false);
}

TEST(SpatialJoin, getMaxDistFromMidpointToAnyPointInsideTheBox) {
  SpatialJoinAlgorithms sja = getDummySpatialJoinAlgsForWrapperTesting();

  // the following polygon is from the eiffel tower
  BoostGeometryNamespace::AnyGeometry geometryEiffel;
  std::string wktEiffel =
      "POLYGON((2.2933119 48.858248,2.2935432 48.8581003,2.2935574 "
      "48.8581099,2.2935712 48.8581004,2.2936112 48.8581232,2.2936086 "
      "48.8581249,2.293611 48.8581262,2.2936415 48.8581385,2.293672 "
      "48.8581477,2.2937035 48.8581504,2.293734 48.858149,2.2937827 "
      "48.8581439,2.2938856 48.8581182,2.2939778 48.8580882,2.2940648 "
      "48.8580483,2.2941435 48.8579991,2.2941937 48.8579588,2.2942364 "
      "48.8579197,2.2942775 48.8578753,2.2943096 48.8578312,2.2943307 "
      "48.8577908,2.2943447 48.857745,2.2943478 48.8577118,2.2943394 "
      "48.8576885,2.2943306 48.8576773,2.2943205 48.8576677,2.2943158 "
      "48.8576707,2.2942802 48.8576465,2.2942977 48.8576355,2.2942817 "
      "48.8576248,2.2942926 48.8576181,2.2944653 48.8575069,2.2945144 "
      "48.8574753,2.2947414 48.8576291,2.294725 48.8576392,2.2947426 "
      "48.857651,2.294706 48.8576751,2.294698 48.8576696,2.2946846 "
      "48.8576782,2.2946744 48.8576865,2.2946881 48.8576957,2.2946548 "
      "48.857717,2.2946554 48.8577213,2.2946713 48.8577905,2.2946982 "
      "48.8578393,2.2947088 48.8578585,2.2947529 48.8579196,2.2948133 "
      "48.8579803,2.2948836 48.85803,2.2949462 48.8580637,2.2950051 "
      "48.8580923,2.2950719 48.85812,2.2951347 48.8581406,2.2951996 "
      "48.8581564,2.2952689 48.8581663,2.295334 48.8581699,2.2953613 "
      "48.8581518,2.2953739 48.8581604,2.2953965 48.8581497,2.2954016 "
      "48.8581464,2.2953933 48.8581409,2.2954304 48.8581172,2.2954473 "
      "48.8581285,2.2954631 48.8581182,2.2956897 48.8582718,2.295653 "
      "48.8582954,2.2955837 48.85834,2.2954575 48.8584212,2.2954416 "
      "48.858411,2.2954238 48.8584227,2.2953878 48.8583981,2.2953925 "
      "48.858395,2.2953701 48.8583857,2.2953419 48.8583779,2.2953057 "
      "48.8583737,2.2952111 48.8583776,2.2951081 48.858403,2.2950157 "
      "48.8584326,2.2949284 48.8584723,2.2948889 48.8584961,2.2947988 "
      "48.8585613,2.2947558 48.8586003,2.2947144 48.8586446,2.294682 "
      "48.8586886,2.2946605 48.8587289,2.2946462 48.8587747,2.294644 "
      "48.8587962,2.2946462 48.8588051,2.2946486 48.8588068,2.2946938 "
      "48.8588377,2.2946607 48.8588587,2.294663 48.8588603,2.294681 "
      "48.858849,2.2947169 48.8588737,2.2946988 48.858885,2.2947154 "
      "48.8588961,2.2944834 48.8590453,2.2943809 48.8589771,2.2943708 "
      "48.8589703,2.2942571 48.8588932,2.2942741 48.8588824,2.2942567 "
      "48.8588708,2.2942893 48.8588493,2.294306 48.8588605,2.2943103 "
      "48.8588577,2.2942883 48.8588426,2.2943122 48.8588275,2.2943227 "
      "48.8588209,2.2943283 48.8588173,2.2943315 48.8588125,2.2943333 "
      "48.8588018,2.2943166 48.8587327,2.294301 48.8586978,2.2942783 "
      "48.8586648,2.2942406 48.8586191,2.2942064 48.858577,2.2941734 "
      "48.8585464,2.2941015 48.8584943,2.2940384 48.8584609,2.2939792 "
      "48.8584325,2.293912 48.8584052,2.2938415 48.8583828,2.293784 "
      "48.8583695,2.2937145 48.8583599,2.2936514 48.8583593,2.2936122 "
      "48.8583846,2.293606 48.8583807,2.2935688 48.8584044,2.2935515 "
      "48.8583929,2.293536 48.8584028,2.2933119 48.858248))";
  boost::geometry::read_wkt(wktEiffel, geometryEiffel);
  Box boxEiffel = boost::apply_visitor(
      BoostGeometryNamespace::BoundingBoxVisitor(), geometryEiffel);
  auto midpoint_eiffel = sja.calculateMidpointOfBox(boxEiffel);

  // call the function without the precalculated midpoint, the upper bound max
  // distance needs to be bigger than 130 (the tower has a square base of length
  // 125m. Therefore from the midpoint to the side of the box and then to the
  // top of the box results in 125m/2 + 125m/2 = 125m). As the tower is not that
  // near to the equator and the square base has a worst case alignment to the
  // longitude and latitude lines (45 degrees tilted), the distance estimate
  // gets a little more than 125m (it's upper bound estimate is 219m)
  ASSERT_GE(sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxEiffel), 125);
  ASSERT_DOUBLE_EQ(sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxEiffel),
                   sja.getMaxDistFromMidpointToAnyPointInsideTheBox(
                       boxEiffel, midpoint_eiffel));

  // the following polygon is from the Minster of Freiburg
  BoostGeometryNamespace::AnyGeometry geometryMinster;
  std::string wktMinster =
      "POLYGON((7.8520522 47.9956071,7.8520528 47.9955872,7.8521103 "
      "47.995588,7.8521117 47.9955419,7.852113 47.9954975,7.8520523 "
      "47.9954968,7.8520527 47.995477,7.8521152 47.9954775,7.8521154 "
      "47.9954688,7.8521299 47.995469,7.8521311 47.9954303,7.8521611 "
      "47.9954307,7.8521587 47.9954718,7.8522674 47.9954741,7.8522681 "
      "47.9954676,7.8522746 47.9954643,7.8522832 47.9954599,7.8522976 "
      "47.99546,7.8523031 47.995455,7.8523048 47.9954217,7.8522781 "
      "47.9954213,7.8522786 47.9954058,7.8523123 47.9954065,7.852314 "
      "47.9953744,7.8523383 47.9953748,7.8523373 47.9954062,7.8524164 "
      "47.995408,7.8524176 47.9953858,7.852441 47.9953865,7.8524398 "
      "47.9954085,7.8525077 47.9954101,7.8525088 47.9953886,7.8525316 "
      "47.9953892,7.8525305 47.9954106,7.8526031 47.9954123,7.8526042 "
      "47.9953915,7.8526276 47.9953922,7.8526265 47.9954128,7.8526944 "
      "47.9954144,7.8526954 47.9953943,7.8527183 47.9953949,7.8527173 "
      "47.9954149,7.8527892 47.9954165,7.8527903 47.9953974,7.8528131 "
      "47.9953979,7.8528122 47.9954171,7.852871 47.9954182,7.8528712 "
      "47.995416,7.8528791 47.9954112,7.85289 47.9954113,7.8528971 "
      "47.9954158,7.8528974 47.9954052,7.8528925 47.9954052,7.8528928 "
      "47.9953971,7.8529015 47.9953972,7.8529024 47.9953702,7.852897 "
      "47.9953701,7.8528972 47.9953645,7.8529037 47.9953645,7.8529038 "
      "47.9953613,7.8529069 47.9953614,7.8529071 47.9953541,7.8529151 "
      "47.9953542,7.8529149 47.9953581,7.8529218 47.9953582,7.8529217 "
      "47.9953631,7.8529621 47.9953637,7.8529623 47.9953572,7.8529719 "
      "47.9953573,7.8529716 47.9953642,7.8530114 47.9953648,7.8530116 "
      "47.9953587,7.8530192 47.9953589,7.853019 47.995365,7.8530635 "
      "47.9953657,7.8530637 47.9953607,7.8530716 47.9953608,7.8530715 "
      "47.9953657,7.8530758 47.9953657,7.8530757 47.9953688,7.8530817 "
      "47.9953689,7.8530815 47.9953742,7.8530747 47.9953741,7.8530737 "
      "47.9954052,7.8530794 47.9954053,7.8530792 47.995413,7.8530717 "
      "47.9954129,7.8530708 47.9954199,7.8531165 47.9954207,7.8531229 "
      "47.9954131,7.8531292 47.9954209,7.8531444 47.9954211,7.8531444 "
      "47.9954238,7.8531569 47.995424,7.8531661 47.9954152,7.853171 "
      "47.9954201,7.853183 47.9954203,7.8531829 47.9954234,7.8531973 "
      "47.9954236,7.8531977 47.9954138,7.8532142 47.9954141,7.8532141 "
      "47.9954253,7.8532425 47.9954355,7.8532514 47.9954298,7.8532593 "
      "47.9954353,7.8532915 47.9954255,7.8532923 47.9954155,7.8533067 "
      "47.995416,7.8533055 47.9954261,7.8533304 47.9954368,7.8533399 "
      "47.995431,7.85335 47.9954372,7.8533758 47.9954288,7.853377 "
      "47.9954188,7.8533932 47.9954192,7.8533924 47.9954298,7.8534151 "
      "47.9954395,7.8534278 47.9954345,7.8534373 47.995441,7.8534664 "
      "47.995432,7.8534672 47.9954209,7.8534832 47.9954211,7.8534828 "
      "47.9954322,7.8535077 47.9954449,7.8535224 47.9954375,7.8535325 "
      "47.995448,7.8535644 47.9954403,7.8535717 47.9954305,7.8535866 "
      "47.9954356,7.8535796 47.9954443,7.8536079 47.9954674,7.8536221 "
      "47.9954629,7.8536221 47.9954735,7.8536573 47.9954801,7.8536707 "
      "47.9954728,7.8536813 47.9954812,7.8536686 47.9954876,7.8536776 "
      "47.9955168,7.8536958 47.9955192,7.8536876 47.9955286,7.8537133 "
      "47.9955444,7.85373 47.9955428,7.8537318 47.9955528,7.8537154 "
      "47.9955545,7.8537069 47.9955819,7.8537168 47.995588,7.8537044 "
      "47.9955948,7.8537086 47.9956193,7.8537263 47.9956245,7.8537206 "
      "47.9956347,7.8537069 47.9956317,7.8536802 47.9956473,7.8536819 "
      "47.9956577,7.8536667 47.9956604,7.8536506 47.9956817,7.8536639 "
      "47.9956902,7.8536543 47.9956981,7.8536394 47.9956887,7.8536331 "
      "47.9956931,7.853609 47.9956954,7.8536024 47.9957048,7.8535868 "
      "47.9957028,7.8535591 47.9957206,7.8535642 47.9957285,7.8535487 "
      "47.9957327,7.8535423 47.9957215,7.853508 47.9957131,7.8534942 "
      "47.9957215,7.8534818 47.9957186,7.8534587 47.9957284,7.853458 "
      "47.9957389,7.8534421 47.9957388,7.8534424 47.9957273,7.853418 "
      "47.995714,7.8534099 47.9957194,7.8534021 47.995713,7.8533721 "
      "47.9957242,7.8533712 47.9957359,7.8533558 47.9957351,7.8533565 "
      "47.9957247,7.8533269 47.9957094,7.8533171 47.9957165,7.8533073 "
      "47.9957088,7.8532874 47.9957186,7.8532866 47.9957296,7.8532698 "
      "47.9957295,7.8532698 47.9957189,7.8532466 47.9957048,7.8532372 "
      "47.9957131,7.8532277 47.995705,7.8532014 47.9957171,7.8532009 "
      "47.9957284,7.8531844 47.9957281,7.8531847 47.9957174,7.8531778 "
      "47.9957102,7.853163 47.9957245,7.8530549 47.9957225,7.8530552 "
      "47.9957161,7.8529541 47.9957138,7.8529535 47.9957236,7.8529578 "
      "47.9957237,7.8529577 47.9957269,7.852953 47.9957268,7.8529529 "
      "47.9957308,7.8529477 47.9957307,7.8529478 47.9957271,7.8528964 "
      "47.9957256,7.8528963 47.9957288,7.8528915 47.9957287,7.8528916 "
      "47.9957256,7.8528876 47.9957255,7.8528875 47.9957223,7.8528912 "
      "47.9957224,7.8528908 47.9957195,7.8528811 47.9957194,7.8527983 "
      "47.9957162,7.8527981 47.9957192,7.8527723 47.9957185,7.8527732 "
      "47.9957016,7.852703 47.9957003,7.8527021 47.9957175,7.8526791 "
      "47.9957171,7.8526788 47.9957225,7.8526097 47.9957225,7.8526099 "
      "47.995718,7.8525863 47.9957183,7.8525874 47.9956981,7.8525155 "
      "47.9956967,7.8525144 47.995718,7.8524916 47.9957174,7.8524927 "
      "47.9956963,7.8524241 47.995695,7.852423 47.9957153,7.8523996 "
      "47.9957148,7.8524007 47.9956946,7.8523226 47.9956931,7.8523217 "
      "47.9957212,7.8522948 47.9957208,7.8522957 47.9956927,7.8522663 "
      "47.9956923,7.8522667 47.9956784,7.8522926 47.9956787,7.8522937 "
      "47.9956433,7.8522882 47.995635,7.8522723 47.9956351,7.8522611 "
      "47.9956281,7.8522613 47.9956189,7.8521543 47.9956174,7.852153 "
      "47.9956591,7.8521196 47.9956587,7.8521209 47.995617,7.8521109 "
      "47.9956168,7.8521111 47.9956079,7.8520522 47.9956071))";
  boost::geometry::read_wkt(wktMinster, geometryMinster);
  Box boxMinster = boost::apply_visitor(
      BoostGeometryNamespace::BoundingBoxVisitor(), geometryMinster);
  auto midpointMinster = sja.calculateMidpointOfBox(boxMinster);
  ASSERT_GE(sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxMinster), 80);
  ASSERT_DOUBLE_EQ(sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxMinster),
                   sja.getMaxDistFromMidpointToAnyPointInsideTheBox(
                       boxMinster, midpointMinster));

  // the following polygon is from the university building 101 in freiburg
  BoostGeometryNamespace::AnyGeometry geometryUni;
  std::string wktUni =
      "POLYGON((7.8346338 48.0126612,7.8348921 48.0123905,7.8349457 "
      "48.0124216,7.8349855 48.0124448,7.8353244 48.0126418,7.8354091 "
      "48.0126911,7.8352246 48.0129047,7.8351668 48.0128798,7.8349471 "
      "48.0127886,7.8347248 48.0126986,7.8346338 48.0126612))";
  boost::geometry::read_wkt(wktUni, geometryUni);
  Box boxUni = boost::apply_visitor(
      BoostGeometryNamespace::BoundingBoxVisitor(), geometryUni);
  auto midpointUni = sja.calculateMidpointOfBox(boxUni);
  ASSERT_GE(sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxUni), 40);
  ASSERT_DOUBLE_EQ(
      sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxUni),
      sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxUni, midpointUni));

  // the following polygon is from the London Eye
  BoostGeometryNamespace::AnyGeometry geometryEye;
  std::string wktEye =
      "POLYGON((-0.1198608 51.5027451,-0.1197395 51.5027354,-0.1194922 "
      "51.5039381,-0.1196135 51.5039478,-0.1198608 51.5027451))";
  boost::geometry::read_wkt(wktEye, geometryEye);
  Box boxEye = boost::apply_visitor(
      BoostGeometryNamespace::BoundingBoxVisitor(), geometryEye);
  auto midpointEye = sja.calculateMidpointOfBox(boxEye);
  ASSERT_GE(sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxEye), 70);
  ASSERT_DOUBLE_EQ(
      sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxEye),
      sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxEye, midpointEye));

  // the following polygon is from the Statue of liberty
  BoostGeometryNamespace::AnyGeometry geometryStatue;
  std::string wktStatue =
      "POLYGON((-74.0451069 40.6893455,-74.045004 40.6892215,-74.0451023 "
      "40.6891073,-74.0449107 40.6890721,-74.0449537 40.6889343,-74.0447746 "
      "40.6889506,-74.0446495 40.6888049,-74.0445067 40.6889076,-74.0442008 "
      "40.6888563,-74.0441463 40.6890663,-74.0441411 40.6890854,-74.0441339 "
      "40.6890874,-74.0441198 40.6890912,-74.0439637 40.6891376,-74.0440941 "
      "40.6892849,-74.0440057 40.6894071,-74.0441949 40.6894309,-74.0441638 "
      "40.6895702,-74.0443261 40.6895495,-74.0443498 40.6895782,-74.0443989 "
      "40.6896372,-74.0444277 40.6896741,-74.0445955 40.6895939,-74.0447392 "
      "40.6896561,-74.0447498 40.6896615,-74.0447718 40.6895577,-74.0447983 "
      "40.6895442,-74.0448287 40.6895279,-74.0449638 40.6895497,-74.0449628 "
      "40.6895443,-74.044961 40.6895356,-74.0449576 40.6895192,-74.044935 "
      "40.689421,-74.0451069 40.6893455))";
  boost::geometry::read_wkt(wktStatue, geometryStatue);
  Box boxStatue = boost::apply_visitor(
      BoostGeometryNamespace::BoundingBoxVisitor(), geometryStatue);
  auto midpointStatue = sja.calculateMidpointOfBox(boxStatue);
  ASSERT_GE(sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxStatue), 100);
  ASSERT_DOUBLE_EQ(sja.getMaxDistFromMidpointToAnyPointInsideTheBox(boxStatue),
                   sja.getMaxDistFromMidpointToAnyPointInsideTheBox(
                       boxStatue, midpointStatue));
}

QueryExecutionContext* getAllGeometriesQEC() {
  auto addRow = [](std::string& kg, std::string nr, std::string wktStr) {
    kg += absl::StrCat("<geometry" + nr + "> <asWKT> \"", wktStr, "\".\n");
  };
  // each object (except for the point) has its leftmost coordinate at an
  // integer number and its rightmost coordinate 0.5 units further right. The
  // y coordinate will be 0 for those case. All other points are inbetween these
  // two points with different y coordinates. The reason for that is, that it
  // is very easy to calculate the shortest distance between two geometries.
  std::string point = "POINT(1.5 0)";
  std::string linestring = "LINESTRING(2.0 0, 2.2 1, 2.5 0)";
  std::string polygon = "POLYGON((3.0 0, 3.1 1, 3.2 2, 3.5 0))";
  std::string multiPoint = "MULTIPOINT((4.0 0), (4.2 1), (4.5 0))";
  std::string multiLinestring =
      "MULTILINESTRING((5.0 0, 5.2 1, 5.5 1), (5.1 3, 5.5 0))";
  std::string multiPolygon =
      "MULTIPOLYGON(((6.0 0, 6.1 1, 6.3 5, 6.4 2)), ((6.2 1, 6.3 4, 6.4 3, 6.5 "
      "0)))";

  std::string kg = "";  // tiny test knowledge graph for all geometries
  addRow(kg, "1", point);
  addRow(kg, "2", linestring);
  addRow(kg, "3", polygon);
  addRow(kg, "4", multiPoint);
  addRow(kg, "5", multiLinestring);
  addRow(kg, "6", multiPolygon);

  auto qec = buildQec(kg);
  return qec;
}

TEST(SpatialJoin, areaFormat) {
  auto qec = getAllGeometriesQEC();
  auto leftChild =
      buildIndexScan(qec, {"?geo1", std::string{"<asWKT>"}, "?obj1"});
  auto rightChild =
      buildIndexScan(qec, {"?geo2", std::string{"<asWKT>"}, "?obj2"});

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(
          qec,
          SpatialJoinConfiguration{MaxDistanceConfig(100000000),
                                   Variable{"?obj1"}, Variable{"?obj2"}},
          leftChild, rightChild);

  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
  spatialJoin->selectAlgorithm(SpatialJoinAlgorithm::BOUNDING_BOX);
  auto res = spatialJoin->getResult();
  // if all rows can be parsed correctly, the result should be the cross
  // product. (Lines which can't be parsed will be ignored (and a warning gets
  // printed) and therefore the cross product of all parsed lines would be
  // smaller then 36)
  ASSERT_EQ(res->idTable().numRows(), 36);
}

TEST(SpatialJoin, trueAreaDistance) {
  auto getDist = [](QueryExecutionContext* qec, std::string nr1,
                    std::string nr2, bool useMidpointForAreas) {
    auto makeIndexScan = [&](std::string nr) {
      auto subject = absl::StrCat("<geometry", nr, ">");
      auto objStr = absl::StrCat("?obj", nr);
      TripleComponent object{Variable{objStr}};
      return ad_utility::makeExecutionTree<IndexScan>(
          qec, Permutation::Enum::PSO,
          SparqlTripleSimple{TripleComponent::Iri::fromIriref(subject),
                             TripleComponent::Iri::fromIriref("<asWKT>"),
                             object});
    };
    auto scan1 = makeIndexScan(nr1);
    auto scan2 = makeIndexScan(nr2);
    auto var1 = absl::StrCat("?obj", nr1);
    auto var2 = absl::StrCat("?obj", nr2);

    std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(
            qec,
            SpatialJoinConfiguration{MaxDistanceConfig(100000000),
                                     Variable{var1}, Variable{var2}},
            scan1, scan2);

    std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
    SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
    spatialJoin->selectAlgorithm(SpatialJoinAlgorithm::BOUNDING_BOX);
    PreparedSpatialJoinParams params =
        spatialJoin->onlyForTestingGetPrepareJoin();
    SpatialJoinAlgorithms algorithms{
        qec, params, spatialJoin->onlyForTestingGetConfig(), std::nullopt};
    algorithms.setUseMidpointForAreas_(useMidpointForAreas);
    auto entryLeft = algorithms.onlyForTestingGetRtreeEntry(
        params.idTableLeft_, 0, params.leftJoinCol_);
    auto entryRight = algorithms.onlyForTestingGetRtreeEntry(
        params.idTableRight_, 0, params.rightJoinCol_);
    auto distID = algorithms.computeDist(entryLeft.value(), entryRight.value());
    return distID.getDouble();
  };
  auto qec = buildMixedAreaPointQEC(true);

  // the following tests all calculate the distance from germany to each point.
  // When the areas get approximated by their midpoint, the distance should
  // always be larger or at least equally large compared to areas not being
  // approximated by their midpoint.
  ASSERT_TRUE(getDist(qec, "Area6", "1", true) >=
              getDist(qec, "Area6", "1", false));
  ASSERT_TRUE(getDist(qec, "Area6", "Area2", true) >=
              getDist(qec, "Area6", "Area2", false));
  ASSERT_TRUE(getDist(qec, "Area6", "3", true) >=
              getDist(qec, "Area6", "3", false));
  ASSERT_TRUE(getDist(qec, "Area6", "Area4", true) >=
              getDist(qec, "Area6", "Area4", false));
  ASSERT_TRUE(getDist(qec, "Area6", "5", true) >=
              getDist(qec, "Area6", "5", false));
  ASSERT_TRUE(getDist(qec, "Area6", "Area6", true) >=
              getDist(qec, "Area6", "Area6", false));
}

TEST(SpatialJoin, mixedDataSet) {
  auto testDist = [](QueryExecutionContext* qec, size_t maxDist,
                     size_t nrResultRows) {
    auto leftChild =
        buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?geo1"});
    auto rightChild =
        buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?geo2"});

    std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(
            qec,
            SpatialJoinConfiguration{MaxDistanceConfig(maxDist),
                                     Variable{"?geo1"}, Variable{"?geo2"}},
            leftChild, rightChild);

    std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
    SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
    spatialJoin->selectAlgorithm(SpatialJoinAlgorithm::BOUNDING_BOX);
    PreparedSpatialJoinParams params =
        spatialJoin->onlyForTestingGetPrepareJoin();
    SpatialJoinAlgorithms algorithms{
        qec, params, spatialJoin->onlyForTestingGetConfig(), std::nullopt};
    algorithms.setUseMidpointForAreas_(false);
    auto res = algorithms.BoundingBoxAlgorithm();
    // that the id table contains all the necessary other columns and gets
    // constructed correctly has already been extensively tested elsewhere.
    // Here we only test, that the distance between GeoPoints and areas gets
    // computed correctly. For this purpose it is sufficient to check the number
    // of rows in the result table
    ASSERT_EQ(res.idTable().numRows(), nrResultRows);
  };
  auto qec = buildMixedAreaPointQEC();
  testDist(qec, 1, 5);
  testDist(qec, 5000, 7);
  testDist(qec, 500000, 13);
  testDist(qec, 1000000, 17);
  testDist(qec, 10000000, 25);
}

}  // namespace boundingBox

}  // namespace

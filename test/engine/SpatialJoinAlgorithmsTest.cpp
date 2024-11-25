#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>

#include <cstdlib>
#include <fstream>
#include <regex>

#include "../util/IndexTestHelpers.h"
#include "./../../src/util/GeoSparqlHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/SpatialJoinAlgorithms.h"
#include "parser/data/Variable.h"

namespace {  // anonymous namespace to avoid linker problems

using namespace ad_utility::testing;
using namespace SpatialJoinTestHelpers;

namespace computeResultTest {

class SpatialJoinParamTest
    : public ::testing::TestWithParam<SpatialJoin::Algorithm> {
 public:
  void createAndTestSpatialJoin(
      QueryExecutionContext* qec, SparqlTriple spatialJoinTriple,
      std::shared_ptr<QueryExecutionTree> leftChild,
      std::shared_ptr<QueryExecutionTree> rightChild, bool addLeftChildFirst,
      std::vector<std::vector<std::string>> expectedOutputUnorderedRows,
      std::vector<std::string> columnNames,
      bool isWrongPointInputTest = false) {
    // this function is like transposing a matrix. An entry which has been
    // stored at (i, k) is now stored at (k, i). The reason this is needed is
    // the following: this function receives the input as a vector of vector of
    // strings. Each vector contains a vector, which contains a row of the
    // result. This row contains all columns. After swapping, each vector
    // contains a vector, which contains all entries of one column. As now each
    // of the vectors contain only one column, we can later order them according
    // to the variable to column map and then compare the result.
    auto swapColumns = [&](std::vector<std::vector<std::string>> toBeSwapped) {
      std::vector<std::vector<std::string>> result;
      bool firstIteration = true;
      for (size_t i = 0; i < toBeSwapped.size(); i++) {
        for (size_t k = 0; k < toBeSwapped.at(i).size(); k++) {
          if (firstIteration) {
            result.push_back(std::vector<std::string>{toBeSwapped.at(i).at(k)});
          } else {
            result.at(k).push_back(toBeSwapped.at(i).at(k));
          }
        }
        firstIteration = false;
      }
      return result;
    };

    std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(qec, spatialJoinTriple,
                                                   std::nullopt, std::nullopt);

    // add first child
    std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
    SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
    auto firstChild = addLeftChildFirst ? leftChild : rightChild;
    auto secondChild = addLeftChildFirst ? rightChild : leftChild;
    Variable firstVariable = addLeftChildFirst
                                 ? spatialJoinTriple.s_.getVariable()
                                 : spatialJoinTriple.o_.getVariable();
    Variable secondVariable = addLeftChildFirst
                                  ? spatialJoinTriple.o_.getVariable()
                                  : spatialJoinTriple.s_.getVariable();

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
    spatialJoin->selectAlgorithm(GetParam());

    // At worst quadratic time
    ASSERT_LE(
        spatialJoin->getCostEstimate(),
        std::pow(firstChild->getSizeEstimate() * secondChild->getSizeEstimate(),
                 2));

    auto res = spatialJoin->computeResult(false);
    auto vec = printTable(qec, &res);
    /*
    for (size_t i = 0; i < vec.size(); ++i) {
      EXPECT_STREQ(vec.at(i).c_str(), expectedOutput.at(i).c_str());
    }*/

    EXPECT_THAT(vec, ::testing::UnorderedElementsAreArray(expectedOutput));

    if (isWrongPointInputTest &&
        GetParam() == SpatialJoin::Algorithm::BoundingBox) {
      auto warnings = spatialJoin->collectWarnings();
      bool containsWrongPointWarning = false;
      std::string warningMessage =
          "The input to a spatial join contained at least one element, "
          "that is not a point geometry and is thus skipped. Note that "
          "QLever currently only accepts point geometries for the "
          "spatial joins";
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
  void buildAndTestSmallTestSetLargeChildren(
      std::string specialPredicate, bool addLeftChildFirst,
      std::vector<std::vector<std::string>> expectedOutput,
      std::vector<std::string> columnNames) {
    auto qec = buildTestQEC();
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

    createAndTestSpatialJoin(
        qec,
        SparqlTriple{TripleComponent{Variable{"?point1"}}, specialPredicate,
                     TripleComponent{Variable{"?point2"}}},
        leftChild, rightChild, addLeftChildFirst, expectedOutput, columnNames);
  }

  // build the test using the small dataset. Let the SpatialJoin operation.
  // The following Query will be simulated, the max distance will be different
  // depending on the test:
  // Select * where {
  //   ?geo1 <asWKT> ?point1
  //   ?geo2 <asWKT> ?point2
  //   ?point1 <max-distance-in-meters:XXXX> ?point2 .
  // }
  void buildAndTestSmallTestSetSmallChildren(
      std::string specialPredicate, bool addLeftChildFirst,
      std::vector<std::vector<std::string>> expectedOutput,
      std::vector<std::string> columnNames) {
    auto qec = buildTestQEC();
    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);
    // ====================== build inputs ===================================
    TripleComponent point1{Variable{"?point1"}};
    TripleComponent point2{Variable{"?point2"}};
    auto leftChild =
        buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
    auto rightChild =
        buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

    createAndTestSpatialJoin(
        qec, SparqlTriple{point1, specialPredicate, point2}, leftChild,
        rightChild, addLeftChildFirst, expectedOutput, columnNames);
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
  void buildAndTestSmallTestSetDiffSizeChildren(
      std::string specialPredicate, bool addLeftChildFirst,
      std::vector<std::vector<std::string>> expectedOutput,
      std::vector<std::string> columnNames, bool bigChildLeft) {
    auto qec = buildTestQEC();
    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);
    // ========================= build big child =============================
    auto bigChild = buildMediumChild(
        qec, {"?obj1", std::string{"<name>"}, "?name1"},
        {"?obj1", std::string{"<hasGeometry>"}, "?geo1"},
        {"?geo1", std::string{"<asWKT>"}, "?point1"}, "?obj1", "?geo1");

    // ========================= build small child ===========================
    TripleComponent point2{Variable{"?point2"}};
    auto smallChild =
        buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

    auto firstChild = bigChildLeft ? bigChild : smallChild;
    auto secondChild = bigChildLeft ? smallChild : bigChild;
    auto firstVariable =
        bigChildLeft ? TripleComponent{Variable{"?point1"}} : point2;
    auto secondVariable =
        bigChildLeft ? point2 : TripleComponent{Variable{"?point1"}};

    createAndTestSpatialJoin(
        qec, SparqlTriple{firstVariable, specialPredicate, secondVariable},
        firstChild, secondChild, addLeftChildFirst, expectedOutput,
        columnNames);
  }

  void testDiffSizeIdTables(
      std::string specialPredicate, bool addLeftChildFirst,
      std::vector<std::vector<std::string>> expectedOutput,
      std::vector<std::string> columnNames, bool bigChildLeft) {
    auto qec = buildTestQEC();
    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);
    // ====================== build small input ==============================
    TripleComponent point1{Variable{"?point1"}};
    TripleComponent subject{
        ad_utility::triple_component::Iri::fromIriref("<geometry1>")};
    auto smallChild = ad_utility::makeExecutionTree<IndexScan>(
        qec, Permutation::Enum::PSO,
        SparqlTriple{subject, std::string{"<asWKT>"}, point1});
    // ====================== build big input ================================
    auto bigChild =
        buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

    auto firstChild = bigChildLeft ? bigChild : smallChild;
    auto secondChild = bigChildLeft ? smallChild : bigChild;
    auto firstVariable =
        bigChildLeft ? TripleComponent{Variable{"?point2"}} : point1;
    auto secondVariable =
        bigChildLeft ? point1 : TripleComponent{Variable{"?point2"}};

    createAndTestSpatialJoin(
        qec, SparqlTriple{firstVariable, specialPredicate, secondVariable},
        firstChild, secondChild, addLeftChildFirst, expectedOutput,
        columnNames);
  }

  void testWrongPointInInput(
      std::string specialPredicate, bool addLeftChildFirst,
      std::vector<std::vector<std::string>> expectedOutput,
      std::vector<std::string> columnNames) {
    auto kg = createSmallDatasetWithPoints();
    // make first point wrong:
    auto pos = kg.find("POINT(");
    kg = kg.insert(pos + 7, "wrongStuff");

    ad_utility::MemorySize blocksizePermutations = 128_MB;
    auto qec = ad_utility::testing::getQec(kg, true, true, false,
                                           blocksizePermutations, false);
    auto numTriples = qec->getIndex().numTriples().normal;
    ASSERT_EQ(numTriples, 15);
    // ====================== build inputs ================================
    TripleComponent point1{Variable{"?point1"}};
    TripleComponent point2{Variable{"?point2"}};
    auto leftChild =
        buildIndexScan(qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
    auto rightChild =
        buildIndexScan(qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

    createAndTestSpatialJoin(
        qec, SparqlTriple{point1, specialPredicate, point2}, leftChild,
        rightChild, addLeftChildFirst, expectedOutput, columnNames, true);
  }

 protected:
  bool useBaselineAlgorithm_;
};

std::vector<std::string> mergeToRow(std::vector<std::string> part1,
                                    std::vector<std::string> part2,
                                    std::vector<std::string> part3) {
  std::vector<std::string> result = part1;
  for (size_t i = 0; i < part2.size(); i++) {
    result.push_back(part2.at(i));
  }
  for (size_t i = 0; i < part3.size(); i++) {
    result.push_back(part3.at(i));
  }
  return result;
};

std::vector<std::vector<std::string>> unordered_rows{
    std::vector<std::string>{"\"Uni Freiburg TF\"", "<node_1>", "<geometry1>",
                             "POINT(7.835050 48.012670)"},
    std::vector<std::string>{"\"Minster Freiburg\"", "<node_2>", "<geometry2>",
                             "POINT(7.852980 47.995570)"},
    std::vector<std::string>{"\"London Eye\"", "<node_3>", "<geometry3>",
                             "POINT(-0.119570 51.503330)"},
    std::vector<std::string>{"\"Statue of liberty\"", "<node_4>", "<geometry4>",
                             "POINT(-74.044540 40.689250)"},
    std::vector<std::string>{"\"eiffel tower\"", "<node_5>", "<geometry5>",
                             "POINT(2.294510 48.858250)"},
};

// Shortcuts
auto TF = unordered_rows.at(0);
auto Mun = unordered_rows.at(1);
auto Eye = unordered_rows.at(2);
auto Lib = unordered_rows.at(3);
auto Eif = unordered_rows.at(4);

std::vector<std::vector<std::string>> unordered_rows_small{
    std::vector<std::string>{"<geometry1>", "POINT(7.835050 48.012670)"},
    std::vector<std::string>{"<geometry2>", "POINT(7.852980 47.995570)"},
    std::vector<std::string>{"<geometry3>", "POINT(-0.119570 51.503330)"},
    std::vector<std::string>{"<geometry4>", "POINT(-74.044540 40.689250)"},
    std::vector<std::string>{"<geometry5>", "POINT(2.294510 48.858250)"}};

// Shortcuts
auto sTF = unordered_rows_small.at(0);
auto sMun = unordered_rows_small.at(1);
auto sEye = unordered_rows_small.at(2);
auto sLib = unordered_rows_small.at(3);
auto sEif = unordered_rows_small.at(4);

// in all calculations below, the factor 1000 is used to convert from km to m

// distance from the object to itself should be zero
std::vector<std::string> expectedDistSelf{"0"};

// helper functions
auto P = [](double x, double y) { return GeoPoint(y, x); };

auto expectedDist = [](const GeoPoint& p1, const GeoPoint& p2) {
  auto p1_ = S2Point{S2LatLng::FromDegrees(p1.getLat(), p1.getLng())};
  auto p2_ = S2Point{S2LatLng::FromDegrees(p2.getLat(), p2.getLng())};

  return std::to_string(static_cast<int>(S2Earth::ToMeters(S1Angle(p1_, p2_))));
};

// Places for testing
auto PUni = P(7.83505, 48.01267);
auto PMun = P(7.85298, 47.99557);
auto PEif = P(2.29451, 48.85825);
auto PEye = P(-0.11957, 51.50333);
auto PLib = P(-74.04454, 40.68925);
auto testPlaces = std::vector{PUni, PMun, PEif, PEye, PLib};

// distance from Uni Freiburg to Freiburger Münster is 2,33 km according to
// google maps
std::vector<std::string> expectedDistUniMun{expectedDist(PUni, PMun)};

// distance from Uni Freiburg to Eiffel Tower is 419,32 km according to
// google maps
std::vector<std::string> expectedDistUniEif{expectedDist(PUni, PEif)};

// distance from Minster Freiburg to eiffel tower is 421,09 km according to
// google maps
std::vector<std::string> expectedDistMunEif{expectedDist(PMun, PEif)};

// distance from london eye to eiffel tower is 340,62 km according to
// google maps
std::vector<std::string> expectedDistEyeEif{expectedDist(PEye, PEif)};

// distance from Uni Freiburg to London Eye is 690,18 km according to
// google maps
std::vector<std::string> expectedDistUniEye{expectedDist(PUni, PEye)};

// distance from Minster Freiburg to London Eye is 692,39 km according to
// google maps
std::vector<std::string> expectedDistMunEye{expectedDist(PMun, PEye)};

// distance from Uni Freiburg to Statue of Liberty is 6249,55 km according to
// google maps
std::vector<std::string> expectedDistUniLib{expectedDist(PUni, PLib)};

// distance from Minster Freiburg to Statue of Liberty is 6251,58 km
// according to google maps
std::vector<std::string> expectedDistMunLib{expectedDist(PMun, PLib)};

// distance from london eye to statue of liberty is 5575,08 km according to
// google maps
std::vector<std::string> expectedDistEyeLib{expectedDist(PEye, PLib)};

// distance from eiffel tower to Statue of liberty is 5837,42 km according to
// google maps
std::vector<std::string> expectedDistEifLib{expectedDist(PEif, PLib)};

std::vector<std::vector<std::string>> expectedMaxDist1_rows{
    mergeToRow(TF, TF, expectedDistSelf),
    mergeToRow(Mun, Mun, expectedDistSelf),
    mergeToRow(Eye, Eye, expectedDistSelf),
    mergeToRow(Lib, Lib, expectedDistSelf),
    mergeToRow(Eif, Eif, expectedDistSelf)};

std::vector<std::vector<std::string>> expectedMaxDist5000_rows{
    mergeToRow(TF, TF, expectedDistSelf),
    mergeToRow(TF, Mun, expectedDistUniMun),
    mergeToRow(Mun, Mun, expectedDistSelf),
    mergeToRow(Mun, TF, expectedDistUniMun),
    mergeToRow(Eye, Eye, expectedDistSelf),
    mergeToRow(Lib, Lib, expectedDistSelf),
    mergeToRow(Eif, Eif, expectedDistSelf)};

std::vector<std::vector<std::string>> expectedMaxDist500000_rows{
    mergeToRow(TF, TF, expectedDistSelf),
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
    mergeToRow(Eif, Eye, expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist1000000_rows{
    mergeToRow(TF, TF, expectedDistSelf),
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
    mergeToRow(Eif, Eye, expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist10000000_rows{
    mergeToRow(TF, TF, expectedDistSelf),
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
    mergeToRow(Eif, Lib, expectedDistEifLib)};

std::vector<std::vector<std::string>> expectedMaxDist1_rows_small{
    mergeToRow(sTF, sTF, expectedDistSelf),
    mergeToRow(sMun, sMun, expectedDistSelf),
    mergeToRow(sEye, sEye, expectedDistSelf),
    mergeToRow(sLib, sLib, expectedDistSelf),
    mergeToRow(sEif, sEif, expectedDistSelf),
};

std::vector<std::vector<std::string>> expectedMaxDist5000_rows_small{
    mergeToRow(sTF, sTF, expectedDistSelf),
    mergeToRow(sTF, sMun, expectedDistUniMun),
    mergeToRow(sMun, sMun, expectedDistSelf),
    mergeToRow(sMun, sTF, expectedDistUniMun),
    mergeToRow(sEye, sEye, expectedDistSelf),
    mergeToRow(sLib, sLib, expectedDistSelf),
    mergeToRow(sEif, sEif, expectedDistSelf)};

std::vector<std::vector<std::string>> expectedMaxDist500000_rows_small{
    mergeToRow(sTF, sTF, expectedDistSelf),
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
    mergeToRow(sEif, sEye, expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist1000000_rows_small{
    mergeToRow(sTF, sTF, expectedDistSelf),
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
    mergeToRow(sEif, sEye, expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist10000000_rows_small{
    mergeToRow(sTF, sTF, expectedDistSelf),
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
    mergeToRow(sEif, sLib, expectedDistEifLib)};

std::vector<std::vector<std::string>> expectedMaxDist1_rows_small_wrong_point{
    mergeToRow(sMun, sMun, expectedDistSelf),
    mergeToRow(sEye, sEye, expectedDistSelf),
    mergeToRow(sLib, sLib, expectedDistSelf),
    mergeToRow(sEif, sEif, expectedDistSelf),
};

std::vector<std::vector<std::string>>
    expectedMaxDist5000_rows_small_wrong_point{
        mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sEif, sEif, expectedDistSelf)};

std::vector<std::vector<std::string>>
    expectedMaxDist500000_rows_small_wrong_point{
        mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sMun, sEif, expectedDistMunEif),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sEye, sEif, expectedDistEyeEif),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sEif, sEif, expectedDistSelf),
        mergeToRow(sEif, sMun, expectedDistMunEif),
        mergeToRow(sEif, sEye, expectedDistEyeEif)};

std::vector<std::vector<std::string>>
    expectedMaxDist1000000_rows_small_wrong_point{
        mergeToRow(sMun, sMun, expectedDistSelf),
        mergeToRow(sMun, sEif, expectedDistMunEif),
        mergeToRow(sMun, sEye, expectedDistMunEye),
        mergeToRow(sEye, sEye, expectedDistSelf),
        mergeToRow(sEye, sEif, expectedDistEyeEif),
        mergeToRow(sEye, sMun, expectedDistMunEye),
        mergeToRow(sLib, sLib, expectedDistSelf),
        mergeToRow(sEif, sEif, expectedDistSelf),
        mergeToRow(sEif, sMun, expectedDistMunEif),
        mergeToRow(sEif, sEye, expectedDistEyeEif)};

std::vector<std::vector<std::string>>
    expectedMaxDist10000000_rows_small_wrong_point{
        mergeToRow(sMun, sMun, expectedDistSelf),
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
        mergeToRow(sEif, sLib, expectedDistEifLib)};

std::vector<std::vector<std::string>> expectedMaxDist1_rows_diff{
    mergeToRow(TF, sTF, expectedDistSelf),
    mergeToRow(Mun, sMun, expectedDistSelf),
    mergeToRow(Eye, sEye, expectedDistSelf),
    mergeToRow(Lib, sLib, expectedDistSelf),
    mergeToRow(Eif, sEif, expectedDistSelf),
};

std::vector<std::vector<std::string>> expectedMaxDist5000_rows_diff{
    mergeToRow(TF, sTF, expectedDistSelf),
    mergeToRow(TF, sMun, expectedDistUniMun),
    mergeToRow(Mun, sMun, expectedDistSelf),
    mergeToRow(Mun, sTF, expectedDistUniMun),
    mergeToRow(Eye, sEye, expectedDistSelf),
    mergeToRow(Lib, sLib, expectedDistSelf),
    mergeToRow(Eif, sEif, expectedDistSelf)};

std::vector<std::vector<std::string>> expectedMaxDist500000_rows_diff{
    mergeToRow(TF, sTF, expectedDistSelf),
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
    mergeToRow(Eif, sEye, expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist1000000_rows_diff{
    mergeToRow(TF, sTF, expectedDistSelf),
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
    mergeToRow(Eif, sEye, expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist10000000_rows_diff{
    mergeToRow(TF, sTF, expectedDistSelf),
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
    mergeToRow(Eif, sLib, expectedDistEifLib)};

std::vector<std::vector<std::string>> expectedMaxDist1_rows_diffIDTable{
    mergeToRow({sTF.at(1)}, sTF, expectedDistSelf)};

std::vector<std::vector<std::string>> expectedMaxDist5000_rows_diffIDTable{
    mergeToRow({sTF.at(1)}, sTF, expectedDistSelf),
    mergeToRow({sTF.at(1)}, sMun, expectedDistUniMun)};

std::vector<std::vector<std::string>> expectedMaxDist500000_rows_diffIDTable{
    mergeToRow({sTF.at(1)}, sTF, expectedDistSelf),
    mergeToRow({sTF.at(1)}, sMun, expectedDistUniMun),
    mergeToRow({sTF.at(1)}, sEif, expectedDistUniEif)};

std::vector<std::vector<std::string>> expectedMaxDist1000000_rows_diffIDTable{
    mergeToRow({sTF.at(1)}, sTF, expectedDistSelf),
    mergeToRow({sTF.at(1)}, sMun, expectedDistUniMun),
    mergeToRow({sTF.at(1)}, sEif, expectedDistUniEif),
    mergeToRow({sTF.at(1)}, sEye, expectedDistUniEye)};

std::vector<std::vector<std::string>> expectedMaxDist10000000_rows_diffIDTable{
    mergeToRow({sTF.at(1)}, sTF, expectedDistSelf),
    mergeToRow({sTF.at(1)}, sMun, expectedDistUniMun),
    mergeToRow({sTF.at(1)}, sEif, expectedDistUniEif),
    mergeToRow({sTF.at(1)}, sEye, expectedDistUniEye),
    mergeToRow({sTF.at(1)}, sLib, expectedDistUniLib)};

std::vector<std::vector<std::string>> expectedNearestNeighbors1{
    mergeToRow(TF, TF, expectedDistSelf),
    mergeToRow(Mun, Mun, expectedDistSelf),
    mergeToRow(Eye, Eye, expectedDistSelf),
    mergeToRow(Lib, Lib, expectedDistSelf),
    mergeToRow(Eif, Eif, expectedDistSelf)};

std::vector<std::vector<std::string>> expectedNearestNeighbors2{
    mergeToRow(TF, TF, expectedDistSelf),
    mergeToRow(Mun, Mun, expectedDistSelf),
    mergeToRow(Eye, Eye, expectedDistSelf),
    mergeToRow(Lib, Lib, expectedDistSelf),
    mergeToRow(Eif, Eif, expectedDistSelf),
    mergeToRow(TF, Mun, expectedDistUniMun),
    mergeToRow(Mun, TF, expectedDistUniMun),
    mergeToRow(Eye, Eif, expectedDistEyeEif),
    mergeToRow(Lib, Eye, expectedDistEyeLib),
    mergeToRow(Eif, Eye, expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedNearestNeighbors2_400000{
    mergeToRow(TF, TF, expectedDistSelf),
    mergeToRow(Mun, Mun, expectedDistSelf),
    mergeToRow(Eye, Eye, expectedDistSelf),
    mergeToRow(Lib, Lib, expectedDistSelf),
    mergeToRow(Eif, Eif, expectedDistSelf),
    mergeToRow(TF, Mun, expectedDistUniMun),
    mergeToRow(Mun, TF, expectedDistUniMun),
    mergeToRow(Eye, Eif, expectedDistEyeEif),
    mergeToRow(Eif, Eye, expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedNearestNeighbors2_4000{
    mergeToRow(TF, TF, expectedDistSelf),
    mergeToRow(Mun, Mun, expectedDistSelf),
    mergeToRow(Eye, Eye, expectedDistSelf),
    mergeToRow(Lib, Lib, expectedDistSelf),
    mergeToRow(Eif, Eif, expectedDistSelf),
    mergeToRow(TF, Mun, expectedDistUniMun),
    mergeToRow(Mun, TF, expectedDistUniMun)};

std::vector<std::vector<std::string>> expectedNearestNeighbors2_40{
    mergeToRow(TF, TF, expectedDistSelf),
    mergeToRow(Mun, Mun, expectedDistSelf),
    mergeToRow(Eye, Eye, expectedDistSelf),
    mergeToRow(Lib, Lib, expectedDistSelf),
    mergeToRow(Eif, Eif, expectedDistSelf)};

std::vector<std::vector<std::string>> expectedNearestNeighbors3_500000{
    mergeToRow(TF, TF, expectedDistSelf),
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
    mergeToRow(Eif, TF, expectedDistUniEif)};

// test the compute result method on small examples
TEST_P(SpatialJoinParamTest, computeResultSmallDatasetLargeChildren) {
  std::vector<std::string> columnNames = {
      "?name1",  "?obj1",   "?geo1",
      "?point1", "?name2",  "?obj2",
      "?geo2",   "?point2", "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:1>", true,
                                        expectedMaxDist1_rows, columnNames);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:1>", false,
                                        expectedMaxDist1_rows, columnNames);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:5000>", true,
                                        expectedMaxDist5000_rows, columnNames);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:5000>", false,
                                        expectedMaxDist5000_rows, columnNames);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:500000>", true,
                                        expectedMaxDist500000_rows,
                                        columnNames);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:500000>",
                                        false, expectedMaxDist500000_rows,
                                        columnNames);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:1000000>",
                                        true, expectedMaxDist1000000_rows,
                                        columnNames);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:1000000>",
                                        false, expectedMaxDist1000000_rows,
                                        columnNames);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:10000000>",
                                        true, expectedMaxDist10000000_rows,
                                        columnNames);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:10000000>",
                                        false, expectedMaxDist10000000_rows,
                                        columnNames);
  buildAndTestSmallTestSetLargeChildren("<nearest-neighbors:1>", true,
                                        expectedNearestNeighbors1, columnNames);
  buildAndTestSmallTestSetLargeChildren("<nearest-neighbors:2>", true,
                                        expectedNearestNeighbors2, columnNames);
  buildAndTestSmallTestSetLargeChildren("<nearest-neighbors:2:400000>", true,
                                        expectedNearestNeighbors2_400000,
                                        columnNames);
  buildAndTestSmallTestSetLargeChildren("<nearest-neighbors:2:4000>", true,
                                        expectedNearestNeighbors2_4000,
                                        columnNames);
  buildAndTestSmallTestSetLargeChildren("<nearest-neighbors:2:40>", true,
                                        expectedNearestNeighbors2_40,
                                        columnNames);
  buildAndTestSmallTestSetLargeChildren("<nearest-neighbors:3:500000>", true,
                                        expectedNearestNeighbors3_500000,
                                        columnNames);
}

TEST_P(SpatialJoinParamTest, computeResultSmallDatasetSmallChildren) {
  std::vector<std::string> columnNames{"?obj1", "?point1", "?obj2", "?point2",
                                       "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:1>", true,
                                        expectedMaxDist1_rows_small,
                                        columnNames);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:1>", false,
                                        expectedMaxDist1_rows_small,
                                        columnNames);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:5000>", true,
                                        expectedMaxDist5000_rows_small,
                                        columnNames);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:5000>", false,
                                        expectedMaxDist5000_rows_small,
                                        columnNames);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:500000>", true,
                                        expectedMaxDist500000_rows_small,
                                        columnNames);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:500000>",
                                        false, expectedMaxDist500000_rows_small,
                                        columnNames);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:1000000>",
                                        true, expectedMaxDist1000000_rows_small,
                                        columnNames);
  buildAndTestSmallTestSetSmallChildren(
      "<max-distance-in-meters:1000000>", false,
      expectedMaxDist1000000_rows_small, columnNames);
  buildAndTestSmallTestSetSmallChildren(
      "<max-distance-in-meters:10000000>", true,
      expectedMaxDist10000000_rows_small, columnNames);
  buildAndTestSmallTestSetSmallChildren(
      "<max-distance-in-meters:10000000>", false,
      expectedMaxDist10000000_rows_small, columnNames);
}

TEST_P(SpatialJoinParamTest, computeResultSmallDatasetDifferentSizeChildren) {
  std::vector<std::string> columnNames{"?name1",
                                       "?obj1",
                                       "?geo1",
                                       "?point1",
                                       "?obj2",
                                       "?point2",
                                       "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:1>", true,
                                           expectedMaxDist1_rows_diff,
                                           columnNames, true);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:1>", false,
                                           expectedMaxDist1_rows_diff,
                                           columnNames, true);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:1>", true,
                                           expectedMaxDist1_rows_diff,
                                           columnNames, false);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:1>", false,
                                           expectedMaxDist1_rows_diff,
                                           columnNames, false);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:5000>",
                                           true, expectedMaxDist5000_rows_diff,
                                           columnNames, true);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:5000>",
                                           false, expectedMaxDist5000_rows_diff,
                                           columnNames, true);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:5000>",
                                           true, expectedMaxDist5000_rows_diff,
                                           columnNames, false);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:5000>",
                                           false, expectedMaxDist5000_rows_diff,
                                           columnNames, false);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:500000>", true, expectedMaxDist500000_rows_diff,
      columnNames, true);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:500000>", false, expectedMaxDist500000_rows_diff,
      columnNames, true);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:500000>", true, expectedMaxDist500000_rows_diff,
      columnNames, false);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:500000>", false, expectedMaxDist500000_rows_diff,
      columnNames, false);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:1000000>", true,
      expectedMaxDist1000000_rows_diff, columnNames, true);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:1000000>", false,
      expectedMaxDist1000000_rows_diff, columnNames, true);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:1000000>", true,
      expectedMaxDist1000000_rows_diff, columnNames, false);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:1000000>", false,
      expectedMaxDist1000000_rows_diff, columnNames, false);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:10000000>", true,
      expectedMaxDist10000000_rows_diff, columnNames, true);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:10000000>", false,
      expectedMaxDist10000000_rows_diff, columnNames, true);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:10000000>", true,
      expectedMaxDist10000000_rows_diff, columnNames, false);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:10000000>", false,
      expectedMaxDist10000000_rows_diff, columnNames, false);
}

TEST_P(SpatialJoinParamTest, maxSizeMaxDistanceTest) {
  auto maxDist = std::numeric_limits<long long>::max();
  std::string maxDistStr =
      absl::StrCat("<max-distance-in-meters:", maxDist, ">");

  // test small children
  std::vector<std::string> columnNames{"?obj1", "?point1", "?obj2", "?point2",
                                       "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetSmallChildren(
      maxDistStr, true, expectedMaxDist10000000_rows_small, columnNames);
  buildAndTestSmallTestSetSmallChildren(
      maxDistStr, false, expectedMaxDist10000000_rows_small, columnNames);

  // test diff size children
  columnNames = {"?name1",
                 "?obj1",
                 "?geo1",
                 "?point1",
                 "?obj2",
                 "?point2",
                 "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetDiffSizeChildren(
      maxDistStr, true, expectedMaxDist10000000_rows_diff, columnNames, false);
  buildAndTestSmallTestSetDiffSizeChildren(
      maxDistStr, false, expectedMaxDist10000000_rows_diff, columnNames, false);

  // test large size children
  columnNames = {"?name1",  "?obj1",   "?geo1",
                 "?point1", "?name2",  "?obj2",
                 "?geo2",   "?point2", "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetLargeChildren(
      maxDistStr, true, expectedMaxDist10000000_rows, columnNames);
  buildAndTestSmallTestSetLargeChildren(
      maxDistStr, false, expectedMaxDist10000000_rows, columnNames);
}

TEST_P(SpatialJoinParamTest, diffSizeIdTables) {
  std::vector<std::string> columnNames{"?point1", "?obj2", "?point2",
                                       "?distOfTheTwoObjectsAddedInternally"};
  testDiffSizeIdTables("<max-distance-in-meters:1>", true,
                       expectedMaxDist1_rows_diffIDTable, columnNames, true);
  testDiffSizeIdTables("<max-distance-in-meters:1>", true,
                       expectedMaxDist1_rows_diffIDTable, columnNames, false);
  testDiffSizeIdTables("<max-distance-in-meters:1>", false,
                       expectedMaxDist1_rows_diffIDTable, columnNames, true);
  testDiffSizeIdTables("<max-distance-in-meters:1>", false,
                       expectedMaxDist1_rows_diffIDTable, columnNames, false);
  testDiffSizeIdTables("<max-distance-in-meters:5000>", true,
                       expectedMaxDist5000_rows_diffIDTable, columnNames, true);
  testDiffSizeIdTables("<max-distance-in-meters:5000>", true,
                       expectedMaxDist5000_rows_diffIDTable, columnNames,
                       false);
  testDiffSizeIdTables("<max-distance-in-meters:5000>", false,
                       expectedMaxDist5000_rows_diffIDTable, columnNames, true);
  testDiffSizeIdTables("<max-distance-in-meters:5000>", false,
                       expectedMaxDist5000_rows_diffIDTable, columnNames,
                       false);
  testDiffSizeIdTables("<max-distance-in-meters:500000>", true,
                       expectedMaxDist500000_rows_diffIDTable, columnNames,
                       true);
  testDiffSizeIdTables("<max-distance-in-meters:500000>", true,
                       expectedMaxDist500000_rows_diffIDTable, columnNames,
                       false);
  testDiffSizeIdTables("<max-distance-in-meters:500000>", false,
                       expectedMaxDist500000_rows_diffIDTable, columnNames,
                       true);
  testDiffSizeIdTables("<max-distance-in-meters:500000>", false,
                       expectedMaxDist500000_rows_diffIDTable, columnNames,
                       false);
  testDiffSizeIdTables("<max-distance-in-meters:1000000>", true,
                       expectedMaxDist1000000_rows_diffIDTable, columnNames,
                       true);
  testDiffSizeIdTables("<max-distance-in-meters:1000000>", true,
                       expectedMaxDist1000000_rows_diffIDTable, columnNames,
                       false);
  testDiffSizeIdTables("<max-distance-in-meters:1000000>", false,
                       expectedMaxDist1000000_rows_diffIDTable, columnNames,
                       true);
  testDiffSizeIdTables("<max-distance-in-meters:1000000>", false,
                       expectedMaxDist1000000_rows_diffIDTable, columnNames,
                       false);
  testDiffSizeIdTables("<max-distance-in-meters:10000000>", true,
                       expectedMaxDist10000000_rows_diffIDTable, columnNames,
                       true);
  testDiffSizeIdTables("<max-distance-in-meters:10000000>", true,
                       expectedMaxDist10000000_rows_diffIDTable, columnNames,
                       false);
  testDiffSizeIdTables("<max-distance-in-meters:10000000>", false,
                       expectedMaxDist10000000_rows_diffIDTable, columnNames,
                       true);
  testDiffSizeIdTables("<max-distance-in-meters:10000000>", false,
                       expectedMaxDist10000000_rows_diffIDTable, columnNames,
                       false);
}

TEST_P(SpatialJoinParamTest, wrongPointInInput) {
  // expected behavior: point is skipped
  std::vector<std::string> columnNames{"?obj1", "?point1", "?obj2", "?point2",
                                       "?distOfTheTwoObjectsAddedInternally"};
  testWrongPointInInput("<max-distance-in-meters:1>", true,
                        expectedMaxDist1_rows_small_wrong_point, columnNames);
  testWrongPointInInput("<max-distance-in-meters:1>", false,
                        expectedMaxDist1_rows_small_wrong_point, columnNames);
  testWrongPointInInput("<max-distance-in-meters:5000>", true,
                        expectedMaxDist5000_rows_small_wrong_point,
                        columnNames);
  testWrongPointInInput("<max-distance-in-meters:5000>", false,
                        expectedMaxDist5000_rows_small_wrong_point,
                        columnNames);
  testWrongPointInInput("<max-distance-in-meters:500000>", true,
                        expectedMaxDist500000_rows_small_wrong_point,
                        columnNames);
  testWrongPointInInput("<max-distance-in-meters:500000>", false,
                        expectedMaxDist500000_rows_small_wrong_point,
                        columnNames);
  testWrongPointInInput("<max-distance-in-meters:1000000>", true,
                        expectedMaxDist1000000_rows_small_wrong_point,
                        columnNames);
  testWrongPointInInput("<max-distance-in-meters:1000000>", false,
                        expectedMaxDist1000000_rows_small_wrong_point,
                        columnNames);
  testWrongPointInInput("<max-distance-in-meters:10000000>", true,
                        expectedMaxDist10000000_rows_small_wrong_point,
                        columnNames);
  testWrongPointInInput("<max-distance-in-meters:10000000>", false,
                        expectedMaxDist10000000_rows_small_wrong_point,
                        columnNames);
}

INSTANTIATE_TEST_SUITE_P(
    SpatialJoin, SpatialJoinParamTest,
    ::testing::Values(SpatialJoin::Algorithm::Baseline,
                      SpatialJoin::Algorithm::S2Geometry,
                      SpatialJoin::Algorithm::BoundingBox));

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
    bool within = spatialJoinAlg->OnlyForTestingWrapperContainedInBoundingBoxes(
        bbox, point1);
    if (!within) {
      GeoPoint geo1{point1.get<1>(), point1.get<0>()};
      GeoPoint geo2{startPoint.get<1>(), startPoint.get<0>()};
      double dist = ad_utility::detail::wktDistImpl(geo1, geo2) * 1000;
      ASSERT_GT(static_cast<long long>(dist), maxDistInMeters);
    }
  };

  PreparedSpatialJoinParams params{
      nullptr, nullptr, nullptr,         nullptr,     0,
      0,       1,       maxDistInMeters, std::nullopt};

  std::variant<NearestNeighborsConfig, MaxDistanceConfig> config{
      MaxDistanceConfig{maxDistInMeters}};

  SpatialJoinAlgorithms spatialJoinAlgs{buildTestQEC(), params, true, config};

  std::vector<Box> bbox =
      spatialJoinAlgs.OnlyForTestingWrapperComputeBoundingBox(startPoint);
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
  // build dummy join to access the containedInBoundingBox and
  // computeBoundingBox functions
  auto qec = buildTestQEC();
  auto spatialJoinTriple = SparqlTriple{TripleComponent{Variable{"?point1"}},
                                        "<max-distance-in-meters:1000>",
                                        TripleComponent{Variable{"?point2"}}};
  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, spatialJoinTriple,
                                                 std::nullopt, std::nullopt);

  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  PreparedSpatialJoinParams params{
      nullptr,     nullptr, nullptr, nullptr,
      0,           0,       1,       spatialJoin->getMaxDist(),
      std::nullopt};

  SpatialJoinAlgorithms spatialJoinAlgs{
      qec, params, true, spatialJoin->onlyForTestingGetActualConfig()};

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
                  ASSERT_TRUE(
                      spatialJoinAlgs
                          .OnlyForTestingWrapperContainedInBoundingBoxes(
                              toTest, shouldBeContained.at(i).at(k)));
                }
              }
              // test all points, which shouldn't be contained in the bounding
              // boxes
              for (size_t i = 0; i < shouldNotBeContained.size(); i++) {
                for (size_t k = 0; k < shouldNotBeContained.at(i).size(); k++) {
                  ASSERT_FALSE(
                      spatialJoinAlgs
                          .OnlyForTestingWrapperContainedInBoundingBoxes(
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

}  // namespace boundingBox

}  // namespace

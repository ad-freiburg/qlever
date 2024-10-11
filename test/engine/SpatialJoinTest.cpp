#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <regex>

#include "../util/IndexTestHelpers.h"
#include "./../../src/global/ValueId.h"
#include "./../../src/util/GeoSparqlHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "parser/data/Variable.h"


#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/foreach.hpp>

namespace {  // anonymous namespace to avoid linker problems

using namespace ad_utility::testing;

auto makePointLiteral = [](std::string_view c1, std::string_view c2) {
  return absl::StrCat(" \"POINT(", c1, " ", c2, ")\"^^<", GEO_WKT_LITERAL, ">");
};

namespace localTestHelpers {

// helper function to create a vector of strings from a result table
std::vector<std::string> printTable(const QueryExecutionContext* qec,
                                    const Result* table) {
  std::vector<std::string> output;
  for (size_t i = 0; i < table->idTable().numRows(); i++) {
    std::string line = "";
    for (size_t k = 0; k < table->idTable().numColumns(); k++) {
      auto test = ExportQueryExecutionTrees::idToStringAndType(
          qec->getIndex(), table->idTable().at(i, k), {});
      line += test.value().first;
      line += " ";
    }
    output.push_back(line.substr(0, line.size() - 1));  // ignore last " "
  }
  return output;
}

// this helper function reorders an input vector according to the variable to
// column map to make the string array match the order of the result, which
// should be tested (it uses a vector of vectors (the first vector is containing
// each column of the result, each column consist of a vector, where each entry
// is a row of this column))
std::vector<std::vector<std::string>> orderColAccordingToVarColMap(
    VariableToColumnMap varColMaps,
    std::vector<std::vector<std::string>> columns,
    std::vector<std::string> columnNames) {
  std::vector<std::vector<std::string>> result;
  auto indVariableMap = copySortedByColumnIndex(varColMaps);
  for (size_t i = 0; i < indVariableMap.size(); i++) {
    for (size_t k = 0; k < columnNames.size(); k++) {
      if (indVariableMap.at(i).first.name() == columnNames.at(k)) {
        result.push_back(columns.at(k));
        break;
      }
    }
  }
  return result;
}

// helper function to create a vector of strings representing rows, from a
// vector of strings representing columns. Please make sure, that the order of
// the columns is already matching the order of the result. If this is not the
// case call the function orderColAccordingToVarColMap
std::vector<std::string> createRowVectorFromColumnVector(
    std::vector<std::vector<std::string>> column_vector) {
  std::vector<std::string> result;
  if (column_vector.size() > 0) {
    for (size_t i = 0; i < column_vector.at(0).size(); i++) {
      std::string str = "";
      for (size_t k = 0; k < column_vector.size(); k++) {
        str += column_vector.at(k).at(i);
        str += " ";
      }
      result.push_back(str.substr(0, str.size() - 1));
    }
  }
  return result;
}

// create a small test dataset, which focuses on points as geometry objects.
// Note, that some of these objects have a polygon representation, but for
// testing purposes, they get represented as a point here. I took those
// points, such that it is obvious, which pair of objects should be included,
// when the maximum distance is x meters. Please note, that these datapoints
// are not copied from a real input file. Copying the query will therefore
// likely not result in the same results as here (also the names, coordinates,
// etc. might be different in the real datasets)
std::string createSmallDatasetWithPoints() {
  auto addPoint = [](std::string& kg, std::string number, std::string name,
                     std::string point) {
    kg += absl::StrCat("<node_", number, "> <name> ", name, " .<node_", number,
                       "> <hasGeometry> <geometry", number, "> .<geometry",
                       number, "> <asWKT> ", point, " .");
  };
  std::string kg2;
  auto p = makePointLiteral;
  addPoint(kg2, "1", "\"Uni Freiburg TF\"", p("7.83505", "48.01267"));
  addPoint(kg2, "2", "\"Minster Freiburg\"", p("7.85298", "47.99557"));
  addPoint(kg2, "3", "\"London Eye\"", p("-0.11957", "51.50333"));
  addPoint(kg2, "4", "\"Statue of liberty\"", p("-74.04454", "40.68925"));
  addPoint(kg2, "5", "\"eiffel tower\"", p("2.29451", "48.85825"));

  return kg2;
}

QueryExecutionContext* buildTestQEC() {
  std::string kg = localTestHelpers::createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  return qec;
}

};  // namespace localTestHelpers

namespace computeResultTest {

std::shared_ptr<QueryExecutionTree> buildIndexScan(
    QueryExecutionContext* qec, std::array<std::string, 3> triple) {
  TripleComponent subject{Variable{triple.at(0)}};
  TripleComponent object{Variable{triple.at(2)}};
  return ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO, SparqlTriple{subject, triple.at(1), object});
}

std::shared_ptr<QueryExecutionTree> buildJoin(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> tree1,
    std::shared_ptr<QueryExecutionTree> tree2, Variable joinVariable) {
  auto varCol1 = tree1->getVariableColumns();
  auto varCol2 = tree2->getVariableColumns();
  size_t col1 = varCol1[joinVariable].columnIndex_;
  size_t col2 = varCol2[joinVariable].columnIndex_;
  return ad_utility::makeExecutionTree<Join>(qec, tree1, tree2, col1, col2,
                                             true);
}

std::shared_ptr<QueryExecutionTree> buildMediumChild(
    QueryExecutionContext* qec, std::array<std::string, 3> triple1,
    std::array<std::string, 3> triple2, std::array<std::string, 3> triple3,
    std::string joinVariable1_, std::string joinVariable2_) {
  Variable joinVariable1{joinVariable1_};
  Variable joinVariable2{joinVariable2_};
  auto scan1 = buildIndexScan(qec, triple1);
  auto scan2 = buildIndexScan(qec, triple2);
  auto scan3 = buildIndexScan(qec, triple3);
  auto join = buildJoin(qec, scan1, scan2, joinVariable1);
  return buildJoin(qec, join, scan3, joinVariable2);
}

std::shared_ptr<QueryExecutionTree> buildSmallChild(
    QueryExecutionContext* qec, std::array<std::string, 3> triple1,
    std::array<std::string, 3> triple2, std::string joinVariable_) {
  Variable joinVariable{joinVariable_};
  auto scan1 = buildIndexScan(qec, triple1);
  auto scan2 = buildIndexScan(qec, triple2);
  return buildJoin(qec, scan1, scan2, joinVariable);
}

void createAndTestSpatialJoin(
    QueryExecutionContext* qec, SparqlTriple spatialJoinTriple,
    std::shared_ptr<QueryExecutionTree> leftChild,
    std::shared_ptr<QueryExecutionTree> rightChild, bool addLeftChildFirst,
    std::vector<std::vector<std::string>> expectedOutputUnorderedRows,
    std::vector<std::string> columnNames, bool useBaselineAlgorithm) {
  // this function is like transposing a matrix. An entry which has been stored
  // at (i, k) is now stored at (k, i). The reason this is needed is the
  // following: this function receives the input as a vector of vector of
  // strings. Each vector contains a vector, which contains a row of the result.
  // This row contains all columns. After swapping, each vector contains a
  // vector, which contains all entries of one column. As now each of the
  // vectors contain only one column, we can later order them according to the
  // variable to column map and then compare the result.
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
  auto expectedOutputOrdered = localTestHelpers::orderColAccordingToVarColMap(
      spatialJoin->computeVariableToColumnMap(), expectedMaxDistCols,
      columnNames);
  auto expectedOutput =
      localTestHelpers::createRowVectorFromColumnVector(expectedOutputOrdered);

  spatialJoin->onlyForTestingSetUseBaselineAlgorithm(useBaselineAlgorithm);
  auto res = spatialJoin->computeResult(false);
  auto vec = localTestHelpers::printTable(qec, &res);
  EXPECT_THAT(vec, ::testing::UnorderedElementsAreArray(expectedOutput));
}

// build the test using the small dataset. Let the SpatialJoin operation be the
// last one (the left and right child are maximally large for this test query)
// the following Query will be simulated, the max distance will be different
// depending on the test:
// Select * where {
//   ?obj1 <name> ?name1 .
//   ?obj1 <hasGeometry> ?geo1 .
//   ?geo1 <asWKT> ?point1
//   ?obj2 <name> ?name2 .
//   ?obj2 <hasGeometry> ?geo2 .
//   ?geo2 <asWKT> ?point2
//   ?point1 <max-distance-in-meters:XXXX> ?point2 .
// }
void buildAndTestSmallTestSetLargeChildren(
    std::string maxDistanceInMetersString, bool addLeftChildFirst,
    std::vector<std::vector<std::string>> expectedOutput,
    std::vector<std::string> columnNames, bool useBaselineAlgorithm) {
  auto qec = localTestHelpers::buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ===================== build the first child ===============================
  auto leftChild = buildMediumChild(
      qec, {"?obj1", std::string{"<name>"}, "?name1"},
      {"?obj1", std::string{"<hasGeometry>"}, "?geo1"},
      {"?geo1", std::string{"<asWKT>"}, "?point1"}, "?obj1", "?geo1");

  // ======================= build the second child ============================
  auto rightChild = buildMediumChild(
      qec, {"?obj2", std::string{"<name>"}, "?name2"},
      {"?obj2", std::string{"<hasGeometry>"}, "?geo2"},
      {"?geo2", std::string{"<asWKT>"}, "?point2"}, "?obj2", "?geo2");

  createAndTestSpatialJoin(qec,
                           SparqlTriple{TripleComponent{Variable{"?point1"}},
                                        maxDistanceInMetersString,
                                        TripleComponent{Variable{"?point2"}}},
                           leftChild, rightChild, addLeftChildFirst,
                           expectedOutput, columnNames, useBaselineAlgorithm);
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
    std::string maxDistanceInMetersString, bool addLeftChildFirst,
    std::vector<std::vector<std::string>> expectedOutput,
    std::vector<std::string> columnNames, bool useBaselineAlgorithm) {
  auto qec = localTestHelpers::buildTestQEC();
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
      qec, SparqlTriple{point1, maxDistanceInMetersString, point2}, leftChild,
      rightChild, addLeftChildFirst, expectedOutput, columnNames, useBaselineAlgorithm);
}

// build the test using the small dataset. Let the SpatialJoin operation be the
// last one.
// the following Query will be simulated, the max distance will be different
// depending on the test:
// Select * where {
//   ?obj1 <name> ?name1 .
//   ?obj1 <hasGeometry> ?geo1 .
//   ?geo1 <asWKT> ?point1
//   ?geo2 <asWKT> ?point2
//   ?point1 <max-distance-in-meters:XXXX> ?point2 .
// }
void buildAndTestSmallTestSetDiffSizeChildren(
    std::string maxDistanceInMetersString, bool addLeftChildFirst,
    std::vector<std::vector<std::string>> expectedOutput,
    std::vector<std::string> columnNames, bool bigChildLeft, bool useBaselineAlgorithm) {
  auto qec = localTestHelpers::buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ========================= build big child =================================
  auto bigChild = buildMediumChild(
      qec, {"?obj1", std::string{"<name>"}, "?name1"},
      {"?obj1", std::string{"<hasGeometry>"}, "?geo1"},
      {"?geo1", std::string{"<asWKT>"}, "?point1"}, "?obj1", "?geo1");

  // ========================= build small child ===============================
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
      qec,
      SparqlTriple{firstVariable, maxDistanceInMetersString, secondVariable},
      firstChild, secondChild, addLeftChildFirst, expectedOutput, columnNames, useBaselineAlgorithm);
}

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

std::vector<std::vector<std::string>> unordered_rows_small{
    std::vector<std::string>{"<geometry1>", "POINT(7.835050 48.012670)"},
    std::vector<std::string>{"<geometry2>", "POINT(7.852980 47.995570)"},
    std::vector<std::string>{"<geometry3>", "POINT(-0.119570 51.503330)"},
    std::vector<std::string>{"<geometry4>", "POINT(-74.044540 40.689250)"},
    std::vector<std::string>{"<geometry5>", "POINT(2.294510 48.858250)"}};

// in all calculations below, the factor 1000 is used to convert from km to m

// distance from the object to itself should be zero
std::vector<std::string> expectedDistSelf{"0"};

// distance from Uni Freiburg to Freiburger Müsnster is 2,33 km according to
// google maps
auto P = [](double x, double y) { return GeoPoint(y, x); };

std::vector<std::string> expectedDistUniMun{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(P(7.83505, 48.01267),
                                                     P(7.85298, 47.99557)) *
                     1000))};

// distance from Uni Freiburg to Eiffel Tower is 419,32 km according to
// google maps
std::vector<std::string> expectedDistUniEif{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(P(7.83505, 48.01267),
                                                     P(2.29451, 48.85825)) *
                     1000))};

// distance from Minster Freiburg to eiffel tower is 421,09 km according to
// google maps
std::vector<std::string> expectedDistMunEif{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(P(7.85298, 47.99557),
                                                     P(2.29451, 48.85825)) *
                     1000))};

// distance from london eye to eiffel tower is 340,62 km according to
// google maps
std::vector<std::string> expectedDistEyeEif{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(P(-0.11957, 51.50333),
                                                     P(2.29451, 48.85825)) *
                     1000))};

// distance from Uni Freiburg to London Eye is 690,18 km according to
// google maps
std::vector<std::string> expectedDistUniEye{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(P(7.83505, 48.01267),
                                                     P(-0.11957, 51.50333)) *
                     1000))};

// distance from Minster Freiburg to London Eye is 692,39 km according to
// google maps
std::vector<std::string> expectedDistMunEye{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(P(7.85298, 47.99557),
                                                     P(-0.11957, 51.50333)) *
                     1000))};

// distance from Uni Freiburg to Statue of Liberty is 6249,55 km according to
// google maps
std::vector<std::string> expectedDistUniLib{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(P(7.83505, 48.01267),
                                                     P(-74.04454, 40.68925)) *
                     1000))};

// distance from Minster Freiburg to Statue of Liberty is 6251,58 km
// according to google maps
std::vector<std::string> expectedDistMunLib{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(P(7.85298, 47.99557),
                                                     P(-74.04454, 40.68925)) *
                     1000))};

// distance from london eye to statue of liberty is 5575,08 km according to
// google maps
std::vector<std::string> expectedDistEyeLib{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(P(-0.11957, 51.50333),
                                                     P(-74.04454, 40.68925)) *
                     1000))};

// distance from eiffel tower to Statue of liberty is 5837,42 km according to
// google maps
std::vector<std::string> expectedDistEifLib{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(P(2.29451, 48.85825),
                                                     P(-74.04454, 40.68925)) *
                     1000))};

std::vector<std::vector<std::string>> expectedMaxDist1_rows{
    mergeToRow(unordered_rows.at(0), unordered_rows.at(0), expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(1), expectedDistSelf),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(2), expectedDistSelf),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(3), expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(4), expectedDistSelf)};

std::vector<std::vector<std::string>> expectedMaxDist5000_rows{
    mergeToRow(unordered_rows.at(0), unordered_rows.at(0), expectedDistSelf),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(1), expectedDistUniMun),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(1), expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(0), expectedDistUniMun),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(2), expectedDistSelf),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(3), expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(4), expectedDistSelf)};

std::vector<std::vector<std::string>> expectedMaxDist500000_rows{
    mergeToRow(unordered_rows.at(0), unordered_rows.at(0), expectedDistSelf),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(1), expectedDistUniMun),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(4), expectedDistUniEif),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(1), expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(0), expectedDistUniMun),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(4), expectedDistMunEif),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(2), expectedDistSelf),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(4), expectedDistEyeEif),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(3), expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(4), expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(0), expectedDistUniEif),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(1), expectedDistMunEif),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(2), expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist1000000_rows{
    mergeToRow(unordered_rows.at(0), unordered_rows.at(0), expectedDistSelf),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(1), expectedDistUniMun),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(4), expectedDistUniEif),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(2), expectedDistUniEye),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(1), expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(0), expectedDistUniMun),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(4), expectedDistMunEif),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(2), expectedDistMunEye),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(2), expectedDistSelf),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(4), expectedDistEyeEif),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(0), expectedDistUniEye),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(1), expectedDistMunEye),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(3), expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(4), expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(0), expectedDistUniEif),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(1), expectedDistMunEif),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(2), expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist10000000_rows{
    mergeToRow(unordered_rows.at(0), unordered_rows.at(0), expectedDistSelf),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(1), expectedDistUniMun),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(4), expectedDistUniEif),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(2), expectedDistUniEye),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(3), expectedDistUniLib),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(1), expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(0), expectedDistUniMun),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(4), expectedDistMunEif),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(2), expectedDistMunEye),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(3), expectedDistMunLib),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(2), expectedDistSelf),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(4), expectedDistEyeEif),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(0), expectedDistUniEye),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(1), expectedDistMunEye),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(3), expectedDistEyeLib),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(3), expectedDistSelf),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(0), expectedDistUniLib),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(1), expectedDistMunLib),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(2), expectedDistEyeLib),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(4), expectedDistEifLib),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(4), expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(0), expectedDistUniEif),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(1), expectedDistMunEif),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(2), expectedDistEyeEif),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(3), expectedDistEifLib)};

std::vector<std::vector<std::string>> expectedMaxDist1_rows_small{
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(0),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(1),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(2),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(3), unordered_rows_small.at(3),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(4),
               expectedDistSelf),
};

std::vector<std::vector<std::string>> expectedMaxDist5000_rows_small{
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(0),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(1),
               expectedDistUniMun),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(1),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(0),
               expectedDistUniMun),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(2),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(3), unordered_rows_small.at(3),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(4),
               expectedDistSelf)};

std::vector<std::vector<std::string>> expectedMaxDist500000_rows_small{
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(0),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(1),
               expectedDistUniMun),
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(4),
               expectedDistUniEif),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(1),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(0),
               expectedDistUniMun),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(4),
               expectedDistMunEif),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(2),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(4),
               expectedDistEyeEif),
    mergeToRow(unordered_rows_small.at(3), unordered_rows_small.at(3),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(4),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(0),
               expectedDistUniEif),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(1),
               expectedDistMunEif),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(2),
               expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist1000000_rows_small{
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(0),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(1),
               expectedDistUniMun),
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(4),
               expectedDistUniEif),
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(2),
               expectedDistUniEye),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(1),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(0),
               expectedDistUniMun),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(4),
               expectedDistMunEif),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(2),
               expectedDistMunEye),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(2),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(4),
               expectedDistEyeEif),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(0),
               expectedDistUniEye),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(1),
               expectedDistMunEye),
    mergeToRow(unordered_rows_small.at(3), unordered_rows_small.at(3),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(4),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(0),
               expectedDistUniEif),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(1),
               expectedDistMunEif),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(2),
               expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist10000000_rows_small{
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(0),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(1),
               expectedDistUniMun),
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(4),
               expectedDistUniEif),
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(2),
               expectedDistUniEye),
    mergeToRow(unordered_rows_small.at(0), unordered_rows_small.at(3),
               expectedDistUniLib),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(1),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(0),
               expectedDistUniMun),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(4),
               expectedDistMunEif),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(2),
               expectedDistMunEye),
    mergeToRow(unordered_rows_small.at(1), unordered_rows_small.at(3),
               expectedDistMunLib),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(2),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(4),
               expectedDistEyeEif),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(0),
               expectedDistUniEye),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(1),
               expectedDistMunEye),
    mergeToRow(unordered_rows_small.at(2), unordered_rows_small.at(3),
               expectedDistEyeLib),
    mergeToRow(unordered_rows_small.at(3), unordered_rows_small.at(3),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(3), unordered_rows_small.at(0),
               expectedDistUniLib),
    mergeToRow(unordered_rows_small.at(3), unordered_rows_small.at(1),
               expectedDistMunLib),
    mergeToRow(unordered_rows_small.at(3), unordered_rows_small.at(2),
               expectedDistEyeLib),
    mergeToRow(unordered_rows_small.at(3), unordered_rows_small.at(4),
               expectedDistEifLib),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(4),
               expectedDistSelf),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(0),
               expectedDistUniEif),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(1),
               expectedDistMunEif),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(2),
               expectedDistEyeEif),
    mergeToRow(unordered_rows_small.at(4), unordered_rows_small.at(3),
               expectedDistEifLib)};

std::vector<std::vector<std::string>> expectedMaxDist1_rows_diff{
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(0),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(1),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(2),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(3), unordered_rows_small.at(3),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(4),
               expectedDistSelf),
};

std::vector<std::vector<std::string>> expectedMaxDist5000_rows_diff{
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(0),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(1),
               expectedDistUniMun),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(1),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(0),
               expectedDistUniMun),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(2),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(3), unordered_rows_small.at(3),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(4),
               expectedDistSelf)};

std::vector<std::vector<std::string>> expectedMaxDist500000_rows_diff{
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(0),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(1),
               expectedDistUniMun),
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(4),
               expectedDistUniEif),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(1),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(0),
               expectedDistUniMun),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(4),
               expectedDistMunEif),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(2),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(4),
               expectedDistEyeEif),
    mergeToRow(unordered_rows.at(3), unordered_rows_small.at(3),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(4),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(0),
               expectedDistUniEif),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(1),
               expectedDistMunEif),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(2),
               expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist1000000_rows_diff{
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(0),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(1),
               expectedDistUniMun),
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(4),
               expectedDistUniEif),
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(2),
               expectedDistUniEye),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(1),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(0),
               expectedDistUniMun),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(4),
               expectedDistMunEif),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(2),
               expectedDistMunEye),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(2),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(4),
               expectedDistEyeEif),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(0),
               expectedDistUniEye),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(1),
               expectedDistMunEye),
    mergeToRow(unordered_rows.at(3), unordered_rows_small.at(3),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(4),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(0),
               expectedDistUniEif),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(1),
               expectedDistMunEif),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(2),
               expectedDistEyeEif)};

std::vector<std::vector<std::string>> expectedMaxDist10000000_rows_diff{
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(0),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(1),
               expectedDistUniMun),
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(4),
               expectedDistUniEif),
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(2),
               expectedDistUniEye),
    mergeToRow(unordered_rows.at(0), unordered_rows_small.at(3),
               expectedDistUniLib),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(1),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(0),
               expectedDistUniMun),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(4),
               expectedDistMunEif),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(2),
               expectedDistMunEye),
    mergeToRow(unordered_rows.at(1), unordered_rows_small.at(3),
               expectedDistMunLib),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(2),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(4),
               expectedDistEyeEif),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(0),
               expectedDistUniEye),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(1),
               expectedDistMunEye),
    mergeToRow(unordered_rows.at(2), unordered_rows_small.at(3),
               expectedDistEyeLib),
    mergeToRow(unordered_rows.at(3), unordered_rows_small.at(3),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(3), unordered_rows_small.at(0),
               expectedDistUniLib),
    mergeToRow(unordered_rows.at(3), unordered_rows_small.at(1),
               expectedDistMunLib),
    mergeToRow(unordered_rows.at(3), unordered_rows_small.at(2),
               expectedDistEyeLib),
    mergeToRow(unordered_rows.at(3), unordered_rows_small.at(4),
               expectedDistEifLib),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(4),
               expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(0),
               expectedDistUniEif),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(1),
               expectedDistMunEif),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(2),
               expectedDistEyeEif),
    mergeToRow(unordered_rows.at(4), unordered_rows_small.at(3),
               expectedDistEifLib)};

// test the compute result method on small examples
void computeResultSmallDatasetLargeChildren(bool useBaselineAlgorithm) {
  std::vector<std::string> columnNames = {
      "?name1",  "?obj1",   "?geo1",
      "?point1", "?name2",  "?obj2",
      "?geo2",   "?point2", "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:1>", true,
                                        expectedMaxDist1_rows, columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:1>", false,
                                        expectedMaxDist1_rows, columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:5000>", true,
                                        expectedMaxDist5000_rows, columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:5000>", false,
                                        expectedMaxDist5000_rows, columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:500000>", true,
                                        expectedMaxDist500000_rows,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:500000>",
                                        false, expectedMaxDist500000_rows,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:1000000>",
                                        true, expectedMaxDist1000000_rows,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:1000000>",
                                        false, expectedMaxDist1000000_rows,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:10000000>",
                                        true, expectedMaxDist10000000_rows,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetLargeChildren("<max-distance-in-meters:10000000>",
                                        false, expectedMaxDist10000000_rows,
                                        columnNames, useBaselineAlgorithm);
}

void computeResultSmallDatasetSmallChildren(bool useBaselineAlgorithm) {
  std::vector<std::string> columnNames{"?obj1", "?point1", "?obj2", "?point2",
                                       "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:1>", true,
                                        expectedMaxDist1_rows_small,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:1>", false,
                                        expectedMaxDist1_rows_small,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:5000>", true,
                                        expectedMaxDist5000_rows_small,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:5000>", false,
                                        expectedMaxDist5000_rows_small,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:500000>", true,
                                        expectedMaxDist500000_rows_small,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:500000>",
                                        false, expectedMaxDist500000_rows_small,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetSmallChildren("<max-distance-in-meters:1000000>",
                                        true, expectedMaxDist1000000_rows_small,
                                        columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetSmallChildren(
      "<max-distance-in-meters:1000000>", false,
      expectedMaxDist1000000_rows_small, columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetSmallChildren(
      "<max-distance-in-meters:10000000>", true,
      expectedMaxDist10000000_rows_small, columnNames, useBaselineAlgorithm);
  buildAndTestSmallTestSetSmallChildren(
      "<max-distance-in-meters:10000000>", false,
      expectedMaxDist10000000_rows_small, columnNames, useBaselineAlgorithm);
}

void computeResultSmallDatasetDifferentSizeChildren(bool useBaselineAlgorithm) {
  std::vector<std::string> columnNames{"?name1",
                                       "?obj1",
                                       "?geo1",
                                       "?point1",
                                       "?obj2",
                                       "?point2",
                                       "?distOfTheTwoObjectsAddedInternally"};
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:1>", true,
                                           expectedMaxDist1_rows_diff,
                                           columnNames, true, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:1>", false,
                                           expectedMaxDist1_rows_diff,
                                           columnNames, true, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:1>", true,
                                           expectedMaxDist1_rows_diff,
                                           columnNames, false, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:1>", false,
                                           expectedMaxDist1_rows_diff,
                                           columnNames, false, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:5000>",
                                           true, expectedMaxDist5000_rows_diff,
                                           columnNames, true, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:5000>",
                                           false, expectedMaxDist5000_rows_diff,
                                           columnNames, true, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:5000>",
                                           true, expectedMaxDist5000_rows_diff,
                                           columnNames, false, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren("<max-distance-in-meters:5000>",
                                           false, expectedMaxDist5000_rows_diff,
                                           columnNames, false, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:500000>", true, expectedMaxDist500000_rows_diff,
      columnNames, true, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:500000>", false, expectedMaxDist500000_rows_diff,
      columnNames, true, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:500000>", true, expectedMaxDist500000_rows_diff,
      columnNames, false, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:500000>", false, expectedMaxDist500000_rows_diff,
      columnNames, false, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:1000000>", true,
      expectedMaxDist1000000_rows_diff, columnNames, true, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:1000000>", false,
      expectedMaxDist1000000_rows_diff, columnNames, true, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:1000000>", true,
      expectedMaxDist1000000_rows_diff, columnNames, false, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:1000000>", false,
      expectedMaxDist1000000_rows_diff, columnNames, false, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:10000000>", true,
      expectedMaxDist10000000_rows_diff, columnNames, true, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:10000000>", false,
      expectedMaxDist10000000_rows_diff, columnNames, true, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:10000000>", true,
      expectedMaxDist10000000_rows_diff, columnNames, false, useBaselineAlgorithm);
  buildAndTestSmallTestSetDiffSizeChildren(
      "<max-distance-in-meters:10000000>", false,
      expectedMaxDist10000000_rows_diff, columnNames, false, useBaselineAlgorithm);
}

TEST(SpatialJoin, computeResultSmallDatasetLargeChildrenBaseline) {
  computeResultSmallDatasetLargeChildren(true);
}

TEST(SpatialJoin, computeResultSmallDatasetLargeChildrenBoundingBox) {
  computeResultSmallDatasetLargeChildren(false);
}

TEST(SpatialJoin, computeResultSmallDatasetSmallChildrenBaseline) {
  computeResultSmallDatasetSmallChildren(true);
}

TEST(SpatialJoin, computeResultSmallDatasetSmallChildrenBoundingBox) {
  computeResultSmallDatasetSmallChildren(false);
}


TEST(SpatialJoin, computeResultSmallDatasetDifferentSizeChildrenBaseline) {
  computeResultSmallDatasetDifferentSizeChildren(true);
}

TEST(SpatialJoin, computeResultSmallDatasetDifferentSizeChildrenBoundingBox) {
  computeResultSmallDatasetDifferentSizeChildren(false);
}

}  // end of Namespace computeResultTest

namespace maxDistanceParsingTest {

// test if the SpatialJoin operation parses the maximum distance correctly
void testMaxDistance(std::string distanceIRI, long long distance,
                     bool shouldThrow) {
  auto qec = getQec();
  TripleComponent subject{Variable{"?subject"}};
  TripleComponent object{Variable{"?object"}};
  SparqlTriple triple{subject, distanceIRI, object};
  if (shouldThrow) {
    ASSERT_ANY_THROW(ad_utility::makeExecutionTree<SpatialJoin>(
        qec, triple, std::nullopt, std::nullopt));
  } else {
    std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(qec, triple, std::nullopt,
                                                   std::nullopt);
    std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
    SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
    ASSERT_EQ(spatialJoin->getMaxDist(), distance);
  }
}

TEST(SpatialJoin, maxDistanceParsingTest) {
  testMaxDistance("<max-distance-in-meters:1000>", 1000, false);

  testMaxDistance("<max-distance-in-meters:0>", 0, false);

  testMaxDistance("<max-distance-in-meters:20000000>", 20000000, false);

  testMaxDistance("<max-distance-in-meters:123456789>", 123456789, false);

  // the following distance is slightly bigger than earths circumference.
  // This distance should still be representable
  testMaxDistance("<max-distance-in-meters:45000000000>", 45000000000, false);

  // distance must be positive
  testMaxDistance("<max-distance-in-meters:-10>", -10, true);

  // some words start with an upper case
  testMaxDistance("<max-Distance-In-Meters:1000>", 1000, true);

  // wrong keyword for the spatialJoin operation
  testMaxDistance("<maxDistanceInMeters:1000>", 1000, true);

  // "M" in meters is upper case
  testMaxDistance("<max-distance-in-Meters:1000>", 1000, true);

  // two > at the end
  testMaxDistance("<maxDistanceInMeters:1000>>", 1000, true);

  // distance must be given as integer
  testMaxDistance("<maxDistanceInMeters:oneThousand>", 1000, true);

  // distance must be given as integer
  testMaxDistance("<maxDistanceInMeters:1000.54>>", 1000, true);

  // missing > at the end
  testMaxDistance("<maxDistanceInMeters:1000", 1000, true);

  // prefix before correct iri
  testMaxDistance("<asdfmax-distance-in-meters:1000>", 1000, true);

  // suffix after correct iri
  testMaxDistance("<max-distance-in-metersjklö:1000>", 1000, true);

  // suffix after correct iri
  testMaxDistance("<max-distance-in-meters:qwer1000>", 1000, true);

  // suffix after number.
  // Note that the usual stoll function would return
  // 1000 instead of throwing an exception. To fix this mistake, a for loop
  // has been added to the parsing, which checks, if each character (which
  // should be converted to a number) is a digit
  testMaxDistance("<max-distance-in-meters:1000asff>", 1000, true);

  // prefix before <
  testMaxDistance("yxcv<max-distance-in-metersjklö:1000>", 1000, true);

  // suffix after >
  testMaxDistance("<max-distance-in-metersjklö:1000>dfgh", 1000, true);
}

}  // namespace maxDistanceParsingTest

namespace childrenTesting {

void testAddChild(bool addLeftChildFirst) {
  auto checkVariable = [](SpatialJoin* spatialJoin, bool checkLeftVariable) {
    if (checkLeftVariable) {
      std::shared_ptr<Operation> op =
          spatialJoin->onlyForTestingGetLeftChild()->getRootOperation();
      IndexScan* scan = static_cast<IndexScan*>(op.get());
      ASSERT_EQ(scan->subject().getVariable().name(), "?obj1");
      ASSERT_EQ(scan->object().getVariable().name(), "?point1");
    } else {
      std::shared_ptr<Operation> op =
          spatialJoin->onlyForTestingGetRightChild()->getRootOperation();
      IndexScan* scan = static_cast<IndexScan*>(op.get());
      ASSERT_EQ(scan->subject().getVariable().name(), "?obj2");
      ASSERT_EQ(scan->object().getVariable().name(), "?point2");
    }
  };

  auto qec = localTestHelpers::buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild = computeResultTest::buildIndexScan(
      qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild = computeResultTest::buildIndexScan(
      qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});
  SparqlTriple triple{point1, "<max-distance-in-meters:1000>", point2};

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, triple, std::nullopt,
                                                 std::nullopt);

  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  auto firstChild = addLeftChildFirst ? leftChild : rightChild;
  auto secondChild = addLeftChildFirst ? rightChild : leftChild;
  Variable firstVariable =
      addLeftChildFirst ? point1.getVariable() : point2.getVariable();
  Variable secondVariable =
      addLeftChildFirst ? point2.getVariable() : point1.getVariable();
  std::string firstSubject = addLeftChildFirst ? "?obj1" : "?obj2";
  std::string secondSubject = addLeftChildFirst ? "?obj2" : "?obj1";
  std::string firstObject = addLeftChildFirst ? "?point1" : "?point2";
  std::string secondObject = addLeftChildFirst ? "?point2" : "?point1";

  ASSERT_EQ(spatialJoin->onlyForTestingGetLeftChild(), nullptr);
  ASSERT_EQ(spatialJoin->onlyForTestingGetRightChild(), nullptr);

  ASSERT_ANY_THROW(spatialJoin->addChild(firstChild, Variable{"?wrongVar"}));
  ASSERT_ANY_THROW(spatialJoin->addChild(secondChild, Variable{"?wrongVar"}));

  ASSERT_EQ(spatialJoin->onlyForTestingGetLeftChild(), nullptr);
  ASSERT_EQ(spatialJoin->onlyForTestingGetRightChild(), nullptr);

  auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());

  checkVariable(spatialJoin, addLeftChildFirst);

  auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

  checkVariable(spatialJoin, (!addLeftChildFirst));
}

TEST(SpatialJoin, addChild) {
  testAddChild(true);
  testAddChild(false);
}

TEST(SpatialJoin, isConstructed) {
  auto qec = localTestHelpers::buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild = computeResultTest::buildIndexScan(
      qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild = computeResultTest::buildIndexScan(
      qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});
  SparqlTriple triple{point1, "<max-distance-in-meters:1000>", point2};

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, triple, std::nullopt,
                                                 std::nullopt);

  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  ASSERT_FALSE(spatialJoin->isConstructed());

  auto spJoin1 = spatialJoin->addChild(leftChild, point1.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());

  ASSERT_FALSE(spatialJoin->isConstructed());

  auto spJoin2 = spatialJoin->addChild(rightChild, point2.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

  ASSERT_TRUE(spatialJoin->isConstructed());
}

TEST(SpatialJoin, getChildren) {
  auto qec = localTestHelpers::buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild = computeResultTest::buildIndexScan(
      qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild = computeResultTest::buildIndexScan(
      qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});
  SparqlTriple triple{point1, "<max-distance-in-meters:1000>", point2};

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, triple, std::nullopt,
                                                 std::nullopt);

  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  ASSERT_ANY_THROW(spatialJoin->getChildren());

  auto spJoin1 = spatialJoin->addChild(leftChild, point1.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());

  ASSERT_ANY_THROW(spatialJoin->getChildren());

  auto spJoin2 = spatialJoin->addChild(rightChild, point2.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

  ASSERT_EQ(spatialJoin->getChildren().size(), 2);

  auto assertScanVariables = [](IndexScan* scan1, IndexScan* scan2,
                                bool isSubjectNotObject, std::string varName1,
                                std::string varName2) {
    std::string value1 = scan1->subject().getVariable().name();
    std::string value2 = scan2->subject().getVariable().name();
    if (!isSubjectNotObject) {
      value1 = scan1->object().getVariable().name();
      value2 = scan2->object().getVariable().name();
    }
    ASSERT_TRUE(value1 == varName1 || value1 == varName2);
    ASSERT_TRUE(value2 == varName1 || value2 == varName2);
    ASSERT_TRUE(value1 != value2);
  };

  std::shared_ptr<Operation> op1 =
      spatialJoin->getChildren().at(0)->getRootOperation();
  IndexScan* scan1 = static_cast<IndexScan*>(op1.get());

  std::shared_ptr<Operation> op2 =
      spatialJoin->getChildren().at(1)->getRootOperation();
  IndexScan* scan2 = static_cast<IndexScan*>(op2.get());

  assertScanVariables(scan1, scan2, true, "?obj1", "?obj2");
  assertScanVariables(scan1, scan2, false, "?point1", "?point2");
}

}  // namespace childrenTesting

namespace variableColumnMapAndResultWidth {

// only test one at a time. Then the gtest will fail on the test, which
// failed, instead of failing for both getResultWidth() and
// computeVariableToColumnMap() if only one of them is wrong
void testGetResultWidthOrVariableToColumnMap(bool leftSideBigChild,
                                             bool rightSideBigChild,
                                             bool addLeftChildFirst,
                                             size_t expectedResultWidth,
                                             bool testVarToColMap = false,
                                             bool useBaselineAlgorithm = false) {
  auto getChild = [](QueryExecutionContext* qec, bool getBigChild,
                     std::string numberOfChild) {
    std::string obj = absl::StrCat("?obj", numberOfChild);
    std::string name = absl::StrCat("?name", numberOfChild);
    std::string geo = absl::StrCat("?geo", numberOfChild);
    std::string point = absl::StrCat("?point", numberOfChild);
    if (getBigChild) {
      return computeResultTest::buildMediumChild(
          qec, {obj, std::string{"<name>"}, name},
          {obj, std::string{"<hasGeometry>"}, geo},
          {geo, std::string{"<asWKT>"}, point}, obj, geo);
    } else {
      return computeResultTest::buildSmallChild(
          qec, {obj, std::string{"<hasGeometry>"}, geo},
          {geo, std::string{"<asWKT>"}, point}, geo);
    }
  };
  auto addExpectedColumns =
      [](std::vector<std::pair<std::string, std::string>> expectedColumns,
         bool bigChild, std::string numberOfChild) {
        std::string obj = absl::StrCat("?obj", numberOfChild);
        std::string name = absl::StrCat("?name", numberOfChild);
        std::string geo = absl::StrCat("?geo", numberOfChild);
        std::string point = absl::StrCat("?point", numberOfChild);
        expectedColumns.push_back({obj, "<node_"});
        expectedColumns.push_back({geo, "<geometry"});
        expectedColumns.push_back({point, "\"POINT("});
        if (bigChild) {
          expectedColumns.push_back({name, "\""});
        }
        return expectedColumns;
      };

  auto qec = localTestHelpers::buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);

  auto leftChild = getChild(qec, leftSideBigChild, "1");
  auto rightChild = getChild(qec, rightSideBigChild, "2");

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(
          qec,
          SparqlTriple{TripleComponent{Variable{"?point1"}},
                       std::string{"<max-distance-in-meters:0>"},
                       TripleComponent{Variable{"?point2"}}},
          std::nullopt, std::nullopt);
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
  auto firstChild = addLeftChildFirst ? leftChild : rightChild;
  auto secondChild = addLeftChildFirst ? rightChild : leftChild;
  Variable firstVariable =
      addLeftChildFirst ? Variable{"?point1"} : Variable{"?point2"};
  Variable secondVariable =
      addLeftChildFirst ? Variable{"?point2"} : Variable{"?point1"};
  auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
  auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

  if (!testVarToColMap) {
    ASSERT_EQ(spatialJoin->getResultWidth(), expectedResultWidth);
  } else {
    std::vector<std::pair<std::string, std::string>> expectedColumns{};

    expectedColumns =
        addExpectedColumns(expectedColumns, leftSideBigChild, "1");
    expectedColumns =
        addExpectedColumns(expectedColumns, rightSideBigChild, "2");

    expectedColumns.push_back({"?distOfTheTwoObjectsAddedInternally", "0"});

    spatialJoin->onlyForTestingSetUseBaselineAlgorithm(useBaselineAlgorithm);
    auto varColMap = spatialJoin->computeVariableToColumnMap();
    auto resultTable = spatialJoin->computeResult(false);

    // if the size of varColMap and expectedColumns is the same and each element
    // of expectedColumns is contained in varColMap, then they are the same
    // (assuming that each element is unique)
    ASSERT_EQ(varColMap.size(), expectedColumns.size());

    for (size_t i = 0; i < expectedColumns.size(); i++) {
      ASSERT_TRUE(varColMap.contains(Variable{expectedColumns.at(i).first}));

      // test, that the column contains the correct values
      ColumnIndex ind =
          varColMap[Variable{expectedColumns.at(i).first}].columnIndex_;
      ValueId tableEntry = resultTable.idTable().at(0, ind);
      if (tableEntry.getDatatype() == Datatype::VocabIndex) {
        std::string value = ExportQueryExecutionTrees::idToStringAndType(
                                qec->getIndex(), tableEntry, {})
                                .value()
                                .first;
        ASSERT_TRUE(value.find(expectedColumns.at(i).second, 0) !=
                    string::npos);
      } else if (tableEntry.getDatatype() == Datatype::Int) {
        std::string value = ExportQueryExecutionTrees::idToStringAndType(
                                qec->getIndex(), tableEntry, {})
                                .value()
                                .first;
        ASSERT_EQ(value, expectedColumns.at(i).second);
      } else if (tableEntry.getDatatype() == Datatype::GeoPoint) {
        auto [value, type] = ExportQueryExecutionTrees::idToStringAndType(
                                 qec->getIndex(), tableEntry, {})
                                 .value();
        value = absl::StrCat("\"", value, "\"^^<", type, ">");
        ASSERT_TRUE(value.find(expectedColumns.at(i).second, 0) !=
                    string::npos);
      }
    }
  }
}

TEST(SpatialJoin, getResultWidth) {
  testGetResultWidthOrVariableToColumnMap(true, true, false, 9);
  testGetResultWidthOrVariableToColumnMap(true, true, true, 9);
  testGetResultWidthOrVariableToColumnMap(true, false, false, 8);
  testGetResultWidthOrVariableToColumnMap(true, false, true, 8);
  testGetResultWidthOrVariableToColumnMap(false, true, false, 8);
  testGetResultWidthOrVariableToColumnMap(false, true, true, 8);
  testGetResultWidthOrVariableToColumnMap(false, false, false, 7);
  testGetResultWidthOrVariableToColumnMap(false, false, true, 7);
}

TEST(SpatialJoin, variableToColumnMapBaseLineAlgorithm) {
  testGetResultWidthOrVariableToColumnMap(true, true, false, 9, true, true);
  testGetResultWidthOrVariableToColumnMap(true, true, true, 9, true, true);
  testGetResultWidthOrVariableToColumnMap(true, false, false, 8, true, true);
  testGetResultWidthOrVariableToColumnMap(true, false, true, 8, true, true);
  testGetResultWidthOrVariableToColumnMap(false, true, false, 8, true, true);
  testGetResultWidthOrVariableToColumnMap(false, true, true, 8, true, true);
  testGetResultWidthOrVariableToColumnMap(false, false, false, 7, true, true);
  testGetResultWidthOrVariableToColumnMap(false, false, true, 7, true, true);
}

TEST(SpatialJoin, variableToColumnMapBoundingBoxAlgorithm) {
  testGetResultWidthOrVariableToColumnMap(true, true, false, 9, true, false);
  testGetResultWidthOrVariableToColumnMap(true, true, true, 9, true, false);
  testGetResultWidthOrVariableToColumnMap(true, false, false, 8, true, false);
  testGetResultWidthOrVariableToColumnMap(true, false, true, 8, true, false);
  testGetResultWidthOrVariableToColumnMap(false, true, false, 8, true, false);
  testGetResultWidthOrVariableToColumnMap(false, true, true, 8, true, false);
  testGetResultWidthOrVariableToColumnMap(false, false, false, 7, true, false);
  testGetResultWidthOrVariableToColumnMap(false, false, true, 7, true, false);
}

}  // namespace variableColumnMapAndResultWidth

namespace knownEmptyResult {

void testKnownEmptyResult(bool leftSideEmptyChild, bool rightSideEmptyChild,
                          bool addLeftChildFirst) {
  auto checkEmptyResult = [](SpatialJoin* spatialJoin_, bool shouldBeEmpty) {
    ASSERT_EQ(spatialJoin_->knownEmptyResult(), shouldBeEmpty);
  };

  auto getChild = [](QueryExecutionContext* qec, bool emptyChild) {
    std::string predicate = emptyChild ? "<notExistingPred>" : "<hasGeometry>";
    return computeResultTest::buildSmallChild(
        qec, {"?obj1", predicate, "?geo1"},
        {"?geo1", std::string{"<asWKT>"}, "?point1"}, "?geo1");
  };

  auto qec = localTestHelpers::buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);

  auto leftChild = getChild(qec, leftSideEmptyChild);
  auto rightChild = getChild(qec, rightSideEmptyChild);

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(
          qec,
          SparqlTriple{TripleComponent{Variable{"?point1"}},
                       std::string{"<max-distance-in-meters:0>"},
                       TripleComponent{Variable{"?point2"}}},
          std::nullopt, std::nullopt);
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
  auto firstChild = addLeftChildFirst ? leftChild : rightChild;
  auto secondChild = addLeftChildFirst ? rightChild : leftChild;
  Variable firstVariable =
      addLeftChildFirst ? Variable{"?point1"} : Variable{"?point2"};
  Variable secondVariable =
      addLeftChildFirst ? Variable{"?point2"} : Variable{"?point1"};
  bool firstChildEmpty =
      addLeftChildFirst ? leftSideEmptyChild : rightSideEmptyChild;
  bool secondChildEmpty =
      addLeftChildFirst ? rightSideEmptyChild : leftSideEmptyChild;

  checkEmptyResult(spatialJoin, false);

  auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
  checkEmptyResult(spatialJoin, firstChildEmpty);

  auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
  checkEmptyResult(spatialJoin, (firstChildEmpty || secondChildEmpty));
}

TEST(SpatialJoin, knownEmpyResult) {
  testKnownEmptyResult(true, true, true);
  testKnownEmptyResult(true, true, false);
  testKnownEmptyResult(true, false, true);
  testKnownEmptyResult(true, false, false);
  testKnownEmptyResult(false, true, true);
  testKnownEmptyResult(false, true, false);
  testKnownEmptyResult(false, false, true);
  testKnownEmptyResult(false, false, false);
}

}  // namespace knownEmptyResult

namespace resultSortedOn {

TEST(SpatialJoin, resultSortedOn) {
  std::string kg = localTestHelpers::createSmallDatasetWithPoints();

  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);

  auto spatialJoinTriple = SparqlTriple{TripleComponent{Variable{"?point1"}},
                                        "<max-distance-in-meters:10000000>",
                                        TripleComponent{Variable{"?point2"}}};

  TripleComponent obj1{Variable{"?point1"}};
  TripleComponent obj2{Variable{"?point2"}};
  auto leftChild = computeResultTest::buildIndexScan(
      qec, {"?geometry1", "<asWKT>", "?point1"});
  auto rightChild = computeResultTest::buildIndexScan(
      qec, {"?geometry2", "<asWKT>", "?point2"});

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, spatialJoinTriple,
                                                 std::nullopt, std::nullopt);

  // add children and test, that multiplicity is a dummy return before all
  // children are added
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
  ASSERT_EQ(spatialJoin->getResultSortedOn().size(), 0);
  auto spJoin1 = spatialJoin->addChild(leftChild, obj1.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
  ASSERT_EQ(spatialJoin->getResultSortedOn().size(), 0);
  auto spJoin2 = spatialJoin->addChild(rightChild, obj2.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
  ASSERT_EQ(spatialJoin->getResultSortedOn().size(), 0);
}

}  // namespace resultSortedOn

namespace stringRepresentation {

TEST(SpatialJoin, getDescriptor) {
  auto qec = getQec();
  TripleComponent subject{Variable{"?subject"}};
  TripleComponent object{Variable{"?object"}};
  SparqlTriple triple{subject, "<max-distance-in-meters:1000>", object};

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, triple, std::nullopt,
                                                 std::nullopt);
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  auto description = spatialJoin->getDescriptor();
  ASSERT_TRUE(description.find(std::to_string(spatialJoin->getMaxDist())) !=
              std::string::npos);
  ASSERT_TRUE(description.find("?subject") != std::string::npos);
  ASSERT_TRUE(description.find("?object") != std::string::npos);
}

TEST(SpatialJoin, getCacheKeyImpl) {
  auto qec = localTestHelpers::buildTestQEC();
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  auto spatialJoinTriple = SparqlTriple{TripleComponent{Variable{"?point1"}},
                                        "<max-distance-in-meters:1000>",
                                        TripleComponent{Variable{"?point2"}}};
  auto leftChild = computeResultTest::buildIndexScan(
      qec, {"?obj1", std::string{"<asWKT>"}, "?point1"});
  auto rightChild = computeResultTest::buildIndexScan(
      qec, {"?obj2", std::string{"<asWKT>"}, "?point2"});

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, spatialJoinTriple,
                                                 std::nullopt, std::nullopt);
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  ASSERT_EQ(spatialJoin->getCacheKeyImpl(), "incomplete SpatialJoin class");

  auto spJoin1 =
      spatialJoin->addChild(leftChild, spatialJoinTriple.s_.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());

  ASSERT_EQ(spatialJoin->getCacheKeyImpl(), "incomplete SpatialJoin class");

  auto spJoin2 =
      spatialJoin->addChild(rightChild, spatialJoinTriple.o_.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());

  auto cacheKeyString = spatialJoin->getCacheKeyImpl();
  auto leftCacheKeyString =
      spatialJoin->onlyForTestingGetLeftChild()->getCacheKey();
  auto rightCacheKeyString =
      spatialJoin->onlyForTestingGetRightChild()->getCacheKey();

  ASSERT_TRUE(cacheKeyString.find(std::to_string(spatialJoin->getMaxDist())) !=
              std::string::npos);
  ASSERT_TRUE(cacheKeyString.find(leftCacheKeyString) != std::string::npos);
  ASSERT_TRUE(cacheKeyString.find(rightCacheKeyString) != std::string::npos);
}

}  // namespace stringRepresentation

namespace getMultiplicityAndSizeEstimate {

void testMultiplicitiesOrSizeEstimate(bool addLeftChildFirst,
                                      bool testMultiplicities) {
  auto multiplicitiesBeforeAllChildrenAdded = [&](SpatialJoin* spatialJoin) {
    for (size_t i = 0; i < spatialJoin->getResultWidth(); i++) {
      ASSERT_EQ(spatialJoin->getMultiplicity(i), 1);
    }
  };

  auto assert_double_with_bounds = [](double value1, double value2) {
    // ASSERT_DOUBLE_EQ did not work properly
    ASSERT_TRUE(value1 * 0.99999 < value2);
    ASSERT_TRUE(value1 * 1.00001 > value2);
  };

  std::string kg = localTestHelpers::createSmallDatasetWithPoints();

  // add multiplicities to test knowledge graph
  kg += "<node_1> <name> \"testing multiplicity\" .";
  kg += "<node_1> <name> \"testing multiplicity 2\" .";

  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  const unsigned int nrTriplesInput = 17;
  ASSERT_EQ(numTriples, nrTriplesInput);

  auto spatialJoinTriple = SparqlTriple{TripleComponent{Variable{"?point1"}},
                                        "<max-distance-in-meters:10000000>",
                                        TripleComponent{Variable{"?point2"}}};
  // ===================== build the first child ===============================
  auto leftChild = computeResultTest::buildMediumChild(
      qec, {"?obj1", std::string{"<name>"}, "?name1"},
      {"?obj1", std::string{"<hasGeometry>"}, "?geo1"},
      {"?geo1", std::string{"<asWKT>"}, "?point1"}, "?obj1", "?geo1");
  // result table of leftChild:
  // ?obj1    ?name1                    ?geo1       ?point1
  // node_1   Uni Freiburg TF           geometry1   Point(7.83505 48.01267)
  // node_1   testing multiplicity      geometry1   Point(7.83505 48.01267)
  // node_1   testing multiplicity 2    geometry1   Point(7.83505 48.01267)
  // node_2   Minster Freiburg          geometry2   POINT(7.85298 47.99557)
  // node_3   London Eye                geometry3   POINT(-0.11957 51.50333)
  // node_4   Statue of Liberty         geometry4   POINT(-74.04454 40.68925)
  // node_5   eiffel tower              geometry5   POINT(2.29451 48.85825)

  // ======================= build the second child ============================
  auto rightChild = computeResultTest::buildMediumChild(
      qec, {"?obj2", std::string{"<name>"}, "?name2"},
      {"?obj2", std::string{"<hasGeometry>"}, "?geo2"},
      {"?geo2", std::string{"<asWKT>"}, "?point2"}, "?obj2", "?geo2");
  // result table of rightChild is identical to leftChild, see above

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, spatialJoinTriple,
                                                 std::nullopt, std::nullopt);

  // add children and test, that multiplicity is a dummy return before all
  // children are added
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

  if (testMultiplicities) {
    multiplicitiesBeforeAllChildrenAdded(spatialJoin);
    auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
    spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
    multiplicitiesBeforeAllChildrenAdded(spatialJoin);
    auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
    spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
    auto varColsMap = spatialJoin->getExternallyVisibleVariableColumns();
    auto varColsVec = copySortedByColumnIndex(varColsMap);
    auto leftVarColMap = leftChild->getVariableColumns();
    auto rightVarColMap = rightChild->getVariableColumns();
    for (size_t i = 0; i < spatialJoin->getResultWidth(); i++) {
      // get variable at column 0 of the result table
      Variable var = varColsVec.at(i).first;
      auto varChildLeft = leftVarColMap.find(var);
      auto varChildRight = rightVarColMap.find(var);
      auto inputChild = leftChild;
      if (varChildLeft == leftVarColMap.end()) {
        inputChild = rightChild;
      }
      if (varChildRight == rightVarColMap.end() &&
          var.name() == spatialJoin->getInternalDistanceName()) {
        // as each distance is very likely to be unique (even if only after
        // a few decimal places), no multiplicities are assumed
        ASSERT_EQ(spatialJoin->getMultiplicity(i), 1);
      } else {
        ColumnIndex colIndex;
        if (inputChild == leftChild) {
          colIndex = varChildLeft->second.columnIndex_;
        } else {
          colIndex = varChildRight->second.columnIndex_;
        }
        auto multiplicityChild = inputChild->getMultiplicity(colIndex);
        auto sizeEstimateChild = inputChild->getSizeEstimate();
        double distinctnessChild = sizeEstimateChild / multiplicityChild;
        auto mult = spatialJoin->getMultiplicity(i);
        auto sizeEst = spatialJoin->getSizeEstimate();
        double distinctness = sizeEst / mult;
        // multiplicity, distinctness and size are related via the formula
        // size = distinctness * multiplicity. Therefore if we have two of them,
        // we can calculate the third one. Here we check, that this formula
        // holds true. The distinctness must not change after the operation, the
        // other two variables can change. Therefore we check the correctness
        // via distinctness.
        assert_double_with_bounds(distinctnessChild, distinctness);
      }
    }
  } else {
    // test getSizeEstimate
    ASSERT_EQ(spatialJoin->getSizeEstimate(), 1);
    auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
    spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
    ASSERT_EQ(spatialJoin->getSizeEstimate(), 1);
    auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
    spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
    // the size should be 49, because both input tables have 7 rows and it is
    // assumed, that the whole cross product is build
    auto estimate =
        spatialJoin->onlyForTestingGetLeftChild()->getSizeEstimate() *
        spatialJoin->onlyForTestingGetRightChild()->getSizeEstimate();
    ASSERT_EQ(estimate, spatialJoin->getSizeEstimate());
  }

  {  // new block for hard coded testing
    // ======================== hard coded test ================================
    // here the children are only index scans, as they are perfectly predictable
    // in relation to size and multiplicity estimates
    std::string kg = localTestHelpers::createSmallDatasetWithPoints();

    // add multiplicities to test knowledge graph
    kg += "<geometry1> <asWKT> \"POINT(7.12345 48.12345)\".";
    kg += "<geometry1> <asWKT> \"POINT(7.54321 48.54321)\".";

    ad_utility::MemorySize blocksizePermutations = 16_MB;
    auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
    auto numTriples = qec->getIndex().numTriples().normal;
    const unsigned int nrTriplesInput = 17;
    ASSERT_EQ(numTriples, nrTriplesInput);

    auto spatialJoinTriple = SparqlTriple{TripleComponent{Variable{"?point1"}},
                                          "<max-distance-in-meters:10000000>",
                                          TripleComponent{Variable{"?point2"}}};

    TripleComponent subj1{Variable{"?geometry1"}};
    TripleComponent obj1{Variable{"?point1"}};
    TripleComponent subj2{Variable{"?geometry2"}};
    TripleComponent obj2{Variable{"?point2"}};
    auto leftChild = computeResultTest::buildIndexScan(
        qec, {"?geometry1", "<asWKT>", "?point1"});
    auto rightChild = computeResultTest::buildIndexScan(
        qec, {"?geometry2", "<asWKT>", "?point2"});

    std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(qec, spatialJoinTriple,
                                                   std::nullopt, std::nullopt);

    // add children and test, that multiplicity is a dummy return before all
    // children are added
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

    // each of the input child result tables should look like this:
    // ?geometry           ?point
    // <geometry1>         POINT(7.83505 48.01267)
    // <geometry1>         POINT(7.12345 48.12345)
    // <geometry1>         POINT(7.54321 48.54321)
    // <geometry2>         POINT(7.85298 47.99557)
    // <geometry3>         POINT(-0.11957 51.50333)
    // <geometry4>         POINT(-74.04454 40.68925)
    // <geometry5>         POINT(2.29451 48.85825)
    // multiplicity of ?geometry: 1,4   multiplicity of ?point: 1   size: 7

    if (testMultiplicities) {
      auto assertMultiplicity = [&](Variable var, double multiplicity,
                                    SpatialJoin* spatialJoin,
                                    VariableToColumnMap& varColsMap) {
        assert_double_with_bounds(
            spatialJoin->getMultiplicity(varColsMap[var].columnIndex_),
            multiplicity);
      };
      multiplicitiesBeforeAllChildrenAdded(spatialJoin);
      auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
      spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
      multiplicitiesBeforeAllChildrenAdded(spatialJoin);
      auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
      spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
      auto varColsMap = spatialJoin->getExternallyVisibleVariableColumns();
      Variable distance{spatialJoin->getInternalDistanceName()};

      assertMultiplicity(subj1.getVariable(), 9.8, spatialJoin, varColsMap);
      assertMultiplicity(obj1.getVariable(), 7.0, spatialJoin, varColsMap);
      assertMultiplicity(subj2.getVariable(), 9.8, spatialJoin, varColsMap);
      assertMultiplicity(obj2.getVariable(), 7.0, spatialJoin, varColsMap);
      assertMultiplicity(distance, 1, spatialJoin, varColsMap);
    } else {
      ASSERT_EQ(leftChild->getSizeEstimate(), 7);
      ASSERT_EQ(rightChild->getSizeEstimate(), 7);
      auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
      spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
      auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
      spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
      ASSERT_EQ(spatialJoin->getSizeEstimate(), 49);
    }
  }
}

TEST(SpatialJoin, getMultiplicity) {
  // expected behavior:
  // assert (result table at column i has the same distinctness as the
  // corresponding input table (via varToColMap))
  // assert, that distinctness * multiplicity = sizeEstimate

  testMultiplicitiesOrSizeEstimate(false, true);
  testMultiplicitiesOrSizeEstimate(true, true);
}

TEST(SpatialJoin, getSizeEstimate) {
  testMultiplicitiesOrSizeEstimate(false, false);
  testMultiplicitiesOrSizeEstimate(true, false);
}

}  // namespace getMultiplicityAndSizeEstimate

namespace boundingBox {

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

typedef bg::model::point<double, 2, bg::cs::cartesian> point;
typedef bg::model::box<point> box;
typedef std::pair<point, size_t> value;

inline void testBoundingBox(const long long& maxDistInMeters, const point& startPoint) {
  auto checkOutside = [&](const point& point1, const point& startPoint, 
                          const std::vector<box>& bbox, SpatialJoin* spatialJoin) {
    // check if the point is contained in any bounding box
    bool within = spatialJoin->containedInBoundingBoxes(bbox, point1);
      if (!within) {
        GeoPoint geo1{point1.get<1>(), point1.get<0>()};
        GeoPoint geo2{startPoint.get<1>(), startPoint.get<0>()};
        double dist = ad_utility::detail::wktDistImpl(geo1, geo2) * 1000;
        ASSERT_GT(static_cast<long long>(dist), maxDistInMeters);
      }
  };

  auto testBounds = [] (double x, double y, const box& bbox, bool shouldBeWithin) {
    // correct lon bounds if necessary
    if (x < -180) {
      x += 360;
    } else if (x > 180) {
      x -= 360;
    }

    // testing only possible, if lat bounds are correct and the lon bounds
    // don't cover everything (as then left or right of the box is again
    // inside the box because of the spherical geometry)
    double minLonBox = bbox.min_corner().get<0>();
    double maxLonBox = bbox.max_corner().get<0>();
    if (y < 90 && y > -90 && !(minLonBox < 179.9999 && maxLonBox > 179.9999)) {
      bool within = boost::geometry::covered_by(point(x, y), bbox);
      ASSERT_TRUE(within == shouldBeWithin);
    }
  };

  // build dummy join to access the containedInBoundingBox and computeBoundingBox
  // functions. Note that maxDistInMeters has to be accurate, otherwise the
  // functions of spatialJoin don't work correctly
  auto maxDistInMetersStr = "<max-distance-in-meters:" + std::to_string(maxDistInMeters) + ">";
  auto qec = localTestHelpers::buildTestQEC();
  auto spatialJoinTriple = SparqlTriple{TripleComponent{Variable{"?point1"}},
                                        maxDistInMetersStr,
                                        TripleComponent{Variable{"?point2"}}};
  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(qec, spatialJoinTriple,
                                                   std::nullopt, std::nullopt);

  // add children and test, that multiplicity is a dummy return before all
  // children are added
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  std::vector<box> bbox = spatialJoin->computeBoundingBox(startPoint);
  // broad grid test
  for (int lon = -180; lon < 180; lon+=20) {
    for (int lat = -90; lat < 90; lat+=20) {
      checkOutside(point(lon, lat), startPoint, bbox, spatialJoin);
    }
  }

  // do tests at the border of the box
  for (size_t k = 0; k < bbox.size(); k++) {
    // use a small delta for testing because of floating point inaccuracies
    const double delta = 0.00000001;
    const point minPoint = bbox.at(k).min_corner();
    const point maxPoint = bbox.at(k).max_corner();
    const double lowX = minPoint.get<0>();
    const double lowY = minPoint.get<1>();
    const double highX = maxPoint.get<0>();
    const double highY = maxPoint.get<1>();
    const double xRange = highX - lowX - 2 * delta;
    const double yRange = highY - lowY - 2 * delta;
    for (size_t i = 0; i <= 100; i++) {
      // barely in or out at the left edge
      testBounds(lowX + delta, lowY + delta + (yRange / 100) * i, bbox.at(k), true);
      testBounds(lowX - delta, lowY + delta + (yRange / 100) * i, bbox.at(k), false);
      checkOutside(point(lowX - delta, lowY + (yRange / 100) * i), startPoint, bbox, spatialJoin);
      // barely in or out at the bottom edge
      testBounds(lowX + delta + (xRange / 100) * i, lowY + delta, bbox.at(k), true);
      testBounds(lowX + delta + (xRange / 100) * i, lowY - delta, bbox.at(k), false);
      checkOutside(point(lowX + (xRange / 100) * i, lowY - delta), startPoint, bbox, spatialJoin);
      // barely in or out at the right edge
      testBounds(highX - delta, lowY + delta + (yRange / 100) * i, bbox.at(k), true);
      testBounds(highX + delta, lowY + delta + (yRange / 100) * i, bbox.at(k), false);
      checkOutside(point(highX + delta, lowY + (yRange / 100) * i), startPoint, bbox, spatialJoin);
      // barely in or out at the top edge
      testBounds(lowX + delta + (xRange / 100) * i, highY - delta, bbox.at(k), true);
      testBounds(lowX + delta + (xRange / 100) * i, highY + delta, bbox.at(k), false);
      checkOutside(point(lowX + (xRange / 100) * i, highY + delta), startPoint, bbox, spatialJoin);
    }
  }
}

TEST(SpatialJoin, computeBoundingBox) {
  //ASSERT_EQ("", "uncomment the part below again");
  double circ = 40075 * 1000;  // circumference of the earth (at the equator)
  // 180.0001 in case 180 is represented internally as 180.0000000001
  for (double lon = -180; lon <= 180.0001; lon += 15) {
    // 90.0001 in case 90 is represented internally as 90.000000001
    for (double lat = -90; lat <= 90.0001; lat += 15) {
      // circ / 2 means, that all points on earth are within maxDist km of any
      // starting point
      for (int maxDist = 0; maxDist <= circ / 2.0; maxDist += circ / 36.0) {
        testBoundingBox(maxDist, point(lon, lat));
      }
    }
  }
}

TEST(SpatialJoin, containedInBoundingBoxes) {
  // build dummy join to access the containedInBoundingBox and computeBoundingBox
  // functions
  auto qec = localTestHelpers::buildTestQEC();
  auto spatialJoinTriple = SparqlTriple{TripleComponent{Variable{"?point1"}},
                                        "<max-distance-in-meters:1000>",
                                        TripleComponent{Variable{"?point2"}}};
  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
        ad_utility::makeExecutionTree<SpatialJoin>(qec, spatialJoinTriple,
                                                   std::nullopt, std::nullopt);

  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  // note that none of the boxes is overlapping, therefore we can check, that
  // none of the points which should be contained in one box are contained in
  // another box
  std::vector<box> boxes = {
    box(point(20, 40), point(40, 60)),
    box(point(-180, -20), point(-150, 30)),  // touching left border
    box(point(50, -30), point(180, 10)),  // touching right border
    box(point(-30, 50), point(10, 90)),  // touching north pole
    box(point(-45, -90), point(0, -45))  // touching south pole
  };
  
  // the first entry in this vector is a vector of points, which is contained
  // in the first box, the second entry contains points, which are contained in
  // the second box and so on
  std::vector<std::vector<point>> containedInBox = {
    {point(20, 40), point(40, 40), point(40, 60), point(20, 60), point(30, 50)}, 
    {point(-180, -20), point(-150, -20), point(-150, 30), point(-180, 30), point(-150, 0)},
    {point(50, -30), point(180, -30), point(180, 10), point(50, 10), point(70, -10)},
    {point(-30, 50), point(10, 50), point(10, 90), point(-30, 90), point(-20, 60)},
    {point(-45, -90), point(0, -90), point(0, -45), point(-45, -45), point(-10, -60)}
  };

  // all combinations of box is contained in bounding boxes and is not contained.
  // a one encodes, the bounding box is contained in the set of bounding boxes, 
  // a zero encodes, it isn't. If a box is contained, it's checked, that the points
  // which should be contained in the box are contained. If the box is not
  // contained, it's checked, that the points which are contained in that box are
  // not contained in the box set (because the boxes don't overlap)
  for (size_t a = 0; a <= 1; a++) {
    for (size_t b = 0; a <= 1; a++) {
      for (size_t c = 0; a <= 1; a++) {
        for (size_t d = 0; a <= 1; a++) {
          for (size_t e = 0; a <= 1; a++) {
            std::vector<box> toTest;  // the set of bounding boxes
            std::vector<std::vector<point>> shouldBeContained;
            std::vector<std::vector<point>> shouldNotBeContained;
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
              // test all points, which should be contained in the bounding boxes
              for (size_t i = 0; i < shouldBeContained.size(); i++) {
                for (size_t k = 0; k < shouldBeContained.at(i).size(); k++) {
                  ASSERT_TRUE(spatialJoin->containedInBoundingBoxes(toTest, shouldBeContained.at(i).at(k)));
                }
              }
              // test all points, which shouldn't be contained in the bounding boxes
              for (size_t i = 0; i < shouldNotBeContained.size(); i++) {
                for (size_t k = 0; k < shouldNotBeContained.at(i).size(); k++) {
                  ASSERT_FALSE(spatialJoin->containedInBoundingBoxes(toTest, shouldNotBeContained.at(i).at(k)));
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

}  // anonymous namespace

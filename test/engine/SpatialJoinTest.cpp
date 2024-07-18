#include <gtest/gtest.h>

#include <cstdlib>

#include "../util/IndexTestHelpers.h"
#include "./../../src/global/ValueId.h"
#include "./../../src/util/GeoSparqlHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "parser/data/Variable.h"

using namespace ad_utility::testing;

// helper function in debugging for outputting stuff
void print_vecs(std::vector<std::vector<std::string>> vec) {
  for (size_t i = 0; i < vec.size(); i++) {
    for (size_t k = 0; k < vec.at(i).size(); k++) {
      std::cerr << vec.at(i).at(k) << " ";
    }
    std::cerr << std::endl;
  }
}

// helper function in debugging for outputting stuff
void print_vec(std::vector<std::string> vec) {
  for (size_t i = 0; i < vec.size(); i++) {
    std::cerr << vec.at(i) << std::endl;
  }
}

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
    bool foundVariable = false;
    for (size_t k = 0; k < columnNames.size(); k++) {
      if (indVariableMap.at(i).first.name() == columnNames.at(k)) {
        foundVariable = true;
        result.push_back(columns.at(k));
        break;
      }
    }
    assert(foundVariable);
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

// this function compares the ResultTable resultTableToTest with the
// expected_output. It assumes, that all rows are unique. Therefore the tables
// are equal, if each row of the expected_output is contained in the table and
// the tables have the same size
void compareResultTable(  // const QueryExecutionContext* qec,
                          // const std::shared_ptr<const ResultTable>
                          // resultTableToTest,
    std::vector<std::string> tableToTest,
    std::vector<std::string>* expected_output) {
  // rows will be a reordered version of the tableToTest
  std::string rows[expected_output->size()];
  // std::vector<std::string> tableToTest = printTable(qec, resultTableToTest);
  for (size_t i = 0; i < expected_output->size(); i++) {
    rows[i] = tableToTest[i];  // initialize as copy, reorder when needed
  }
  for (size_t i = 0; i < expected_output->size(); i++) {
    for (size_t k = 0; k < tableToTest.size(); k++) {
      if (tableToTest.at(k) == expected_output->at(i)) {
        rows[i] = tableToTest.at(k);  // change order of expected output
        break;
      }
    }
  }
  // if all rows from the expected output are in the result table and the tables
  // are equally large, then both tables are equal (assuming each row is unique)
  ASSERT_EQ(tableToTest.size(), expected_output->size());
  for (size_t i = 0; i < expected_output->size(); i++) {
    ASSERT_EQ(expected_output->at(i), rows[i]);
  }
}

// create a small test dataset, which focuses on points as geometry objects
std::string createSmallDatasetWithPoints() {
  // note, that some of these objects have a polygon representation, but for
  // testing purposes, they get represented as a point here. I took those
  // points, such that it is obvious, which pair of objects should be included,
  // when the maximum distance is x meters. Please note, that these datapoints
  // are not copied from a real input file. Copying the query will therefore
  // likely not result in the same results as here (also the names, coordinates,
  // etc. might be different in the real datasets)

  // Uni Freiburg TF, building 101
  std::string kg = "<node_1> <name> \"Uni Freiburg TF\" .";
  kg += "<node_1> <hasGeometry> <geometry1> .";
  kg += "<geometry1> <asWKT> \"POINT(7.83505 48.01267)\" .";
  // Minster Freiburg
  kg += "<node_2> <name> \"Minster Freiburg\" .";
  kg += "<node_2> <hasGeometry> <geometry2> .";
  kg += "<geometry2> <asWKT> \"POINT(7.85298 47.99557)\" .";
  // London Eye
  kg += "<node_3> <name> \"London Eye\" .";
  kg += "<node_3> <hasGeometry> <geometry3> .";
  kg += "<geometry3> <asWKT> \"POINT(-0.11957 51.50333)\" .";
  // statue of liberty
  kg += "<node_4> <name> \"Statue of liberty\" .";
  kg += "<node_4> <hasGeometry> <geometry4> .";
  kg += "<geometry4> <asWKT> \"POINT(-74.04454 40.68925)\" .";
  // eiffel tower
  kg += "<node_5> <name> \"eiffel tower\" .";
  kg += "<node_5> <hasGeometry> <geometry5> .";
  kg += "<geometry5> <asWKT> \"POINT(2.29451 48.85825)\" .";
  return kg;
}

// this function creates an input as a test set and returns it
std::string createTestKnowledgeGraph(bool verbose) {
  auto addPoint = [](string* kg, double lon, double lat) {
    string name =
        "Point_" + std::to_string(lon) + "_" + std::to_string(lat) + "";
    *kg += "";

    *kg += name;
    *kg += " geo:asWKT Point(";
    *kg += std::to_string(lon);
    *kg += " ";
    *kg += std::to_string(lat);
    *kg += ")^^geo:wktLiteral .\n";

    double fraction = std::abs(lon - (int)lon);
    if (fraction > 0.49 && fraction < 0.51) {
      *kg += name;
      *kg += " <lon-has-fractional-part> <one-half> .\n";
    } else if (fraction > 0.33 && fraction < 0.34) {
      *kg += name;
      *kg += " <lon-has-fractional-part> <one-third> .\n";
    } else if (fraction > 0.66 && fraction < 0.67) {
      *kg += name;
      *kg += " <lon-has-fractional-part> <two-third> .\n";
    } else if (fraction < 0.01) {
      if ((int)lon % 2 == 0) {
        *kg += name;
        *kg += " <lon-is-div-by> <two> .\n";  // divisible by two
      }
      if ((int)lon % 3 == 0) {
        *kg += name;
        *kg += " <lon-is-div-by> <three> .\n";  // divisible by three
      }
      if ((int)lon % 4 == 0) {
        *kg += name;
        *kg += " <lon-is-div-by> <four> .\n";  // divisible by four
      }
      if ((int)lon % 5 == 0) {
        *kg += name;
        *kg += " <lon-is-div-by> <five> .\n";  // divisible by five
      }
    }

    fraction = std::abs(lat - (int)lat);
    if (fraction > 0.49 && fraction < 0.51) {
      *kg += name;
      *kg += " <lat-has-fractional-part> <one-half> .\n";
    } else if (fraction > 0.33 && fraction < 0.34) {
      *kg += name;
      *kg += " <lat-has-fractional-part> <one-third> .\n";
    } else if (fraction > 0.66 && fraction < 0.67) {
      *kg += name;
      *kg += " <lat-has-fractional-part> <two-third> .\n";
    } else if (fraction < 0.01) {
      if ((int)lat % 2 == 0) {
        *kg += name;
        *kg += " <lat-is-div-by> <two> .\n";  // divisible by two
      }
      if ((int)lat % 3 == 0) {
        *kg += name;
        *kg += " <lat-is-div-by> <three> .\n";  // divisible by three
      }
      if ((int)lat % 4 == 0) {
        *kg += name;
        *kg += " <lat-is-div-by> <four> .\n";  // divisible by four
      }
      if ((int)lat % 5 == 0) {
        *kg += name;
        *kg += " <lat-is-div-by> <five> .\n";  // divisible by five
      }
    }
  };

  string kg = "";  // knowlegde graph
  // for loop to iterate over the longitudes
  for (int lon = -90; lon <= 90; lon++) {     // iterate over longitude
    for (int lat = -180; lat < 180; lat++) {  // iterate over latitude
      if (lon == -90 || lon == 90) {
        // only add one point for the poles
        addPoint(&kg, lon, 0);
        break;
      }

      addPoint(&kg, lon, lat);

      if (!verbose) {
        if (lon % 2 == 1 || (lat > -160 && lat < -20) ||
            (lat > 20 && lat < 160)) {
          continue;
        }
      }

      addPoint(&kg, lon, lat + 1 / 3.0);
      addPoint(&kg, lon, lat + 1 / 2.0);
      addPoint(&kg, lon, lat + 2 / 3.0);

      addPoint(&kg, lon + 1 / 3.0, lat);
      addPoint(&kg, lon + 1 / 3.0, lat + 1 / 3.0);
      addPoint(&kg, lon + 1 / 3.0, lat + 1 / 2.0);
      addPoint(&kg, lon + 1 / 3.0, lat + 2 / 3.0);

      addPoint(&kg, lon + 1 / 2.0, lat);
      addPoint(&kg, lon + 1 / 2.0, lat + 1 / 3.0);
      addPoint(&kg, lon + 1 / 2.0, lat + 1 / 2.0);
      addPoint(&kg, lon + 1 / 2.0, lat + 2 / 3.0);

      addPoint(&kg, lon + 2 / 3.0, lat);
      addPoint(&kg, lon + 2 / 3.0, lat + 1 / 3.0);
      addPoint(&kg, lon + 2 / 3.0, lat + 1 / 2.0);
      addPoint(&kg, lon + 2 / 3.0, lat + 2 / 3.0);
    }
  }
  return kg;
}

namespace computeResultTest {

std::shared_ptr<QueryExecutionTree> buildIndexScan(QueryExecutionContext* qec,
                                                   TripleComponent subject,
                                                   std::string predicate,
                                                   TripleComponent object) {
  return ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO, SparqlTriple{subject, predicate, object});
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
    QueryExecutionContext* qec, std::string subject1_, std::string predicate1,
    std::string object1_, std::string subject2_, std::string predicate2,
    std::string object2_, std::string subject3_, std::string predicate3,
    std::string object3_, std::string joinVariable1_,
    std::string joinVariable2_) {
  TripleComponent subject1{Variable{subject1_}};
  TripleComponent subject2{Variable{subject2_}};
  TripleComponent subject3{Variable{subject3_}};
  TripleComponent object1{Variable{object1_}};
  TripleComponent object2{Variable{object2_}};
  TripleComponent object3{Variable{object3_}};
  Variable joinVariable1{joinVariable1_};
  Variable joinVariable2{joinVariable2_};
  auto scan1 = buildIndexScan(qec, subject1, predicate1, object1);
  auto scan2 = buildIndexScan(qec, subject2, predicate2, object2);
  auto scan3 = buildIndexScan(qec, subject3, predicate3, object3);
  auto join = buildJoin(qec, scan1, scan2, joinVariable1);
  return buildJoin(qec, join, scan3, joinVariable2);
}

std::shared_ptr<QueryExecutionTree> buildSmallChild(
    QueryExecutionContext* qec, std::string subject1_, std::string predicate1,
    std::string object1_, std::string subject2_, std::string predicate2,
    std::string object2_, std::string joinVariable_) {
  TripleComponent subject1{Variable{subject1_}};
  TripleComponent subject2{Variable{subject2_}};
  TripleComponent object1{Variable{object1_}};
  TripleComponent object2{Variable{object2_}};
  Variable joinVariable{joinVariable_};
  auto scan1 = buildIndexScan(qec, subject1, predicate1, object1);
  auto scan2 = buildIndexScan(qec, subject2, predicate2, object2);
  return buildJoin(qec, scan1, scan2, joinVariable);
}

void createAndTestSpatialJoin(
    QueryExecutionContext* qec, SparqlTriple spatialJoinTriple,
    std::shared_ptr<QueryExecutionTree> leftChild,
    std::shared_ptr<QueryExecutionTree> rightChild, bool addLeftChildFirst,
    std::vector<std::vector<std::string>> expectedOutputUnorderedRows,
    std::vector<std::string> columnNames) {
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
  auto expectedOutput = createRowVectorFromColumnVector(expectedOutputOrdered);

  auto res = spatialJoin->computeResult(false);
  auto vec = printTable(qec, &res);
  compareResultTable(vec, &expectedOutput);
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
    std::vector<std::string> columnNames) {
  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ===================== build the first child ===============================
  auto leftChild =
      buildMediumChild(qec, "?obj1", std::string{"<name>"}, "?name1", "?obj1",
                       std::string{"<hasGeometry>"}, "?geo1", "?geo1",
                       std::string{"<asWKT>"}, "?point1", "?obj1", "?geo1");

  // ======================= build the second child ============================
  auto rightChild =
      buildMediumChild(qec, "?obj2", std::string{"<name>"}, "?name2", "?obj2",
                       std::string{"<hasGeometry>"}, "?geo2", "?geo2",
                       std::string{"<asWKT>"}, "?point2", "?obj2", "?geo2");

  createAndTestSpatialJoin(qec,
                           SparqlTriple{TripleComponent{Variable{"?point1"}},
                                        maxDistanceInMetersString,
                                        TripleComponent{Variable{"?point2"}}},
                           leftChild, rightChild, addLeftChildFirst,
                           expectedOutput, columnNames);
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
    std::vector<std::string> columnNames) {
  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  TripleComponent obj1{Variable{"?obj1"}};
  TripleComponent obj2{Variable{"?obj2"}};
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild = buildIndexScan(qec, obj1, std::string{"<asWKT>"}, point1);
  auto rightChild = buildIndexScan(qec, obj2, std::string{"<asWKT>"}, point2);

  createAndTestSpatialJoin(
      qec, SparqlTriple{point1, maxDistanceInMetersString, point2}, leftChild,
      rightChild, addLeftChildFirst, expectedOutput, columnNames);
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
    std::vector<std::string> columnNames, bool bigChildLeft) {
  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ========================= build big child =================================
  auto bigChild =
      buildMediumChild(qec, "?obj1", std::string{"<name>"}, "?name1", "?obj1",
                       std::string{"<hasGeometry>"}, "?geo1", "?geo1",
                       std::string{"<asWKT>"}, "?point1", "?obj1", "?geo1");

  // ========================= build small child ===============================
  TripleComponent obj2{Variable{"?obj2"}};
  TripleComponent point2{Variable{"?point2"}};
  auto smallChild = buildIndexScan(qec, obj2, std::string{"<asWKT>"}, point2);

  auto firstChild = bigChildLeft ? bigChild : smallChild;
  auto secondChild = bigChildLeft ? smallChild : bigChild;
  auto firstVariable =
      bigChildLeft ? TripleComponent{Variable{"?point1"}} : point2;
  auto secondVariable =
      bigChildLeft ? point2 : TripleComponent{Variable{"?point1"}};

  createAndTestSpatialJoin(
      qec,
      SparqlTriple{firstVariable, maxDistanceInMetersString, secondVariable},
      firstChild, secondChild, addLeftChildFirst, expectedOutput, columnNames);
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
                             "\"POINT(7.83505 48.01267)\""},
    std::vector<std::string>{"\"Minster Freiburg\"", "<node_2>", "<geometry2>",
                             "\"POINT(7.85298 47.99557)\""},
    std::vector<std::string>{"\"London Eye\"", "<node_3>", "<geometry3>",
                             "\"POINT(-0.11957 51.50333)\""},
    std::vector<std::string>{"\"Statue of liberty\"", "<node_4>", "<geometry4>",
                             "\"POINT(-74.04454 40.68925)\""},
    std::vector<std::string>{"\"eiffel tower\"", "<node_5>", "<geometry5>",
                             "\"POINT(2.29451 48.85825)\""},
};

std::vector<std::vector<std::string>> unordered_rows_small{
    std::vector<std::string>{"<geometry1>", "\"POINT(7.83505 48.01267)\""},
    std::vector<std::string>{"<geometry2>", "\"POINT(7.85298 47.99557)\""},
    std::vector<std::string>{"<geometry3>", "\"POINT(-0.11957 51.50333)\""},
    std::vector<std::string>{"<geometry4>", "\"POINT(-74.04454 40.68925)\""},
    std::vector<std::string>{"<geometry5>", "\"POINT(2.29451 48.85825)\""}};

// in all calculations below, the factor 1000 is used to convert from km to m

// distance from the object to itself should be zero
std::vector<std::string> expectedDistSelf{"0"};

// distance from Uni Freiburg to Freiburger Müsnster is 2,33 km according to
// google maps
std::vector<std::string> expectedDistUniMun{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(
                         "POINT(7.83505 48.01267)", "POINT(7.85298 47.99557)") *
                     1000))};

// distance from Uni Freiburg to Eiffel Tower is 419,32 km according to
// google maps
std::vector<std::string> expectedDistUniEif{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(
                         "POINT(7.83505 48.01267)", "POINT(2.29451 48.85825)") *
                     1000))};

// distance from Minster Freiburg to eiffel tower is 421,09 km according to
// google maps
std::vector<std::string> expectedDistMunEif{std::to_string(
    static_cast<int>(ad_utility::detail::wktDistImpl(
                         "POINT(7.85298 47.99557)", "POINT(2.29451 48.85825)") *
                     1000))};

// distance from london eye to eiffel tower is 340,62 km according to
// google maps
std::vector<std::string> expectedDistEyeEif{std::to_string(static_cast<int>(
    ad_utility::detail::wktDistImpl("POINT(-0.11957 51.50333)",
                                    "POINT(2.29451 48.85825)") *
    1000))};

// distance from Uni Freiburg to London Eye is 690,18 km according to
// google maps
std::vector<std::string> expectedDistUniEye{std::to_string(static_cast<int>(
    ad_utility::detail::wktDistImpl("POINT(7.83505 48.01267)",
                                    "POINT(-0.11957 51.50333)") *
    1000))};

// distance from Minster Freiburg to London Eye is 692,39 km according to
// google maps
std::vector<std::string> expectedDistMunEye{std::to_string(static_cast<int>(
    ad_utility::detail::wktDistImpl("POINT(7.85298 47.99557)",
                                    "POINT(-0.11957 51.50333)") *
    1000))};

// distance from Uni Freiburg to Statue of Liberty is 6249,55 km according to
// google maps
std::vector<std::string> expectedDistUniLib{std::to_string(static_cast<int>(
    ad_utility::detail::wktDistImpl("POINT(7.83505 48.01267)",
                                    "POINT(-74.04454 40.68925)") *
    1000))};

// distance from Minster Freiburg to Statue of Liberty is 6251,58 km
// according to google maps
std::vector<std::string> expectedDistMunLib{std::to_string(static_cast<int>(
    ad_utility::detail::wktDistImpl("POINT(7.85298 47.99557)",
                                    "POINT(-74.04454 40.68925)") *
    1000))};

// distance from london eye to statue of liberty is 5575,08 km according to
// google maps
std::vector<std::string> expectedDistEyeLib{std::to_string(static_cast<int>(
    ad_utility::detail::wktDistImpl("POINT(-0.11957 51.50333)",
                                    "POINT(-74.04454 40.68925)") *
    1000))};

// distance from eiffel tower to Statue of liberty is 5837,42 km according to
// google maps
std::vector<std::string> expectedDistEifLib{std::to_string(static_cast<int>(
    ad_utility::detail::wktDistImpl("POINT(2.29451 48.85825)",
                                    "POINT(-74.04454 40.68925)") *
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
TEST(SpatialJoin, computeResultSmallDatasetLargeChildren) {
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
}

TEST(SpatialJoin, computeResultSmallDatasetSmallChildren) {
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

TEST(SpatialJoin, computeResultSmallDatasetDifferentSizeChildren) {
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
      ASSERT_EQ(scan->getSubject().getVariable().name(), "?obj1");
      ASSERT_EQ(scan->getObject().getVariable().name(), "?point1");
    } else {
      std::shared_ptr<Operation> op =
          spatialJoin->onlyForTestingGetRightChild()->getRootOperation();
      IndexScan* scan = static_cast<IndexScan*>(op.get());
      ASSERT_EQ(scan->getSubject().getVariable().name(), "?obj2");
      ASSERT_EQ(scan->getObject().getVariable().name(), "?point2");
    }
  };

  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  TripleComponent obj1{Variable{"?obj1"}};
  TripleComponent obj2{Variable{"?obj2"}};
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild = computeResultTest::buildIndexScan(
      qec, obj1, std::string{"<asWKT>"}, point1);
  auto rightChild = computeResultTest::buildIndexScan(
      qec, obj2, std::string{"<asWKT>"}, point2);
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
  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  TripleComponent obj1{Variable{"?obj1"}};
  TripleComponent obj2{Variable{"?obj2"}};
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild = computeResultTest::buildIndexScan(
      qec, obj1, std::string{"<asWKT>"}, point1);
  auto rightChild = computeResultTest::buildIndexScan(
      qec, obj2, std::string{"<asWKT>"}, point2);
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
  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  TripleComponent obj1{Variable{"?obj1"}};
  TripleComponent obj2{Variable{"?obj2"}};
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild = computeResultTest::buildIndexScan(
      qec, obj1, std::string{"<asWKT>"}, point1);
  auto rightChild = computeResultTest::buildIndexScan(
      qec, obj2, std::string{"<asWKT>"}, point2);
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

  std::shared_ptr<Operation> op1 =
      spatialJoin->getChildren().at(0)->getRootOperation();
  IndexScan* scan1 = static_cast<IndexScan*>(op1.get());
  ASSERT_TRUE((scan1->getSubject().getVariable().name() == "?obj1" ||
               scan1->getSubject().getVariable().name() == "?obj2"));

  std::shared_ptr<Operation> op2 =
      spatialJoin->getChildren().at(1)->getRootOperation();
  IndexScan* scan2 = static_cast<IndexScan*>(op2.get());
  ASSERT_TRUE((scan2->getSubject().getVariable().name() == "?obj1" ||
               scan2->getSubject().getVariable().name() == "?obj2"));

  ASSERT_TRUE(scan1->getSubject().getVariable().name() !=
              scan2->getSubject().getVariable().name());

  ASSERT_TRUE((scan1->getObject().getVariable().name() == "?point1" ||
               scan1->getObject().getVariable().name() == "?point2"));

  ASSERT_TRUE((scan2->getObject().getVariable().name() == "?point1" ||
               scan2->getObject().getVariable().name() == "?point2"));

  ASSERT_TRUE(scan1->getObject().getVariable().name() !=
              scan2->getObject().getVariable().name());
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
                                             bool testVarToColMap = false) {
  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);

  auto leftChild =
      leftSideBigChild
          ? computeResultTest::buildMediumChild(
                qec, "?obj1", std::string{"<name>"}, "?name1", "?obj1",
                std::string{"<hasGeometry>"}, "?geo1", "?geo1",
                std::string{"<asWKT>"}, "?point1", "?obj1", "?geo1")
          : computeResultTest::buildSmallChild(
                qec, "?obj1", std::string{"<hasGeometry>"}, "?geo1", "?geo1",
                std::string{"<asWKT>"}, "?point1", "?geo1");
  auto rightChild =
      rightSideBigChild
          ? computeResultTest::buildMediumChild(
                qec, "?obj2", std::string{"<name>"}, "?name2", "?obj2",
                std::string{"<hasGeometry>"}, "?geo2", "?geo2",
                std::string{"<asWKT>"}, "?point2", "?obj2", "?geo2")
          : computeResultTest::buildSmallChild(
                qec, "?obj2", std::string{"<hasGeometry>"}, "?geo2", "?geo2",
                std::string{"<asWKT>"}, "?point2", "?geo2");

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
    std::vector<std::pair<std::string, std::string>> expectedColumns{
        std::pair{"?distOfTheTwoObjectsAddedInternally", "0"}};

    if (leftSideBigChild) {
      expectedColumns.push_back({"?obj1", "<node_"});
      expectedColumns.push_back({"?geo1", "<geometry"});
      expectedColumns.push_back({"?name1", "\""});
      expectedColumns.push_back({"?point1", "\"POINT("});
    } else {
      expectedColumns.push_back({"?obj1", "<node_"});
      expectedColumns.push_back({"?geo1", "<geometry"});
      expectedColumns.push_back({"?point1", "\"POINT("});
    }

    if (rightSideBigChild) {
      expectedColumns.push_back({"?obj2", "<node_"});
      expectedColumns.push_back({"?geo2", "<geometry"});
      expectedColumns.push_back({"?name2", "\""});
      expectedColumns.push_back({"?point2", "\"POINT("});
    } else {
      expectedColumns.push_back({"?obj2", "<node_"});
      expectedColumns.push_back({"?geo2", "<geometry"});
      expectedColumns.push_back({"?point2", "\"POINT("});
    }

    auto varColMap = spatialJoin->computeVariableToColumnMap();
    auto resultTable = spatialJoin->computeResult(false);

    // if the size of varColMap and expectedColumns is the same and each element
    // of expectedColumns is contained in varColMap, then they are the same
    // (assuming that each element is unique)
    ASSERT_EQ(varColMap.size(), expectedColumns.size());

    for (size_t i = 0; i < expectedColumns.size(); i++) {
      ASSERT_TRUE(varColMap.contains(Variable{expectedColumns.at(i).first}));

      // test, that the column containes the correct values
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
      } else {
        ASSERT_TRUE(false);  // this line should not be reachable (see dataset)
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

TEST(SpatialJoin, variableToColumnMap) {
  testGetResultWidthOrVariableToColumnMap(true, true, false, 9, true);
  testGetResultWidthOrVariableToColumnMap(true, true, true, 9, true);
  testGetResultWidthOrVariableToColumnMap(true, false, false, 8, true);
  testGetResultWidthOrVariableToColumnMap(true, false, true, 8, true);
  testGetResultWidthOrVariableToColumnMap(false, true, false, 8, true);
  testGetResultWidthOrVariableToColumnMap(false, true, true, 8, true);
  testGetResultWidthOrVariableToColumnMap(false, false, false, 7, true);
  testGetResultWidthOrVariableToColumnMap(false, false, true, 7, true);
}

}  // namespace variableColumnMapAndResultWidth

namespace knownEmptyResult {

void testKnownEmptyResult(bool leftSideEmptyChild, bool rightSideEmptyChild,
                          bool addLeftChildFirst) {
  auto checkEmptyResult = [](SpatialJoin* spatialJoin_, bool shouldBeEmpty) {
    ASSERT_EQ(spatialJoin_->knownEmptyResult(), shouldBeEmpty);
  };

  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);

  auto leftChild =
      leftSideEmptyChild
          ? computeResultTest::buildMediumChild(
                qec, "?obj1", std::string{"<notExistingPredicate>"}, "?name1",
                "?obj1", std::string{"<hasGeometry>"}, "?geo1", "?geo1",
                std::string{"<asWKT>"}, "?point1", "?obj1", "?geo1")
          : computeResultTest::buildSmallChild(
                qec, "?obj1", std::string{"<hasGeometry>"}, "?geo1", "?geo1",
                std::string{"<asWKT>"}, "?point1", "?geo1");
  auto rightChild =
      rightSideEmptyChild
          ? computeResultTest::buildMediumChild(
                qec, "?obj2", std::string{"<notExistingPredicate>"}, "?name2",
                "?obj2", std::string{"<hasGeometry>"}, "?geo2", "?geo2",
                std::string{"<asWKT>"}, "?point2", "?obj2", "?geo2")
          : computeResultTest::buildSmallChild(
                qec, "?obj2", std::string{"<hasGeometry>"}, "?geo2", "?geo2",
                std::string{"<asWKT>"}, "?point2", "?geo2");

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
  std::string kg = createSmallDatasetWithPoints();

  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);

  auto spatialJoinTriple = SparqlTriple{TripleComponent{Variable{"?point1"}},
                                        "<max-distance-in-meters:10000000>",
                                        TripleComponent{Variable{"?point2"}}};

  TripleComponent subj1{Variable{"?geometry1"}};
  TripleComponent obj1{Variable{"?point1"}};
  TripleComponent subj2{Variable{"?geometry2"}};
  TripleComponent obj2{Variable{"?point2"}};
  auto leftChild =
      computeResultTest::buildIndexScan(qec, subj1, "<asWKT>", obj1);
  auto rightChild =
      computeResultTest::buildIndexScan(qec, subj2, "<asWKT>", obj2);

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
  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal;
  ASSERT_EQ(numTriples, 15);
  // ====================== build inputs ===================================
  auto spatialJoinTriple = SparqlTriple{TripleComponent{Variable{"?point1"}},
                                        "<max-distance-in-meters:1000>",
                                        TripleComponent{Variable{"?point2"}}};
  TripleComponent obj1{Variable{"?obj1"}};
  TripleComponent obj2{Variable{"?obj2"}};
  TripleComponent point1{Variable{"?point1"}};
  TripleComponent point2{Variable{"?point2"}};
  auto leftChild = computeResultTest::buildIndexScan(
      qec, obj1, std::string{"<asWKT>"}, point1);
  auto rightChild = computeResultTest::buildIndexScan(
      qec, obj2, std::string{"<asWKT>"}, point2);

  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, spatialJoinTriple,
                                                 std::nullopt, std::nullopt);
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());

  ASSERT_EQ(spatialJoin->getCacheKeyImpl(), "incomplete SpatialJoin class");

  auto spJoin1 = spatialJoin->addChild(leftChild, spatialJoinTriple.s_.getVariable());
  spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());

  ASSERT_EQ(spatialJoin->getCacheKeyImpl(), "incomplete SpatialJoin class");

  auto spJoin2 = spatialJoin->addChild(rightChild, spatialJoinTriple.o_.getVariable());
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

  std::string kg = createSmallDatasetWithPoints();

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
      qec, "?obj1", std::string{"<name>"}, "?name1", "?obj1",
      std::string{"<hasGeometry>"}, "?geo1", "?geo1", std::string{"<asWKT>"},
      "?point1", "?obj1", "?geo1");
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
      qec, "?obj2", std::string{"<name>"}, "?name2", "?obj2",
      std::string{"<hasGeometry>"}, "?geo2", "?geo2", std::string{"<asWKT>"},
      "?point2", "?obj2", "?geo2");
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
      auto varChild = leftVarColMap.find(var);
      auto inputChild = leftChild;
      if (varChild == leftVarColMap.end()) {
        varChild = rightVarColMap.find(var);
        inputChild = rightChild;
      }
      if (varChild == rightVarColMap.end() &&
          var.name() == spatialJoin->getInternalDistanceName()) {
        // as each distance is very likely to be unique (even if only after
        // a few decimal places), no multiplicities are assumed
        ASSERT_EQ(spatialJoin->getMultiplicity(i), 1);
      } else {
        auto col_index = varChild->second.columnIndex_;
        auto multiplicityChild = inputChild->getMultiplicity(col_index);
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
    // here the child are only index scans, as they are perfectly predictable in
    // relation to size and multiplicity estimates
    std::string kg = createSmallDatasetWithPoints();

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
    auto leftChild =
        computeResultTest::buildIndexScan(qec, subj1, "<asWKT>", obj1);
    auto rightChild =
        computeResultTest::buildIndexScan(qec, subj2, "<asWKT>", obj2);

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
      multiplicitiesBeforeAllChildrenAdded(spatialJoin);
      auto spJoin1 = spatialJoin->addChild(firstChild, firstVariable);
      spatialJoin = static_cast<SpatialJoin*>(spJoin1.get());
      multiplicitiesBeforeAllChildrenAdded(spatialJoin);
      auto spJoin2 = spatialJoin->addChild(secondChild, secondVariable);
      spatialJoin = static_cast<SpatialJoin*>(spJoin2.get());
      auto varColsMap = spatialJoin->getExternallyVisibleVariableColumns();
      Variable distance{spatialJoin->getInternalDistanceName()};

      assert_double_with_bounds(
          spatialJoin->getMultiplicity(
              varColsMap[subj1.getVariable()].columnIndex_),
          9.8);
      assert_double_with_bounds(
          spatialJoin->getMultiplicity(
              varColsMap[obj1.getVariable()].columnIndex_),
          7.0);
      assert_double_with_bounds(
          spatialJoin->getMultiplicity(
              varColsMap[subj2.getVariable()].columnIndex_),
          9.8);
      assert_double_with_bounds(
          spatialJoin->getMultiplicity(
              varColsMap[obj2.getVariable()].columnIndex_),
          7.0);
      assert_double_with_bounds(
          spatialJoin->getMultiplicity(varColsMap[distance].columnIndex_), 1);
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

// test the compute result method on the large dataset from above
// TODO

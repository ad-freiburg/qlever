#include <gtest/gtest.h>
#include <cstdlib>
#include "engine/SpatialJoin.h"
#include "../IndexTestHelpers.h"
#include "engine/QueryExecutionTree.h"
#include "parser/data/Variable.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "./../../src/util/GeoSparqlHelpers.h"

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
                  ResultTable* table) {
  std::vector<std::string> output;
  for (size_t i = 0; i < table->size(); i++) {
    std::string line = "";
    for (size_t k = 0; k < table->width(); k++) {
      auto test = ExportQueryExecutionTrees::idToStringAndType(qec->getIndex(),
            table->idTable().at(i, k), {});
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
void compareResultTable(//const QueryExecutionContext* qec,
              //const std::shared_ptr<const ResultTable> resultTableToTest, 
              std::vector<std::string> tableToTest,
              std::vector<std::string> *expected_output) {
  // rows will be a reordered version of the tableToTest
  std::string rows[expected_output->size()];
  // std::vector<std::string> tableToTest = printTable(qec, resultTableToTest);
  for (size_t i = 0; i < expected_output->size(); i++) {
    rows[i] = tableToTest[i];  // initialize as copy, reorder when needed
  }
  for (size_t i = 0; i < expected_output->size(); i++) {
    for (size_t k = 0; k < tableToTest.size(); k++) {
      if (tableToTest.at(k) == expected_output->at(i)){
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

std::shared_ptr<QueryExecutionTree> buildIndexScan(QueryExecutionContext* qec,
      TripleComponent subject, std::string predicate, TripleComponent object) {
  return ad_utility::makeExecutionTree<IndexScan>(qec, 
        Permutation::Enum::PSO, SparqlTriple{subject, predicate, object});
}

std::shared_ptr<QueryExecutionTree> buildJoin(QueryExecutionContext* qec,
      std::shared_ptr<QueryExecutionTree> tree1,
      std::shared_ptr<QueryExecutionTree> tree2, Variable joinVariable) {
  auto varCol1 = tree1->getVariableColumns();
  auto varCol2 = tree2->getVariableColumns();
  size_t col1 = varCol1[joinVariable].columnIndex_;
  size_t col2 = varCol2[joinVariable].columnIndex_;
  return ad_utility::makeExecutionTree<Join>(
          qec, tree1, tree2, col1, col2, true);
}

std::shared_ptr<QueryExecutionTree> buildSmallChild(QueryExecutionContext* qec,
    TripleComponent subject1, std::string predicate1, TripleComponent object1,
    TripleComponent subject2, std::string predicate2, TripleComponent object2,
    Variable joinVariable) {
  auto scan1 = buildIndexScan(qec, subject1, predicate1, object1);
  auto scan2 = buildIndexScan(qec, subject2, predicate2, object2);
  return buildJoin(qec, scan1, scan2, joinVariable);
}

std::shared_ptr<QueryExecutionTree> buildMediumChild(QueryExecutionContext* qec,
    TripleComponent subject1, std::string predicate1, TripleComponent object1,
    TripleComponent subject2, std::string predicate2, TripleComponent object2,
    TripleComponent subject3, std::string predicate3, TripleComponent object3,
    Variable joinVariable1, Variable joinVariable2) {
  auto scan1 = buildIndexScan(qec, subject1, predicate1, object1);
  auto scan2 = buildIndexScan(qec, subject2, predicate2, object2);
  auto scan3 = buildIndexScan(qec, subject3, predicate3, object3);
  auto join = buildJoin(qec, scan1, scan2, joinVariable1);
  return buildJoin(qec, join, scan3, joinVariable2);
}

void createAndTestSpatialJoin(QueryExecutionContext* qec,
      SparqlTriple spatialJoinTriple,
      std::shared_ptr<QueryExecutionTree> leftChild,
      std::shared_ptr<QueryExecutionTree> rightChild, bool addLeftChildFirst,
      std::vector<std::vector<std::string>> expectedOutputUnorderedRows) {
  
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

  std::cerr << "starting createAndTest" << std::endl;
  std::shared_ptr<QueryExecutionTree> spatialJoinOperation = 
        ad_utility::makeExecutionTree<SpatialJoin>(qec, spatialJoinTriple,
        std::nullopt, std::nullopt);
  // test VariableToColumnMap
  // test sizeEstimate
  // test multiplicities
  // add first child
  std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
  SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
  auto firstChild = addLeftChildFirst ? leftChild : rightChild;
  auto secondChild = addLeftChildFirst ? rightChild : leftChild;
  Variable firstVariable = addLeftChildFirst ? 
        spatialJoinTriple._s.getVariable() : spatialJoinTriple._o.getVariable();
  Variable secondVariable = addLeftChildFirst ? 
        spatialJoinTriple._o.getVariable() : spatialJoinTriple._s.getVariable();
  spatialJoin->addChild(firstChild, firstVariable);
  
  // add second child
  spatialJoin->addChild(secondChild, secondVariable);

  // prepare expected output
  std::vector<std::string> columnNames = {"?name1", "?obj1", "?geo1", "?point1",
                                        "?name2", "?obj2", "?geo2", "?point2",
                                        "?distOfTheTwoObjectsAddedInternally"};
  // swap rows and columns to use the function orderColAccordingToVarColMap
  auto expectedMaxDistCols = swapColumns(expectedOutputUnorderedRows);
  auto expectedOutputOrdered = orderColAccordingToVarColMap(
          spatialJoin->computeVariableToColumnMap(),
          expectedMaxDistCols, columnNames);
  auto expectedOutput =
          createRowVectorFromColumnVector(expectedOutputOrdered);

  auto res = spatialJoin->computeResult();
  auto vec = printTable(qec, &res);
  compareResultTable(vec, &expectedOutput);
  /*expected output geht bei kommazahlen noch nicht, das noch beheben, eventuell bei
  idtostringandtype von printtable schauen, ob da immer nur 2 nachkommastellen
  ausgegeben werden*/

  std::cerr << res.size() << " " << spatialJoinTriple._p.getIri()
            << spatialJoin->getMaxDist() << " " << std::endl;
  //auto vec = printTable(qec, &res);
  std::cerr << "output" << std::endl;
  print_vec(vec);
  std::cerr << "expected" << std::endl;

  print_vec(expectedOutput);
  std::cerr << "ending createAndTest" << std::endl;
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
        std::vector<std::vector<std::string>> expectedOutput) {
  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, true, true, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal_;
  ASSERT_EQ(numTriples, 15);
  // ===================== build the left input ===============================
  TripleComponent obj1{Variable{"?obj1"}};
  TripleComponent geo1{Variable{"?geo1"}};
  TripleComponent point1{Variable{"?point1"}};
  // needed as getVariable() returns a const variable
  Variable joinVar1_1 = obj1.getVariable();
  Variable joinVar1_2 = geo1.getVariable();
  auto leftChild = buildMediumChild(qec,
          obj1,
          std::string{"<name>"},
          TripleComponent{Variable{"?name1"}},
          obj1,
          std::string{"<hasGeometry>"},
          geo1,
          geo1, std::string{"<asWKT>"}, 
          point1, joinVar1_1, joinVar1_2);
  
  // ======================= build the right input ============================
  TripleComponent obj2{Variable{"?obj2"}};
  TripleComponent geo2{Variable{"?geo2"}};
  TripleComponent point2{Variable{"?point2"}};
  // needed as getVariable() returns a const variable
  Variable joinVar2_1 = obj2.getVariable();
  Variable joinVar2_2 = geo2.getVariable();
  auto rightChild = buildMediumChild(qec,
          obj2,
          std::string{"<name>"},
          TripleComponent{Variable{"?name2"}},
          obj2,
          std::string{"<hasGeometry>"},
          geo2,
          geo2, std::string{"<asWKT>"},
          point2, joinVar2_1, joinVar2_2);

  createAndTestSpatialJoin(qec,
        SparqlTriple{point1, maxDistanceInMetersString, point2}, leftChild,
        rightChild, addLeftChildFirst, expectedOutput);
}

// this function creates an input as a test set and returns it
std::string createTestKnowledgeGraph(bool verbose) {
  auto addPoint = [] (string* kg, double lon, double lat) {
    string name = "Point_" + std::to_string(lon) + "_"
                        + std::to_string(lat) + "";
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
  for (int lon = -90; lon <= 90; lon++) {  // iterate over longitude
    for (int lat = -180; lat < 180; lat++) {  // iterate over latitude
      if (lon == -90 || lon == 90) {
        // only add one point for the poles
        addPoint(&kg, lon, 0);
        break;
      }
      
      
      addPoint(&kg, lon, lat);

      if (!verbose) {
        if (lon % 2 == 1
              || (lat > -160 && lat < -20)
              || (lat > 20 && lat < 160) ) {
          continue;
        }
      }
      
      addPoint(&kg, lon, lat + 1/3.0);
      addPoint(&kg, lon, lat + 1/2.0);
      addPoint(&kg, lon, lat + 2/3.0);

      addPoint(&kg, lon + 1/3.0, lat);
      addPoint(&kg, lon + 1/3.0, lat + 1/3.0);
      addPoint(&kg, lon + 1/3.0, lat + 1/2.0);
      addPoint(&kg, lon + 1/3.0, lat + 2/3.0);
      
      addPoint(&kg, lon + 1/2.0, lat);
      addPoint(&kg, lon + 1/2.0, lat + 1/3.0);
      addPoint(&kg, lon + 1/2.0, lat + 1/2.0);
      addPoint(&kg, lon + 1/2.0, lat + 2/3.0);
      
      addPoint(&kg, lon + 2/3.0, lat);
      addPoint(&kg, lon + 2/3.0, lat + 1/3.0);
      addPoint(&kg, lon + 2/3.0, lat + 1/2.0);
      addPoint(&kg, lon + 2/3.0, lat + 2/3.0);
    }
  }
  return kg;
}

// test the compute result method on small examples
TEST(SpatialJoin, computeResultSmallDataset) {
  
  auto mergeToRow = [&](std::vector<std::string> part1,
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

  // in all calculations below, the factor 1000 is used to convert from km to m
  // distance from the object to itself should be zero
  std::vector<std::string> expectedDistSelf{"0"};

  // distance from Uni Freiburg to Freiburger Müsnster is 2,33 km according to
  // google maps
  std::vector<std::string> expectedDistUniMun{
    std::to_string(static_cast<int>(ad_utility::detail::wktDistImpl(
      "POINT(7.83505 48.01267)", "POINT(7.85298 47.99557)") * 1000))};

  // distance from Uni Freiburg to Eiffel Tower is 419,32 km according to
  // google maps
  std::vector<std::string> expectedDistUniEif{
    std::to_string(static_cast<int>(ad_utility::detail::wktDistImpl(
      "POINT(7.83505 48.01267)", "POINT(2.29451 48.85825)") * 1000))
  };

  // distance from Minster Freiburg to eiffel tower is 421,09 km according to
  // google maps
  std::vector<std::string> expectedDistMunEif{
    std::to_string(static_cast<int>(ad_utility::detail::wktDistImpl(
      "POINT(7.85298 47.99557)", "POINT(2.29451 48.85825)") * 1000))
  };
  
  // distance from london eye to eiffel tower is 340,62 km according to
  // google maps
  std::vector<std::string> expectedDistEyeEif{
    std::to_string(static_cast<int>(ad_utility::detail::wktDistImpl(
      "POINT(-0.11957 51.50333)", "POINT(2.29451 48.85825)") * 1000))
  };
  
  // distance from Uni Freiburg to London Eye is 690,18 km according to
  // google maps
  std::vector<std::string> expectedDistUniEye{
    std::to_string(static_cast<int>(ad_utility::detail::wktDistImpl(
      "POINT(7.83505 48.01267)", "POINT(-0.11957 51.50333)") * 1000))
  };
  
  // distance from Minster Freiburg to London Eye is 692,39 km according to
  // google maps
  std::vector<std::string> expectedDistMunEye{
    std::to_string(static_cast<int>(ad_utility::detail::wktDistImpl(
      "POINT(7.85298 47.99557)", "POINT(-0.11957 51.50333)") * 1000))
  };
  
  // distance from Uni Freiburg to Statue of Liberty is 6249,55 km according to
  // google maps
  std::vector<std::string> expectedDistUniLib{
    std::to_string(static_cast<int>(ad_utility::detail::wktDistImpl(
      "POINT(7.83505 48.01267)", "POINT(-74.04454 40.68925)") * 1000))
  };
  
  // distance from Minster Freiburg to Statue of Liberty is 6251,58 km
  // according to google maps
  std::vector<std::string> expectedDistMunLib{
    std::to_string(static_cast<int>(ad_utility::detail::wktDistImpl(
      "POINT(7.85298 47.99557)", "POINT(-74.04454 40.68925)") * 1000))
  };
  
  // distance from london eye to statue of liberty is 5575,08 km according to
  // google maps
  std::vector<std::string> expectedDistEyeLib{
    std::to_string(static_cast<int>(ad_utility::detail::wktDistImpl(
      "POINT(-0.11957 51.50333)", "POINT(-74.04454 40.68925)") * 1000))
  };
  
  // distance from eiffel tower to Statue of liberty is 5837,42 km according to
  // google maps
  std::vector<std::string> expectedDistEifLib{
    std::to_string(static_cast<int>(ad_utility::detail::wktDistImpl(
      "POINT(2.29451 48.85825)", "POINT(-74.04454 40.68925)") * 1000))
  };

  std::vector<std::vector<std::string>> expectedMaxDist1_rows{
    mergeToRow(unordered_rows.at(0), unordered_rows.at(0), expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(1), expectedDistSelf),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(2), expectedDistSelf),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(3), expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(4), expectedDistSelf)
  };

  std::vector<std::vector<std::string>> expectedMaxDist5000_rows{
    mergeToRow(unordered_rows.at(0), unordered_rows.at(0), expectedDistSelf),
    mergeToRow(unordered_rows.at(0), unordered_rows.at(1), expectedDistUniMun),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(1), expectedDistSelf),
    mergeToRow(unordered_rows.at(1), unordered_rows.at(0), expectedDistUniMun),
    mergeToRow(unordered_rows.at(2), unordered_rows.at(2), expectedDistSelf),
    mergeToRow(unordered_rows.at(3), unordered_rows.at(3), expectedDistSelf),
    mergeToRow(unordered_rows.at(4), unordered_rows.at(4), expectedDistSelf)
  };
  
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
    mergeToRow(unordered_rows.at(4), unordered_rows.at(2), expectedDistEyeEif)
  };

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
    mergeToRow(unordered_rows.at(4), unordered_rows.at(2), expectedDistEyeEif)
  };

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
    mergeToRow(unordered_rows.at(4), unordered_rows.at(3), expectedDistEifLib)
  };

  
  
  
  buildAndTestSmallTestSetLargeChildren(
    "<max-distance-in-meters:1>", true, expectedMaxDist1_rows);
  buildAndTestSmallTestSetLargeChildren(
    "<max-distance-in-meters:1>", false, expectedMaxDist1_rows);
  buildAndTestSmallTestSetLargeChildren(
    "<max-distance-in-meters:5000>", true, expectedMaxDist5000_rows);
  buildAndTestSmallTestSetLargeChildren(
    "<max-distance-in-meters:5000>", false, expectedMaxDist5000_rows);
  buildAndTestSmallTestSetLargeChildren(
    "<max-distance-in-meters:500000>", true, expectedMaxDist500000_rows);
  buildAndTestSmallTestSetLargeChildren(
    "<max-distance-in-meters:500000>", false, expectedMaxDist500000_rows);
  buildAndTestSmallTestSetLargeChildren(
    "<max-distance-in-meters:1000000>", true, expectedMaxDist1000000_rows);
  buildAndTestSmallTestSetLargeChildren(
    "<max-distance-in-meters:1000000>", false, expectedMaxDist1000000_rows);
  buildAndTestSmallTestSetLargeChildren(
    "<max-distance-in-meters:10000000>", true, expectedMaxDist10000000_rows);
  buildAndTestSmallTestSetLargeChildren(
    "<max-distance-in-meters:10000000>", false, expectedMaxDist10000000_rows);

  // also build the queryexecutiontree with the spatialjoin beeing at different
  // positions in the tree, e.g. one time basically at a leaf and the other time
  // at the root of the tree

  // also test the other available functions, even though their main tests are
  // done in other functions in this file
  // TODO ====================================================================
}

// test if the SpatialJoin operation parses the maximum distance correctly
void testMaxDistance(std::string distanceIRI, long long distance,
              bool shouldThrow) {
  auto qec = getQec();
  TripleComponent subject{Variable{"?subject"}};
  TripleComponent object{Variable{"?object"}};
  SparqlTriple triple{subject, distanceIRI, object};
  if (shouldThrow) {
    ASSERT_ANY_THROW(ad_utility::makeExecutionTree<SpatialJoin>(qec, triple,
        std::nullopt, std::nullopt));
  } else {
    std::shared_ptr<QueryExecutionTree> spatialJoinOperation = 
        ad_utility::makeExecutionTree<SpatialJoin>(qec, triple,
        std::nullopt, std::nullopt);
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

// test the compute result method on the large dataset from above
// TODO
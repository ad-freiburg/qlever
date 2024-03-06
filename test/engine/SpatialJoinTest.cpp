#include <gtest/gtest.h>
#include <cstdlib>
#include "engine/SpatialJoin.h"
#include "../IndexTestHelpers.h"
#include "engine/QueryExecutionTree.h"
#include "parser/data/Variable.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/ExportQueryExecutionTrees.h"

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

// helper function to create a vector of strings representing rows, from a
// vector of strings representing columns
std::vector<std::string> create_row_vector_from_column_vector(
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


// helper function to create a vector of strings from a result table
std::vector<std::string> print_table(const QueryExecutionContext* qec,
                  const std::shared_ptr<const ResultTable> table) {
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

// this function compares the ResultTable resultTableToTest with the
// expected_output. It assumes, that all rows are unique. Therefore the tables
// are equal, if each row of the expected_output is contained in the table and
// the tables have the same size
void compare_result_table(//const QueryExecutionContext* qec,
              //const std::shared_ptr<const ResultTable> resultTableToTest, 
              std::vector<std::string> tableToTest,
              std::vector<std::string> *expected_output) {
  // rows will be a reordered version of the tableToTest
  std::string rows[expected_output->size()];
  // std::vector<std::string> tableToTest = print_table(qec, resultTableToTest);
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
  // Muenster Freiburg
  kg += "<node_2> <name> \"Muenster Freiburg\" .";
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
  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = getQec(kg, false, false, false, blocksizePermutations, false);
  auto numTriples = qec->getIndex().numTriples().normal_;
  ASSERT_EQ(numTriples, 15);
  // the following Query will be simulated, the max distance will be different
  // depending on the test:
  // Select * where {
  //   ?obj1 <name> ?name1 .
  //   ?obj1 <hasGeometry> ?geo1 .
  //   ?geo1 <asWKT> ?point1
  //   ?obj2 <name> ?name2 .
  //   ?obj2 <hasGeometry> ?geo2 .
  //   ?geo2 <asWKT> ?point2
  //   ?point1 <max-distance-in-meters:5000> ?point2 .
  // }
  // ===================== build the left input ===============================
  //TripleComponent obj1{Variable{"?obj1"}};
  //TripleComponent name1{Variable{"?name1"}};
  //std::shared_ptr<QueryExecutionTree> scan1 =
  //      ad_utility::makeExecutionTree<IndexScan>(qec, 
  //      Permutation::Enum::PSO, SparqlTriple{obj1, "<name>", name1});
  

  /* test of the first IndexScan, not needed anymore
  std::shared_ptr<const ResultTable> res1 = scan1->getResult();
  size_t result_size = res1->size();
  ASSERT_EQ(result_size, 5);
  std::string line1 = "<node_1> \"Uni Freiburg TF\"";
  std::string line2 = "<node_2> \"Muenster Freiburg\"";
  std::string line3 = "<node_3> \"London Eye\"";
  std::string line4 = "<node_4> \"Statue of liberty\"";
  std::string line5 = "<node_5> \"eiffel tower\"";
  std::vector<std::string> expected_output1{line1, line2, line3, line4, line5};
  compare_result_table(print_table(qec, res1), &expected_output1);
  */
  
  //TripleComponent geo1{Variable{"?geo1"}};
  //std::shared_ptr<QueryExecutionTree> scan2 =
  //      ad_utility::makeExecutionTree<IndexScan>(qec,
  //      Permutation::Enum::PSO, SparqlTriple{obj1, "<hasGeometry>", geo1});
  /* test of the second IndexScan, not needed anymore
  std::shared_ptr<const ResultTable> res2 = scan2->getResult();
  result_size = res2->size();
  ASSERT_EQ(result_size, 5);
  line1 = "<node_1> <geometry1>";
  line2 = "<node_2> <geometry2>";
  line3 = "<node_3> <geometry3>";
  line4 = "<node_4> <geometry4>";
  line5 = "<node_5> <geometry5>";
  std::vector<std::string> expected_output2{line1, line2, line3, line4, line5};
  compare_result_table(print_table(qec, res2), &expected_output2);
  */

  //TripleComponent point1{Variable{"?point1"}};
  //std::shared_ptr<QueryExecutionTree> scan3 =
  //      ad_utility::makeExecutionTree<IndexScan>(qec,
  //      Permutation::Enum::PSO, SparqlTriple{geo1, "<asWKT>", point1});
  
  /* test of the third IndexScan, not needed anymore
  std::shared_ptr<const ResultTable> res3 = scan3->getResult();
  result_size = res3->size();
  ASSERT_EQ(result_size, 5);
  line1 = "<geometry1> \"POINT(7.83505 48.01267)\"";
  line2 = "<geometry2> \"POINT(7.85298 47.99557)\"";
  line3 = "<geometry3> \"POINT(-0.11957 51.50333)\"";
  line4 = "<geometry4> \"POINT(-74.04454 40.68925)\"";
  line5 = "<geometry5> \"POINT(2.29451 48.85825)\"";
  std::vector<std::string> expected_output3{line1, line2, line3, line4, line5};
  compare_result_table(print_table(qec, res3), &expected_output3);
  */

  
  //std::shared_ptr<QueryExecutionTree> join1 =
  //      ad_utility::makeExecutionTree<Join>(qec, scan1, scan2, 0, 0, true);
  /* test of the first Join
  std::shared_ptr<const ResultTable> res4 = join1->getResult();
  size_t result_size4 = res4->size();
  ASSERT_EQ(result_size4, 5);
  std::vector<std::string> columnName{
          "\"Uni Freiburg TF\"", "\"Muenster Freiburg\"", "\"London Eye\"",
          "\"Statue of liberty\"", "\"eiffel tower\""};
  std::vector<std::string> columnNode{
          "<node_1>", "<node_2>", "<node_3>", "<node_4>", "<node_5>"};
  std::vector<std::string> columnGeometry{
          "<geometry1>", "<geometry2>", "<geometry3>", "<geometry4>",
          "<geometry5>"};
  std::vector<std::vector<std::string>> columns{
                columnName, columnNode, columnGeometry};
  std::vector<std::string> columnNames{"?name1", "?obj1", "?geo1"};
  auto columnsTest = orderColAccordingToVarColMap(join1->getVariableColumns(),
        columns, columnNames);
  auto rows = create_row_vector_from_column_vector(columnsTest);
  compare_result_table(print_table(qec, res4), &rows);
  */

 //auto varCol1 = join1->getVariableColumns();
 //auto varCol2 = scan3->getVariableColumns();
 //Variable varPoint = geo1.getVariable();
 //size_t col1 = varCol1[varPoint].columnIndex_;
 //size_t col2 = varCol2[varPoint].columnIndex_;
 //std::shared_ptr<QueryExecutionTree> join2 = 
  //    ad_utility::makeExecutionTree<Join>(qec, join1, scan3, col1, col2, true);
  TripleComponent obj1{Variable{"?obj1"}};
  TripleComponent geo1{Variable{"?geo1"}};
  // needed as getVariable() returns a const variable
  Variable joinVar1 = obj1.getVariable();
  Variable joinVar2 = geo1.getVariable();
  auto join2 = buildMediumChild(qec,
          obj1,
          std::string{"<name>"},
          TripleComponent{Variable{"?name1"}},
          obj1,
          std::string{"<hasGeometry>"},
          geo1,
          geo1, std::string{"<asWKT>"}, 
          TripleComponent{Variable{"?point1"}}, joinVar1, joinVar2);
  // test of the second Join
  std::shared_ptr<const ResultTable> res5 = join2->getResult();
  size_t result_size5 = res5->size();
  ASSERT_EQ(result_size5, 5);
  std::vector<std::string> columnName{
          "\"Uni Freiburg TF\"", "\"Muenster Freiburg\"", "\"London Eye\"",
          "\"Statue of liberty\"", "\"eiffel tower\""};
  std::vector<std::string> columnNode{
          "<node_1>", "<node_2>", "<node_3>", "<node_4>", "<node_5>"};
  std::vector<std::string> columnGeometry{
          "<geometry1>", "<geometry2>", "<geometry3>", "<geometry4>",
          "<geometry5>"};
  std::vector<std::string> columnPoint{
          "\"POINT(7.83505 48.01267)\"", "\"POINT(7.85298 47.99557)\"",
          "\"POINT(-0.11957 51.50333)\"", "\"POINT(-74.04454 40.68925)\"",
          "\"POINT(2.29451 48.85825)\""};
  std::vector<std::vector<std::string>> columns{
                columnName, columnNode, columnGeometry, columnPoint};
  std::vector<std::string> columnNames{"?name1", "?obj1", "?geo1", "?point1"};
  auto columnsTest = orderColAccordingToVarColMap(join2->getVariableColumns(),
        columns, columnNames);
  auto rows = create_row_vector_from_column_vector(columnsTest);
  compare_result_table(print_table(qec, res5), &rows);
  print_vec(rows);
  
  
  
  // ======================= build the right input ============================

  // also build the queryexecutiontree with the spatialjoin beeing at different
  // positions in the tree, e.g. one time basically at a leaf and the other time
  // at the root of the tree

  // also test the other available functions, even though their main tests are
  // done in other functions in this file
  // TODO ====================================================================
}

// test the compute result method on the large dataset from above
// TODO
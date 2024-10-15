#pragma once

#include <cstdlib>

#include "../util/IndexTestHelpers.h"
#include "./../../src/util/GeoSparqlHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/SpatialJoinAlgorithms.h"
#include "parser/data/Variable.h"

namespace SpatialJoinTestHelpers {

auto makePointLiteral = [](std::string_view c1, std::string_view c2) {
  return absl::StrCat(" \"POINT(", c1, " ", c2, ")\"^^<", GEO_WKT_LITERAL, ">");
};

// helper function to create a vector of strings from a result table
inline std::vector<std::string> printTable(const QueryExecutionContext* qec,
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
inline std::vector<std::vector<std::string>> orderColAccordingToVarColMap(
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
inline std::vector<std::string> createRowVectorFromColumnVector(
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
inline std::string createSmallDatasetWithPoints() {
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

inline QueryExecutionContext* buildTestQEC() {
  std::string kg = createSmallDatasetWithPoints();
  ad_utility::MemorySize blocksizePermutations = 16_MB;
  auto qec = ad_utility::testing::getQec(kg, true, true, false,
                                         blocksizePermutations, false);
  return qec;
}

inline std::shared_ptr<QueryExecutionTree> buildIndexScan(
    QueryExecutionContext* qec, std::array<std::string, 3> triple) {
  TripleComponent subject{Variable{triple.at(0)}};
  TripleComponent object{Variable{triple.at(2)}};
  return ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO, SparqlTriple{subject, triple.at(1), object});
}

inline std::shared_ptr<QueryExecutionTree> buildJoin(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> tree1,
    std::shared_ptr<QueryExecutionTree> tree2, Variable joinVariable) {
  auto varCol1 = tree1->getVariableColumns();
  auto varCol2 = tree2->getVariableColumns();
  size_t col1 = varCol1[joinVariable].columnIndex_;
  size_t col2 = varCol2[joinVariable].columnIndex_;
  return ad_utility::makeExecutionTree<Join>(qec, tree1, tree2, col1, col2,
                                             true);
}

inline std::shared_ptr<QueryExecutionTree> buildMediumChild(
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

inline std::shared_ptr<QueryExecutionTree> buildSmallChild(
    QueryExecutionContext* qec, std::array<std::string, 3> triple1,
    std::array<std::string, 3> triple2, std::string joinVariable_) {
  Variable joinVariable{joinVariable_};
  auto scan1 = buildIndexScan(qec, triple1);
  auto scan2 = buildIndexScan(qec, triple2);
  return buildJoin(qec, scan1, scan2, joinVariable);
}

}  // namespace SpatialJoinTestHelpers

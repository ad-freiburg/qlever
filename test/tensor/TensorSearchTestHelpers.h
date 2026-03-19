#ifndef QLEVER_TEST_TENSOR_TENSORSEARCHHELPERS_H
#define QLEVER_TEST_TENSOR_TENSORSEARCHHELPERS_H

#include <absl/strings/str_cat.h>

#include <cstdlib>

#include "../util/IndexTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TensorSearch.h"
#include "engine/TensorSearchCachedIndex.h"
#include "index/vocabulary/VocabularyType.h"
#include "rdfTypes/Variable.h"
#include "util/GeoSparqlHelpers.h"

namespace TensorSearchTestHelpers {

// constexpr inline auto makeLineLiteral = [](std::string_view coordinateList) {
//   return absl::StrCat("\"LINESTRING(", coordinateList, ")\"^^<",
//                       GEO_WKT_LITERAL, ">");
// };

// const std::string approximatedAreaGermany = makeLineLiteral(
//     "7.20369317867016 53.62121249029073, "
//     "9.335040870259194 54.77156944262062, 13.97127141588071 53.7058383745324,
//     " "14.77327338230339 51.01654754091759, 11.916828022441791 "
//     "50.36932046223437, "
//     "13.674640551587391 48.68663848319227, 12.773761630400273 "
//     "47.74969625921073, "
//     "7.58917 47.59002, 8.03916 49.01783, "
//     "6.50056816701192 49.535220384133375, 6.0391423781112 51.804566644690524,
//     " "7.20369317867016 53.62121249029073");

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

// Build a `QueryExecutionContext` from the given turtle, but set some memory
// defaults to higher values to make it possible to test large geometric
// literals. `vocabType` can be set
inline auto buildQec(std::string turtleKg, bool useGeoVocab = false) {
  ad_utility::testing::TestIndexConfig config{turtleKg};
  std::optional<ad_utility::VocabularyType> vocabType = std::nullopt;
  if (useGeoVocab) {
    using enum ad_utility::VocabularyType::Enum;
    vocabType = ad_utility::VocabularyType{OnDiskCompressedGeoSplit};
  }
  config.vocabularyType = vocabType;
  config.blocksizePermutations = 16_MB;
  config.parserBufferSize = 10_kB;
  return ad_utility::testing::getQec(std::move(config));
}

inline std::shared_ptr<QueryExecutionTree> buildIndexScan(
    QueryExecutionContext* qec, std::array<std::string, 3> triple) {
  TripleComponent subject{Variable{triple.at(0)}};
  TripleComponent object{Variable{triple.at(2)}};
  return ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{
          subject, TripleComponent::Iri::fromIriref(triple.at(1)), object});
}

inline std::shared_ptr<QueryExecutionTree> buildTensorSearch(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> tree1,
    std::shared_ptr<QueryExecutionTree> tree2, Variable joinVariable) {
  auto varCol1 = tree1->getVariableColumns();
  auto varCol2 = tree2->getVariableColumns();
  size_t col1 = varCol1[joinVariable].columnIndex_;
  size_t col2 = varCol2[joinVariable].columnIndex_;
  return ad_utility::makeExecutionTree<TensorSearch>(qec, tree1, tree2, col1,
                                                     col2);
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
  auto tensorSearch = buildTensorSearch(qec, scan1, scan2, joinVariable1);
  return buildTensorSearch(qec, tensorSearch, scan3, joinVariable2);
}

inline std::shared_ptr<QueryExecutionTree> buildSmallChild(
    QueryExecutionContext* qec, std::array<std::string, 3> triple1,
    std::array<std::string, 3> triple2, std::string joinVariable_) {
  Variable joinVariable{joinVariable_};
  auto scan1 = buildIndexScan(qec, triple1);
  auto scan2 = buildIndexScan(qec, triple2);
  return buildTensorSearch(qec, scan1, scan2, joinVariable);
}
}  // namespace TensorSearchTestHelpers

#endif  // QLEVER_TEST_TENSOR_TENSORSEARCHHELPERS_H

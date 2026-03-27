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
// Helper to build a small deterministic dataset of vectors. The returned
// Turtle contains subjects <s0> .. <sN-1> with predicate <p1> and a
// tensor literal as required by the tensor datatype.
std::string makeVectorKg(size_t n, size_t dim = 3) {
  std::string out;
  for (size_t i = 0; i < n; ++i) {
    std::vector<double> vec(dim, 0.0);
    vec[i % dim] = 1.0;
    std::string data = R"({\"data\":[)";
    for (size_t d = 0; d < dim; ++d) {
      data += absl::StrCat(vec[d]);
      if (d + 1 < dim) data += ",";
    }
    data +=
        R"(],\"shape\":[)" + std::to_string(dim) + R"(],\"type\":\"float32\"})";
    out += absl::StrCat(
        "<s", i, "> <p1> \"", data,
        "\"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .\n");
    out += absl::StrCat("<s", i, "> <name> \"", "Name", i, "\" .\n");
    if (i < n - 1) {
      out += absl::StrCat("<s", i, "> <rel> <s", i + 1, "> .\n");
    }
  }
  return out;
}
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
// should be tested (it uses a vector of vectors (the first vector is
// containing each column of the result, each column consist of a vector,
// where each entry is a row of this column))
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

inline std::shared_ptr<QueryExecutionTree> buildJoin(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> tree1,
    std::shared_ptr<QueryExecutionTree> tree2, Variable joinVariable) {
  auto varCol1 = tree1->getVariableColumns();
  auto varCol2 = tree2->getVariableColumns();
  size_t col1 = varCol1[joinVariable].columnIndex_;
  size_t col2 = varCol2[joinVariable].columnIndex_;
  return ad_utility::makeExecutionTree<Join>(qec, tree1, tree2, col1, col2);
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
  auto tensorSearch = buildJoin(qec, scan1, scan2, joinVariable1);
  return buildJoin(qec, tensorSearch, scan3, joinVariable2);
}

inline std::shared_ptr<QueryExecutionTree> buildSmallChild(
    QueryExecutionContext* qec, std::array<std::string, 3> triple1,
    std::array<std::string, 3> triple2, std::string joinVariable_) {
  Variable joinVariable{joinVariable_};
  auto scan1 = buildIndexScan(qec, triple1);
  auto scan2 = buildIndexScan(qec, triple2);
  return buildJoin(qec, scan1, scan2, joinVariable);
}

struct AlgDistParam {
  TensorSearchAlgorithm algo;
  TensorDistanceAlgorithm dist;
  std::string name;
  bool reverse = false;
};

class TensorSearchFunctionalTest
    : public ::testing::TestWithParam<AlgDistParam> {};

inline std::shared_ptr<QueryExecutionTree> buildSmallestChild(
    QueryExecutionContext* qec, std::array<std::string, 3> triple) {
  auto scan = buildIndexScan(qec, triple);
  return scan;
}

std::shared_ptr<TensorSearch> makeTensorSearch(
    QueryExecutionContext* qec, bool addDist = true,
    PayloadVariables pv = PayloadVariables::all(),
    TensorSearchAlgorithm alg = TENSOR_SEARCH_DEFAULT_ALGORITHM,
    TensorDistanceAlgorithm distAlg = TENSOR_SEARCH_DEFAULT_DISTANCE,
    std::optional<std::string> cacheName = std::nullopt,
    ssize_t max_num_results = -1, bool addLeftChildFirst = false) {
  auto leftChild =
      buildSmallestChild(qec, {"?obja", std::string{"<p1>"}, "?t1"});
  auto rightChild =
      buildSmallestChild(qec, {"?objb", std::string{"<p1>"}, "?t2"});

  std::optional<Variable> dist = std::nullopt;
  if (addDist) {
    dist = Variable{"?dist"};
  }
  std::shared_ptr<QueryExecutionTree> tensorSearchOperation =
      ad_utility::makeExecutionTree<TensorSearch>(
          qec,
          TensorSearchConfiguration{Variable{"?t1"}, Variable{"?t2"}, dist, pv,
                                    alg, distAlg, max_num_results, std::nullopt,
                                    std::nullopt, cacheName},
          std::nullopt, std::nullopt);
  std::shared_ptr<Operation> op = tensorSearchOperation->getRootOperation();
  TensorSearch* tensorSearch = static_cast<TensorSearch*>(op.get());
  auto firstChild = addLeftChildFirst ? leftChild : rightChild;
  auto secondChild = addLeftChildFirst ? rightChild : leftChild;
  Variable firstVariable =
      addLeftChildFirst ? Variable{"?t1"} : Variable{"?t2"};
  Variable secondVariable =
      addLeftChildFirst ? Variable{"?t2"} : Variable{"?t1"};
  auto tensorSearch1 = tensorSearch->addChild(firstChild, firstVariable);
  tensorSearch = static_cast<TensorSearch*>(tensorSearch1.get());
  auto tensorSearch2 = tensorSearch->addChild(secondChild, secondVariable);
  return tensorSearch2;
}

void expectSelfResult(const IdTable* idTable, VariableToColumnMap& varColMap,
                      QueryExecutionContext* qec, bool reverse = false) {
  for (size_t i = 0; i < idTable->numRows(); ++i) {
    // id of obja:
    auto col_id_obja = varColMap.at(Variable{"?obja"}).columnIndex_;
    auto col_id_objb = varColMap.at(Variable{"?objb"}).columnIndex_;
    auto col_id_dist = varColMap.at(Variable{"?dist"}).columnIndex_;
    auto aid = idTable->at(i, col_id_obja);
    auto bid = idTable->at(i, col_id_objb);
    auto dist = idTable->at(i, col_id_dist);
    auto aStrOpt =
        ExportQueryExecutionTrees::idToStringAndType(qec->getIndex(), aid, {});
    auto bStrOpt =
        ExportQueryExecutionTrees::idToStringAndType(qec->getIndex(), bid, {});
    auto distOpt =
        ExportQueryExecutionTrees::idToStringAndType(qec->getIndex(), dist, {});
    std::cout << "Result row " << i << ": subject id " << aStrOpt->first
              << ", value id " << bStrOpt->first << ", distance "
              << distOpt->first << "\n";
    ASSERT_TRUE(aStrOpt.has_value());
    ASSERT_TRUE(bStrOpt.has_value());
    if (reverse) {
      // EXPECT_NEQ(aStrOpt->first, bStrOpt->first);
    } else {
      EXPECT_EQ(aStrOpt->first, bStrOpt->first);
    }
  }
}
}  // namespace TensorSearchTestHelpers

#endif  // QLEVER_TEST_TENSOR_TENSORSEARCHHELPERS_H

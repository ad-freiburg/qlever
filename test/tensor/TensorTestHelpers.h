// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
// Adapted from TextIndexScanTestHelpers.h
#ifndef QLEVER_TEST_TENSORTESTHELPERS_H_
#define QLEVER_TEST_TENSORTESTHELPERS_H_

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <fstream>

#include "../util/GTestHelpers.h"
#include "libqlever/Qlever.h"
#include "rdfTypes/TensorData.h"
#include "util/Exception.h"

namespace TensorTestHelpers {
using namespace ad_utility;
using namespace ::testing;

// ____________________________________________________________________________
inline auto tensorDataMatcher = liftOptionalMatcher<TensorData>(
    [](TensorData expected) -> Matcher<TensorData> {
      return AllOf(Property(&TensorData::dtype, Eq(expected.dtype())),
                   Property(&TensorData::size, Eq(expected.size())),
                   Property(&TensorData::tensorData,
                            Pointwise(FloatEq(), expected.tensorData())));
    });
#define EXPECT_TENSOR_DATA(a, b) \
  EXPECT_THAT(a, TensorTestHelpers::tensorDataMatcher(b))

static constexpr std::string_view exampleTensorLit =
    R"("{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type":\"float32\"}")"
    "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>";
static constexpr std::string_view dummyTurtle = R"(
  <s1> <p2> <o1> .
  <s3> <p1> "{\"data\":[1.0,1.0,-2.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s2> <p2> <o2> .
  <s1> <p1> "{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s2> <p1> "{\"data\":[1.0,1.0,3.0],\"shape\":[3],\"type\":\"float32\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s2> <p2> <o2> .
)";
// _____________________________________________________________________________
inline void makeTestIndex(const std::string& basename, const std::string& kg,
                          VocabularyType type) {
  // Write dummy turtle file
  auto ttlFilename = absl::StrCat(basename, ".ttl");
  {
    std::ofstream ttl{ttlFilename};
    ttl << kg;
  }

  // Build index on dummy turtle file
  qlever::IndexBuilderConfig config;
  config.vocabType_ = type;
  config.inputFiles_.emplace_back(ttlFilename, qlever::Filetype::Turtle);
  config.baseName_ = basename;
  qlever::Qlever::buildIndex(config);
}

// _____________________________________________________________________________
inline void removeTestIndex(const std::string& basename) {
  std::regex pattern(absl::StrCat(basename, "\\..*"));
  std::cout << "Removing test files " << basename << ".*" << std::endl;
  for (const auto& entry :
       std::filesystem::directory_iterator(std::filesystem::current_path())) {
    if (entry.is_regular_file() &&
        std::regex_match(entry.path().filename().string(), pattern)) {
      std::filesystem::remove(entry.path());
    }
  }
}

// _____________________________________________________________________________
template <typename T = ::testing::Test>
class TensorQueryTest : public T {
 private:
  std::shared_ptr<qlever::Qlever> qlv_;

 protected:
  const std::string testIndexBase_ = "_tensorTestIndex";
  std::stringstream log_;

  // ___________________________________________________________________________
  virtual std::string getDummyTurtle() const {
    return std::string{dummyTurtle};
  }

  // ___________________________________________________________________________
  void SetUp() override { qlv_ = nullptr; }

  // ___________________________________________________________________________
  void TearDown() override {
    qlv_ = nullptr;
    removeTestIndex(testIndexBase_);
    ad_utility::setGlobalLoggingStream(&std::cout);
  }

  // ___________________________________________________________________________
  qlever::Qlever& qlv(VocabularyType vocabType = VocabularyType(
                          VocabularyType::Enum::OnDiskCompressed)) {
    if (qlv_ == nullptr) {
      // ad_utility::setGlobalLoggingStream(&log_);
      ad_utility::setGlobalLoggingStream(&std::cout);
      makeTestIndex(testIndexBase_, getDummyTurtle(), vocabType);
      qlever::EngineConfig config;
      config.baseName_ = testIndexBase_;
      qlv_ = std::make_shared<qlever::Qlever>(config);
    }
    return *qlv_;
  }

  // ___________________________________________________________________________
  void clearLog() { log_.str(""); }

  // Helper that evaluates a query on the test index and returns its result as
  // an `IdTable` with the same column ordering as the columns in the `SELECT`
  // statement.
  IdTable getQueryResultAsIdTable(std::string query) {
    auto [qet, qec, parsed] = qlv().parseAndPlanQuery(std::move(query));

    // Get the visible variables' column indices in the correct order.
    if (!parsed.hasSelectClause()) {
      throw std::runtime_error(
          "Only IdTables for SELECT can be exported so far.");
    }
    auto selectColOrdering =
        qet->selectedVariablesToColumnIndices(parsed.selectClause());
    auto columns = ::ranges::to<std::vector<ColumnIndex>>(
        ql::views::transform(selectColOrdering, [](const auto& colIdxAndType) {
          if (!colIdxAndType.has_value()) {
            throw std::runtime_error("Binds in SELECT clause not allowed.");
          }
          return colIdxAndType.value().columnIndex_;
        }));

    // Compute the result and permute the `IdTable` as expected.
    auto res = qet->getResult(false);
    auto idTable = res->idTable().clone();
    idTable.setColumnSubset(columns);
    return idTable;
  }
};
}  // namespace TensorTestHelpers
#endif  // QLEVER_TEST_TENSORTESTHELPERS_H_

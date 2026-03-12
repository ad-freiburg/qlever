// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
// Adapted from TextIndexScanTestHelpers.h
#ifndef QLEVER_TEST_TENSORTESTHELPERS_H_
#define QLEVER_TEST_TENSORTESTHELPERS_H_

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <fstream>

#include "../util/GTestHelpers.h"
#include "util/TensorData.h"
#include "libqlever/Qlever.h"
#include "util/Exception.h"

namespace tensorTestHelpers {

static constexpr std::string_view dummyTurtle = R"(
  <s3> <p1> "{\"data\":[1.0,1.0,-2.0],\"shape\":[3],\"type\":\"float64\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s1> <p1> "{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type\":\"float64\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
  <s2> <p1> "{\"data\":[1.0,1.0,3.0],\"shape\":[3],\"type\":\"float64\"}"^^<https://w3id.org/rdf-tensor/datatypes#DataTensor> .
)";
// _____________________________________________________________________________
inline void makeTestIndex(const std::string& basename, const std::string& kg) {
  // Write dummy turtle file
  auto ttlFilename = absl::StrCat(basename, ".ttl");
  {
    std::ofstream ttl{ttlFilename};
    ttl << kg;
  }

  // Build index on dummy turtle file
  qlever::IndexBuilderConfig config;
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
class TensorQueryTest : public ::testing::Test {
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
  void SetUp() override {
    ad_utility::setGlobalLoggingStream(&log_);
    makeTestIndex(testIndexBase_, getDummyTurtle());
    qlever::EngineConfig config;
    config.baseName_ = testIndexBase_;
    qlv_ = std::make_shared<qlever::Qlever>(config);
  }

  // ___________________________________________________________________________
  void TearDown() override {
    qlv_ = nullptr;
    removeTestIndex(testIndexBase_);
    ad_utility::setGlobalLoggingStream(&std::cout);
  }

  // ___________________________________________________________________________
  qlever::Qlever& qlv() {
    AD_CORRECTNESS_CHECK(qlv_ != nullptr);
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
}  // namespace tensorTestHelpers
#endif  // QLEVER_TEST_TENSORTESTHELPERS_H_

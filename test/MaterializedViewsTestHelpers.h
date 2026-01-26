// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_TEST_MATERIALIZEDVIEWSTESTHELPERS_H_
#define QLEVER_TEST_MATERIALIZEDVIEWSTESTHELPERS_H_

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <fstream>

#include "./util/GTestHelpers.h"
#include "engine/MaterializedViews.h"
#include "libqlever/Qlever.h"
#include "util/Exception.h"

namespace materializedViewsTestHelpers {

static constexpr std::string_view dummyTurtle = R"(
  <s1> <p1> "abc" .
  <s1> <p2> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
  <s2> <p1> "xyz" .
  <s2> <p3> <http://example.com/> .
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
class MaterializedViewsTest : public ::testing::Test {
 private:
  std::shared_ptr<qlever::Qlever> qlv_;

 protected:
  const std::string testIndexBase_ = "_materializedViewsTestIndex";
  const std::string simpleWriteQuery_ = "SELECT * { ?s ?p ?o . BIND(1 AS ?g) }";
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

// _____________________________________________________________________________
class MaterializedViewsTestLarge : public MaterializedViewsTest {
 protected:
  static constexpr size_t numFakeSubjects_ = 10'000;

  std::string getDummyTurtle() const override {
    std::string dummy;
    for (size_t i = 0; i < numFakeSubjects_; ++i) {
      dummy =
          absl::StrCat(dummy, "<s", i,
                       "> <p1> \"abc\" ."
                       "<s",
                       i, "> <p2> \"", 2 * i,
                       "\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n");
    }
    return dummy;
  }
};

}  // namespace materializedViewsTestHelpers

#endif  // QLEVER_TEST_MATERIALIZEDVIEWSTESTHELPERS_H_

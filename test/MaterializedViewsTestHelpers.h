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

#include "./QueryPlannerTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/MaterializedViews.h"
#include "engine/QueryExecutionContext.h"
#include "libqlever/Qlever.h"
#include "util/Exception.h"

namespace materializedViewsTestHelpers {

namespace h = queryPlannerTestHelpers;

static constexpr std::string_view dummyTurtle = R"(
  <s1> <p1> "abc" .
  <s1> <p2> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
  <s2> <p1> "xyz" .
  <s2> <p3> <http://example.com/> .
)";

static constexpr std::string_view cacheKeyRewriteDummyTurtle = R"(
  @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
  <s1> <p1> "abc" .
  <s1> <p3> "abc1" .
  <s1> <p3> "abc2" .
  <s1> <p3> "abc3" .
  <s2> <p1> "xyz" .
  <s1> <p2> "1"^^xsd:integer .
  <s2> <p3> <s3> .
  <s3> <p2> "7"^^xsd:integer .
  <s2> <p3> <s4> .
  <s3> <p2> "5"^^xsd:integer .
  <s3> <p4> <http://example.com/> .
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
  const std::string testIndexBase_ = gtestCurrentTestName();
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
  std::shared_ptr<QueryExecutionContext> getQec() {
    return qlv_->createQueryExecutionContext(qlv_->indexAndViewsSnapshot());
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
    auto idTable = res->cloneIdTable();
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

// _____________________________________________________________________________
class MaterializedViewsCacheKeyRewriteTest : public MaterializedViewsTest {
 protected:
  std::string getDummyTurtle() const override {
    return std::string{cacheKeyRewriteDummyTurtle};
  }
};

// _____________________________________________________________________________
struct RewriteTestParams {
  // Query to write the test view.
  std::string writeQuery_;

  // Enforce a query planning budget to allow testing the greedy query planner
  // with toy examples.
  size_t queryPlanningBudget_;
};

// _____________________________________________________________________________
class MaterializedViewsQueryRewriteTest
    : public ::testing::TestWithParam<RewriteTestParams> {
 protected:
  std::stringstream log_;

  // ___________________________________________________________________________
  void SetUp() override { ad_utility::setGlobalLoggingStream(&log_); }

  // ___________________________________________________________________________
  void TearDown() override { ad_utility::setGlobalLoggingStream(&std::cout); }
};

// We make subclasses of `MaterializedViewsQueryRewriteTest` here s.t. we can
// use different `INSTANTIATE_TEST_SUITE_P` calls for different rewriting tests.
class MaterializedViewsChainRewriteTest
    : public MaterializedViewsQueryRewriteTest {};
class MaterializedViewsStarRewriteTest
    : public MaterializedViewsQueryRewriteTest {};

// _____________________________________________________________________________
inline void PrintTo(const RewriteTestParams& p, std::ostream* os) {
  auto& s = *os;
  s << "write query = '" << p.writeQuery_
    << "', budget = " << p.queryPlanningBudget_;
}

// _____________________________________________________________________________
inline void qpExpect(qlever::Qlever& qlv, const auto& query,
                     ::testing::Matcher<const QueryExecutionTree&> matcher,
                     source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  // For query planning to produce the expected results reliably, we need to
  // clear the cache.
  qlv.clearQueryResultCache();
  auto [qet, qec, parsed] = qlv.parseAndPlanQuery(std::string{query});
  EXPECT_THAT(*qet, matcher);
};

// _____________________________________________________________________________
inline auto viewScan(
    std::string viewName, std::string a, std::string b, std::string c,
    std::optional<size_t> strippedSize = std::nullopt,
    std::vector<std::pair<ColumnIndex, Variable>> additionalColumns = {}) {
  return h::IndexScanFromStrings(std::move(a), std::move(b), std::move(c),
                                 {Permutation::Enum::SPO}, std::monostate{},
                                 additionalColumns | ql::views::values |
                                     ::ranges::to<std::vector<Variable>>(),
                                 additionalColumns | ql::views::keys |
                                     ::ranges::to<std::vector<ColumnIndex>>(),
                                 strippedSize, viewName);
};

// _____________________________________________________________________________
inline auto viewScanSimple(std::string viewName, std::string a, std::string b,
                           std::string c) {
  // Helper because `std::bind_front` does not like argument default values.
  return viewScan(std::move(viewName), std::move(a), std::move(b),
                  std::move(c));
};

// _____________________________________________________________________________
inline void expectNotSuitableForRewrite(
    const qlever::Qlever& qlv, const MaterializedViewsManager& manager,
    const auto& viewName, const auto& query,
    source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  materializedViewsQueryAnalysis::QueryPatternCache qpc;
  auto plan = qlv.parseAndPlanQuery(query);
  auto qec = qlv.createQueryExecutionContext(qlv.indexAndViewsSnapshot());
  manager.writeViewToDisk(viewName, plan);
  auto view = manager.getView(viewName, qec);
  EXPECT_FALSE(qpc.analyzeView(view, qec));
  manager.unloadViewIfLoaded(viewName);
};

}  // namespace materializedViewsTestHelpers

#endif  // QLEVER_TEST_MATERIALIZEDVIEWSTESTHELPERS_H_

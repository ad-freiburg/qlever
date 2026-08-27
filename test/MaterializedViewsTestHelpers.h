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
#include "./util/RuntimeParametersTestHelpers.h"
#include "backports/filesystem.h"
#include "engine/MaterializedViews.h"
#include "engine/QueryExecutionContext.h"
#include "libqlever/Qlever.h"
#include "util/Exception.h"
#include "util/FilesystemHelpers.h"

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
  qlever::util::deleteFilesInDirectory(
      ql::filesystem::current_path(), [&pattern](const auto& path) {
        return std::regex_match(path.filename().string(), pattern);
      });
}

// _____________________________________________________________________________
class MaterializedViewsTest : public ::testing::Test {
 private:
  std::shared_ptr<qlever::Qlever> qlv_;

 protected:
  const std::string testIndexBase_ = gtestCurrentTestName();
  const std::string simpleWriteQuery_ = "SELECT * { ?s ?p ?o . BIND(1 AS ?g) }";
  std::stringstream log_;
  std::optional<decltype(setGlobalLoggingStreamForTesting(nullptr))>
      logStreamCleanup_;

  // ___________________________________________________________________________
  virtual std::string getDummyTurtle() const {
    return std::string{dummyTurtle};
  }

  // ___________________________________________________________________________
  void SetUp() override {
    logStreamCleanup_.emplace(setGlobalLoggingStreamForTesting(&log_));
    makeTestIndex(testIndexBase_, getDummyTurtle());
    qlever::EngineConfig config;
    config.baseName_ = testIndexBase_;
    qlv_ = std::make_shared<qlever::Qlever>(config);
  }

  // ___________________________________________________________________________
  void TearDown() override {
    qlv_ = nullptr;
    removeTestIndex(testIndexBase_);
    // Calls the cleanup, restoring the log stream to the previous value.
    logStreamCleanup_.reset();
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
    auto plannedQuery = qlv().parseAndPlanQuery(std::move(query));
    auto qet = plannedQuery.sharedQueryExecutionTree();
    auto& parsed = plannedQuery.parsedQuery();

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
class MaterializedViewsRewriteTestBase : public ::testing::Test {
 protected:
  std::stringstream log_;
  std::optional<decltype(setGlobalLoggingStreamForTesting(nullptr))>
      logStreamCleanup_;

  // ___________________________________________________________________________
  void SetUp() override {
    logStreamCleanup_.emplace(setGlobalLoggingStreamForTesting(&log_));
  }

  // ___________________________________________________________________________
  void TearDown() override {
    // Calls the cleanup, restoring the log stream to the previous value.
    logStreamCleanup_.reset();
  }
};

// Parameterized on the query used to write the test view (`qpExpect` itself
// tests both the greedy and the DP query planner, so no budget parameter is
// needed here).
class MaterializedViewsChainRewriteTest
    : public MaterializedViewsRewriteTestBase,
      public ::testing::WithParamInterface<std::string> {};

// Only one write query is tested here, so this is not parameterized.
class MaterializedViewsStarRewriteTest
    : public MaterializedViewsRewriteTestBase {};

// _____________________________________________________________________________
// Force both the greedy and the DP query planner and check that both produce
// `matcher`. The greedy planner may build a subtree whose cache key doesn't
// match any materialized view, even where the DP planner's does, so callers
// that test cache-key-based rewriting must pass `TestBothPlanners = false`
// to only test with the DP planner.
template <bool TestBothPlanners = true>
inline void qpExpect(qlever::Qlever& qlv, std::string_view query,
                     ::testing::Matcher<const QueryExecutionTree&> matcher,
                     source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  static constexpr size_t kDpBudget = 1500;
  auto budgets = TestBothPlanners ? std::vector<size_t>{1, kDpBudget}
                                  : std::vector<size_t>{kDpBudget};
  for (size_t budget : budgets) {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::queryPlanningBudget_>(
            budget);
    // For query planning to produce the expected results reliably, we need to
    // clear the cache.
    qlv.clearQueryResultCache();
    auto plannedQuery = qlv.parseAndPlanQuery(std::string{query});
    EXPECT_THAT(plannedQuery.queryExecutionTree(), matcher)
        << "budget = " << budget;
  }
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
// `expectedLogMessage` must be a substring of the `AD_LOG_INFO` message that
// `analyzeView` logs to explain why it ignored the view for pattern-based
// rewriting. This ensures that the query is actually rejected for the reason
// under test, rather than for some unrelated (and possibly accidental) one.
template <typename ViewName, typename Query>
inline void expectNotSuitableForRewrite(
    const qlever::Qlever& qlv, const MaterializedViewsManager& manager,
    const ViewName& viewName, const Query& query,
    std::string_view expectedLogMessage,
    source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
  materializedViewsQueryAnalysis::QueryPatternCache qpc;
  auto plan = qlv.parseAndPlanQuery(query);
  auto qec = qlv.createQueryExecutionContext(qlv.indexAndViewsSnapshot());
  manager.writeViewToDisk(viewName, plan);
  auto view = manager.getView(viewName, qec.get());
  qpc.analyzeView(view, qec.get());
  EXPECT_THAT(logStream.str(), ::testing::HasSubstr(expectedLogMessage));
  // `analyzeView` may still return `true` because the view got registered for
  // cache-key based rewriting, even for queries that (by design) are not
  // suitable for the pattern-based (star/chain) rewriting tested here. So
  // check the latter directly instead of relying on the overall return value.
  const auto& graphPattern = plan.parsedQuery()._rootGraphPattern;
  ASSERT_EQ(graphPattern._graphPatterns.size(), 1u);
  EXPECT_TRUE(qpc.makeJoinReplacementIndexScans(
                     qec.get(), graphPattern._graphPatterns.at(0).getBasic())
                  .empty());
  manager.unloadViewIfLoaded(viewName);
};

// _____________________________________________________________________________
// Writes and loads a view from `viewQuery`, then checks that planning
// `testQuery` produces exactly `matcher`. `testQuery` must not
// be satisfiable by cache-key-based rewriting alone, or this would pass
// without exercising the pattern matcher; make it `viewQuery` plus at least
// one extra triple.
template <typename ViewName, typename ViewQuery, typename TestQuery>
inline void expectRewrite(
    qlever::Qlever& qlv, const ViewName& viewName, const ViewQuery& viewQuery,
    const TestQuery& testQuery,
    ::testing::Matcher<const QueryExecutionTree&> matcher,
    source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  qlv.writeMaterializedView(viewName, std::string{viewQuery});
  qlv.loadMaterializedView(viewName);
  qpExpect(qlv, testQuery, matcher, sourceLocation);
};

}  // namespace materializedViewsTestHelpers

#endif  // QLEVER_TEST_MATERIALIZEDVIEWSTESTHELPERS_H_

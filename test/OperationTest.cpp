// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include <optional>

#include "engine/IndexScan.h"
#include "engine/NeutralElementOperation.h"
#include "engine/ValuesForTesting.h"
#include "global/RuntimeParameters.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

using namespace ad_utility::testing;
using namespace ::testing;
using ad_utility::CacheStatus;
using ad_utility::CancellationException;
using ad_utility::CancellationHandle;
using ad_utility::CancellationState;
using Status = RuntimeInformation::Status;

namespace {
// Helper function to perform actions at various stages of a generator
template <typename Range, typename T = ql::ranges::range_value_t<Range>>
auto expectAtEachStageOfGenerator(
    Range generator, std::vector<std::function<void()>> functions,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto locationTrace = generateLocationTrace(l);
  size_t index = 0;
  for ([[maybe_unused]] T& _ : generator) {
    functions.at(index)();
    ++index;
  }
  EXPECT_EQ(index, functions.size());
}

void expectRtiHasDimensions(
    RuntimeInformation& rti, uint64_t cols, uint64_t rows,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto locationTrace = generateLocationTrace(l);
  EXPECT_EQ(rti.numCols_, cols);
  EXPECT_EQ(rti.numRows_, rows);
}
}  // namespace

// ________________________________________________
TEST(OperationTest, limitIsRepresentedInCacheKey) {
  NeutralElementOperation n{getQec()};
  EXPECT_THAT(n.getCacheKey(), testing::Not(testing::HasSubstr("LIMIT 20")));
  LimitOffsetClause l;
  l._limit = 20;
  n.setLimit(l);
  EXPECT_THAT(n.getCacheKey(), testing::HasSubstr("LIMIT 20"));
  EXPECT_THAT(n.getCacheKey(), testing::Not(testing::HasSubstr("OFFSET 34")));

  l._offset = 34;
  n.setLimit(l);
  EXPECT_THAT(n.getCacheKey(), testing::HasSubstr("OFFSET 34"));
}

// ________________________________________________
TEST(OperationTest, getResultOnlyCached) {
  auto qec = getQec();
  qec->getQueryTreeCache().clearAll();
  NeutralElementOperation n{qec};
  // The second `true` means "only read the result if it was cached".
  // We have just cleared the cache, and so this should return `nullptr`.
  EXPECT_EQ(n.getResult(true, ComputationMode::ONLY_IF_CACHED), nullptr);
  EXPECT_EQ(n.runtimeInfo().status_, Status::notStarted);
  // Nothing has been stored in the cache by this call.
  EXPECT_EQ(qec->getQueryTreeCache().numNonPinnedEntries(), 0);
  EXPECT_EQ(qec->getQueryTreeCache().numPinnedEntries(), 0);

  // This "ordinary" call to `getResult` also stores the result in the cache.
  NeutralElementOperation n2{qec};
  auto result = n2.getResult();
  EXPECT_NE(result, nullptr);
  EXPECT_EQ(n2.runtimeInfo().status_, Status::fullyMaterialized);
  EXPECT_EQ(n2.runtimeInfo().cacheStatus_, CacheStatus::computed);
  EXPECT_EQ(qec->getQueryTreeCache().numNonPinnedEntries(), 1);
  EXPECT_EQ(qec->getQueryTreeCache().numPinnedEntries(), 0);

  // When we now request to only return the result if it is cached, we should
  // get exactly the same `shared_ptr` as with the previous call.
  NeutralElementOperation n3{qec};
  EXPECT_EQ(n3.getResult(true, ComputationMode::ONLY_IF_CACHED), result);
  EXPECT_EQ(n3.runtimeInfo().cacheStatus_, CacheStatus::cachedNotPinned);

  // We can even use the `onlyReadFromCache` case to upgrade a non-pinned
  // cache-entry to a pinned cache entry
  QueryExecutionContext qecCopy{*qec};
  qecCopy._pinResult = true;
  NeutralElementOperation n4{&qecCopy};
  EXPECT_EQ(n4.getResult(true, ComputationMode::ONLY_IF_CACHED), result);

  // The cache status is `cachedNotPinned` because we found the element cached
  // but not pinned (it does reflect the status BEFORE the operation).
  EXPECT_EQ(n4.runtimeInfo().cacheStatus_, CacheStatus::cachedNotPinned);
  EXPECT_EQ(qec->getQueryTreeCache().numNonPinnedEntries(), 0);
  EXPECT_EQ(qec->getQueryTreeCache().numPinnedEntries(), 1);

  // We have pinned the result, so requesting it again should return a pinned
  // result.
  qecCopy._pinResult = false;
  EXPECT_EQ(n4.getResult(true, ComputationMode::ONLY_IF_CACHED), result);
  EXPECT_EQ(n4.runtimeInfo().cacheStatus_, CacheStatus::cachedPinned);

  // Clear the (global) cache again to not possibly interfere with other unit
  // tests.
  qec->getQueryTreeCache().clearAll();
}

// _____________________________________________________________________________

/// Fixture to work with a generic operation
class OperationTestFixture : public testing::Test {
 protected:
  std::vector<std::string> jsonHistory;

  Index index = []() {
    TestIndexConfig indexConfig{};
    indexConfig.blocksizePermutations = 32_B;
    return makeTestIndex("OperationTest", std::move(indexConfig));
  }();
  QueryResultCache cache;
  QueryExecutionContext qec{
      index, &cache, makeAllocator(), SortPerformanceEstimator{},
      [&](std::string json) { jsonHistory.emplace_back(std::move(json)); }};
  IdTable table = makeIdTableFromVector({{}, {}, {}});
  ValuesForTesting operation{&qec, std::move(table), {}};
};

// _____________________________________________________________________________

TEST_F(OperationTestFixture,
       verifyOperationStatusChangesToInProgressAndComputed) {
  // Ignore result, we only care about the side effects
  operation.getResult(true);

  EXPECT_THAT(
      jsonHistory,
      ElementsAre(
          ParsedAsJson(HasKeyMatching("status", Eq("not started"))),
          ParsedAsJson(HasKeyMatching("status", Eq("in progress"))),
          // Note: Currently the implementation triggers twice if a value
          // is not cached. This is not a requirement, just an implementation
          // detail that we account for here.
          ParsedAsJson(HasKeyMatching("status", Eq("fully materialized"))),
          ParsedAsJson(HasKeyMatching("status", Eq("fully materialized")))));
}

// _____________________________________________________________________________

TEST_F(OperationTestFixture, verifyCachePreventsInProgressState) {
  // Run twice and clear history to get cached values
  operation.getResult(true);
  jsonHistory.clear();
  operation.getResult(true);

  EXPECT_THAT(
      jsonHistory,
      ElementsAre(
          ParsedAsJson(HasKeyMatching("status", Eq("not started"))),
          ParsedAsJson(HasKeyMatching("status", Eq("fully materialized")))));
}

// _____________________________________________________________________________

TEST_F(OperationTestFixture, getPrecomputedResultBecauseSiblingOfService) {
  // If a precomputedResult is set, it will be returned by `getResult`
  auto idTable = makeIdTableFromVector({{1, 6, 0}, {2, 5, 0}, {3, 4, 0}});
  auto result = std::make_shared<const Result>(
      idTable.clone(), std::vector<ColumnIndex>{0}, LocalVocab{});
  operation.precomputedResultBecauseSiblingOfService() =
      std::make_optional(result);
  EXPECT_EQ(operation.getResult(), result);
  EXPECT_FALSE(
      operation.precomputedResultBecauseSiblingOfService().has_value());
}

// _____________________________________________________________________________

TEST(OperationTest, verifyExceptionIsThrownOnCancellation) {
  auto qec = getQec();
  auto handle = std::make_shared<CancellationHandle<>>();
  ShallowParentOperation operation =
      ShallowParentOperation::of<StallForeverOperation>(qec);
  operation.recursivelySetCancellationHandle(handle);

  ad_utility::JThread thread{[&]() {
    std::this_thread::sleep_for(5ms);
    handle->cancel(CancellationState::TIMEOUT);
  }};
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(operation.computeResult(false),
                                        ::testing::HasSubstr("timed out"),
                                        ad_utility::CancellationException);
}

// _____________________________________________________________________________

TEST(OperationTest, verifyRemainingTimeDoesCountDown) {
  constexpr auto timeout = 5ms;
  auto qec = getQec();
  ShallowParentOperation operation =
      ShallowParentOperation::of<StallForeverOperation>(qec);
  operation.recursivelySetTimeConstraint(timeout);

  auto childOperation = std::dynamic_pointer_cast<StallForeverOperation>(
      operation.getChildren().at(0)->getRootOperation());

  EXPECT_GT(operation.publicRemainingTime(), 0ms);
  EXPECT_GT(childOperation->publicRemainingTime(), 0ms);
  std::this_thread::sleep_for(timeout);
  // Verify time is up for parent and child
  EXPECT_EQ(operation.publicRemainingTime(), 0ms);
  EXPECT_EQ(childOperation->publicRemainingTime(), 0ms);
}

// ________________________________________________
TEST(OperationTest, estimatesForCachedResults) {
  // Create an operation with manually specified size and cost estimates which
  // are deliberately wrong, so they can be "corrected" when the operation is
  // read from the cache.
  auto makeQet = []() {
    auto idTable = makeIdTableFromVector({{3, 4}, {7, 8}, {9, 123}});
    auto qet = ad_utility::makeExecutionTree<ValuesForTesting>(
        getQec(), std::move(idTable),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}},
        false);
    auto& op = dynamic_cast<ValuesForTesting&>(*qet->getRootOperation());
    // Set those to some arbitrary values so we can test them.
    op.sizeEstimate() = 24;
    op.costEstimate() = 210;
    return qet;
  };
  {
    auto qet = makeQet();
    EXPECT_EQ(qet->getCacheKey(), qet->getRootOperation()->getCacheKey());
    EXPECT_EQ(qet->getSizeEstimate(), 24u);
    EXPECT_EQ(qet->getCostEstimate(), 210u);

    [[maybe_unused]] auto res = qet->getResult();
  }
  // The result is now cached inside the static execution context. If we create
  // the same operation again and `zero-cost-estimate-for-cached-subtrees` is
  // set to `true`, the cost estimate should be zero. The size estimate does not
  // change (see the `getCostEstimate` function for details on why).
  {
    auto restoreWhenScopeEnds =
        setRuntimeParameterForTest<"zero-cost-estimate-for-cached-subtree">(
            true);
    auto qet = makeQet();
    EXPECT_EQ(qet->getCacheKey(), qet->getRootOperation()->getCacheKey());
    EXPECT_EQ(qet->getSizeEstimate(), 24u);
    EXPECT_EQ(qet->getCostEstimate(), 0u);
  }
  {
    auto restoreWhenScopeEnds =
        setRuntimeParameterForTest<"zero-cost-estimate-for-cached-subtree">(
            false);
    auto qet = makeQet();
    EXPECT_EQ(qet->getCacheKey(), qet->getRootOperation()->getCacheKey());
    EXPECT_EQ(qet->getSizeEstimate(), 24u);
    EXPECT_EQ(qet->getCostEstimate(), 210u);
  }
}

// ________________________________________________
TEST(Operation, createRuntimInfoFromEstimates) {
  NeutralElementOperation operation{getQec()};
  operation.setLimit({12, 3});
  operation.createRuntimeInfoFromEstimates(nullptr);
  EXPECT_EQ(operation.runtimeInfo().details_["limit"], 12);
  EXPECT_EQ(operation.runtimeInfo().details_["offset"], 3);
}

// _____________________________________________________________________________
TEST(Operation, lazilyEvaluatedOperationIsNotCached) {
  using V = Variable;
  auto qec = getQec();
  SparqlTripleSimple scanTriple{V{"?x"}, V{"?y"}, V{"?z"}};
  IndexScan scan{qec, Permutation::Enum::POS, scanTriple};

  qec->getQueryTreeCache().clearAll();
  auto result = scan.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
  ASSERT_NE(result, nullptr);
  EXPECT_FALSE(result->isFullyMaterialized());

  EXPECT_EQ(qec->getQueryTreeCache().numNonPinnedEntries(), 0);
  EXPECT_EQ(qec->getQueryTreeCache().numPinnedEntries(), 0);
}

// _____________________________________________________________________________
TEST(Operation, updateRuntimeStatsWorksCorrectly) {
  auto qec = getQec();
  auto idTable = makeIdTableFromVector({{3, 4}, {7, 8}, {9, 123}});
  ValuesForTesting valuesForTesting{
      qec, std::move(idTable), {Variable{"?x"}, Variable{"?y"}}};

  auto& rti = valuesForTesting.runtimeInfo();

  // Test operation with built-in filter
  valuesForTesting.externalLimitApplied_ = false;
  valuesForTesting.updateRuntimeStats(false, 11, 13, 17ms);

  EXPECT_EQ(rti.numCols_, 13);
  EXPECT_EQ(rti.numRows_, 11);
  EXPECT_EQ(rti.totalTime_, 17ms);
  EXPECT_EQ(rti.originalTotalTime_, 17ms);
  EXPECT_EQ(rti.originalOperationTime_, 17ms);

  // Test built-in filter
  valuesForTesting.externalLimitApplied_ = false;
  valuesForTesting.updateRuntimeStats(true, 5, 3, 7ms);

  EXPECT_EQ(rti.numCols_, 13);
  EXPECT_EQ(rti.numRows_, 11);
  EXPECT_EQ(rti.totalTime_, 17ms + 7ms);
  EXPECT_EQ(rti.originalTotalTime_, 17ms + 7ms);
  EXPECT_EQ(rti.originalOperationTime_, 17ms + 7ms);

  rti.children_ = {std::make_shared<RuntimeInformation>()};
  rti.numCols_ = 0;
  rti.numRows_ = 0;
  rti.totalTime_ = 0ms;
  rti.originalOperationTime_ = 0ms;
  auto& childRti = *rti.children_.at(0);

  // Test operation with external filter
  valuesForTesting.externalLimitApplied_ = true;
  valuesForTesting.updateRuntimeStats(false, 31, 37, 41ms);

  EXPECT_EQ(rti.numCols_, 0);
  EXPECT_EQ(rti.numRows_, 0);
  EXPECT_EQ(rti.totalTime_, 41ms);
  EXPECT_EQ(rti.originalTotalTime_, 41ms);
  EXPECT_EQ(rti.originalOperationTime_, 0ms);

  EXPECT_EQ(childRti.numCols_, 37);
  EXPECT_EQ(childRti.numRows_, 31);
  EXPECT_EQ(childRti.totalTime_, 41ms);
  EXPECT_EQ(childRti.originalTotalTime_, 41ms);
  EXPECT_EQ(childRti.originalOperationTime_, 41ms);

  // Test external filter
  valuesForTesting.externalLimitApplied_ = true;
  valuesForTesting.updateRuntimeStats(true, 19, 23, 29ms);

  EXPECT_EQ(rti.numCols_, 23);
  EXPECT_EQ(rti.numRows_, 19);
  EXPECT_EQ(rti.totalTime_, 41ms + 29ms);
  EXPECT_EQ(rti.originalTotalTime_, 41ms + 29ms);
  EXPECT_EQ(rti.originalOperationTime_, 29ms);

  EXPECT_EQ(childRti.numCols_, 37);
  EXPECT_EQ(childRti.numRows_, 31);
  EXPECT_EQ(childRti.totalTime_, 41ms);
  EXPECT_EQ(childRti.originalTotalTime_, 41ms);
  EXPECT_EQ(childRti.originalOperationTime_, 41ms);
}

// _____________________________________________________________________________
TEST(Operation, verifyRuntimeInformationIsUpdatedForLazyOperations) {
  auto qec = getQec();
  std::vector<IdTable> idTablesVector{};
  idTablesVector.push_back(makeIdTableFromVector({{3, 4}}));
  idTablesVector.push_back(makeIdTableFromVector({{7, 8}}));
  LocalVocab localVocab{};
  localVocab.getIndexAndAddIfNotContained(LocalVocabEntry{
      ad_utility::triple_component::Literal::literalWithoutQuotes("Test")});
  ValuesForTesting valuesForTesting{
      qec,   std::move(idTablesVector),  {Variable{"?x"}, Variable{"?y"}},
      false, std::vector<ColumnIndex>{}, std::move(localVocab)};

  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};
  EXPECT_THROW(
      valuesForTesting.runComputation(timer, ComputationMode::ONLY_IF_CACHED),
      ad_utility::Exception);

  auto result = valuesForTesting.runComputation(
      timer, ComputationMode::LAZY_IF_SUPPORTED);

  auto& rti = valuesForTesting.runtimeInfo();

  EXPECT_EQ(rti.status_, Status::lazilyMaterialized);
  EXPECT_EQ(rti.totalTime_, 0ms);
  EXPECT_EQ(rti.originalTotalTime_, 0ms);
  EXPECT_EQ(rti.originalOperationTime_, 0ms);

  expectAtEachStageOfGenerator(
      std::move(result.idTables()),
      {[&]() {
         EXPECT_EQ(rti.status_, Status::lazilyMaterialized);
         expectRtiHasDimensions(rti, 2, 1);
         ASSERT_TRUE(rti.details_.contains("non-empty-local-vocabs"));
         EXPECT_EQ(rti.details_["non-empty-local-vocabs"],
                   "1 / 1, Ø = 1, max = 1");
       },
       [&]() {
         EXPECT_EQ(rti.status_, Status::lazilyMaterialized);
         expectRtiHasDimensions(rti, 2, 2);
         ASSERT_TRUE(rti.details_.contains("non-empty-local-vocabs"));
         EXPECT_EQ(rti.details_["non-empty-local-vocabs"],
                   "2 / 2, Ø = 1, max = 1");
       }});

  EXPECT_EQ(rti.status_, Status::lazilyMaterialized);
  expectRtiHasDimensions(rti, 2, 2);
  ASSERT_TRUE(rti.details_.contains("non-empty-local-vocabs"));
  EXPECT_EQ(rti.details_["non-empty-local-vocabs"], "2 / 2, Ø = 1, max = 1");
}

// _____________________________________________________________________________
TEST(Operation, ensureFailedStatusIsSetWhenGeneratorThrowsException) {
  bool signaledUpdate = false;
  const Index& index = ad_utility::testing::getQec()->getIndex();
  QueryResultCache cache{};
  QueryExecutionContext context{
      index, &cache, makeAllocator(ad_utility::MemorySize::megabytes(100)),
      SortPerformanceEstimator{}, [&](std::string) { signaledUpdate = true; }};
  AlwaysFailOperation operation{&context};
  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};
  auto result =
      operation.runComputation(timer, ComputationMode::LAZY_IF_SUPPORTED);

  EXPECT_EQ(operation.runtimeInfo().status_, Status::lazilyMaterialized);

  EXPECT_THROW(result.idTables().begin(), std::runtime_error);

  EXPECT_EQ(operation.runtimeInfo().status_, Status::failed);
  EXPECT_TRUE(signaledUpdate);
}

// _____________________________________________________________________________
TEST(Operation, ensureSignalUpdateIsOnlyCalledEvery50msAndAtTheEnd) {
#ifdef _QLEVER_NO_TIMING_TESTS
  GTEST_SKIP_("because _QLEVER_NO_TIMING_TESTS defined");
#endif
  uint32_t updateCallCounter = 0;
  auto idTable = makeIdTableFromVector({{}});
  const Index& index = getQec()->getIndex();
  QueryResultCache cache{};
  QueryExecutionContext context{
      index, &cache, makeAllocator(ad_utility::MemorySize::megabytes(100)),
      SortPerformanceEstimator{}, [&](std::string) { ++updateCallCounter; }};
  CustomGeneratorOperation operation{
      &context, [](const IdTable& idTable) -> Result::Generator {
        std::this_thread::sleep_for(50ms);
        co_yield {idTable.clone(), LocalVocab{}};
        // This one should not trigger because it's below the 50ms threshold
        std::this_thread::sleep_for(30ms);
        co_yield {idTable.clone(), LocalVocab{}};
        std::this_thread::sleep_for(30ms);
        co_yield {idTable.clone(), LocalVocab{}};
        // This one should not trigger directly, but trigger because it's the
        // last one
        std::this_thread::sleep_for(30ms);
        co_yield {idTable.clone(), LocalVocab{}};
      }(idTable)};

  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};
  auto result =
      operation.runComputation(timer, ComputationMode::LAZY_IF_SUPPORTED);

  EXPECT_EQ(updateCallCounter, 1);

  expectAtEachStageOfGenerator(std::move(result.idTables()),
                               {
                                   [&]() { EXPECT_EQ(updateCallCounter, 2); },
                                   [&]() { EXPECT_EQ(updateCallCounter, 2); },
                                   [&]() { EXPECT_EQ(updateCallCounter, 3); },
                                   [&]() { EXPECT_EQ(updateCallCounter, 3); },
                               });

  EXPECT_EQ(updateCallCounter, 4);
}

// _____________________________________________________________________________
TEST(Operation, ensureSignalUpdateIsCalledAtTheEndOfPartialConsumption) {
  uint32_t updateCallCounter = 0;
  auto idTable = makeIdTableFromVector({{}});
  const Index& index = getQec()->getIndex();
  QueryResultCache cache{};
  QueryExecutionContext context{
      index, &cache, makeAllocator(ad_utility::MemorySize::megabytes(100)),
      SortPerformanceEstimator{}, [&](std::string) { ++updateCallCounter; }};
  CustomGeneratorOperation operation{
      &context, [](const IdTable& idTable) -> Result::Generator {
        co_yield {idTable.clone(), LocalVocab{}};
        co_yield {idTable.clone(), LocalVocab{}};
      }(idTable)};

  {
    ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};
    auto result =
        operation.runComputation(timer, ComputationMode::LAZY_IF_SUPPORTED);

    EXPECT_EQ(updateCallCounter, 1);
    auto& idTables = result.idTables();
    // Only consume partially
    auto iterator = idTables.begin();
    ASSERT_NE(iterator, idTables.end());
    EXPECT_EQ(updateCallCounter, 1);
  }

  // Destructor of result should call this function
  EXPECT_EQ(updateCallCounter, 2);
}

// _____________________________________________________________________________
TEST(Operation, verifyLimitIsProperlyAppliedAndUpdatesRuntimeInfoCorrectly) {
  auto qec = getQec();
  std::vector<IdTable> idTablesVector{};
  idTablesVector.push_back(makeIdTableFromVector({{3, 4}}));
  idTablesVector.push_back(makeIdTableFromVector({{7, 8}, {9, 123}}));
  ValuesForTesting valuesForTesting{
      qec, std::move(idTablesVector), {Variable{"?x"}, Variable{"?y"}}};

  valuesForTesting.setLimit({._limit = 1, ._offset = 1});

  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};

  auto result = valuesForTesting.runComputation(
      timer, ComputationMode::LAZY_IF_SUPPORTED);

  auto& rti = valuesForTesting.runtimeInfo();
  auto& childRti = *rti.children_.at(0);

  expectRtiHasDimensions(rti, 0, 0);
  expectRtiHasDimensions(childRti, 0, 0);

  expectAtEachStageOfGenerator(std::move(result.idTables()),
                               {[&]() {
                                  expectRtiHasDimensions(rti, 2, 0);
                                  expectRtiHasDimensions(childRti, 2, 1);
                                },
                                [&]() {
                                  expectRtiHasDimensions(rti, 2, 1);
                                  expectRtiHasDimensions(childRti, 2, 3);
                                }});

  expectRtiHasDimensions(rti, 2, 1);
  expectRtiHasDimensions(childRti, 2, 3);
}

namespace {
QueryCacheKey makeQueryCacheKey(std::string s) {
  return {std::move(s), 102394857};
}
}  // namespace

// _____________________________________________________________________________
TEST(Operation, ensureLazyOperationIsCachedIfSmallEnough) {
  auto qec = getQec();
  qec->getQueryTreeCache().clearAll();
  std::vector<IdTable> idTablesVector{};
  idTablesVector.push_back(makeIdTableFromVector({{3, 4}}));
  idTablesVector.push_back(makeIdTableFromVector({{7, 8}, {9, 123}}));
  ValuesForTesting valuesForTesting{
      qec, std::move(idTablesVector), {Variable{"?x"}, Variable{"?y"}}};

  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};

  auto cacheValue = valuesForTesting.runComputationAndPrepareForCache(
      timer, ComputationMode::LAZY_IF_SUPPORTED, makeQueryCacheKey("test"),
      false, false);
  EXPECT_FALSE(
      qec->getQueryTreeCache().cacheContains(makeQueryCacheKey("test")));

  for ([[maybe_unused]] Result::IdTableVocabPair& _ :
       cacheValue.resultTable().idTables()) {
  }

  auto aggregatedValue =
      qec->getQueryTreeCache().getIfContained(makeQueryCacheKey("test"));
  ASSERT_TRUE(aggregatedValue.has_value());

  ASSERT_TRUE(aggregatedValue.value()._resultPointer);

  auto newRti = aggregatedValue.value()._resultPointer->runtimeInfo();
  auto& oldRti = valuesForTesting.runtimeInfo();

  EXPECT_EQ(newRti.descriptor_, oldRti.descriptor_);
  EXPECT_EQ(newRti.numCols_, oldRti.numCols_);
  EXPECT_EQ(newRti.numRows_, oldRti.numRows_);
  EXPECT_EQ(newRti.totalTime_, oldRti.totalTime_);
  EXPECT_EQ(newRti.originalTotalTime_, oldRti.originalTotalTime_);
  EXPECT_EQ(newRti.originalOperationTime_, oldRti.originalOperationTime_);
  EXPECT_EQ(newRti.status_, Status::fullyMaterialized);

  const auto& aggregatedResult =
      aggregatedValue.value()._resultPointer->resultTable();
  ASSERT_TRUE(aggregatedResult.isFullyMaterialized());

  const auto& idTable = aggregatedResult.idTable();
  ASSERT_EQ(idTable.numColumns(), 2);
  ASSERT_EQ(idTable.numRows(), 3);

  EXPECT_EQ(idTable, makeIdTableFromVector({{3, 4}, {7, 8}, {9, 123}}));
}

// _____________________________________________________________________________
TEST(Operation, checkLazyOperationIsNotCachedIfTooLarge) {
  auto qec = getQec();
  qec->getQueryTreeCache().clearAll();
  std::vector<IdTable> idTablesVector{};
  idTablesVector.push_back(makeIdTableFromVector({{3, 4}}));
  idTablesVector.push_back(makeIdTableFromVector({{7, 8}, {9, 123}}));
  ValuesForTesting valuesForTesting{
      qec, std::move(idTablesVector), {Variable{"?x"}, Variable{"?y"}}};

  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};

  std::optional<CacheValue> cacheValue = std::nullopt;
  {
    // Too small for storage, make sure to change back before consuming
    // generator to additionally assert sure it is not re-read on every
    // iteration.
    auto cleanup =
        setRuntimeParameterForTest<"cache-max-size-lazy-result">(1_B);

    cacheValue = valuesForTesting.runComputationAndPrepareForCache(
        timer, ComputationMode::LAZY_IF_SUPPORTED, makeQueryCacheKey("test"),
        false, false);
    EXPECT_FALSE(
        qec->getQueryTreeCache().cacheContains(makeQueryCacheKey("test")));
  }

  for ([[maybe_unused]] Result::IdTableVocabPair& _ :
       cacheValue->resultTable().idTables()) {
  }

  EXPECT_FALSE(
      qec->getQueryTreeCache().cacheContains(makeQueryCacheKey("test")));
}

// _____________________________________________________________________________
TEST(Operation, checkLazyOperationIsNotCachedIfUnlikelyToFitInCache) {
  auto qec = getQec();
  qec->getQueryTreeCache().clearAll();
  std::vector<IdTable> idTablesVector{};
  idTablesVector.push_back(makeIdTableFromVector({{3, 4}}));
  idTablesVector.push_back(makeIdTableFromVector({{7, 8}, {9, 123}}));
  ValuesForTesting valuesForTesting{
      qec, std::move(idTablesVector), {Variable{"?x"}, Variable{"?y"}}, true};

  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};

  auto cacheValue = valuesForTesting.runComputationAndPrepareForCache(
      timer, ComputationMode::LAZY_IF_SUPPORTED, makeQueryCacheKey("test"),
      false, false);
  EXPECT_FALSE(
      qec->getQueryTreeCache().cacheContains(makeQueryCacheKey("test")));

  for ([[maybe_unused]] Result::IdTableVocabPair& _ :
       cacheValue.resultTable().idTables()) {
  }

  EXPECT_FALSE(
      qec->getQueryTreeCache().cacheContains(makeQueryCacheKey("test")));
}

// _____________________________________________________________________________
TEST(Operation, checkMaxCacheSizeIsComputedCorrectly) {
  auto runTest = [](ad_utility::MemorySize cacheLimit,
                    ad_utility::MemorySize runtimeParameterLimit, bool isRoot,
                    ad_utility::MemorySize expectedSize,
                    ad_utility::source_location sourceLocation =
                        ad_utility::source_location::current()) {
    auto loc = generateLocationTrace(sourceLocation);
    auto qec = getQec();
    qec->getQueryTreeCache().clearAll();
    std::vector<IdTable> idTablesVector{};
    idTablesVector.push_back(makeIdTableFromVector({{3, 4}}));
    ValuesForTesting valuesForTesting{
        qec, std::move(idTablesVector), {Variable{"?x"}, Variable{"?y"}}, true};

    ad_utility::MemorySize actualCacheSize;
    valuesForTesting.setCacheSizeStorage(&actualCacheSize);

    absl::Cleanup restoreOriginalSize{
        [qec, original = qec->getQueryTreeCache().getMaxSizeSingleEntry()]() {
          qec->getQueryTreeCache().setMaxSizeSingleEntry(original);
        }};
    qec->getQueryTreeCache().setMaxSizeSingleEntry(cacheLimit);

    auto cleanup = setRuntimeParameterForTest<"cache-max-size-lazy-result">(
        runtimeParameterLimit);

    ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};

    auto cacheValue = valuesForTesting.runComputationAndPrepareForCache(
        timer, ComputationMode::LAZY_IF_SUPPORTED, makeQueryCacheKey("test"),
        false, isRoot);

    EXPECT_EQ(actualCacheSize, expectedSize);
  };

  runTest(10_B, 10_B, true, 10_B);
  runTest(10_B, 10_B, false, 10_B);
  runTest(10_B, 1_B, false, 1_B);
  runTest(1_B, 10_B, false, 1_B);
  runTest(10_B, 1_B, true, 10_B);
  runTest(1_B, 10_B, true, 1_B);
}

// _____________________________________________________________________________
TEST(OperationTest, disableCaching) {
  auto qec = getQec();
  qec->getQueryTreeCache().clearAll();
  std::vector<IdTable> idTablesVector{};
  idTablesVector.push_back(makeIdTableFromVector({{3, 4}}));
  idTablesVector.push_back(makeIdTableFromVector({{7, 8}, {9, 123}}));
  ValuesForTesting valuesForTesting{
      qec, std::move(idTablesVector), {Variable{"?x"}, Variable{"?y"}}, true};

  QueryCacheKey cacheKey{valuesForTesting.getCacheKey(),
                         qec->locatedTriplesSnapshot().index_};

  // By default, the result of `valuesForTesting` is cached because it is
  // sufficiently small, no matter if it was computed lazily or fully
  // materialized.
  EXPECT_FALSE(qec->getQueryTreeCache().cacheContains(cacheKey));
  valuesForTesting.getResult(true);
  EXPECT_TRUE(qec->getQueryTreeCache().cacheContains(cacheKey));
  qec->getQueryTreeCache().clearAll();
  EXPECT_FALSE(qec->getQueryTreeCache().cacheContains(cacheKey));
  valuesForTesting.getResult(false);
  EXPECT_TRUE(qec->getQueryTreeCache().cacheContains(cacheKey));

  // We now disable caching for the `valuesForTesting`. Then the result is never
  // cached, no matter if it is computed lazily or fully materialized.
  valuesForTesting.disableStoringInCache();
  qec->getQueryTreeCache().clearAll();

  EXPECT_FALSE(qec->getQueryTreeCache().cacheContains(cacheKey));
  valuesForTesting.getResult(true);
  EXPECT_FALSE(qec->getQueryTreeCache().cacheContains(cacheKey));
  qec->getQueryTreeCache().clearAll();
  EXPECT_FALSE(qec->getQueryTreeCache().cacheContains(cacheKey));
  valuesForTesting.getResult(false);
  EXPECT_FALSE(qec->getQueryTreeCache().cacheContains(cacheKey));
}

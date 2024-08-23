// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "engine/NeutralElementOperation.h"
#include "engine/ValuesForTesting.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

using namespace ad_utility::testing;
using namespace ::testing;
using ad_utility::CacheStatus;
using ad_utility::CancellationException;
using ad_utility::CancellationHandle;
using ad_utility::CancellationState;
using Status = RuntimeInformation::Status;

namespace {
// Helper function to perform actions at various stages of a generator
template <typename T>
auto expectAtEachStageOfGenerator(
    cppcoro::generator<T> generator,
    std::vector<std::function<void()>> functions,
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

  Index index =
      makeTestIndex("OperationTest", std::nullopt, true, true, true, 32_B);
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
  // The result is now cached inside the static execution context, if we create
  // the same operation again, the cost estimate is 0 and the size estimate is
  // exact (3 rows).
  {
    auto qet = makeQet();
    EXPECT_EQ(qet->getCacheKey(), qet->getRootOperation()->getCacheKey());
    EXPECT_EQ(qet->getSizeEstimate(), 3u);
    EXPECT_EQ(qet->getCostEstimate(), 0u);
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
  ValuesForTesting valuesForTesting{
      qec, std::move(idTablesVector), {Variable{"?x"}, Variable{"?y"}}};

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
       },
       [&]() {
         EXPECT_EQ(rti.status_, Status::lazilyMaterialized);
         expectRtiHasDimensions(rti, 2, 2);
       }});

  EXPECT_EQ(rti.status_, Status::lazilyMaterialized);
  expectRtiHasDimensions(rti, 2, 2);
}

// _____________________________________________________________________________
TEST(Operation, ensureFailedStatusIsSetWhenGeneratorThrowsException) {
  bool signaledUpdate = false;
  Index index = makeTestIndex(
      "ensureFailedStatusIsSetWhenGeneratorThrowsException", std::nullopt, true,
      true, true, ad_utility::MemorySize::bytes(16), false);
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
  Index index = makeTestIndex(
      "ensureSignalUpdateIsOnlyCalledEvery50msAndAtTheEnd", std::nullopt, true,
      true, true, ad_utility::MemorySize::bytes(16), false);
  QueryResultCache cache{};
  QueryExecutionContext context{
      index, &cache, makeAllocator(ad_utility::MemorySize::megabytes(100)),
      SortPerformanceEstimator{}, [&](std::string) { ++updateCallCounter; }};
  CustomGeneratorOperation operation{
      &context, [](const IdTable& idTable) -> cppcoro::generator<IdTable> {
        std::this_thread::sleep_for(50ms);
        co_yield idTable.clone();
        // This one should not trigger because it's below the 50ms threshold
        std::this_thread::sleep_for(30ms);
        co_yield idTable.clone();
        std::this_thread::sleep_for(30ms);
        co_yield idTable.clone();
        // This one should not trigger directly, but trigger because it's the
        // last one
        std::this_thread::sleep_for(30ms);
        co_yield idTable.clone();
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
  Index index = makeTestIndex(
      "ensureSignalUpdateIsCalledAtTheEndOfPartialConsumption", std::nullopt,
      true, true, true, ad_utility::MemorySize::bytes(16), false);
  QueryResultCache cache{};
  QueryExecutionContext context{
      index, &cache, makeAllocator(ad_utility::MemorySize::megabytes(100)),
      SortPerformanceEstimator{}, [&](std::string) { ++updateCallCounter; }};
  CustomGeneratorOperation operation{
      &context, [](const IdTable& idTable) -> cppcoro::generator<IdTable> {
        co_yield idTable.clone();
        co_yield idTable.clone();
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
      timer, ComputationMode::LAZY_IF_SUPPORTED, "test", false);
  EXPECT_FALSE(qec->getQueryTreeCache().cacheContains("test"));

  for ([[maybe_unused]] IdTable& _ : cacheValue.resultTable().idTables()) {
  }

  auto aggregatedValue = qec->getQueryTreeCache().getIfContained("test");
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

  auto originalSize = qec->getQueryTreeCache().getMaxSizeSingleEntry();

  // Too small for storage
  qec->getQueryTreeCache().setMaxSizeSingleEntry(1_B);

  auto cacheValue = valuesForTesting.runComputationAndPrepareForCache(
      timer, ComputationMode::LAZY_IF_SUPPORTED, "test", false);
  EXPECT_FALSE(qec->getQueryTreeCache().cacheContains("test"));
  qec->getQueryTreeCache().setMaxSizeSingleEntry(originalSize);

  for ([[maybe_unused]] IdTable& _ : cacheValue.resultTable().idTables()) {
  }

  EXPECT_FALSE(qec->getQueryTreeCache().cacheContains("test"));
}

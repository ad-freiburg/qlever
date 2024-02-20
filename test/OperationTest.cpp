// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/NeutralElementOperation.h"
#include "engine/ValuesForTesting.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

using namespace ad_utility::testing;
using namespace ::testing;
using ad_utility::CancellationException;
using ad_utility::CancellationHandle;
using ad_utility::CancellationState;

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
  EXPECT_EQ(n.getResult(true, true), nullptr);
  EXPECT_EQ(n.runtimeInfo().status_, RuntimeInformation::Status::notStarted);
  // Nothing has been stored in the cache by this call.
  EXPECT_EQ(qec->getQueryTreeCache().numNonPinnedEntries(), 0);
  EXPECT_EQ(qec->getQueryTreeCache().numPinnedEntries(), 0);

  // This "ordinary" call to `getResult` also stores the result in the cache.
  NeutralElementOperation n2{qec};
  auto result = n2.getResult();
  EXPECT_NE(result, nullptr);
  EXPECT_EQ(n2.runtimeInfo().status_,
            RuntimeInformation::Status::fullyMaterialized);
  EXPECT_EQ(n2.runtimeInfo().cacheStatus_, ad_utility::CacheStatus::computed);
  EXPECT_EQ(qec->getQueryTreeCache().numNonPinnedEntries(), 1);
  EXPECT_EQ(qec->getQueryTreeCache().numPinnedEntries(), 0);

  // When we now request to only return the result if it is cached, we should
  // get exactly the same `shared_ptr` as with the previous call.
  NeutralElementOperation n3{qec};
  EXPECT_EQ(n3.getResult(true, true), result);
  EXPECT_EQ(n3.runtimeInfo().cacheStatus_,
            ad_utility::CacheStatus::cachedNotPinned);

  // We can even use the `onlyReadFromCache` case to upgrade a non-pinned
  // cache-entry to a pinned cache entry
  QueryExecutionContext qecCopy{*qec};
  qecCopy._pinResult = true;
  NeutralElementOperation n4{&qecCopy};
  EXPECT_EQ(n4.getResult(true, true), result);

  // The cache status is `cachedNotPinned` because we found the element cached
  // but not pinned (it does reflect the status BEFORE the operation).
  EXPECT_EQ(n4.runtimeInfo().cacheStatus_,
            ad_utility::CacheStatus::cachedNotPinned);
  EXPECT_EQ(qec->getQueryTreeCache().numNonPinnedEntries(), 0);
  EXPECT_EQ(qec->getQueryTreeCache().numPinnedEntries(), 1);

  // We have pinned the result, so requesting it again should return a pinned
  // result.
  qecCopy._pinResult = false;
  EXPECT_EQ(n4.getResult(true, true), result);
  EXPECT_EQ(n4.runtimeInfo().cacheStatus_,
            ad_utility::CacheStatus::cachedPinned);

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
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(operation.computeResult(),
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

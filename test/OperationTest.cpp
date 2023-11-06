// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "IndexTestHelpers.h"
#include "engine/NeutralElementOperation.h"
#include "engine/ValuesForTesting.h"
#include "util/IdTableHelpers.h"

using namespace ad_utility::testing;
using namespace ::testing;
using namespace std::chrono_literals;
using ad_utility::CancellationException;
using ad_utility::CancellationHandle;
using ad_utility::CancellationState;

// ________________________________________________
TEST(OperationTest, limitIsRepresentedInCacheKey) {
  NeutralElementOperation n{getQec()};
  EXPECT_THAT(n.asString(), testing::Not(testing::HasSubstr("LIMIT 20")));
  LimitOffsetClause l;
  l._limit = 20;
  n.setLimit(l);
  EXPECT_THAT(n.asString(), testing::HasSubstr("LIMIT 20"));
  EXPECT_THAT(n.asString(), testing::Not(testing::HasSubstr("OFFSET 34")));

  l._offset = 34;
  n.setLimit(l);
  EXPECT_THAT(n.asString(), testing::HasSubstr("OFFSET 34"));
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
      makeTestIndex("OperationTest", std::nullopt, true, true, true, 32);
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

class StallForeverOperation : public Operation {
  std::vector<QueryExecutionTree*> getChildren() override { return {}; }
  string asStringImpl([[maybe_unused]] size_t) const override {
    return "StallForEverOperation";
  }
  string getDescriptor() const override {
    return "StallForEverOperationDescriptor";
  }
  size_t getResultWidth() const override { return 0; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity([[maybe_unused]] size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }
  vector<ColumnIndex> resultSortedOn() const override { return {}; }
  VariableToColumnMap computeVariableToColumnMap() const override { return {}; }

 public:
  using Operation::Operation;
  // Do-nothing operation that runs for 100ms without computing anything, but
  // which can be cancelled.
  ResultTable computeResult() override {
    auto end = std::chrono::steady_clock::now() + 100ms;
    while (std::chrono::steady_clock::now() < end) {
      checkCancellation();
    }
    throw std::runtime_error{"Loop was not interrupted for 100ms, aborting"};
  }

  // Provide public view of remainingTime for tests
  std::chrono::milliseconds publicRemainingTime() const {
    return remainingTime();
  }
};
// _____________________________________________________________________________

// Dummy parent to test recursive application of a function
class ShallowParentOperation : public Operation {
  std::shared_ptr<QueryExecutionTree> child_;

  explicit ShallowParentOperation(std::shared_ptr<QueryExecutionTree> child)
      : child_{std::move(child)} {}
  string asStringImpl([[maybe_unused]] size_t) const override {
    return "ParentOperation";
  }
  string getDescriptor() const override { return "ParentOperationDescriptor"; }
  size_t getResultWidth() const override { return 0; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity([[maybe_unused]] size_t) override { return 0; }
  bool knownEmptyResult() override { return false; }
  vector<ColumnIndex> resultSortedOn() const override { return {}; }
  VariableToColumnMap computeVariableToColumnMap() const override { return {}; }

 public:
  template <typename ChildOperation, typename... Args>
  static ShallowParentOperation of(QueryExecutionContext* qec, Args&&... args) {
    return ShallowParentOperation{
        ad_utility::makeExecutionTree<ChildOperation>(qec, args...)};
  }

  std::vector<QueryExecutionTree*> getChildren() override {
    return {child_.get()};
  }

  ResultTable computeResult() override {
    auto childResult = child_->getResult();
    return {childResult->idTable().clone(), resultSortedOn(),
            childResult->getSharedLocalVocab()};
  }

  // Provide public view of remainingTime for tests
  std::chrono::milliseconds publicRemainingTime() const {
    return remainingTime();
  }
};

template <>
void QueryExecutionTree::setOperation<StallForeverOperation>(
    std::shared_ptr<StallForeverOperation> operation) {
  _rootOperation = std::move(operation);
}

// _____________________________________________________________________________

TEST(OperationTest, verifyExceptionIsThrownOnCancellation) {
  auto qec = getQec();
  auto handle = std::make_shared<CancellationHandle>();
  ShallowParentOperation operation =
      ShallowParentOperation::of<StallForeverOperation>(qec);
  operation.recursivelySetCancellationHandle(handle);

  std::thread thread{[&]() {
    std::this_thread::sleep_for(5ms);
    handle->cancel(CancellationState::TIMEOUT);
  }};
  EXPECT_THROW(
      {
        try {
          operation.computeResult();
        } catch (const ad_utility::AbortException& exception) {
          EXPECT_THAT(exception.what(),
                      ::testing::HasSubstr("Cancelled due to timeout"));
          throw;
        }
      },
      ad_utility::AbortException);
  thread.join();
}

// _____________________________________________________________________________

TEST(OperationTest, verifyRemainingTimeDoesCountDown) {
  auto qec = getQec();
  ShallowParentOperation operation =
      ShallowParentOperation::of<StallForeverOperation>(qec);
  operation.recursivelySetTimeConstraint(1ms);
  std::this_thread::sleep_for(1ms);
  // Verify time is up for parent and child
  EXPECT_EQ(operation.publicRemainingTime(), 0ms);
  auto childOperation = std::dynamic_pointer_cast<StallForeverOperation>(
      operation.getChildren().at(0)->getRootOperation());
  EXPECT_EQ(childOperation->publicRemainingTime(), 0ms);
}

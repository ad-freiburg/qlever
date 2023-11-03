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

MATCHER_P2(HasJsonKeyValue, key, value, "") {
  try {
    auto json = nlohmann::json::parse(arg);
    if (!json.contains(key)) {
      *result_listener << "JSON object does not contain key \"" << key << '"';
      return false;
    }
    if (json[key] == value) {
      return true;
    }
    *result_listener << "JSON key \"" << key << "\" has value \"" << json[key]
                     << '"';
  } catch (const nlohmann::json::parse_error& error) {
    *result_listener << "failed to parse JSON";
  }
  return false;
}

TEST(OperationTest, verifyOperationStatusChangesToInProgressAndComputed) {
  std::vector<std::string> jsonHistory;

  Index index =
      makeTestIndex("OperationTest", std::nullopt, true, true, true, 32);
  QueryResultCache cache;
  QueryExecutionContext qec{
      index, &cache, makeAllocator(), SortPerformanceEstimator{},
      [&](std::string json) { jsonHistory.emplace_back(std::move(json)); }};
  auto table = makeIdTableFromVector({{}, {}, {}});
  ValuesForTesting operation{&qec, std::move(table), {}};

  // Ignore result, we only care about the side effects
  operation.getResult(true);

  EXPECT_THAT(jsonHistory,
              ElementsAre(HasJsonKeyValue("status", "not started"),
                          HasJsonKeyValue("status", "in progress"),
                          HasJsonKeyValue("status", "fully materialized"),
                          HasJsonKeyValue("status", "fully materialized")));
}

TEST(OperationTest, verifyCachePreventsInProgressState) {
  std::vector<std::string> jsonHistory;

  Index index =
      makeTestIndex("OperationTest", std::nullopt, true, true, true, 32);
  QueryResultCache cache;
  QueryExecutionContext qec{
      index, &cache, makeAllocator(), SortPerformanceEstimator{},
      [&](std::string json) { jsonHistory.emplace_back(std::move(json)); }};
  auto table = makeIdTableFromVector({{}, {}, {}});
  ValuesForTesting operation{&qec, std::move(table), {}};

  // Run twice and clear history to get cached values
  operation.getResult(true);
  jsonHistory.clear();
  operation.getResult(true);

  EXPECT_THAT(jsonHistory,
              ElementsAre(HasJsonKeyValue("status", "fully materialized"),
                          HasJsonKeyValue("status", "fully materialized")));
}

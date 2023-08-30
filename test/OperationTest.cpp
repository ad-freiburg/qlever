// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <kalmbach@cs.uni-freiburg.de>

#include "./IndexTestHelpers.h"
#include "engine/NeutralElementOperation.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using namespace ad_utility::testing;

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
  EXPECT_EQ(n.getRuntimeInfo().status_, RuntimeInformation::Status::notStarted);
  // Nothing has been stored in the cache by this call.
  EXPECT_EQ(qec->getQueryTreeCache().numNonPinnedEntries(), 0);
  EXPECT_EQ(qec->getQueryTreeCache().numPinnedEntries(), 0);

  // This "ordinary" call to `getResult` also stores the result in the cache.
  NeutralElementOperation n2{qec};
  auto result = n2.getResult();
  EXPECT_NE(result, nullptr);
  EXPECT_EQ(n2.getRuntimeInfo().status_,
            RuntimeInformation::Status::fullyMaterialized);
  EXPECT_EQ(n2.getRuntimeInfo().cacheStatus_,
            ad_utility::CacheStatus::computed);
  EXPECT_EQ(qec->getQueryTreeCache().numNonPinnedEntries(), 1);
  EXPECT_EQ(qec->getQueryTreeCache().numPinnedEntries(), 0);

  // When we now request to only return the result if it is cached, we should
  // get exactly the same `shared_ptr` as with the previous call.
  NeutralElementOperation n3{qec};
  EXPECT_EQ(n3.getResult(true, true), result);
  EXPECT_EQ(n3.getRuntimeInfo().cacheStatus_,
            ad_utility::CacheStatus::cachedNotPinned);

  // We can even use the `onlyReadFromCache` case to upgrade a non-pinned
  // cache-entry to a pinned cache entry
  QueryExecutionContext qecCopy{*qec};
  qecCopy._pinResult = true;
  NeutralElementOperation n4{&qecCopy};
  EXPECT_EQ(n4.getResult(true, true), result);

  // The cache status is `cachedNotPinned` because we found the element cached
  // but not pinned (it does reflect the status BEFORE the operation).
  EXPECT_EQ(n4.getRuntimeInfo().cacheStatus_,
            ad_utility::CacheStatus::cachedNotPinned);
  EXPECT_EQ(qec->getQueryTreeCache().numNonPinnedEntries(), 0);
  EXPECT_EQ(qec->getQueryTreeCache().numPinnedEntries(), 1);

  // We have pinned the result, so requesting it again should return a pinned
  // result.
  qecCopy._pinResult = false;
  EXPECT_EQ(n4.getResult(true, true), result);
  EXPECT_EQ(n4.getRuntimeInfo().cacheStatus_,
            ad_utility::CacheStatus::cachedPinned);

  // Clear the (global) cache again to not possibly interfere with other unit
  // tests.
  qec->getQueryTreeCache().clearAll();
}

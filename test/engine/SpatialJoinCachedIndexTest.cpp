// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <s2/mutable_s2shape_index.h>

#include "../QueryPlannerTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/NamedResultCache.h"
#include "engine/SpatialJoinCachedIndex.h"
#include "rdfTypes/Variable.h"

namespace {
// _________________________________________________________________________
TEST(SpatialJoinCachedIndex, Basic) {
  // Sample data and query
  std::string kb =
      "<s> <p> \"LINESTRING(1.5 2.5, 1.55 2.5)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . "
      "<s> <p> \"LINESTRING(15.5 2.5, 16.0 3.0)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . "
      "<s2> <p> \"LINESTRING(11.5 21.5, 11.5 22.0)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . "
      "<s3> <p> <o2> . "
      "<s4> <p> \"LINESTRING\" . "
      "<s5> <other-p>  \"LINESTRING(11.05 21.5, 11.5 22.0)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . ";
  std::string pinned = "SELECT * { ?s <p> ?o }";

  // Build a `QueryExecutionContext` and pin the query result of `?s <p> ?o`
  // together with an s2 index on `?o`.
  auto qec = ad_utility::testing::getQec(kb);
  qec->pinResultWithName() = {"dummy", Variable{"?o"}};
  auto plan = queryPlannerTestHelpers::parseAndPlan(pinned, qec);
  [[maybe_unused]] auto pinResult = plan.getResult();

  // Retrieve and check the result table and geo index from the named cache
  auto cacheEntry = qec->namedResultCache().get("dummy");

  ASSERT_NE(cacheEntry.get(), nullptr);
  ASSERT_NE(cacheEntry->result_.get(), nullptr);
  EXPECT_EQ(cacheEntry->result_->numColumns(), 2);
  EXPECT_EQ(cacheEntry->result_->numRows(), 5);

  ASSERT_TRUE(cacheEntry->cachedGeoIndex_.has_value());
  EXPECT_EQ(cacheEntry->cachedGeoIndex_.value().getGeometryColumn().name(),
            "?o");
  auto index = cacheEntry->cachedGeoIndex_.value().getIndex();
  ASSERT_NE(index.get(), nullptr);
  EXPECT_EQ(index->num_shape_ids(), 3);

  const auto& cachedIndex = cacheEntry->cachedGeoIndex_.value();
  EXPECT_EQ(cachedIndex.getRow(0), 0);
  EXPECT_EQ(cachedIndex.getRow(1), 1);
  EXPECT_EQ(cachedIndex.getRow(2), 2);

  // The method `is_fresh()` tells us that there are no pending updates to be
  // applied (which would slow down the first query).
  EXPECT_TRUE(index->is_fresh());
}

}  // namespace

// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <s2/mutable_s2shape_index.h>

#include "../QueryPlannerTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/NamedResultCache.h"
#include "engine/SpatialJoinCachedIndex.h"
#include "engine/SpatialJoinConfig.h"
#include "global/ValueId.h"
#include "rdfTypes/Variable.h"

namespace {

using namespace SpatialJoinTestHelpers;

// _____________________________________________________________________________
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

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, UseOfIndexByS2PointPolylineAlgorithm) {
  // We use real-world examples here for meaningful and better-to-understand
  // results: The examples <s1> to <s4> are rail segments in Freiburg Central
  // Railway Station (osmway:88297213, osmway:300061067, osmway:392142142,
  // osmway:300060683) which will be related to the station node <p1>
  // (osmnode:21769883). Additionally there is an unrelated line <w1>, a rail
  // segment in Berlin (osmway:69254641).
  const std::string kb =
      "<s1> <asWKT> \"LINESTRING(7.8428469 47.9995367,7.8423373 "
      "47.9988434,7.8420709 47.9984901,7.8417183 47.9980174,7.8417069 "
      "47.9980066,7.8413941 47.9975806,7.8413556 47.9975293,7.8413293 "
      "47.9974942)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s2> <asWKT> \"LINESTRING(7.8409068 47.9975041,7.8409391 "
      "47.9975489,7.8411011 47.9977637,7.8413442 47.9980941,7.8416097 "
      "47.9984351,7.8417572 47.9986299,7.8419403 47.9988452,7.8420114 "
      "47.9989233)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s3> <asWKT> \"LINESTRING(7.8427369 47.9995806,7.8426653 "
      "47.9994852,7.8411672 47.9975175)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s4> <asWKT> \"LINESTRING(7.8422376 47.9990144,7.8416416 "
      "47.9982311,7.8415671 47.9981344,7.8412301 47.9976974,7.8412265 "
      "47.9976927,7.8412028 47.9976619,7.8411016 47.9975307)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<p1> <asWKT2> \"POINT(7.841295 47.997731)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<w1> <asWKT> \"LINESTRING(13.4363731 52.5100129,13.4358858 "
      "52.5102196,13.4350587 52.5105704)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n";
  const MaxDistanceConfig maxDistance{1000};  // Use a radius of 1 km
  const std::vector<std::string> expectedResultIris{
      {"<s1>", "<s2>", "<s3>", "<s4>"}};

  // First, pin the linestrings as a named s2 index
  const std::string pinQuery = "SELECT * { ?s2 <asWKT> ?geo2 }";
  auto qec = ad_utility::testing::getQec(kb);
  qec->pinResultWithName() = {"dummy", Variable{"?geo2"}};
  auto plan = queryPlannerTestHelpers::parseAndPlan(pinQuery, qec);
  const auto pinResultCacheKey = plan.getCacheKey();
  [[maybe_unused]] auto pinResult = plan.getResult();

  // Check expected cache size
  const auto cacheEntry = qec->namedResultCache().get("dummy");
  EXPECT_EQ(cacheEntry->result_->numColumns(), 2);
  EXPECT_EQ(cacheEntry->result_->numRows(), 5);
  EXPECT_TRUE(cacheEntry->cachedGeoIndex_.has_value());

  // Prepare a spatial join using the s2 point polyline algorithm on this
  // dataset and use the `QueryExecutionContext` which holds the cached index.
  auto leftChild =
      buildIndexScan(qec, {"?s1", std::string{"<asWKT2>"}, "?geo1"});
  SpatialJoinConfiguration config{maxDistance, Variable{"?geo1"},
                                  Variable{"?geo2"}};
  config.algo_ = SpatialJoinAlgorithm::S2_POINT_POLYLINE;
  config.rightCacheName_ = "dummy";

  // The spatial join gets an index scan returning points as the left child and
  // no right child (it will construct a `ExplicitResult` itself).
  std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
      ad_utility::makeExecutionTree<SpatialJoin>(qec, config, leftChild,
                                                 std::nullopt);
  auto spatialJoin = std::dynamic_pointer_cast<SpatialJoin>(
      spatialJoinOperation->getRootOperation());
  const auto res = spatialJoin->computeResult(false);

  EXPECT_TRUE(res.isFullyMaterialized());
  EXPECT_EQ(res.idTable().numRows(), expectedResultIris.size());
  EXPECT_EQ(res.idTable().numColumns(), 4);  // ?s1 ?s2 ?geo1 ?geo2

  std::vector<std::string> resultIris;

  const auto subjectColIdx = spatialJoin->computeVariableToColumnMap()
                                 .at(Variable{"?s2"})
                                 .columnIndex_;
  for (size_t i = 0; i < res.idTable().numRows(); i++) {
    auto valueId = res.idTable().at(i, subjectColIdx);
    ASSERT_EQ(valueId.getDatatype(), Datatype::VocabIndex);
    auto entry = qec->getIndex().getVocab()[valueId.getVocabIndex()];
    resultIris.push_back(entry);
  }

  EXPECT_THAT(resultIris,
              ::testing::UnorderedElementsAreArray(expectedResultIris));

  const auto cacheKey = spatialJoin->getCacheKey();
  EXPECT_THAT(cacheKey, ::testing::HasSubstr("right cache name:dummy"));
  EXPECT_THAT(cacheKey, ::testing::HasSubstr(absl::StrCat(
                            "cache entry: (", pinResultCacheKey, ")")));
}

}  // namespace

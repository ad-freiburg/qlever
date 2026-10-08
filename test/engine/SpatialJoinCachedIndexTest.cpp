// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <s2/mutable_s2shape_index.h>
#include <s2/s2closest_edge_query.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2polyline.h>
#include <s2/s2shapeutil_coding.h>
#include <s2/util/units/length-units.h>

#include "../QueryPlannerTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/NamedResultCache.h"
#include "engine/NamedResultCacheSerializer.h"
#include "engine/SpatialJoinCachedIndex.h"
#include "engine/SpatialJoinConfig.h"
#include "global/ValueId.h"
#include "index/vocabulary/VocabularyType.h"
#include "rdfTypes/Variable.h"
#include "util/HashMap.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace {

using namespace SpatialJoinTestHelpers;

void serializeAndDeserializeCache(NamedResultCache& cache,
                                  QueryExecutionContext* qec) {
  using namespace ad_utility::serialization;
  ByteBufferWriteSerializer writer;
  cache.writeToSerializer(writer);
  cache.clear();
  ByteBufferReadSerializer reader{std::move(writer).data()};
  cache.readFromSerializer(reader, ad_utility::makeUnlimitedAllocator<Id>(),
                           qec->getLocalVocabContext());
}

// Three linestrings (the first three rows of the result of
// `SELECT * { ?s <p> ?o }`), followed by two rows that are no linestrings.
const std::string linestringsKb =
    "<s> <p> \"LINESTRING(1.5 2.5, 1.55 2.5)\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . "
    "<s> <p> \"LINESTRING(15.5 2.5, 16.0 3.0)\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . "
    "<s2> <p> \"LINESTRING(11.5 21.5, 11.5 22.0)\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . "
    "<s3> <p> <o2> . "
    "<s4> <p> \"LINESTRING\" . ";

// _____________________________________________________________________________
class SpatialJoinCachedIndexTest : public ::testing::TestWithParam<bool> {};

// _____________________________________________________________________________
TEST_P(SpatialJoinCachedIndexTest, Basic) {
  bool shouldSerialize = GetParam();
  // Sample data and query
  std::string kb =
      absl::StrCat(linestringsKb,
                   "<s5> <other-p>  \"LINESTRING(11.05 21.5, 11.5 22.0)\""
                   "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . ");
  std::string pinned = "SELECT * { ?s <p> ?o }";

  // Build a `QueryExecutionContext` and pin the query result of `?s <p> ?o`
  // together with an s2 index on `?o`.
  auto qec = ad_utility::testing::getQec(kb);
  qec->pinResultWithName() = {"dummy", Variable{"?o"}};
  auto plan = queryPlannerTestHelpers::parseAndPlan(pinned, qec);
  [[maybe_unused]] auto pinResult = plan->getResult();

  auto& cache = qec->namedResultCache();
  if (shouldSerialize) {
    serializeAndDeserializeCache(cache, qec);
  }

  // Retrieve and check the result table and geo index from the named cache
  auto cacheEntry = qec->namedResultCache().get("dummy");

  ASSERT_NE(cacheEntry.get(), nullptr);
  ASSERT_THAT(cacheEntry->result_,
              ::testing::VariantWith<std::shared_ptr<const IdTable>>(
                  ::testing::Ne(nullptr)));
  auto resultView = ExplicitIdTableOperation::viewOf(cacheEntry->result_);
  EXPECT_EQ(resultView.numColumns(), 2);
  EXPECT_EQ(resultView.numRows(), 5);

  ASSERT_TRUE(cacheEntry->cachedGeoIndex_.has_value());
  EXPECT_EQ(cacheEntry->cachedGeoIndex_.value().getGeometryColumn().name(),
            "?o");
  const auto& cachedIndex = cacheEntry->cachedGeoIndex_.value();
  ASSERT_EQ(cachedIndex.numSegments(), 1);
  auto index = cachedIndex.segments().at(0);
  ASSERT_NE(index.get(), nullptr);
  EXPECT_EQ(index->num_shape_ids(), 3);

  EXPECT_EQ(cachedIndex.getRow(0, 0), 0);
  EXPECT_EQ(cachedIndex.getRow(0, 1), 1);
  EXPECT_EQ(cachedIndex.getRow(0, 2), 2);

  // The method `is_fresh()` tells us that there are no pending updates to be
  // applied (which would slow down the first query).
  EXPECT_TRUE(index->is_fresh());
}

// _____________________________________________________________________________
TEST_P(SpatialJoinCachedIndexTest, UseOfIndexByS2PointPolylineAlgorithm) {
  bool shouldSerialize = GetParam();
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
  const auto pinResultCacheKey = plan->getCacheKey();
  [[maybe_unused]] auto pinResult = plan->getResult();

  auto& cache = qec->namedResultCache();
  if (shouldSerialize) {
    serializeAndDeserializeCache(cache, qec);
  }

  // Check expected cache size
  const auto cacheEntry = qec->namedResultCache().get("dummy");
  auto resultView = ExplicitIdTableOperation::viewOf(cacheEntry->result_);
  EXPECT_EQ(resultView.numColumns(), 2);
  EXPECT_EQ(resultView.numRows(), 5);
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
  EXPECT_EQ(res.idTableView().numRows(), expectedResultIris.size());
  EXPECT_EQ(res.idTableView().numColumns(), 4);  // ?s1 ?s2 ?geo1 ?geo2

  std::vector<std::string> resultIris;

  const auto subjectColIdx = spatialJoin->computeVariableToColumnMap()
                                 .at(Variable{"?s2"})
                                 .columnIndex_;
  for (size_t i = 0; i < res.idTableView().numRows(); i++) {
    auto valueId = res.idTableView()(i, subjectColIdx);
    ASSERT_EQ(valueId.getDatatype(), Datatype::VocabIndex);
    auto entry = qec->getIndex().getVocab()[valueId.getVocabIndex()];
    resultIris.push_back(entry);
  }

  EXPECT_THAT(resultIris,
              ::testing::UnorderedElementsAreArray(expectedResultIris));

  const auto cacheKey = spatialJoin->getCacheKey();
  EXPECT_THAT(cacheKey, ::testing::HasSubstr("right cache name:dummy"));
  EXPECT_THAT(cacheKey, ::testing::HasSubstr(pinResultCacheKey));
}

// _____________________________________________________________________________
INSTANTIATE_TEST_SUITE_P(WithAndWithoutSerialization,
                         SpatialJoinCachedIndexTest, ::testing::Bool());

// _____________________________________________________________________________
// Tests for `SpatialJoinCachedIndex` with and without simplification.
class SpatialJoinCachedIndexSimplificationTest
    : public ::testing::TestWithParam<bool> {};

// _____________________________________________________________________________
TEST_P(SpatialJoinCachedIndexSimplificationTest, WithoutSimplification) {
  bool shouldSerialize = GetParam();
  // A 3-vertex linestring: without simplification all 3 vertices (= 2 edges)
  // must be stored in the S2 shape index.
  const std::string kb =
      "<s1> <p> \"LINESTRING(7.840000 47.999000, 7.840045 47.999050, 7.841000 "
      "47.999900)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .";
  const std::string query = "SELECT * { ?s <p> ?o }";

  auto qec = ad_utility::testing::getQec(kb);
  qec->pinResultWithName() = {"idx", Variable{"?o"}};
  auto plan = queryPlannerTestHelpers::parseAndPlan(query, qec);
  [[maybe_unused]] auto pinResult = plan->getResult();

  auto& cache = qec->namedResultCache();
  if (shouldSerialize) {
    serializeAndDeserializeCache(cache, qec);
  }

  const auto entry = qec->namedResultCache().get("idx");
  ASSERT_TRUE(entry->cachedGeoIndex_.has_value());
  auto s2idx = entry->cachedGeoIndex_.value().segments().at(0);
  ASSERT_EQ(s2idx->num_shape_ids(), 1);
  // 3 vertices → 2 edges, stored as a single shape.
  EXPECT_EQ(s2idx->shape(0)->num_edges(), 2);
}

// _____________________________________________________________________________
TEST_P(SpatialJoinCachedIndexSimplificationTest, WithSimplification) {
  bool shouldSerialize = GetParam();
  // Same 3-vertex linestring, but the middle vertex is only ~6 m off the
  // direct path, so a 10 m simplification tolerance should remove it, leaving
  // 2 vertices (= 1 edge) in the index.
  const std::string kb =
      "<s1> <p> \"LINESTRING(7.840000 47.999000, 7.840045 47.999050, 7.841000 "
      "47.999900)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .";
  const std::string query = "SELECT * { ?s <p> ?o }";

  auto qec = ad_utility::testing::getQec(kb);
  qec->pinResultWithName() = {"idx", Variable{"?o"}, 10.0};
  auto plan = queryPlannerTestHelpers::parseAndPlan(query, qec);
  [[maybe_unused]] auto pinResult = plan->getResult();

  auto& cache = qec->namedResultCache();
  if (shouldSerialize) {
    serializeAndDeserializeCache(cache, qec);
  }

  const auto entry = qec->namedResultCache().get("idx");
  ASSERT_TRUE(entry->cachedGeoIndex_.has_value());
  auto s2idx = entry->cachedGeoIndex_.value().segments().at(0);
  ASSERT_EQ(s2idx->num_shape_ids(), 1);
  // Middle vertex removed by simplification: 2 vertices → 1 edge.
  EXPECT_EQ(s2idx->shape(0)->num_edges(), 1);
}

// _____________________________________________________________________________
INSTANTIATE_TEST_SUITE_P(WithAndWithoutSerialization,
                         SpatialJoinCachedIndexSimplificationTest,
                         ::testing::Bool());

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, GetPolylineGeometryTypeCheck) {
  // Test that `getPolyline` correctly checks the geometry type of its input
  // literals.
  std::string kb =
      "<s1> <asWKT> \"LINESTRING(7.8428469 47.9995367,7.8423373 "
      "47.9988434,7.8420709 47.9984901,7.8417183 47.9980174,7.8417069 "
      "47.9980066,7.8413941 47.9975806,7.8413556 47.9975293,7.8413293 "
      "47.9974942)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s2> <asWKT> \"POLYGON((7.8428469 47.9995367,7.8423373 "
      "47.9988434,7.8420709 47.9984901,7.8417183 47.9980174,7.8417069 "
      "47.9980066,7.8413941 47.9975806,7.8413556 47.9975293,7.8413293 "
      "47.9974942, 7.8428469 47.9995367))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s3> <asWKT> \"POINT(1 2)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n";

  auto vocabType =
      ad_utility::VocabularyType::fromString("on-disk-compressed-geo-split");
  auto qec = ad_utility::testing::getQec(kb, vocabType);
  auto scan = buildIndexScan(qec, {"?s", std::string{"<asWKT>"}, "?geo"});
  auto result = scan->getResult();
  auto col = scan->getVariableColumn(Variable{"?geo"});

  auto check = [&](size_t row) {
    return SpatialJoinCachedIndex::getPolyline(result->idTableView(), row, col,
                                               qec->getIndex());
  };

  EXPECT_TRUE(check(0).has_value());
  EXPECT_FALSE(check(1).has_value());
  EXPECT_FALSE(check(2).has_value());
}

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, withPermutedRows) {
  auto qec = ad_utility::testing::getQec(linestringsKb);
  qec->pinResultWithName() = {"permuted", Variable{"?o"}};
  auto plan =
      queryPlannerTestHelpers::parseAndPlan("SELECT * { ?s <p> ?o }", qec);
  [[maybe_unused]] auto pinResult = plan->getResult();
  auto cacheEntry = qec->namedResultCache().get("permuted");
  ASSERT_TRUE(cacheEntry->cachedGeoIndex_.has_value());
  const auto& original = cacheEntry->cachedGeoIndex_.value();

  // Reverse the order of the five rows.
  std::vector<size_t> newRowOfOldRow{4, 3, 2, 1, 0};
  auto permuted = original.withPermutedRows(newRowOfOldRow);
  EXPECT_EQ(permuted.getRow(0, 0), 4);
  EXPECT_EQ(permuted.getRow(0, 1), 3);
  EXPECT_EQ(permuted.getRow(0, 2), 2);
  EXPECT_EQ(permuted.getGeometryColumn(), original.getGeometryColumn());
  // The S2 index itself is shared, and the original index is unchanged.
  EXPECT_EQ(permuted.segments().at(0).get(), original.segments().at(0).get());
  EXPECT_EQ(original.getRow(0, 0), 0);
  EXPECT_EQ(original.getRow(0, 1), 1);
  EXPECT_EQ(original.getRow(0, 2), 2);

  // A permutation that does not cover all the rows of the index is rejected.
  std::vector<size_t> tooShort{0, 1};
  EXPECT_ANY_THROW(original.withPermutedRows(tooShort));

  // A mapping that is not a bijection (two rows are mapped to the same row)
  // is rejected instead of silently dropping the shape of one of them.
  AD_EXPECT_THROW_WITH_MESSAGE(
      original.withPermutedRows(std::vector<size_t>{0, 0, 2, 3, 4}),
      ::testing::HasSubstr("same row"));
}

// Helpers and tests for the segmented index (`extend`, serialization).
namespace segmented {
using Index = SpatialJoinCachedIndex;

// Six linestrings far apart from each other (the `i`-th starts at the longitude
// `10 * i`), and two rows that are no linestrings.
std::string segmentedKb() {
  std::string kb;
  for (int i = 1; i <= 6; ++i) {
    absl::StrAppend(&kb, "<s", i, "> <p> \"LINESTRING(", 10 * i, " 0, ", 10 * i,
                    " 0.1)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"
                    " . ");
  }
  absl::StrAppend(&kb, "<n1> <p> <o1> . ");
  absl::StrAppend(&kb,
                  "<n2> <p> \"POINT(1 2)\"^^<http://www.opengis.net/ont/"
                  "geosparql#wktLiteral> . ");
  return kb;
}

// A table together with everything that is needed to build indices on it.
struct TestTables {
  QueryExecutionContext* qec_;
  std::shared_ptr<const Result> result_;
  ColumnIndex col_;
  // The rows of the full table that contain a linestring, in the order of
  // their longitude, and the rows that contain no linestring.
  std::vector<size_t> lines_;
  std::vector<size_t> others_;

  explicit TestTables(const std::string& kb)
      : qec_{ad_utility::testing::getQec(kb)} {
    auto plan =
        queryPlannerTestHelpers::parseAndPlan("SELECT * { ?s <p> ?o }", qec_);
    result_ = plan->getResult();
    col_ = plan->getVariableColumn(Variable{"?o"});
    auto view = result_->idTableView();
    for (size_t row = 0; row < view.size(); ++row) {
      auto polyline = Index::getPolyline(view, row, col_, qec_->getIndex());
      (polyline.has_value() ? lines_ : others_).push_back(row);
    }
    // The rows are sorted by `?s`, which is also the order of the longitudes.
    AD_CORRECTNESS_CHECK(lines_.size() == 6 && others_.size() == 2);
  }

  // Create a table that consists of the given rows of the full table.
  IdTable select(const std::vector<size_t>& rows) const {
    auto view = result_->idTableView();
    IdTable table{view.numColumns(), ad_utility::makeUnlimitedAllocator<Id>()};
    table.resize(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
      for (size_t c = 0; c < view.numColumns(); ++c) {
        table(i, c) = view(rows[i], c);
      }
    }
    return table;
  }

  Index build(const IdTable& table,
              std::optional<double> simplification = std::nullopt) const {
    return Index{Variable{"?o"}, col_, table.asStaticView<0>(),
                 qec_->getIndex(), simplification};
  }

  Index extend(const Index& base, const std::vector<size_t>& baseRowOfNewRow,
               const IdTable& newTable) const {
    return Index::extend(base, baseRowOfNewRow, newTable.asStaticView<0>(),
                         col_, qec_->getIndex());
  }
};

// Return all rows of the `index` within `meters` of the given point, together
// with their distances (in km), by querying all segments.
std::map<size_t, double> queryRows(const Index& index, double lon, double lat,
                                   double meters) {
  std::map<size_t, double> result;
  S2ClosestEdgeQuery::PointTarget target{
      S2LatLng::FromDegrees(lat, lon).ToPoint()};
  for (size_t segment = 0; segment < index.segments().size(); ++segment) {
    S2ClosestEdgeQuery query{index.segments()[segment].get()};
    query.mutable_options()->set_inclusive_max_distance(
        S2Earth::ToAngle(util::units::Meters(meters)));
    for (const auto& neighbor : query.FindClosestEdges(&target)) {
      auto row = index.getRow(segment, neighbor.shape_id());
      if (row.has_value()) {
        result[row.value()] = S2Earth::ToKm(neighbor.distance());
      }
    }
  }
  return result;
}

// Check that the two indices give the same results for queries around all six
// linestrings, and one query that is far away from all of them.
void expectSameResults(const Index& a, const Index& b) {
  for (int i = 1; i <= 6; ++i) {
    EXPECT_EQ(queryRows(a, 10 * i, 0.05, 50'000),
              queryRows(b, 10 * i, 0.05, 50'000))
        << "line " << i;
  }
  // A large radius, which finds all lines.
  EXPECT_EQ(queryRows(a, 35, 0, 10'000'000), queryRows(b, 35, 0, 10'000'000));
  EXPECT_EQ(queryRows(a, -100, 50, 50'000), queryRows(b, -100, 50, 50'000));
}

// Serialize `index` into a byte string (format version 2).
std::string serializeToBytes(const Index& index) {
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  index.writeToSerializer(writer);
  auto data = std::move(writer).data();
  return std::string{data.begin(), data.end()};
}

// Deserialize the bytes written by `serializeToBytes`.
Index deserializeFromBytes(const std::string& bytes, size_t numRows,
                           uint16_t version = 2) {
  ad_utility::serialization::ByteBufferReadSerializer reader{
      std::vector<char>(bytes.begin(), bytes.end())};
  return Index::readFromSerializer(reader, numRows, version);
}

// Serialize an index in the format of version 2 that has the given `segments`
// (taken from `index`) and the given `rowToShape` (which does not have to be
// valid), by replicating the layout documented at `writeToSerializer`.
std::string serializeV2WithRowToShape(const Index& index,
                                      const std::vector<uint64_t>& rowToShape) {
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  writer << index.getGeometryColumn();
  writer << uint8_t{0};
  writer << static_cast<uint64_t>(index.segments().size());
  for (const auto& segment : index.segments()) {
    Encoder encoder;
    s2shapeutil::CompactEncodeTaggedShapes(*segment, &encoder);
    segment->Encode(&encoder);
    writer << std::string{encoder.base(), encoder.length()};
  }
  writer << rowToShape;
  auto data = std::move(writer).data();
  return std::string{data.begin(), data.end()};
}

// Serialize the single segment of `index` and the given `shapeToRow` in the
// legacy format (version 1).
std::string serializeLegacy(
    const Index& index, const ad_utility::HashMap<size_t, size_t>& shapeToRow) {
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  writer << index.getGeometryColumn();
  Encoder encoder;
  s2shapeutil::CompactEncodeTaggedShapes(*index.segments()[0], &encoder);
  index.segments()[0]->Encode(&encoder);
  writer << std::string{encoder.base(), encoder.length()};
  writer << shapeToRow;
  auto data = std::move(writer).data();
  return std::string{data.begin(), data.end()};
}
}  // namespace segmented

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, extendWithInsertedDeletedAndReorderedRows) {
  using namespace segmented;
  TestTables t{segmentedKb()};
  const auto& L = t.lines_;
  const auto& M = t.others_;
  for (auto simplification : {std::optional<double>{}, std::optional{10.0}}) {
    // The base table has the lines 0 to 3 and a row without a linestring.
    auto baseTable = t.select({L[0], L[1], M[0], L[2], L[3]});
    auto base = t.build(baseTable, simplification);
    EXPECT_EQ(base.numSegments(), 1);
    EXPECT_EQ(base.numShapes(), 4);
    EXPECT_EQ(base.numLiveShapes(), 4);
    EXPECT_EQ(base.numRows(), 5);
    EXPECT_EQ(base.simplificationErrorInMeters(), simplification);

    // The new table is reordered, lacks the lines 1 and 3 (deleted), and has
    // the new lines 4 and 5 and a new row without a linestring.
    auto newTable = t.select({L[2], L[4], L[0], M[1], L[5]});
    std::vector<size_t> baseRowOfNewRow{3, Index::NO_ROW, 0, Index::NO_ROW,
                                        Index::NO_ROW};
    auto extended = t.extend(base, baseRowOfNewRow, newTable);

    // The base is unchanged, and its first segment is shared.
    EXPECT_EQ(base.numSegments(), 1);
    EXPECT_EQ(base.numLiveShapes(), 4);
    ASSERT_EQ(extended.numSegments(), 2);
    EXPECT_EQ(extended.segments()[0].get(), base.segments()[0].get());
    EXPECT_EQ(extended.numRows(), 5);
    EXPECT_EQ(extended.numShapes(), 6);
    EXPECT_EQ(extended.numLiveShapes(), 4);
    EXPECT_EQ(extended.simplificationErrorInMeters(), simplification);
    EXPECT_EQ(extended.getGeometryColumn(), base.getGeometryColumn());

    // The results are the same as for a fresh index on the new table.
    auto fresh = t.build(newTable, simplification);
    expectSameResults(extended, fresh);
    EXPECT_EQ(fresh.numLiveShapes(), 4);

    // Extending with only matched rows adds no segment.
    auto onlyMatched = t.select({L[3], L[0]});
    auto extended2 = t.extend(base, {4, 0}, onlyMatched);
    EXPECT_EQ(extended2.numSegments(), 1);
    EXPECT_EQ(extended2.numLiveShapes(), 2);
    expectSameResults(extended2, t.build(onlyMatched, simplification));

    // Extending an extended index again works.
    auto newTable2 = t.select({L[5], L[2], L[1]});
    auto extended3 = t.extend(extended, {4, 0, Index::NO_ROW}, newTable2);
    EXPECT_EQ(extended3.numSegments(), 3);
    EXPECT_EQ(extended3.numLiveShapes(), 3);
    expectSameResults(extended3, t.build(newTable2, simplification));

    // Invalid input is rejected: wrong size, base row out of range, and a base
    // row that is used twice.
    EXPECT_ANY_THROW(t.extend(base, {0}, newTable));
    EXPECT_ANY_THROW(t.extend(base, {0, 1, 2, 3, 17}, newTable));
    EXPECT_ANY_THROW(t.extend(base, {0, 0, 1, 2, 3}, newTable));
  }
}

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, deadShapesAreSkipped) {
  using namespace segmented;
  TestTables t{segmentedKb()};
  const auto& L = t.lines_;
  auto baseTable = t.select({L[0], L[1], L[2]});
  auto base = t.build(baseTable);
  // Delete the middle line.
  auto newTable = t.select({L[0], L[2]});
  auto extended = t.extend(base, {0, 2}, newTable);
  EXPECT_EQ(extended.numSegments(), 1);
  EXPECT_EQ(extended.numShapes(), 3);
  EXPECT_EQ(extended.numLiveShapes(), 2);
  EXPECT_EQ(extended.getRow(0, 0), 0);
  EXPECT_EQ(extended.getRow(0, 1), std::nullopt);
  EXPECT_EQ(extended.getRow(0, 2), 1);
  // The deleted line is still in the S2 index, but no row is returned.
  EXPECT_EQ(extended.segments()[0]->num_shape_ids(), 3);
  EXPECT_TRUE(queryRows(extended, 20, 0.05, 50'000).empty());
  EXPECT_EQ(queryRows(extended, 10, 0.05, 50'000).size(), 1);
  EXPECT_EQ(queryRows(extended, 30, 0.05, 50'000).size(), 1);
  // Delete everything.
  auto empty = t.extend(base, {}, t.select({}));
  EXPECT_EQ(empty.numRows(), 0);
  EXPECT_EQ(empty.numLiveShapes(), 0);
  EXPECT_EQ(empty.numShapes(), 3);
  EXPECT_TRUE(queryRows(empty, 10, 0.05, 50'000).empty());
}

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, statistics) {
  using namespace segmented;
  TestTables t{segmentedKb()};
  const auto& L = t.lines_;
  const auto& M = t.others_;
  // No linestring at all: One (empty) segment.
  auto noLines = t.build(t.select({M[0], M[1]}));
  EXPECT_EQ(noLines.numSegments(), 1);
  EXPECT_EQ(noLines.numShapes(), 0);
  EXPECT_EQ(noLines.numLiveShapes(), 0);
  EXPECT_EQ(noLines.numRows(), 2);

  auto base = t.build(t.select({L[0], M[0], L[1]}));
  EXPECT_EQ(base.numSegments(), 1);
  EXPECT_EQ(base.numShapes(), 2);
  EXPECT_EQ(base.numLiveShapes(), 2);
  EXPECT_EQ(base.getRow(0, 0), 0);
  EXPECT_EQ(base.getRow(0, 1), 2);

  // Only new rows without a linestring: No new segment, one dead shape.
  auto ext = t.extend(base, {0, Index::NO_ROW}, t.select({L[0], M[1]}));
  EXPECT_EQ(ext.numSegments(), 1);
  EXPECT_EQ(ext.numShapes(), 2);
  EXPECT_EQ(ext.numLiveShapes(), 1);
}

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, withPermutedRowsAndMultipleSegments) {
  using namespace segmented;
  TestTables t{segmentedKb()};
  const auto& L = t.lines_;
  const auto& M = t.others_;
  auto base = t.build(t.select({L[0], L[1]}));
  auto newTable = t.select({L[1], M[0], L[2], L[3], L[0]});
  auto ext = t.extend(base, {1, Index::NO_ROW, Index::NO_ROW, Index::NO_ROW, 0},
                      newTable);
  ASSERT_EQ(ext.numSegments(), 2);
  EXPECT_EQ(ext.numLiveShapes(), 4);

  std::vector<size_t> newRowOfOldRow{4, 3, 2, 1, 0};
  auto permuted = ext.withPermutedRows(newRowOfOldRow);
  EXPECT_EQ(permuted.numSegments(), 2);
  EXPECT_EQ(permuted.numLiveShapes(), 4);
  EXPECT_EQ(permuted.segments()[0].get(), ext.segments()[0].get());
  EXPECT_EQ(permuted.segments()[1].get(), ext.segments()[1].get());
  EXPECT_EQ(permuted.simplificationErrorInMeters(),
            ext.simplificationErrorInMeters());
  for (int i = 1; i <= 4; ++i) {
    auto before = queryRows(ext, 10 * i, 0.05, 50'000);
    auto after = queryRows(permuted, 10 * i, 0.05, 50'000);
    ASSERT_EQ(before.size(), 1);
    ASSERT_EQ(after.size(), 1);
    EXPECT_EQ(newRowOfOldRow[before.begin()->first], after.begin()->first);
    EXPECT_EQ(before.begin()->second, after.begin()->second);
  }
  EXPECT_ANY_THROW(ext.withPermutedRows(std::vector<size_t>{0, 1}));
  EXPECT_ANY_THROW(ext.withPermutedRows(std::vector<size_t>{0, 1, 2, 3, 5}));
}

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, serializationOfVersion2) {
  using namespace segmented;
  TestTables t{segmentedKb()};
  const auto& L = t.lines_;
  const auto& M = t.others_;
  for (auto simplification : {std::optional<double>{}, std::optional{10.0}}) {
    auto base = t.build(t.select({L[0], L[1], M[0]}), simplification);
    auto newTable = t.select({L[1], L[2], M[1], L[3]});
    auto ext = t.extend(base, {1, Index::NO_ROW, Index::NO_ROW, Index::NO_ROW},
                        newTable);
    for (const auto* index : {&base, &ext}) {
      auto bytes = serializeToBytes(*index);
      // Deterministic.
      EXPECT_EQ(bytes, serializeToBytes(*index));
      auto restored = deserializeFromBytes(bytes, index->numRows());
      EXPECT_EQ(restored.getGeometryColumn(), index->getGeometryColumn());
      EXPECT_EQ(restored.simplificationErrorInMeters(), simplification);
      EXPECT_EQ(restored.numRows(), index->numRows());
      EXPECT_EQ(restored.numSegments(), index->numSegments());
      EXPECT_EQ(restored.numShapes(), index->numShapes());
      EXPECT_EQ(restored.numLiveShapes(), index->numLiveShapes());
      expectSameResults(restored, *index);
      // Byte-identical after a round trip.
      EXPECT_EQ(serializeToBytes(restored), bytes);
      // The wrong number of rows is detected.
      EXPECT_ANY_THROW(deserializeFromBytes(bytes, index->numRows() + 1));
    }
    // A restored index can be extended, with the persisted simplification.
    auto restoredBase =
        deserializeFromBytes(serializeToBytes(base), base.numRows());
    auto ext2 =
        t.extend(restoredBase, {1, Index::NO_ROW, Index::NO_ROW, Index::NO_ROW},
                 newTable);
    expectSameResults(ext2, ext);
    EXPECT_EQ(serializeToBytes(ext2), serializeToBytes(ext));
  }
}

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, legacyVersion1Format) {
  using namespace segmented;
  TestTables t{segmentedKb()};
  const auto& L = t.lines_;
  const auto& M = t.others_;
  auto table = t.select({M[0], L[0], L[1], M[1], L[2]});
  auto index = t.build(table);
  ASSERT_EQ(index.numSegments(), 1);

  // Write the legacy format by hand.
  ad_utility::serialization::ByteBufferWriteSerializer writer;
  writer << index.getGeometryColumn();
  Encoder encoder;
  s2shapeutil::CompactEncodeTaggedShapes(*index.segments()[0], &encoder);
  index.segments()[0]->Encode(&encoder);
  writer << std::string{encoder.base(), encoder.length()};
  ad_utility::HashMap<size_t, size_t> shapeToRow;
  for (size_t shape = 0; shape < index.numShapes(); ++shape) {
    shapeToRow[shape] = index.getRow(0, shape).value();
  }
  writer << shapeToRow;
  auto data = std::move(writer).data();

  ad_utility::serialization::ByteBufferReadSerializer reader{std::move(data)};
  auto loaded = Index::readFromSerializer(reader, index.numRows(), 1);
  EXPECT_EQ(loaded.getGeometryColumn(), index.getGeometryColumn());
  EXPECT_EQ(loaded.simplificationErrorInMeters(), std::nullopt);
  EXPECT_EQ(loaded.numSegments(), 1);
  EXPECT_EQ(loaded.numRows(), 5);
  EXPECT_EQ(loaded.numShapes(), 3);
  EXPECT_EQ(loaded.numLiveShapes(), 3);
  EXPECT_EQ(loaded.getRow(0, 0), 1);
  EXPECT_EQ(loaded.getRow(0, 1), 2);
  EXPECT_EQ(loaded.getRow(0, 2), 4);
  expectSameResults(loaded, index);
  // The loaded index can be written in the new format and extended.
  auto newBytes = serializeToBytes(loaded);
  EXPECT_EQ(newBytes, serializeToBytes(index));
  auto ext = t.extend(loaded, {1, Index::NO_ROW}, t.select({L[0], L[4]}));
  EXPECT_EQ(ext.numSegments(), 2);
  EXPECT_EQ(ext.numLiveShapes(), 2);

  // Unknown versions are rejected.
  EXPECT_ANY_THROW(deserializeFromBytes(newBytes, 5, 3));
}

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, extendWithExplicitRowOrderIsDeterministic) {
  using namespace segmented;
  TestTables t{segmentedKb()};
  const auto& L = t.lines_;
  const auto& M = t.others_;
  auto base = t.build(t.select({L[0]}));

  // The same rows (the line 0 of the base, the new lines 1 to 3, and a row
  // without a linestring) in two different orders.
  auto tableA = t.select({L[0], L[1], M[0], L[2], L[3]});
  auto tableB = t.select({L[3], M[0], L[1], L[0], L[2]});
  std::vector<size_t> baseRowsA{0, Index::NO_ROW, Index::NO_ROW, Index::NO_ROW,
                                Index::NO_ROW};
  std::vector<size_t> baseRowsB{Index::NO_ROW, Index::NO_ROW, Index::NO_ROW, 0,
                                Index::NO_ROW};
  // The visiting order that corresponds to the canonical order
  // `L[0], L[1], M[0], L[2], L[3]` of the rows of the tables.
  std::vector<size_t> rowOrderA{0, 1, 2, 3, 4};
  std::vector<size_t> rowOrderB{3, 2, 1, 4, 0};
  auto extend = [&](const IdTable& table, const std::vector<size_t>& baseRows,
                    const std::vector<size_t>& rowOrder) {
    auto extended = Index::extend(base, baseRows, table.asStaticView<0>(),
                                  t.col_, t.qec_->getIndex(), rowOrder);
    // Permute to the canonical order, as a writer that extends a serialized
    // index against the rows of a new table in canonical order would do.
    std::vector<size_t> newRowOfOldRow(rowOrder.size());
    for (size_t i = 0; i < rowOrder.size(); ++i) {
      newRowOfOldRow[rowOrder[i]] = i;
    }
    return extended.withPermutedRows(newRowOfOldRow);
  };
  auto extendedA = extend(tableA, baseRowsA, rowOrderA);
  auto extendedB = extend(tableB, baseRowsB, rowOrderB);
  ASSERT_EQ(extendedA.numSegments(), 2);
  EXPECT_EQ(extendedA.numLiveShapes(), 4);
  EXPECT_EQ(serializeToBytes(extendedA), serializeToBytes(extendedB));

  // The overload without a row order visits the rows in their natural order.
  EXPECT_EQ(
      serializeToBytes(t.extend(base, baseRowsA, tableA)),
      serializeToBytes(Index::extend(base, baseRowsA, tableA.asStaticView<0>(),
                                     t.col_, t.qec_->getIndex(), rowOrderA)));

  // An invalid row order is rejected.
  auto extendWith = [&](const std::vector<size_t>& rowOrder) {
    return Index::extend(base, baseRowsA, tableA.asStaticView<0>(), t.col_,
                         t.qec_->getIndex(), rowOrder);
  };
  EXPECT_ANY_THROW(extendWith({0, 1, 2, 3}));
  EXPECT_ANY_THROW(extendWith({0, 1, 2, 3, 3}));
  EXPECT_ANY_THROW(extendWith({0, 1, 2, 3, 5}));
}

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, readingCorruptVersion2IndexThrows) {
  using namespace segmented;
  TestTables t{segmentedKb()};
  const auto& L = t.lines_;
  auto index = t.build(t.select({L[0], L[1]}));
  ASSERT_EQ(index.numSegments(), 1);
  constexpr uint64_t none = Index::NO_SHAPE;
  const auto msg = [](std::string_view s) { return ::testing::HasSubstr(s); };

  // The valid case works.
  EXPECT_EQ(deserializeFromBytes(serializeV2WithRowToShape(index, {0, 1}), 2)
                .numLiveShapes(),
            2);
  EXPECT_EQ(deserializeFromBytes(serializeV2WithRowToShape(index, {none, 1}), 2)
                .numLiveShapes(),
            1);
  // A segment that does not exist.
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeFromBytes(
          serializeV2WithRowToShape(index, {0, (uint64_t{1} << 32) | 0}), 2),
      msg("does not exist"));
  // A shape that does not exist.
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeFromBytes(serializeV2WithRowToShape(index, {0, 99}), 2),
      msg("does not exist"));
  // A shape that is referenced twice.
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeFromBytes(serializeV2WithRowToShape(index, {1, 1}), 2),
      msg("more than one row"));
}

// _____________________________________________________________________________
TEST(SpatialJoinCachedIndex, readingCorruptLegacyIndexThrows) {
  using namespace segmented;
  TestTables t{segmentedKb()};
  const auto& L = t.lines_;
  auto index = t.build(t.select({L[0], L[1]}));
  using Map = ad_utility::HashMap<size_t, size_t>;
  const auto msg = ::testing::HasSubstr("corrupt");

  EXPECT_EQ(
      deserializeFromBytes(serializeLegacy(index, Map{{0, 0}, {1, 1}}), 2, 1)
          .numLiveShapes(),
      2);
  // A row that is not smaller than the number of rows.
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeFromBytes(serializeLegacy(index, Map{{0, 0}, {1, 2}}), 2, 1),
      msg);
  // A shape id that does not fit into 32 bits.
  AD_EXPECT_THROW_WITH_MESSAGE(
      deserializeFromBytes(
          serializeLegacy(index, Map{{0, 0}, {size_t{1} << 32, 1}}), 2, 1),
      msg);
}

}  // namespace

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/IndexTestHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"
#include "QueryPlannerTestHelpers.h"
#include "QueryRewriteUtilTestHelpers.h"
#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_replace.h"
#include "engine/GeoRectangleRowFilter.h"
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/PrefilterExpressionIndex.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "global/RuntimeParameters.h"
#include "global/ValueId.h"
#include "index/IndexImpl.h"
#include "rdfTypes/GeoRectangle.h"

namespace {

using ad_utility::GeoRectangle;
using ad_utility::geoRectangleSelectivity;
using ad_utility::padGeoRectangle;
using prefilterExpressions::GeoRectangleExpression;

constexpr std::string_view wktDatatype =
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

// Turtle input with a few geometries near (10, 10): two linestrings, a point
// of type `<P>`, plus `numFar` far-away points of type `<T>` in southern
// latitude bands (so that the latitude band of a query near (10, 10) can
// prune whole blocks of points), and one far-away linestring.
std::string geoTurtleInput(int numFar = 64,
                           std::string_view extraTriples = "") {
  auto wktTriple = [](std::string_view subject, std::string_view content) {
    return absl::StrCat(subject, " <hasGeom> \"", content, "\"", wktDatatype,
                        " . \n");
  };
  return absl::StrCat(
      wktTriple("<lineA>", "LINESTRING(10 10, 11 10)"),
      wktTriple("<lineB>", "LINESTRING(12 10, 13 10)"),
      wktTriple("<lineFar>", "LINESTRING(-100 -50, -101 -50)"),
      wktTriple("<pointNear>", "POINT(10.5 10.01)"),
      wktTriple("<pointFar>", "POINT(-100.5 -50.01)"),
      "<lineA> <hasType> <T> . \n"
      "<lineB> <hasType> <T> . \n"
      "<lineFar> <hasType> <T> . \n"
      "<pointNear> <hasType> <P> . \n"
      "<pointFar> <hasType> <P> . \n",
      [numFar] {
        std::string result;
        for (int i = 0; i < numFar; ++i) {
          result +=
              absl::StrCat("<far", i, "> <hasGeom> \"POINT(", -170 + i % 320,
                           " -", 60 - i / 320, ".0)\"", wktDatatype, " . \n",
                           "<far", i, "> <hasType> <T> . \n");
        }
        return result;
      }(),
      extraTriples);
}

// A `QueryExecutionContext` for an index over `geoTurtleInput` with the
// geo-split vocabulary (which has the precomputed geometry info).
QueryExecutionContext* geoQec(int numFar = 64,
                              std::string_view extraTriples = "") {
  ad_utility::testing::TestIndexConfig config{
      geoTurtleInput(numFar, extraTriples)};
  config.vocabularyType = ad_utility::VocabularyType{
      ad_utility::VocabularyType::Enum::OnDiskCompressedGeoSplit};
  config.parserBufferSize = 1000_B;
  return ad_utility::testing::getQec(std::move(config));
}

// Find the first operation of type `T` in `tree`, depth first.
template <typename T>
const T* findOperation(const QueryExecutionTree& tree) {
  if (const auto* op = dynamic_cast<const T*>(tree.getRootOperation().get())) {
    return op;
  }
  for (const auto* child :
       std::as_const(*tree.getRootOperation()).getChildren()) {
    if (const auto* found = findOperation<T>(*child)) {
      return found;
    }
  }
  return nullptr;
}

TEST(GeoRectanglePrefilter, padGeoRectangle) {
  GeoRectangle r{6.0, 49.0, 6.5, 49.5};
  auto padded = padGeoRectangle(r, 1000.0);
  // The padding must be conservative: at least 1000 m on each side.
  EXPECT_LT(padded.minLat_, 49.0 - 1000.0 / 111'000.0);
  EXPECT_GT(padded.maxLat_, 49.5 + 1000.0 / 111'000.0);
  EXPECT_LT(padded.minLng_, 6.0 - 1000.0 / 111'320.0);
  EXPECT_GT(padded.maxLng_, 6.5 + 1000.0 / 111'320.0);
  // ... but not absurdly large (less than 3 times the exact padding).
  EXPECT_GT(padded.minLat_, 49.0 - 3000.0 / 111'000.0);
  EXPECT_GT(padded.minLng_, 6.0 - 3000.0 / (111'320.0 * 0.6));

  // Zero distance keeps the rectangle (up to clamping).
  EXPECT_EQ(padGeoRectangle(r, 0.0), (GeoRectangle{6.0, 49.0, 6.5, 49.5}));

  // Near the pole and across the antimeridian the longitude range degrades
  // to the full range.
  EXPECT_EQ(padGeoRectangle(GeoRectangle{0.0, 89.5, 0.0, 89.5}, 100.0).minLng_,
            -180.0);
  auto wrapped = padGeoRectangle(GeoRectangle{179.99, 0.0, 179.99, 0.0}, 5000);
  EXPECT_EQ(wrapped.minLng_, -180.0);
  EXPECT_EQ(wrapped.maxLng_, 180.0);
  // Latitudes are clamped.
  EXPECT_EQ(padGeoRectangle(GeoRectangle{0, 89.99, 0, 89.99}, 50000).maxLat_,
            90.0);
}

// Test that the intersection of two rectangles is their common part, and
// that rectangles without a common part have no intersection.
TEST(GeoRectanglePrefilter, intersectGeoRectangles) {
  using ad_utility::intersectGeoRectangles;
  GeoRectangle a{0, 0, 10, 10};
  // A rectangle that overlaps `a`, one inside it, and `a` itself.
  EXPECT_EQ(intersectGeoRectangles(a, GeoRectangle{5, -5, 15, 5}),
            (GeoRectangle{5, 0, 10, 5}));
  EXPECT_EQ(intersectGeoRectangles(a, GeoRectangle{2, 3, 4, 5}),
            (GeoRectangle{2, 3, 4, 5}));
  EXPECT_EQ(intersectGeoRectangles(a, a), a);
  // Rectangles that only share an edge intersect in that edge.
  EXPECT_EQ(intersectGeoRectangles(a, GeoRectangle{10, 0, 20, 10}),
            (GeoRectangle{10, 0, 10, 10}));
  // Disjoint in longitude, and disjoint in latitude.
  EXPECT_EQ(intersectGeoRectangles(a, GeoRectangle{11, 0, 20, 10}),
            std::nullopt);
  EXPECT_EQ(intersectGeoRectangles(a, GeoRectangle{0, -20, 10, -1}),
            std::nullopt);
}

TEST(GeoRectanglePrefilter, geoRectangleSelectivity) {
  EXPECT_NEAR(geoRectangleSelectivity(GeoRectangle{0, 0, 36, 10}), 0.1, 1e-9);
  EXPECT_NEAR(geoRectangleSelectivity(GeoRectangle{-180, -90, 180, 90}), 1.0,
              1e-9);
  EXPECT_EQ(geoRectangleSelectivity(GeoRectangle{10, 10, 10, 10}), 0.0);
}

TEST(GeoRectanglePrefilter, geoRectangleOfConstantGeometry) {
  using sparqlExpression::geoRectangleOfConstantGeometry;
  // A GeoPoint Id yields a point rectangle.
  auto pointId = Id::makeFromGeoPoint(GeoPoint{49.61, 6.13});
  auto rect = geoRectangleOfConstantGeometry(TripleComponent{pointId});
  ASSERT_TRUE(rect.has_value());
  EXPECT_NEAR(rect.value().minLng_, 6.13, 1e-6);
  EXPECT_NEAR(rect.value().maxLat_, 49.61, 1e-6);
  EXPECT_EQ(rect.value().minLng_, rect.value().maxLng_);

  // A WKT literal yields the bounding box of its geometry.
  auto literal = TripleComponent{
      ad_utility::triple_component::Literal::fromStringRepresentation(
          absl::StrCat("\"LINESTRING(6 49, 7 50)\"", wktDatatype))};
  auto rect2 = geoRectangleOfConstantGeometry(literal);
  ASSERT_TRUE(rect2.has_value());
  EXPECT_NEAR(rect2.value().minLng_, 6.0, 1e-6);
  EXPECT_NEAR(rect2.value().maxLat_, 50.0, 1e-6);

  // Variables and non-geometry values yield nothing.
  EXPECT_FALSE(geoRectangleOfConstantGeometry(TripleComponent{Variable{"?x"}})
                   .has_value());
  EXPECT_FALSE(
      geoRectangleOfConstantGeometry(
          TripleComponent{
              ad_utility::triple_component::Literal::fromStringRepresentation(
                  "\"foo\"")})
          .has_value());
}

TEST(GeoRectanglePrefilter, geoRectangleIdPrefilter) {
  ad_utility::GeoRectangleIdPrefilter prefilter{GeoRectangle{9, 9, 11, 11}};
  EXPECT_FALSE(
      prefilter.canBeSkipped(Id::makeFromGeoPoint(GeoPoint{10.0, 10.0})));
  EXPECT_TRUE(
      prefilter.canBeSkipped(Id::makeFromGeoPoint(GeoPoint{10.0, 20.0})));
  EXPECT_TRUE(
      prefilter.canBeSkipped(Id::makeFromGeoPoint(GeoPoint{-50.0, 10.0})));
  // A point on the border is kept (quantization slack).
  EXPECT_FALSE(
      prefilter.canBeSkipped(Id::makeFromGeoPoint(GeoPoint{11.0, 9.0})));
  // Anything that is not a point is never skipped.
  EXPECT_FALSE(
      prefilter.canBeSkipped(Id::makeFromVocabIndex(VocabIndex::make(12345))));
  EXPECT_FALSE(prefilter.canBeSkipped(ad_utility::testing::IntId(7)));
  EXPECT_FALSE(prefilter.canBeSkipped(Id::makeUndefined()));
}

// Test the block-level evaluation of the `GeoRectangleExpression` against
// synthetic block metadata.
class GeoRectangleExpressionTest : public ::testing::Test {
 protected:
  const IndexImpl& indexImpl_ = geoQec()->getIndex().getImpl();
  size_t blockIdx_ = 0;

  // Build one block whose evaluation column (column 2) spans [first, last].
  CompressedBlockMetadata makeBlock(ValueId first, ValueId last) {
    AD_CONTRACT_CHECK(first <= last);
    auto vocabId10 = Id::makeFromVocabIndex(VocabIndex::make(10));
    ++blockIdx_;
    return {{{},
             0,
             {vocabId10, vocabId10, first, Id::makeUndefined()},
             {vocabId10, vocabId10, last, Id::makeUndefined()},
             {},
             false},
            blockIdx_};
  }

  static std::vector<const CompressedBlockMetadata*> toPointers(
      const BlockMetadataRanges& ranges) {
    std::vector<const CompressedBlockMetadata*> result;
    for (const auto& range : ranges) {
      for (const auto& block : range) {
        result.push_back(&block);
      }
    }
    return result;
  }
};

TEST_F(GeoRectangleExpressionTest, evaluate) {
  // The blocks of points below are kept or pruned by the latitude band of the
  // rectangle, which is the `LatMajor` behavior (see `evaluateZOrder` below
  // for `ZOrder`).
  absl::Cleanup restoreEncoding{
      [encoding = GeoPoint::encoding()] { GeoPoint::setEncoding(encoding); }};
  GeoPoint::setEncoding(GeoPointEncodingEnum::LatMajor);

  // Query rectangle in the far south east.
  GeoRectangle rectangle{170.0, -81.0, 172.0, -79.0};
  GeoRectangleExpression expr{rectangle};

  std::vector<CompressedBlockMetadata> blocks;
  // Block 0: ints -> pruned (`Datatype::Int` sorts below the index types).
  blocks.push_back(
      makeBlock(ad_utility::testing::IntId(1), ad_utility::testing::IntId(5)));
  // Block 1: vocabulary entries -> kept (a WKT literal's coordinates cannot
  // be seen from its ID).
  blocks.push_back(makeBlock(Id::makeFromVocabIndex(VocabIndex::make(5)),
                             Id::makeFromVocabIndex(VocabIndex::make(20))));
  // Block 2: GeoPoints within the latitude band -> kept, although their
  // longitudes are far off (the band is all the block prefilter can see).
  blocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{-80.5, 0.0}),
                             Id::makeFromGeoPoint(GeoPoint{-79.5, 10.0})));
  // Block 3: GeoPoints far north -> pruned.
  blocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{70.0, 0.0}),
                             Id::makeFromGeoPoint(GeoPoint{80.0, 10.0})));

  auto kept =
      toPointers(expr.evaluate(indexImpl_, {blocks.data(), blocks.size()}, 2));
  EXPECT_THAT(kept, ::testing::ElementsAre(&blocks[1], &blocks[2]));

  // A block of points that spans the whole latitude band (from far south to
  // far north) must also be kept, although neither of its boundary IDs is
  // inside.
  std::vector<CompressedBlockMetadata> spanningBlocks;
  spanningBlocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{-89, 0}),
                                     Id::makeFromGeoPoint(GeoPoint{-85, 0})));
  spanningBlocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{-84, 0}),
                                     Id::makeFromGeoPoint(GeoPoint{-70, 0})));
  spanningBlocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{-69, 0}),
                                     Id::makeFromGeoPoint(GeoPoint{-60, 0})));
  auto keptSpanning = toPointers(expr.evaluate(
      indexImpl_, {spanningBlocks.data(), spanningBlocks.size()}, 2));
  EXPECT_THAT(keptSpanning, ::testing::ElementsAre(&spanningBlocks[1]));

  // Clone and equality.
  auto clone = expr.clone();
  EXPECT_TRUE(*clone == expr);
  GeoRectangleExpression otherExpr{GeoRectangle{0, 0, 1, 1}};
  EXPECT_FALSE(otherExpr == expr);
  EXPECT_THAT(expr.asString(0), ::testing::HasSubstr("GeoRectangleExpression"));

  // The logical complement keeps all blocks.
  auto complement = expr.logicalComplement();
  auto keptComplement = toPointers(
      complement->evaluate(indexImpl_, {blocks.data(), blocks.size()}, 2));
  EXPECT_EQ(keptComplement.size(), blocks.size());
}

// The producer: a `<=` comparison over a geo distance function with a fixed
// geometry yields a `GeoRectangleExpression` for the variable.
// Test the block-level evaluation for points in the `ZOrder` encoding: a block
// of points in the latitude band of the rectangle but far away in longitude is
// pruned now (see `GeoPoint::intervalsForRectangle`).
TEST_F(GeoRectangleExpressionTest, evaluateZOrder) {
  absl::Cleanup restoreEncoding{
      [encoding = GeoPoint::encoding()] { GeoPoint::setEncoding(encoding); }};
  GeoPoint::setEncoding(GeoPointEncodingEnum::ZOrder);
  // Blocks of points, sorted by their IDs, as the evaluation expects.
  auto pointBlocks =
      [this](std::vector<std::pair<GeoPoint, GeoPoint>> firstAndLast) {
        std::vector<CompressedBlockMetadata> blocks;
        for (const auto& [first, last] : firstAndLast) {
          blocks.push_back(makeBlock(Id::makeFromGeoPoint(first),
                                     Id::makeFromGeoPoint(last)));
        }
        ql::ranges::sort(blocks, [](const auto& a, const auto& b) {
          return a.firstTriple_.col2Id_.getBits() <
                 b.firstTriple_.col2Id_.getBits();
        });
        return blocks;
      };
  auto evaluate = [this](GeoRectangleExpression& expr,
                         const std::vector<CompressedBlockMetadata>& blocks) {
    return toPointers(
        expr.evaluate(indexImpl_, {blocks.data(), blocks.size()}, 2));
  };
  // The block whose first point has the given latitude (up to the precision
  // of the quantization).
  auto blockAt = [](const std::vector<CompressedBlockMetadata>& blocks,
                    double firstLat) {
    auto it = ql::ranges::find_if(blocks, [firstLat](const auto& block) {
      return std::abs(block.firstTriple_.col2Id_.getGeoPoint().getLat() -
                      firstLat) < 1e-5;
    });
    AD_CORRECTNESS_CHECK(it != blocks.end());
    return &*it;
  };

  // The same query rectangle in the far south east as above. Of the blocks of
  // points inside it, in its latitude band but far west, and far north, only
  // the first one is kept.
  GeoRectangle rectangle{170.0, -81.0, 172.0, -79.0};
  GeoRectangleExpression expr{rectangle};
  auto blocks = pointBlocks({{GeoPoint{-80.5, 170.5}, GeoPoint{-79.5, 171.5}},
                             {GeoPoint{-80.25, 0.0}, GeoPoint{-79.5, 10.0}},
                             {GeoPoint{70.0, 0.0}, GeoPoint{80.0, 10.0}}});
  EXPECT_THAT(evaluate(expr, blocks),
              ::testing::ElementsAre(blockAt(blocks, -80.5)));

  // A block whose boundary points enclose the rectangle (south west and north
  // east of it) is kept, although neither of them is inside; its neighbors
  // are pruned.
  auto spanningBlocks =
      pointBlocks({{GeoPoint{-89.0, -179.0}, GeoPoint{-83.0, 159.0}},
                   {GeoPoint{-82.0, 160.0}, GeoPoint{-78.0, 175.0}},
                   {GeoPoint{-77.0, 176.0}, GeoPoint{89.0, 179.0}}});
  EXPECT_THAT(evaluate(expr, spanningBlocks),
              ::testing::ElementsAre(blockAt(spanningBlocks, -82.0)));
}

TEST(GeoRectanglePrefilter, getPrefilterExpressionFromDistanceFilter) {
  using namespace queryRewriteUtilTestHelpers;
  using namespace makeSparqlExpression;
  auto* qec = geoQec();

  auto pointId = Id::makeFromGeoPoint(GeoPoint{49.61, 6.13});
  auto makeFilterExpr = [&](bool reversed) {
    auto dist = reversed ? makeMetricDistExpression(getExpr(Variable{"?wkt"}),
                                                    getExpr(pointId))
                         : makeMetricDistExpression(getExpr(pointId),
                                                    getExpr(Variable{"?wkt"}));
    return LessEqualExpression{std::array<SparqlExpression::Ptr, 2>{
        std::move(dist), getExpr(ad_utility::testing::IntId(1000))}};
  };

  // NOTE: the constant is a `GeoPoint` Id, whose coordinates are quantized,
  // so the expected rectangle must be derived from the decoded point.
  GeoRectangle expected = padGeoRectangle(
      sparqlExpression::geoRectangleOfConstantGeometry(TripleComponent{pointId})
          .value(),
      1000.0);
  for (bool reversed : {false, true}) {
    auto expr = makeFilterExpr(reversed);
    auto vec = expr.getPrefilterExpressionForMetadata(
        qec->getLocalVocabContext(), false);
    ASSERT_EQ(vec.size(), 1u) << "reversed = " << reversed;
    EXPECT_EQ(vec[0].second, Variable{"?wkt"});
    EXPECT_TRUE(*vec[0].first == GeoRectangleExpression{expected});
  }
}

// The common configuration of the spatial joins below: within 200 km of a
// fixed point near (10, 10).
SpatialJoinConfiguration withinDistConfig(const Variable& pointVar,
                                          const Variable& wktVar) {
  return SpatialJoinConfiguration{
      LibSpatialJoinConfig{SpatialJoinType::WITHIN_DIST, 200'000.0,
                           std::nullopt},
      pointVar,
      wktVar,
      std::nullopt,
      PayloadVariables::all(),
      SpatialJoinAlgorithm::LIBSPATIALJOIN,
      std::nullopt};
}

SparqlTripleSimple hasGeomTriple(const Variable& wktVar) {
  return SparqlTripleSimple{
      TripleComponent{Variable{"?s"}},
      TripleComponent{TripleComponent::Iri::fromIriref("<hasGeom>")},
      TripleComponent{wktVar}};
}

// The geo rectangle prefilter on an `IndexScan` prunes whole blocks and then
// drops the remaining rows outside the rectangle one by one (in particular
// points, which the block prefilter can only restrict by latitude), so that
// the operations above the scan only see rows that may match.
TEST(GeoRectanglePrefilter, rowFilterOnPrefilteredScan) {
  auto* qec = geoQec();
  Variable wktVar{"?wkt"};
  auto scan = ad_utility::makeExecutionTree<IndexScan>(qec, Permutation::POS,
                                                       hasGeomTriple(wktVar));

  // A rectangle around the geometries near (10, 10).
  std::vector<Operation::PrefilterVariablePair> pairs;
  pairs.emplace_back(std::make_unique<GeoRectangleExpression>(
                         GeoRectangle{9.5, 9.5, 13.5, 10.5}),
                     wktVar);
  auto prefiltered =
      scan->getRootOperation()
          ->getUpdatedQueryExecutionTreeWithPrefilterApplied(pairs);
  ASSERT_TRUE(prefiltered.has_value());
  auto* rowFilter = dynamic_cast<GeoRectangleRowFilter*>(
      prefiltered.value()->getRootOperation().get());
  ASSERT_NE(rowFilter, nullptr);
  qec->clearCacheUnpinnedOnly();
  auto prefilteredRows = rowFilter->getChildren()
                             .at(0)
                             ->getRootOperation()
                             ->getResult()
                             ->idTableView()
                             .numRows();
  EXPECT_FALSE(rowFilter->canResultBeCached());
  EXPECT_EQ(rowFilter->getResultWidth(), scan->getResultWidth());
  EXPECT_EQ(rowFilter->getResultSortedOn(), scan->resultSortedOn());

  // The far-away points are in other latitude bands, so their blocks are
  // pruned, unless they share a block with kept rows; the row filter drops
  // them in any case. The nearby point and all three linestrings are kept
  // (a linestring cannot be decided from its ID).
  auto result = rowFilter->getResult();
  const auto& table = result->idTableView();
  auto wktCol = prefiltered.value()->getVariableColumn(wktVar);
  size_t numPoints = 0;
  for (size_t row = 0; row < table.numRows(); ++row) {
    Id id = table(row, wktCol);
    if (id.getDatatype() == Datatype::GeoPoint) {
      ++numPoints;
      EXPECT_NEAR(id.getGeoPoint().getLat(), 10.01, 0.01);
    }
  }
  EXPECT_EQ(numPoints, 1u);
  EXPECT_EQ(table.numRows(), 4u);
  auto fullScanRows =
      scan->getRootOperation()->getResult()->idTableView().numRows();
  EXPECT_LT(table.numRows(), fullScanRows);
  // The block prefilter alone already read fewer rows than the full scan
  // (measured on the prefiltered scan's own computation, before the full
  // scan's result, which shares the cache key, is in the cache).
  EXPECT_LT(prefilteredRows, fullScanRows);
}

// The runtime block prefilter: with a non-constant (here: two-row) small
// side, plan-time prefiltering is impossible, but `prepareJoin` prunes the
// scan's blocks using the bounding rectangle of the materialized small side.
TEST(GeoRectanglePrefilter, runtimeBlockPrefilter) {
  auto* qec = geoQec();
  Variable pointVar{"?point"};
  Variable wktVar{"?wkt"};
  parsedQuery::SparqlValues values;
  values._variables = {pointVar};
  values._values.push_back(
      {TripleComponent{Id::makeFromGeoPoint(GeoPoint{10.0, 10.5})}});
  values._values.push_back(
      {TripleComponent{Id::makeFromGeoPoint(GeoPoint{10.05, 10.6})}});
  auto valuesTree = ad_utility::makeExecutionTree<Values>(qec, values);
  auto scanTree = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::POS, hasGeomTriple(wktVar));

  auto sj =
      std::make_shared<SpatialJoin>(qec, withinDistConfig(pointVar, wktVar),
                                    std::nullopt, std::nullopt, true);
  sj = sj->addChild(valuesTree, pointVar);
  sj = sj->addChild(scanTree, wktVar);

  // No plan-time prefilter (two rows, no constant): the scan is untouched.
  EXPECT_TRUE(sj->getChildren().at(1)->getRootOperation()->canResultBeCached());

  // Both query points are within 200 km of the two nearby linestrings and
  // the nearby point geometry: 2 x 3 = 6 result rows.
  auto result = sj->computeResultOnlyForTesting();
  EXPECT_EQ(result.idTableView().numRows(), 6u);

  // The runtime block prefilter fired: fewer rows were read than the scan
  // holds in total.
  const auto& details = sj->runtimeInfo().details_;
  ASSERT_TRUE(details.contains("num-geoms-before-block-prefilter"));
  EXPECT_GT(details.at("num-geoms-before-block-prefilter").get<int64_t>(),
            details.at("num-geoms-after-block-prefilter").get<int64_t>());

  // The small side was materialized once, by the prefilter, and that result
  // was reused for the join. A second `getResult` would be a cache hit that
  // records the time of the cache lookup instead of the computation.
  EXPECT_EQ(valuesTree->getRootOperation()->runtimeInfo().cacheStatus_,
            ad_utility::CacheStatus::computed);
}

// The runtime block prefilter reaches a scan whose blocks it can prune even
// when the scan is wrapped in a `Sort` and a `Join` (as happens for a side
// with a type restriction): the prefilter is forwarded through both.
TEST(GeoRectanglePrefilter, runtimeBlockPrefilterThroughSortAndJoin) {
  auto* qec = geoQec();
  Variable pointVar{"?point"};
  Variable wktVar{"?wkt"};
  parsedQuery::SparqlValues values;
  values._variables = {pointVar};
  values._values.push_back(
      {TripleComponent{Id::makeFromGeoPoint(GeoPoint{10.0, 10.5})}});
  values._values.push_back(
      {TripleComponent{Id::makeFromGeoPoint(GeoPoint{10.05, 10.6})}});
  auto valuesTree = ad_utility::makeExecutionTree<Values>(qec, values);

  // A `Join` on `?s` of the geometry scan (sorted by `?wkt`, so the `Join`
  // wraps it in a `Sort` on `?s`) with a second scan of the same predicate.
  auto scanA = ad_utility::makeExecutionTree<IndexScan>(qec, Permutation::POS,
                                                        hasGeomTriple(wktVar));
  auto scanB = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO, hasGeomTriple(Variable{"?wkt2"}));
  auto joinTree = ad_utility::makeExecutionTree<Join>(
      qec, scanA, scanB, scanA->getVariableColumn(Variable{"?s"}),
      scanB->getVariableColumn(Variable{"?s"}));

  auto sj =
      std::make_shared<SpatialJoin>(qec, withinDistConfig(pointVar, wktVar),
                                    std::nullopt, std::nullopt, true);
  sj = sj->addChild(valuesTree, pointVar);
  sj = sj->addChild(joinTree, wktVar);

  sj->createRuntimeInfoFromEstimates(sj->getRuntimeInfoPointer());

  // Each subject has exactly one geometry, so the join is 1:1 and the result
  // is the same as with the bare scan: 2 x 3 = 6 rows.
  auto result = sj->computeResultOnlyForTesting();
  EXPECT_EQ(result.idTableView().numRows(), 6u);

  // The block prefilter was forwarded through the `Sort` and the `Join` down
  // to the scan of `?wkt`: fewer rows were read than the scan holds in total.
  const auto& details = sj->runtimeInfo().details_;
  ASSERT_TRUE(details.contains("num-geoms-before-block-prefilter"));
  EXPECT_GT(details.at("num-geoms-before-block-prefilter").get<int64_t>(),
            details.at("num-geoms-after-block-prefilter").get<int64_t>());

  // The runtime information shows the actually executed (prefiltered)
  // replacement of the join side, not the "not yet started" original.
  for (const auto& childRti : sj->runtimeInfo().children_) {
    EXPECT_NE(childRti->status_, RuntimeInformation::Status::notStarted)
        << childRti->descriptor_;
  }
}

// A `Join` that the prefilter rebuilds (because the geometry scan is below
// it) must keep the column layout of the original join: the operations above
// it (a `Sort`, another `Join`) refer to its columns by index. The `Join`
// constructor orders its children by cache key, and the prefiltered child has
// a new cache key, so the rebuilt join must not reorder them. Both child
// orders are tested, one of them differs from the constructor's order.
TEST(GeoRectanglePrefilter, rebuiltJoinKeepsColumnLayout) {
  auto* qec = geoQec();
  Variable wktVar{"?wkt"};
  Variable sVar{"?s"};
  auto iri = [](std::string_view s) {
    return TripleComponent{TripleComponent::Iri::fromIriref(s)};
  };
  // The geometry scan is sorted by `?wkt`, the type scan by `?t`, so the join
  // on `?s` sorts both of them.
  auto geomScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::POS, hasGeomTriple(wktVar));
  auto typeScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::POS,
      SparqlTripleSimple{TripleComponent{sVar}, iri("<hasType>"),
                         TripleComponent{Variable{"?t"}}});
  auto restrictionScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::POS,
      SparqlTripleSimple{TripleComponent{sVar}, iri("<hasType>"), iri("<T>")});

  auto layout = [](const QueryExecutionTree& tree) {
    std::vector<std::pair<std::string, ColumnIndex>> result;
    for (const auto& [var, info] : tree.getVariableColumns()) {
      result.emplace_back(var.name(), info.columnIndex_);
    }
    ql::ranges::sort(result);
    return result;
  };

  std::vector<Operation::PrefilterVariablePair> pairs;
  pairs.emplace_back(std::make_unique<GeoRectangleExpression>(
                         GeoRectangle{9.5, 9.5, 13.5, 10.5}),
                     wktVar);

  for (bool geometryScanFirst : {true, false}) {
    const auto& first = geometryScanFirst ? geomScan : typeScan;
    const auto& second = geometryScanFirst ? typeScan : geomScan;
    // Fix the order of the children, so that both orders are tested.
    auto innerJoin = ad_utility::makeExecutionTree<Join>(
        qec, first, second, first->getVariableColumn(sVar),
        second->getVariableColumn(sVar), true, false);
    auto outerJoin = ad_utility::makeExecutionTree<Join>(
        qec, innerJoin, restrictionScan, innerJoin->getVariableColumn(sVar),
        restrictionScan->getVariableColumn(sVar));

    // The prefilter is pushed down through both joins (the side is rebuilt),
    // and the rebuilt side has the same column layout as the original.
    auto rebuilt = outerJoin->getUpdatedQueryExecutionTreeWithPrefilterApplied(
        Operation::clonePrefilters(pairs));
    ASSERT_TRUE(rebuilt.has_value())
        << "geometryScanFirst = " << geometryScanFirst;
    ASSERT_NE(rebuilt.value()->getCacheKey(), outerJoin->getCacheKey())
        << "geometryScanFirst = " << geometryScanFirst;
    EXPECT_EQ(layout(*rebuilt.value()), layout(*outerJoin))
        << "geometryScanFirst = " << geometryScanFirst;

    // The rebuilt side drops the 64 far-away points of type `<T>` and keeps
    // the three linestrings (the far-away one survives the prefilter, its
    // coordinates are not in its ID); the original has all 67.
    EXPECT_EQ(rebuilt.value()
                  ->getRootOperation()
                  ->getResult()
                  ->idTableView()
                  .numRows(),
              3u)
        << "geometryScanFirst = " << geometryScanFirst;
    EXPECT_EQ(
        outerJoin->getRootOperation()->getResult()->idTableView().numRows(),
        67u);
  }
}

// The queries for the planner tests below: a fixed point near (10, 10),
// either inlined in the filter or bound by a `BIND`, with a type restriction
// on the geometries.
constexpr std::string_view queryInlined = R"q(
  PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
  PREFIX geo: <http://www.opengis.net/ont/geosparql#>
  SELECT * WHERE {
    ?s <hasType> <T> . ?s <hasGeom> ?g .
    FILTER (geof:metricDistance("POINT(10.5 10.0)"^^geo:wktLiteral, ?g) <= 200000)
})q";
constexpr std::string_view queryBind = R"q(
  PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
  PREFIX geo: <http://www.opengis.net/ont/geosparql#>
  SELECT * WHERE {
    BIND ("POINT(10.5 10.0)"^^geo:wktLiteral AS ?p)
    ?s <hasType> <T> . ?s <hasGeom> ?g .
    FILTER (geof:metricDistance(?p, ?g) <= 200000)
})q";

// The planner prefilters the scans of the geometry variable once, before the
// dynamic programming: the final plan contains a `GeoRectangleRowFilter`
// below the spatial join, for the inlined constant as well as for the `BIND`
// form, and the spatial join carries the selectivity.
TEST(GeoRectanglePrefilter, plannerPrefiltersGeometrySeeds) {
  auto* qec = geoQec();
  // On this small index, every part of the query would be small enough to
  // be evaluated at planning time; here the rectangle comes from the fixed
  // point alone.
  auto budget = setRuntimeParameterForTest<
      &RuntimeParameters::geoPrefilterPlanningMaxRows_>(0);
  for (std::string_view query : {queryInlined, queryBind}) {
    auto qet = queryPlannerTestHelpers::parseAndPlan(std::string{query}, qec);
    const auto* spatialJoin = findOperation<SpatialJoin>(*qet);
    ASSERT_NE(spatialJoin, nullptr) << query;
    const auto* rowFilter = findOperation<GeoRectangleRowFilter>(*qet);
    ASSERT_NE(rowFilter, nullptr) << query;
    // The row filter sits on the scan of `?g`, below the spatial join.
    EXPECT_NE(findOperation<GeoRectangleRowFilter>(
                  *spatialJoin->getChildren().at(1)) == nullptr &&
                  findOperation<GeoRectangleRowFilter>(
                      *spatialJoin->getChildren().at(0)) == nullptr,
              true)
        << query;
    // The two nearby linestrings of type `<T>`.
    auto result = qet->getRootOperation()->getResult();
    EXPECT_EQ(result->idTableView().numRows(), 2u) << query;
  }
}

// The prefilter never changes a result: the same queries with the prefilter
// disabled give the same rows.
TEST(GeoRectanglePrefilter, plannerPrefilterKeepsResultsCorrect) {
  auto* qec = geoQec();
  auto budget = setRuntimeParameterForTest<
      &RuntimeParameters::geoPrefilterPlanningMaxRows_>(0);
  auto numRows = [&qec](std::string_view q) {
    qec->clearCacheUnpinnedOnly();
    auto qet = queryPlannerTestHelpers::parseAndPlan(std::string{q}, qec);
    return qet->getRootOperation()->getResult()->idTableView().size();
  };
  for (std::string_view query : {queryInlined, queryBind}) {
    auto rowsWithPrefilter = numRows(query);
    setRuntimeParameter<&RuntimeParameters::enablePrefilterOnIndexScans_>(
        false);
    absl::Cleanup restoreParameter{[]() {
      setRuntimeParameter<&RuntimeParameters::enablePrefilterOnIndexScans_>(
          true);
    }};
    auto rowsWithoutPrefilter = numRows(query);
    EXPECT_EQ(rowsWithPrefilter, 2u) << query;
    EXPECT_EQ(rowsWithPrefilter, rowsWithoutPrefilter) << query;
  }
}

// All operations of type `T` in `tree`, depth first.
template <typename T>
void collectOperations(const QueryExecutionTree& tree,
                       std::vector<const T*>& result) {
  if (const auto* op = dynamic_cast<const T*>(tree.getRootOperation().get())) {
    result.push_back(op);
  }
  for (const auto* child :
       std::as_const(*tree.getRootOperation()).getChildren()) {
    collectOperations<T>(*child, result);
  }
}

// The rectangles of the `GeoRectangleRowFilter`s in the plan of `query` whose
// subtree binds `variable` (a prefiltered scan always has such a row filter
// above it, see `IndexScan::getUpdatedQueryExecutionTreeWithPrefilterApplied`
// and `QueryPlanner::applyGeoRectanglePrefilters`).
std::vector<GeoRectangle> rowFilterRectangles(
    std::shared_ptr<QueryExecutionTree> qet, const Variable& variable) {
  std::vector<const GeoRectangleRowFilter*> rowFilters;
  collectOperations(*qet, rowFilters);
  std::vector<GeoRectangle> result;
  for (const auto* rowFilter : rowFilters) {
    if (rowFilter->getExternallyVisibleVariableColumns().contains(variable)) {
      result.push_back(rowFilter->rectangle());
    }
  }
  return result;
}

// The queries for the propagation tests below: two unrelated geometry
// variables, where only `?g1` is spatially joined with a fixed rectangle
// around the geometries near (10, 10), and `?g2` only with `?g1`.
constexpr std::string_view queryHead = R"q(
  PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
  PREFIX geo: <http://www.opengis.net/ont/geosparql#>
  SELECT * WHERE { ?s1 <hasGeom> ?g1 . ?s2 <hasGeom> ?g2 . )q";
constexpr std::string_view fixedPolygon =
    R"q("POLYGON((9 9, 14 9, 14 11, 9 11, 9 9))"^^geo:wktLiteral)q";
const GeoRectangle fixedRectangle{9, 9, 14, 11};

// Expect exactly one row filter for `variable` in the plan `qet`, whose
// rectangle equals `expected` up to the quantization of the coordinates (a
// point constant is encoded into a `GeoPoint` ID, the bounding box of a WKT
// literal is precomputed with limited precision).
void expectRowFilterRectangleNear(std::shared_ptr<QueryExecutionTree> qet,
                                  const Variable& variable,
                                  const GeoRectangle& expected,
                                  std::string_view query) {
  auto actual = rowFilterRectangles(std::move(qet), variable);
  ASSERT_EQ(actual.size(), 1u) << query;
  EXPECT_NEAR(actual.at(0).minLng_, expected.minLng_, 1e-3) << query;
  EXPECT_NEAR(actual.at(0).minLat_, expected.minLat_, 1e-3) << query;
  EXPECT_NEAR(actual.at(0).maxLng_, expected.maxLng_, 1e-3) << query;
  EXPECT_NEAR(actual.at(0).maxLat_, expected.maxLat_, 1e-3) << query;
}

// The number of result rows of `query`, computed from scratch.
size_t numResultRows(QueryExecutionContext* qec, const std::string& query) {
  qec->clearCacheUnpinnedOnly();
  return queryPlannerTestHelpers::parseAndPlan(query, qec)
      ->getRootOperation()
      ->getResult()
      ->idTableView()
      .size();
}

// The number of result rows of `query` without any prefilter.
size_t numResultRowsWithoutPrefilter(QueryExecutionContext* qec,
                                     const std::string& query) {
  auto cleanup = setRuntimeParameterForTest<
      &RuntimeParameters::enablePrefilterOnIndexScans_>(false);
  return numResultRows(qec, query);
}

// Test that a rectangle that contains all geometries of `?g1` (the fixed
// polygon contains `?g1`, or `?g1` lies within it) carries over to `?g2`,
// which is spatially joined with `?g1`: the scans of both variables are
// prefiltered with that rectangle, and the result is the same as without
// the prefilter.
TEST(GeoRectanglePrefilter, plannerPropagatesContainedRectangle) {
  auto* qec = geoQec();
  // No part of these queries is small enough to be evaluated at planning
  // time, so the rectangles come from the fixed polygon alone.
  auto budget = setRuntimeParameterForTest<
      &RuntimeParameters::geoPrefilterPlanningMaxRows_>(1);
  for (std::string_view firstFilter : {R"(FILTER geof:sfContains(POLY, ?g1))",
                                       R"(FILTER geof:sfWithin(?g1, POLY))"}) {
    std::string query = absl::StrCat(
        queryHead, absl::StrReplaceAll(firstFilter, {{"POLY", fixedPolygon}}),
        " FILTER geof:sfIntersects(?g1, ?g2) }");
    auto qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
    EXPECT_THAT(rowFilterRectangles(qet, Variable{"?g1"}),
                ::testing::ElementsAre(fixedRectangle))
        << query;
    EXPECT_THAT(rowFilterRectangles(qet, Variable{"?g2"}),
                ::testing::ElementsAre(fixedRectangle))
        << query;
    // At least the two linestrings near (10, 10) intersect themselves.
    auto numRows = qet->getRootOperation()->getResult()->idTableView().size();
    EXPECT_GE(numRows, 2u) << query;
    EXPECT_EQ(numResultRowsWithoutPrefilter(qec, query), numRows) << query;
  }
}

// Test that a rectangle that the geometries of `?g1` only intersect (`?g1`
// intersects the fixed polygon, or contains a fixed point) does not carry
// over to `?g2`: only the scans of `?g1` are prefiltered.
TEST(GeoRectanglePrefilter, plannerDoesNotPropagateIntersectedRectangle) {
  auto* qec = geoQec();
  auto budget = setRuntimeParameterForTest<
      &RuntimeParameters::geoPrefilterPlanningMaxRows_>(1);
  const std::string point = R"q("POINT(10.5 10.0)"^^geo:wktLiteral)q";
  for (const auto& [firstFilter, rectangle] :
       std::vector<std::pair<std::string, GeoRectangle>>{
           {absl::StrCat("FILTER geof:sfIntersects(", fixedPolygon, ", ?g1)"),
            fixedRectangle},
           {absl::StrCat("FILTER geof:sfContains(?g1, ", point, ")"),
            GeoRectangle{10.5, 10.0, 10.5, 10.0}}}) {
    std::string query = absl::StrCat(queryHead, firstFilter,
                                     " FILTER geof:sfIntersects(?g1, ?g2) }");
    auto qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
    expectRowFilterRectangleNear(qet, Variable{"?g1"}, rectangle, query);
    EXPECT_THAT(rowFilterRectangles(qet, Variable{"?g2"}), ::testing::IsEmpty())
        << query;
  }
}

// Test that a rectangle carries over to the other side of a distance join
// padded by the maximal distance, and that a rectangle that arrives at a
// variable from two sides is the intersection of the two.
TEST(GeoRectanglePrefilter, plannerPropagatesPaddedAndIntersectedRectangles) {
  auto* qec = geoQec();
  auto budget = setRuntimeParameterForTest<
      &RuntimeParameters::geoPrefilterPlanningMaxRows_>(1);
  // The geometries of `?g2` are within 1 km of those of `?g1`, which lie in
  // the fixed rectangle.
  std::string query =
      absl::StrCat(queryHead, "FILTER geof:sfContains(", fixedPolygon, ", ?g1)",
                   " FILTER (geof:metricDistance(?g1, ?g2) <= 1000) }");
  auto qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
  auto rectangles = rowFilterRectangles(qet, Variable{"?g2"});
  ASSERT_EQ(rectangles.size(), 1u);
  const auto& padded = rectangles.at(0);
  EXPECT_LT(padded.minLng_, fixedRectangle.minLng_);
  EXPECT_LT(padded.minLat_, fixedRectangle.minLat_);
  EXPECT_GT(padded.maxLng_, fixedRectangle.maxLng_);
  EXPECT_GT(padded.maxLat_, fixedRectangle.maxLat_);
  EXPECT_GT(padded.minLng_, fixedRectangle.minLng_ - 0.1);
  EXPECT_LT(padded.maxLat_, fixedRectangle.maxLat_ + 0.1);

  // `?g2` lies in the fixed rectangle via `?g1`, and in a second fixed
  // rectangle directly, so its scans are prefiltered with the intersection.
  query = absl::StrCat(
      queryHead, "FILTER geof:sfContains(", fixedPolygon, ", ?g1)",
      " FILTER geof:sfContains(?g1, ?g2)",
      R"q( FILTER geof:sfContains("POLYGON((12 5, 20 5, 20 10.5, 12 10.5, 12 5))"^^geo:wktLiteral, ?g2) })q");
  qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
  EXPECT_THAT(rowFilterRectangles(qet, Variable{"?g2"}),
              ::testing::ElementsAre(GeoRectangle{12, 9, 14, 10.5}));
}

// Test that the geometry of a fixed subject, which is bound by a small and
// cheap part of the query, is evaluated at planning time and its rectangle is
// used like that of a fixed geometry (also along a second spatial join if the
// first one is a containment), and that this does not happen when the budget
// of rows is zero.
TEST(GeoRectanglePrefilter, plannerEvaluatesSmallComponent) {
  // The index with an additional polygon whose bounding box is the fixed
  // rectangle of the tests above.
  auto* qec = geoQec(
      64, absl::StrCat("<polyNear> <hasGeom> \"POLYGON((9 9, 14 9, 14 11, 9 "
                       "11, 9 9))\"",
                       wktDatatype, " . \n"));
  // The geometry of `<polyNear>` is the only part small enough to be
  // evaluated.
  auto budget = setRuntimeParameterForTest<
      &RuntimeParameters::geoPrefilterPlanningMaxRows_>(1);

  // `?g1` intersects the polygon: only `?g1` is prefiltered.
  std::string query = absl::StrCat(
      queryHead,
      " <polyNear> <hasGeom> ?r . FILTER geof:sfIntersects(?r, ?g1)"
      " FILTER geof:sfIntersects(?g1, ?g2) }");
  auto qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
  expectRowFilterRectangleNear(qet, Variable{"?g1"}, fixedRectangle, query);
  EXPECT_THAT(rowFilterRectangles(qet, Variable{"?g2"}), ::testing::IsEmpty());
  // At least the two linestrings near (10, 10) intersect themselves.
  auto numRows = qet->getRootOperation()->getResult()->idTableView().size();
  EXPECT_GE(numRows, 2u);
  EXPECT_EQ(numResultRowsWithoutPrefilter(qec, query), numRows);

  // The polygon contains `?g1`: both variables are prefiltered.
  std::string queryContained =
      absl::StrCat(queryHead,
                   " <polyNear> <hasGeom> ?r . FILTER geof:sfContains(?r, ?g1)"
                   " FILTER geof:sfIntersects(?g1, ?g2) }");
  auto qetContained =
      queryPlannerTestHelpers::parseAndPlan(queryContained, qec);
  expectRowFilterRectangleNear(qetContained, Variable{"?g1"}, fixedRectangle,
                               queryContained);
  expectRowFilterRectangleNear(qetContained, Variable{"?g2"}, fixedRectangle,
                               queryContained);
  auto numRowsContained =
      qetContained->getRootOperation()->getResult()->idTableView().size();
  EXPECT_GE(numRowsContained, 2u);
  EXPECT_EQ(numResultRowsWithoutPrefilter(qec, queryContained),
            numRowsContained);

  // With a budget of zero rows, nothing is evaluated and nothing prefiltered.
  {
    auto noBudget = setRuntimeParameterForTest<
        &RuntimeParameters::geoPrefilterPlanningMaxRows_>(0);
    auto qetNoBudget = queryPlannerTestHelpers::parseAndPlan(query, qec);
    EXPECT_THAT(rowFilterRectangles(qetNoBudget, Variable{"?g1"}),
                ::testing::IsEmpty());
  }
}

// The size estimates of the children of the spatial joins in the plan `qet`
// that bind `variable` but not `otherVariable` (the other side of the join).
std::vector<size_t> spatialJoinSideSizeEstimates(
    const std::shared_ptr<QueryExecutionTree>& qet, const Variable& variable,
    const Variable& otherVariable) {
  std::vector<const SpatialJoin*> joins;
  collectOperations(*qet, joins);
  std::vector<size_t> result;
  for (const auto* join : joins) {
    for (const auto* child : std::as_const(*join).getChildren()) {
      if (child->containsVariable(variable) &&
          !child->containsVariable(otherVariable)) {
        // `getSizeEstimate` caches its result and is therefore not `const`.
        result.push_back(
            const_cast<QueryExecutionTree*>(child)->getSizeEstimate());
      }
    }
  }
  return result;
}

// Test that a part of the query that was evaluated at planning time enters the
// plan with its exact size as its size estimate. Here, the part is a join
// whose estimate from the multiplicities is larger than its one actual row.
TEST(GeoRectanglePrefilter, plannerUsesExactSizeOfEvaluatedPart) {
  // `<polyNear>` has three nodes, of which only one has a geometry.
  auto* qec = geoQec(
      64, absl::StrCat("<polyNear> <hasNode> <pn1> . <polyNear> <hasNode> "
                       "<pn2> . <polyNear> <hasNode> <pn3> . <pn1> <hasGeom> "
                       "\"POLYGON((9 9, 14 9, 14 11, 9 11, 9 9))\"",
                       wktDatatype, " . \n"));
  std::string query = absl::StrCat(
      queryHead,
      " <polyNear> <hasNode>/<hasGeom> ?r . FILTER geof:sfContains(?r, ?g1)"
      " FILTER geof:sfIntersects(?g1, ?g2) }");

  // Without the evaluation, the join of the two triples is estimated to have
  // more than one row.
  size_t estimateWithoutEvaluation = 0;
  {
    auto noBudget = setRuntimeParameterForTest<
        &RuntimeParameters::geoPrefilterPlanningMaxRows_>(0);
    auto qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
    auto estimates =
        spatialJoinSideSizeEstimates(qet, Variable{"?r"}, Variable{"?g1"});
    ASSERT_THAT(estimates, ::testing::SizeIs(1));
    estimateWithoutEvaluation = estimates.at(0);
    EXPECT_GT(estimateWithoutEvaluation, 1u);
  }

  // With the evaluation, the estimate is the one actual row, and the result
  // is the same as without the prefilters.
  auto budget = setRuntimeParameterForTest<
      &RuntimeParameters::geoPrefilterPlanningMaxRows_>(
      estimateWithoutEvaluation);
  auto qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
  EXPECT_THAT(
      spatialJoinSideSizeEstimates(qet, Variable{"?r"}, Variable{"?g1"}),
      ::testing::ElementsAre(1u));
  expectRowFilterRectangleNear(qet, Variable{"?g1"}, fixedRectangle, query);
  auto numRows = qet->getRootOperation()->getResult()->idTableView().size();
  EXPECT_GE(numRows, 2u);
  EXPECT_EQ(numResultRowsWithoutPrefilter(qec, query), numRows);
}

// Test that a subquery with a small `LIMIT` counts as a small part of the
// query (its size estimate is the limit), and that a part whose size estimate
// exceeds the budget is not evaluated.
TEST(GeoRectanglePrefilter, plannerEvaluatesSubqueryWithLimit) {
  auto* qec = geoQec();
  auto budget = setRuntimeParameterForTest<
      &RuntimeParameters::geoPrefilterPlanningMaxRows_>(1);
  constexpr std::string_view prefixes = R"q(
    PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>)q";
  // The one geometry of type `<P>` that the subquery returns, within 1 km.
  std::string query = absl::StrCat(
      prefixes,
      " SELECT * WHERE { ?s <hasGeom> ?g ."
      " { SELECT ?r WHERE { ?x <hasType> <P> . ?x <hasGeom> ?r } LIMIT 1 }"
      " FILTER (geof:metricDistance(?r, ?g) <= 1000) }");
  auto qet = queryPlannerTestHelpers::parseAndPlan(query, qec);
  auto rectangles = rowFilterRectangles(qet, Variable{"?g"});
  ASSERT_EQ(rectangles.size(), 1u);
  // A small rectangle around one of the two points of type `<P>`.
  EXPECT_LT(rectangles.at(0).maxLng_ - rectangles.at(0).minLng_, 0.1);
  EXPECT_LT(rectangles.at(0).maxLat_ - rectangles.at(0).minLat_, 0.1);
  EXPECT_EQ(numResultRowsWithoutPrefilter(qec, query),
            numResultRows(qec, query));

  // All geometries of type `<T>` are far more than one row: not evaluated,
  // so `?g` is not prefiltered.
  std::string queryLarge = absl::StrCat(
      prefixes,
      " SELECT * WHERE { ?s <hasGeom> ?g . ?x <hasType> <T> . ?x <hasGeom> ?r ."
      " FILTER geof:sfIntersects(?r, ?g) }");
  auto qetLarge = queryPlannerTestHelpers::parseAndPlan(queryLarge, qec);
  EXPECT_THAT(rowFilterRectangles(qetLarge, Variable{"?g"}),
              ::testing::IsEmpty());
}

}  // namespace

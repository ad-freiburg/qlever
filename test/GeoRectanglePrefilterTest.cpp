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
#include "QueryPlannerTestHelpers.h"
#include "QueryRewriteUtilTestHelpers.h"
#include "absl/cleanup/cleanup.h"
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
#include "rdfTypes/GeoCellGrid.h"

namespace {

using ad_utility::GeoCellGrid;
using ad_utility::GeoRectangle;
using ad_utility::padGeoRectangle;
using prefilterExpressions::GeoRectangleExpression;

constexpr std::string_view wktDatatype =
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

// Turtle input with non-point WKT literals in different cells of a level-2
// grid (4 x 4 cells of 90 x 45 degrees): cell 10 (lng 0..90, lat 0..45),
// cell 0 (bottom left) and the sentinel cell (a linestring crossing a cell
// border). The two `POINT` literals become `GeoPoint` IDs.
std::string geoTurtleInput(int numFar = 16) {
  auto wktTriple = [](std::string_view subject, std::string_view content) {
    return absl::StrCat(subject, " <hasGeom> \"", content, "\"", wktDatatype,
                        " . \n");
  };
  return absl::StrCat(
      wktTriple("<cell10a>", "LINESTRING(10 10, 11 10)"),
      wktTriple("<cell10b>", "LINESTRING(12 10, 13 10)"),
      wktTriple("<cell0a>", "LINESTRING(-100 -50, -101 -50)"),
      wktTriple("<cell0b>", "LINESTRING(-102 -50, -103 -50)"),
      wktTriple("<spanning>", "LINESTRING(-10 10, 20 20)"),
      "<pointNear> <hasGeom> \"POINT(10.5 10.01)\"", wktDatatype, " . \n",
      "<cell10a> <hasType> <T> . \n"
      "<cell10b> <hasType> <T> . \n"
      "<spanning> <hasType> <T> . \n"
      "<pointNear> <hasType> <P> . \n",
      "<pointFar> <hasGeom> \"POINT(-100.5 -50.01)\"", wktDatatype, " . \n",
      [numFar] {
        // A batch of far-away geometries, so that (in every scheme) there
        // are whole blocks that a covering query near (10, 10) can prune.
        std::string result;
        for (int i = 0; i < numFar; ++i) {
          result += absl::StrCat(
              "<far", i, "> <hasGeom> \"LINESTRING(", -170 + i % 320, " -",
              60 - i / 320, ".0, ", -169.5 + i % 320, " -", 60 - i / 320,
              ".0)\"", wktDatatype, " . \n", "<far", i, "> <hasType> <T> . \n");
        }
        return result;
      }());
}

// A `QueryExecutionContext` for an index over `geoTurtleInput` with the
// geo-split vocabulary and a level-2 geo cell grid.
QueryExecutionContext* geoQec(
    uint8_t gridLevel = 2,
    ad_utility::GeoCellGridScheme scheme = ad_utility::GeoCellGridScheme::Flat,
    int numFar = 16) {
  ad_utility::testing::TestIndexConfig config{geoTurtleInput(numFar)};
  config.vocabularyType = ad_utility::VocabularyType{
      ad_utility::VocabularyType::Enum::OnDiskCompressedGeoSplit};
  config.geoCellGridLevel = gridLevel;
  config.geoCellGridScheme = scheme;
  config.parserBufferSize = 1000_B;
  return ad_utility::testing::getQec(std::move(config));
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

// Test the block-level evaluation of the `GeoRectangleExpression` against
// synthetic block metadata over an index with a level-2 geo cell grid.
class GeoRectangleExpressionTest : public ::testing::Test {
 protected:
  const IndexImpl& indexImpl_ = geoQec()->getIndex().getImpl();
  GeoCellGrid grid_{2};

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

  // The Id of a WKT literal with the given cell and position.
  ValueId geoWktId(uint64_t cell, uint64_t position) {
    return Id::makeFromVocabIndex(VocabIndex::make(
        GeoCellGrid::geoVocabMarkerBit | grid_.annotateIndex(cell, position)));
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
  ASSERT_TRUE(indexImpl_.getVocab().getGeoCellGrid().has_value());
  ASSERT_EQ(indexImpl_.getVocab().getGeoCellGrid().value(), grid_);

  // Query rectangle inside cell 3 (bottom right corner of the earth).
  GeoRectangle rectangle{170.0, -81.0, 172.0, -79.0};
  GeoRectangleExpression expr{rectangle};

  std::vector<CompressedBlockMetadata> blocks;
  // Block 0: ints -> pruned (`Datatype::Int` sorts below the index types).
  blocks.push_back(
      makeBlock(ad_utility::testing::IntId(1), ad_utility::testing::IntId(5)));
  // Block 1: plain (non-WKT) vocab entries -> pruned.
  blocks.push_back(makeBlock(Id::makeFromVocabIndex(VocabIndex::make(5)),
                             Id::makeFromVocabIndex(VocabIndex::make(20))));
  // Block 2: WKT literals of cell 0 -> pruned.
  blocks.push_back(makeBlock(geoWktId(0, 0), geoWktId(0, 5)));
  // Block 3: WKT literals of cell 3 -> kept.
  blocks.push_back(makeBlock(geoWktId(3, 6), geoWktId(3, 9)));
  // Block 4: WKT literals of cell 12 -> pruned.
  blocks.push_back(makeBlock(geoWktId(12, 10), geoWktId(12, 12)));
  // Block 5: sentinel cell -> kept.
  blocks.push_back(makeBlock(geoWktId(grid_.sentinelCell(), 13),
                             geoWktId(grid_.sentinelCell(), 15)));
  // Block 6: GeoPoints within the latitude band -> kept.
  blocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{-80.5, 0.0}),
                             Id::makeFromGeoPoint(GeoPoint{-79.5, 10.0})));
  // Block 7: GeoPoints far north -> pruned.
  blocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{70.0, 0.0}),
                             Id::makeFromGeoPoint(GeoPoint{80.0, 10.0})));

  auto kept =
      toPointers(expr.evaluate(indexImpl_, {blocks.data(), blocks.size()}, 2));
  EXPECT_THAT(kept, ::testing::ElementsAre(&blocks[3], &blocks[5], &blocks[6]));

  // A block that spans the whole cell-3 interval (from cell 2 to cell 4)
  // must also be kept, although neither of its boundary IDs is inside.
  std::vector<CompressedBlockMetadata> spanningBlocks;
  spanningBlocks.push_back(makeBlock(geoWktId(0, 0), geoWktId(1, 3)));
  spanningBlocks.push_back(makeBlock(geoWktId(2, 4), geoWktId(4, 8)));
  spanningBlocks.push_back(makeBlock(geoWktId(5, 9), geoWktId(6, 11)));
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

// Without a geo cell grid the whole WKT region of the vocabulary is kept,
// GeoPoints are still pruned by latitude.
TEST_F(GeoRectangleExpressionTest, evaluateWithoutGrid) {
  ad_utility::testing::TestIndexConfig config{geoTurtleInput()};
  config.vocabularyType = ad_utility::VocabularyType{
      ad_utility::VocabularyType::Enum::OnDiskCompressedGeoSplit};
  config.parserBufferSize = 1000_B;
  const IndexImpl& noGridIndex =
      ad_utility::testing::getQec(std::move(config))->getIndex().getImpl();
  ASSERT_FALSE(noGridIndex.getVocab().getGeoCellGrid().has_value());

  GeoRectangleExpression expr{GeoRectangle{170.0, -81.0, 172.0, -79.0}};
  std::vector<CompressedBlockMetadata> blocks;
  blocks.push_back(makeBlock(Id::makeFromVocabIndex(VocabIndex::make(5)),
                             Id::makeFromVocabIndex(VocabIndex::make(20))));
  blocks.push_back(makeBlock(geoWktId(0, 0), geoWktId(0, 5)));
  blocks.push_back(makeBlock(geoWktId(12, 10), geoWktId(12, 12)));
  blocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{70.0, 0.0}),
                             Id::makeFromGeoPoint(GeoPoint{80.0, 10.0})));
  auto kept =
      toPointers(expr.evaluate(noGridIndex, {blocks.data(), blocks.size()}, 2));
  // All WKT blocks are kept, the non-WKT vocab block and the far-away
  // GeoPoints are pruned.
  EXPECT_THAT(kept, ::testing::ElementsAre(&blocks[1], &blocks[2]));
}

// The producer: a `<=` comparison over a geo distance function with a fixed
// geometry yields a `GeoRectangleExpression` for the variable.
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

// The `SpatialJoin` pushes the prefilter into an index scan that is sorted by
// the geometry variable, and only into such a scan. Parameterized over the
// four grid schemes: the pruning machinery is scheme-agnostic and the
// results must be identical for all of them.
class GeoRectanglePrefilterSchemeTest
    : public ::testing::TestWithParam<ad_utility::GeoCellGridScheme> {};

TEST_P(GeoRectanglePrefilterSchemeTest, spatialJoinPushesBlockPrefilter) {
  auto* qec = geoQec(2, GetParam());
  auto point = TripleComponent{Id::makeFromGeoPoint(GeoPoint{10.0, 10.5})};
  Variable pointVar{"?point"};
  Variable wktVar{"?wkt"};
  auto valuesTree = makeValuesForSingleValue(qec, pointVar, point);

  SparqlTripleSimple triple{
      TripleComponent{Variable{"?s"}},
      TripleComponent{TripleComponent::Iri::fromIriref("<hasGeom>")},
      TripleComponent{wktVar}};
  auto makeScan = [&](Permutation::Enum permutation) {
    return ad_utility::makeExecutionTree<IndexScan>(qec, permutation, triple);
  };

  SpatialJoinConfiguration config{
      LibSpatialJoinConfig{SpatialJoinType::WITHIN_DIST, 200'000.0,
                           std::nullopt},
      pointVar,
      wktVar,
      std::nullopt,
      PayloadVariables::all(),
      SpatialJoinAlgorithm::LIBSPATIALJOIN,
      std::nullopt};

  auto makeJoin = [&](Permutation::Enum permutation) {
    auto sj = std::make_shared<SpatialJoin>(qec, config, std::nullopt,
                                            std::nullopt, true);
    sj = sj->addChild(valuesTree, pointVar);
    sj = sj->addChild(makeScan(permutation), wktVar);
    return sj;
  };

  // POS scan: sorted by ?wkt -> the prefilter is applied, which makes the
  // scan uncacheable under its ordinary cache key.
  auto sjPos = makeJoin(Permutation::POS);
  const auto* scanPos = sjPos->getChildren().at(1)->getRootOperation().get();
  EXPECT_FALSE(scanPos->canResultBeCached());

  // PSO scan: sorted by ?s -> no prefilter.
  auto sjPso = makeJoin(Permutation::PSO);
  const auto* scanPso = sjPso->getChildren().at(1)->getRootOperation().get();
  EXPECT_TRUE(scanPso->canResultBeCached());

  // Both plans agree on the result: the two linestrings of cell 3 and the
  // nearby point geometry are within 200 km of the query point.
  auto result = sjPos->computeResultOnlyForTesting();
  EXPECT_EQ(result.idTableView().numRows(), 3u);
  auto resultPso = sjPso->computeResultOnlyForTesting();
  EXPECT_EQ(resultPso.idTableView().numRows(), 3u);
}

// The runtime block prefilter: with a non-constant (here: two-row) small
// side, plan-time prefiltering is impossible, but `prepareJoin` prunes the
// scan's blocks using the bounding rectangle of the materialized small side.
TEST_P(GeoRectanglePrefilterSchemeTest, runtimeBlockPrefilter) {
  auto* qec = geoQec(2, GetParam());
  // Disable the plan-time materialization, so that this test exercises the
  // runtime block prefilter in isolation.
  setRuntimeParameter<&RuntimeParameters::spatialJoinPlanTimePrefilterMaxRows_>(
      0);
  absl::Cleanup restoreParameter{[]() {
    setRuntimeParameter<
        &RuntimeParameters::spatialJoinPlanTimePrefilterMaxRows_>(100'000);
  }};
  Variable pointVar{"?point"};
  Variable wktVar{"?wkt"};
  parsedQuery::SparqlValues values;
  values._variables = {pointVar};
  values._values.push_back(
      {TripleComponent{Id::makeFromGeoPoint(GeoPoint{10.0, 10.5})}});
  values._values.push_back(
      {TripleComponent{Id::makeFromGeoPoint(GeoPoint{10.05, 10.6})}});
  auto valuesTree = ad_utility::makeExecutionTree<Values>(qec, values);

  SparqlTripleSimple triple{
      TripleComponent{Variable{"?s"}},
      TripleComponent{TripleComponent::Iri::fromIriref("<hasGeom>")},
      TripleComponent{wktVar}};
  auto scanTree =
      ad_utility::makeExecutionTree<IndexScan>(qec, Permutation::POS, triple);

  SpatialJoinConfiguration config{
      LibSpatialJoinConfig{SpatialJoinType::WITHIN_DIST, 200'000.0,
                           std::nullopt},
      pointVar,
      wktVar,
      std::nullopt,
      PayloadVariables::all(),
      SpatialJoinAlgorithm::LIBSPATIALJOIN,
      std::nullopt};
  auto sj = std::make_shared<SpatialJoin>(qec, config, std::nullopt,
                                          std::nullopt, true);
  sj = sj->addChild(valuesTree, pointVar);
  sj = sj->addChild(scanTree, wktVar);

  // The plan-time prefilter must NOT have fired (two rows, no constant).
  EXPECT_TRUE(sj->getChildren().at(1)->getRootOperation()->canResultBeCached());

  // Both query points are within 200 km of the two cell-10 linestrings and
  // the nearby point geometry: 2 x 3 = 6 result rows.
  auto result = sj->computeResultOnlyForTesting();
  EXPECT_EQ(result.idTableView().numRows(), 6u);

  // The runtime block prefilter fired: fewer rows were read than the scan
  // holds in total.
  const auto& details = sj->runtimeInfo().details_;
  ASSERT_TRUE(details.contains("num-geoms-before-block-prefilter"));
  EXPECT_GT(details.at("num-geoms-before-block-prefilter").get<int64_t>(),
            details.at("num-geoms-after-block-prefilter").get<int64_t>());
}

// The runtime block prefilter reaches a scan whose blocks it can prune even
// when the scan is wrapped in a `Sort` and a `Join` (as happens for a side
// with a type restriction): the prefilter is forwarded through both.
TEST_P(GeoRectanglePrefilterSchemeTest,
       runtimeBlockPrefilterThroughSortAndJoin) {
  auto* qec = geoQec(2, GetParam());
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
  SparqlTripleSimple tripleA{
      TripleComponent{Variable{"?s"}},
      TripleComponent{TripleComponent::Iri::fromIriref("<hasGeom>")},
      TripleComponent{wktVar}};
  auto scanA =
      ad_utility::makeExecutionTree<IndexScan>(qec, Permutation::POS, tripleA);
  SparqlTripleSimple tripleB{
      TripleComponent{Variable{"?s"}},
      TripleComponent{TripleComponent::Iri::fromIriref("<hasGeom>")},
      TripleComponent{Variable{"?wkt2"}}};
  auto scanB =
      ad_utility::makeExecutionTree<IndexScan>(qec, Permutation::PSO, tripleB);
  auto joinTree = ad_utility::makeExecutionTree<Join>(
      qec, scanA, scanB, scanA->getVariableColumn(Variable{"?s"}),
      scanB->getVariableColumn(Variable{"?s"}));

  SpatialJoinConfiguration config{
      LibSpatialJoinConfig{SpatialJoinType::WITHIN_DIST, 200'000.0,
                           std::nullopt},
      pointVar,
      wktVar,
      std::nullopt,
      PayloadVariables::all(),
      SpatialJoinAlgorithm::LIBSPATIALJOIN,
      std::nullopt};
  auto sj = std::make_shared<SpatialJoin>(qec, config, std::nullopt,
                                          std::nullopt, true);
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

// A query with type restrictions on both geometry sides: the plan-time
// prefilter materializes the small side during planning, computes its
// bounding rectangle, and prunes candidate plans of the other side. This
// must not change the result (whichever plan the cost comparison picks).
TEST_P(GeoRectanglePrefilterSchemeTest, planTimePrefilterKeepsResultsCorrect) {
  auto* qec = geoQec(2, GetParam(), 300);
  constexpr std::string_view query = R"(
    PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
    SELECT * WHERE {
      ?a <hasType> <P> . ?a <hasGeom> ?g1 .
      ?b <hasType> <T> . ?b <hasGeom> ?g2 .
      FILTER (geof:metricDistance(?g1, ?g2) <= 200000)
    })";

  auto numRows = [&qec](std::string_view q) {
    qec->clearCacheUnpinnedOnly();
    auto qet = queryPlannerTestHelpers::parseAndPlan(std::string{q}, qec);
    auto result = qet.getRootOperation()->getResult();
    return result->idTableView().size();
  };

  auto rowsWithPlanTimePrefilter = numRows(query);
  setRuntimeParameter<&RuntimeParameters::spatialJoinPlanTimePrefilterMaxRows_>(
      0);
  absl::Cleanup restoreParameter{[]() {
    setRuntimeParameter<
        &RuntimeParameters::spatialJoinPlanTimePrefilterMaxRows_>(100'000);
  }};
  auto rowsWithoutPlanTimePrefilter = numRows(query);
  EXPECT_GT(rowsWithPlanTimePrefilter, 0u);
  EXPECT_EQ(rowsWithPlanTimePrefilter, rowsWithoutPlanTimePrefilter);
}

INSTANTIATE_TEST_SUITE_P(
    GeoRectanglePrefilter, GeoRectanglePrefilterSchemeTest,
    ::testing::Values(ad_utility::GeoCellGridScheme::Flat,
                      ad_utility::GeoCellGridScheme::Flat4Shifts,
                      ad_utility::GeoCellGridScheme::Hierarchical,
                      ad_utility::GeoCellGridScheme::Hierarchical3Shifts),
    [](const auto& info) {
      std::string name{ad_utility::toString(info.param)};
      std::replace(name.begin(), name.end(), '-', '_');
      return name;
    });

}  // namespace

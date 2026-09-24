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
#include "rdfTypes/GeoCellGrid.h"
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
std::string geoTurtleInput(int numFar = 64) {
  auto wktTriple = [](std::string_view subject, std::string_view content) {
    return absl::StrCat(subject, " <hasGeom> \"", content, "\"", wktDatatype,
                        " . \n");
  };
  return absl::StrCat(wktTriple("<lineA>", "LINESTRING(10 10, 11 10)"),
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
                          result += absl::StrCat(
                              "<far", i, "> <hasGeom> \"POINT(", -170 + i % 320,
                              " -", 60 - i / 320, ".0)\"", wktDatatype, " . \n",
                              "<far", i, "> <hasType> <T> . \n");
                        }
                        return result;
                      }());
}

// A `QueryExecutionContext` for an index over `geoTurtleInput` with the
// geo-split vocabulary (which has the precomputed geometry info).
QueryExecutionContext* geoQec(int numFar = 64) {
  ad_utility::testing::TestIndexConfig config{geoTurtleInput(numFar)};
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

  // NOTE: The IDs of points are Z-order codes here, so the block of points
  // in the latitude band but far away in longitude (block 2) is pruned too.
  auto kept =
      toPointers(expr.evaluate(indexImpl_, {blocks.data(), blocks.size()}, 2));
  EXPECT_THAT(kept, ::testing::ElementsAre(&blocks[1]));
  // A block of points around the rectangle must be kept, although neither of
  // its boundary IDs is inside.
  std::vector<CompressedBlockMetadata> spanningBlocks;
  spanningBlocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{-82, 169}),
                                     Id::makeFromGeoPoint(GeoPoint{-78, 173})));
  auto keptSpanning = toPointers(expr.evaluate(
      indexImpl_, {spanningBlocks.data(), spanningBlocks.size()}, 2));
  EXPECT_THAT(keptSpanning, ::testing::ElementsAre(&spanningBlocks[0]));

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

// -----------------------------------------------------------------------------
// With a geo cell grid, the block prefilter and the row prefilter also decide
// WKT literals, by the cell bits of their IDs.

using ad_utility::fractionOfCoveringCells;
using ad_utility::GeoCellGrid;

// Turtle input with non-point WKT literals in different cells of a level-2
// grid (4 x 4 cells of 90 x 45 degrees): cell 10 (lng 0..90, lat 0..45),
// cell 0 (bottom left) and the sentinel cell (a linestring crossing a cell
// border), plus two points and a batch of far-away linestrings (so that whole
// blocks can be pruned).
std::string gridTurtleInput(int numFar = 16) {
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
      wktTriple("<pointNear>", "POINT(10.5 10.01)"),
      wktTriple("<pointFar>", "POINT(-100.5 -50.01)"),
      "<cell10a> <hasType> <T> . \n"
      "<cell10b> <hasType> <T> . \n"
      "<spanning> <hasType> <T> . \n"
      "<pointNear> <hasType> <P> . \n",
      [numFar] {
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

// A `QueryExecutionContext` for an index over `gridTurtleInput` with the
// geo-split vocabulary and a level-2 geo cell grid.
QueryExecutionContext* gridQec(int numFar = 16) {
  ad_utility::testing::TestIndexConfig config{gridTurtleInput(numFar)};
  config.vocabularyType = ad_utility::VocabularyType{
      ad_utility::VocabularyType::Enum::OnDiskCompressedGeoSplit};
  config.geoCellGridLevel = 2;
  config.parserBufferSize = 1000_B;
  return ad_utility::testing::getQec(std::move(config));
}

TEST(GeoRectanglePrefilterGrid, fractionOfCoveringCells) {
  // Level 2: 4 x 4 cells of 90 x 45 degrees.
  GeoCellGrid grid{2, ad_utility::GeoCellGridScheme::Flat};
  // A rectangle inside one cell: its share of that cell.
  EXPECT_NEAR(fractionOfCoveringCells(GeoRectangle{10, 10, 55, 20}, grid),
              (45.0 * 10.0) / (90.0 * 45.0), 1e-9);
  // A rectangle exactly covering a cell.
  EXPECT_NEAR(fractionOfCoveringCells(GeoRectangle{0, 0, 90, 45}, grid), 1.0,
              1e-9);
  // Spanning two cells in longitude.
  EXPECT_NEAR(fractionOfCoveringCells(GeoRectangle{80, 0, 100, 45}, grid),
              (20.0 * 45.0) / (180.0 * 45.0), 1e-9);
  // A point still touches one cell, the fraction is 0.
  EXPECT_EQ(fractionOfCoveringCells(GeoRectangle{10, 10, 10, 10}, grid), 0.0);
  // The whole world.
  EXPECT_NEAR(fractionOfCoveringCells(GeoRectangle{-180, -90, 180, 90}, grid),
              1.0, 1e-9);
  // The grid-aware selectivity picks the cell share with a grid and the
  // latitude band share without one.
  EXPECT_NEAR(ad_utility::geoRectangleSelectivity(GeoRectangle{10, 10, 55, 20},
                                                  std::optional{grid}),
              (45.0 * 10.0) / (90.0 * 45.0), 1e-9);
  EXPECT_NEAR(ad_utility::geoRectangleSelectivity(GeoRectangle{10, 10, 55, 20},
                                                  std::nullopt),
              45.0 / 360.0, 1e-9);
}

// The block-level evaluation with a grid: WKT literal blocks are pruned by
// their cells, points by the latitude band, the sentinel cell is kept.
TEST(GeoRectanglePrefilterGrid, evaluate) {
  const IndexImpl& indexImpl = gridQec()->getIndex().getImpl();
  ASSERT_TRUE(indexImpl.getVocab().getGeoCellGrid().has_value());
  GeoCellGrid grid{2};
  ASSERT_EQ(indexImpl.getVocab().getGeoCellGrid().value(), grid);

  size_t blockIdx = 0;
  auto makeBlock = [&blockIdx](ValueId first, ValueId last) {
    AD_CONTRACT_CHECK(first <= last);
    auto vocabId10 = Id::makeFromVocabIndex(VocabIndex::make(10));
    ++blockIdx;
    return CompressedBlockMetadata{
        {{},
         0,
         {vocabId10, vocabId10, first, Id::makeUndefined()},
         {vocabId10, vocabId10, last, Id::makeUndefined()},
         {},
         false},
        blockIdx};
  };
  auto geoWktId = [&grid](uint64_t cell, uint64_t position) {
    return Id::makeFromVocabIndex(
        VocabIndex::make(GeoCellGrid::geoVocabMarkerBit |
                         grid.indexFromCellAndPosition(cell, position)));
  };
  auto toPointers = [](const BlockMetadataRanges& ranges) {
    std::vector<const CompressedBlockMetadata*> result;
    for (const auto& range : ranges) {
      for (const auto& block : range) {
        result.push_back(&block);
      }
    }
    return result;
  };

  // Query rectangle inside cell 3 (bottom right corner of the earth).
  GeoRectangleExpression expr{GeoRectangle{170.0, -81.0, 172.0, -79.0}};
  std::vector<CompressedBlockMetadata> blocks;
  // Block 0: ints -> pruned.
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
  blocks.push_back(makeBlock(geoWktId(grid.sentinelCell(), 13),
                             geoWktId(grid.sentinelCell(), 15)));
  // Block 6: GeoPoints inside the rectangle -> kept.
  blocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{-80.5, 170.5}),
                             Id::makeFromGeoPoint(GeoPoint{-79.5, 171.5})));
  // Block 7: GeoPoints far north -> pruned.
  blocks.push_back(makeBlock(Id::makeFromGeoPoint(GeoPoint{70.0, 0.0}),
                             Id::makeFromGeoPoint(GeoPoint{80.0, 10.0})));
  auto kept =
      toPointers(expr.evaluate(indexImpl, {blocks.data(), blocks.size()}, 2));
  EXPECT_THAT(kept, ::testing::ElementsAre(&blocks[3], &blocks[5], &blocks[6]));

  // A block that spans the whole cell-3 interval (from cell 2 to cell 4)
  // must also be kept, although neither of its boundary IDs is inside.
  std::vector<CompressedBlockMetadata> spanningBlocks;
  spanningBlocks.push_back(makeBlock(geoWktId(0, 0), geoWktId(1, 3)));
  spanningBlocks.push_back(makeBlock(geoWktId(2, 4), geoWktId(4, 8)));
  spanningBlocks.push_back(makeBlock(geoWktId(5, 9), geoWktId(6, 11)));
  auto keptSpanning = toPointers(expr.evaluate(
      indexImpl, {spanningBlocks.data(), spanningBlocks.size()}, 2));
  EXPECT_THAT(keptSpanning, ::testing::ElementsAre(&spanningBlocks[1]));
}

// The row prefilter with a grid decides WKT literals by their cell bits.
TEST(GeoRectanglePrefilterGrid, rowFilterDropsLiteralsByCell) {
  auto* qec = gridQec();
  Variable wktVar{"?wkt"};
  auto scan = ad_utility::makeExecutionTree<IndexScan>(qec, Permutation::POS,
                                                       hasGeomTriple(wktVar));
  // A rectangle around the cell-10 geometries and the nearby point.
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
  // Kept: the two cell-10 linestrings, the nearby point and `<spanning>`
  // (no cell information). Dropped: the linestrings of cell 0 and the
  // far-away ones (other cells), and the far-away point.
  auto result = rowFilter->getResult();
  EXPECT_EQ(result->idTableView().numRows(), 4u);
}

// The planner prefilters the seeds on a grid index as well, and the spatial
// join's estimate uses the cell share; the results do not change.
TEST(GeoRectanglePrefilterGrid, plannerOnGridIndex) {
  auto* qec = gridQec(300);
  constexpr std::string_view query = R"q(
    PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
    PREFIX geo: <http://www.opengis.net/ont/geosparql#>
    SELECT * WHERE {
      ?s <hasType> <T> . ?s <hasGeom> ?g .
      FILTER (geof:metricDistance("POINT(10.5 10.0)"^^geo:wktLiteral, ?g) <= 200000)
    })q";
  auto qet = queryPlannerTestHelpers::parseAndPlan(std::string{query}, qec);
  const auto* spatialJoin = findOperation<SpatialJoin>(*qet);
  ASSERT_NE(spatialJoin, nullptr);
  ASSERT_NE(findOperation<GeoRectangleRowFilter>(*qet), nullptr);
  // 200 km around a point cover a tiny part of a 90 x 45 degree cell, so the
  // estimate is the minimum of 1.
  EXPECT_EQ(const_cast<SpatialJoin*>(spatialJoin)->getSizeEstimate(), 1u);

  auto numRows = [&qec](std::string_view q) {
    qec->clearCacheUnpinnedOnly();
    auto qet = queryPlannerTestHelpers::parseAndPlan(std::string{q}, qec);
    return qet->getRootOperation()->getResult()->idTableView().size();
  };
  auto rowsWithPrefilter = numRows(query);
  setRuntimeParameter<&RuntimeParameters::enablePrefilterOnIndexScans_>(false);
  absl::Cleanup restoreParameter{[]() {
    setRuntimeParameter<&RuntimeParameters::enablePrefilterOnIndexScans_>(true);
  }};
  // The two cell-10 linestrings are within 200 km (`<spanning>` is not).
  EXPECT_EQ(rowsWithPrefilter, 2u);
  EXPECT_EQ(numRows(query), rowsWithPrefilter);
}

}  // namespace

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <spatialjoin/WKTParse.h>

#include "rdfTypes/ParsedGeometry.h"

namespace {

using ad_utility::ParsedGeometry;

// A WKT literal as stored in the vocabulary.
std::string lit(std::string_view wkt) {
  return absl::StrCat("\"", wkt,
                      "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
}

// A sweeper as the spatial join uses it, for a `WITHIN_DIST` join with the
// given distance in meters (for no such join if negative).
sj::SweeperCfg sweeperConfig(double withinDist = -1) {
  auto cfg = ParsedGeometry::sweeperConfig();
  cfg.withinDist = withinDist;
  return cfg;
}

// The batch that `libspatialjoin` produces when it parses `literal` at query
// time as the geometry with the given `row` on the given `side`. This is the
// reference for `ParsedGeometry::addToBatch`.
sj::WriteBatch parseAtQueryTime(std::string_view literal, sj::Sweeper& sweeper,
                                size_t row, bool side) {
  struct Parser : public sj::WKTParserBase<sj::ParseJob> {
    explicit Parser(sj::Sweeper* sweeper)
        : sj::WKTParserBase<sj::ParseJob>(sweeper, 1) {}
    void processQueue(size_t) override {}
    using sj::WKTParserBase<sj::ParseJob>::parseLine;
  };
  std::string wkt{literal};
  sj::WriteBatch batch;
  Parser{&sweeper}.parseLine(wkt.data(), wkt.size(), row, 0, batch, side,
                             false);
  return batch;
}

// Compare two serialized geometries by their size and the position of the
// id. They are not compared byte by byte, because `libspatialjoin` writes the
// padding bytes of its structs (points of the rings, box ids) uninitialized.
// That the stored geometry works in a spatial join is tested end to end in
// `SpatialJoinAlgorithmsTest` (`LibspatialJoinWithParsedGeometries`).
void expectEqualRaw(const std::string& a, const std::string& b,
                    const std::string& id) {
  EXPECT_EQ(a.size(), b.size());
  EXPECT_EQ(a.find(id), b.find(id));
  EXPECT_NE(a.find(id), std::string::npos);
}

// Compare two sweep events and two batches field by field.
void expectEqualBoxVals(const sj::BoxVal& a, const sj::BoxVal& b) {
  EXPECT_EQ(a.id, b.id);
  EXPECT_EQ(a.loY, b.loY);
  EXPECT_EQ(a.upY, b.upY);
  EXPECT_EQ(a.val, b.val);
  EXPECT_EQ(a.out, b.out);
  EXPECT_EQ(a.type, b.type);
  EXPECT_EQ(a.areaOrLen, b.areaOrLen);
  EXPECT_EQ(a.point, b.point);
  EXPECT_EQ(a.numAnchors, b.numAnchors);
  EXPECT_EQ(a.b45.getLowerLeft(), b.b45.getLowerLeft());
  EXPECT_EQ(a.b45.getUpperRight(), b.b45.getUpperRight());
  EXPECT_EQ(a.side, b.side);
  EXPECT_EQ(a.large, b.large);
  EXPECT_EQ(a.size, b.size);
}
void expectEqualBatches(const sj::WriteBatch& a, const sj::WriteBatch& b) {
  auto expectEqualCands = [](const std::vector<sj::WriteCand>& a,
                             const std::vector<sj::WriteCand>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
      expectEqualRaw(a[i].raw, b[i].raw, b[i].gid);
      EXPECT_EQ(a[i].gid, b[i].gid);
      EXPECT_EQ(a[i].subid, b[i].subid);
      expectEqualBoxVals(a[i].boxvalIn, b[i].boxvalIn);
      expectEqualBoxVals(a[i].boxvalOut, b[i].boxvalOut);
    }
  };
  expectEqualCands(a.points, b.points);
  expectEqualCands(a.foldedPoints, b.foldedPoints);
  expectEqualCands(a.simpleLines, b.simpleLines);
  expectEqualCands(a.foldedSimpleLines, b.foldedSimpleLines);
  expectEqualCands(a.lines, b.lines);
  expectEqualCands(a.simpleAreas, b.simpleAreas);
  expectEqualCands(a.foldedBoxAreas, b.foldedBoxAreas);
  expectEqualCands(a.areas, b.areas);
  expectEqualCands(a.refs, b.refs);
}

// The literals of the tests, one for each of the five geometry types of
// `libspatialjoin` (a point, a tiny line with two points, a line with more
// points, a tiny polygon with fewer than ten points, a polygon with more), and
// a multipolygon. The tiny ones lie in a single box of the box ids, which
// makes them "simple".
const std::vector<std::pair<std::string, sj::GeomType>> testLiterals{
    {lit("POINT(7.8 48.0)"), sj::POINT},
    {lit("LINESTRING(7.804 48.0008, 7.80402 48.00082)"), sj::SIMPLE_LINE},
    {lit("LINESTRING(7.8 48.0, 7.9 48.1, 7.8 48.2)"), sj::LINE},
    {lit("POLYGON((7.804 48.0008, 7.80402 48.0008, 7.80402 48.00082, "
         "7.804 48.00083, 7.804 48.0008))"),
     sj::SIMPLE_POLYGON},
    {lit("POLYGON((7.80 48.0, 7.81 48.0, 7.82 48.0, 7.83 48.0, 7.84 48.0, "
         "7.85 48.0, 7.86 48.0, 7.87 48.0, 7.88 48.0, 7.9 48.1, 7.8 48.2, "
         "7.80 48.0))"),
     sj::POLYGON},
    {lit("MULTIPOLYGON(((7.8 48.0, 7.9 48.0, 7.9 48.1, 7.8 48.2, 7.8 48.0)), "
         "((8.8 49.0, 8.9 49.0, 8.9 49.1, 8.8 49.2, 8.8 49.0)))"),
     sj::POLYGON}};

// Test that a parsed geometry added to a batch is exactly what `libspatialjoin`
// produces when it parses the literal at query time, without and with the
// padding of the bounding boxes for a `WITHIN_DIST` join.
TEST(ParsedGeometryTest, addToBatch) {
  // The sweeper of the index build, and the sweepers of a query without and
  // with a `WITHIN_DIST` join.
  sj::Sweeper indexSweeper{sweeperConfig(), ".", "ParsedGeometryTest"};
  sj::Sweeper querySweeper{sweeperConfig(), ".", "ParsedGeometryTest"};
  sj::Sweeper withinDistSweeper{sweeperConfig(500), ".", "ParsedGeometryTest"};

  // A row with at least eight digits, so that `libspatialjoin` does not fold
  // the small geometries of the reference into their events (a parsed
  // geometry is never folded, because it is stored with a long placeholder
  // id, so the batches would differ only in that).
  for (const auto& [literal, type] : testLiterals) {
    auto parsed = ParsedGeometry::fromWktLiteral(literal, indexSweeper);
    ASSERT_TRUE(parsed.has_value()) << literal;
    EXPECT_EQ(parsed.value().parts()[0].boxvalIn_.type, type) << literal;
    for (auto* sweeper : {&querySweeper, &withinDistSweeper}) {
      for (bool side : {false, true}) {
        sj::WriteBatch batch;
        parsed.value().addToBatch(*sweeper, 12'345'678, side, batch);
        expectEqualBatches(
            batch, parseAtQueryTime(literal, *sweeper, 12'345'678, side));
      }
    }
  }
}

// Test that the parts of a parsed geometry are those of the literal: one per
// geometry, with the sub ids of a multi geometry.
TEST(ParsedGeometryTest, parts) {
  sj::Sweeper sweeper{sweeperConfig(), ".", "ParsedGeometryTest"};

  // A single geometry has one part with sub id 0.
  auto polygon = ParsedGeometry::fromWktLiteral(testLiterals[4].first, sweeper);
  ASSERT_EQ(polygon.value().parts().size(), 1);
  EXPECT_EQ(polygon.value().parts()[0].subid_, 0);

  // A multipolygon with two polygons has two parts with sub ids 1 and 2.
  auto multi = ParsedGeometry::fromWktLiteral(testLiterals[5].first, sweeper);
  ASSERT_EQ(multi.value().parts().size(), 2);
  EXPECT_EQ(multi.value().parts()[0].subid_, 1);
  EXPECT_EQ(multi.value().parts()[1].subid_, 2);

  // A literal that is not a valid WKT geometry has no parsed geometry.
  EXPECT_FALSE(ParsedGeometry::fromWktLiteral("\"foo\"", sweeper).has_value());
  EXPECT_FALSE(
      ParsedGeometry::fromWktLiteral(lit("POLYGON((7.8 48.0))"), sweeper)
          .has_value());
}

// Test that a parsed geometry survives the round trip through its byte
// representation.
TEST(ParsedGeometryTest, toBytesAndFromBytes) {
  sj::Sweeper sweeper{sweeperConfig(), ".", "ParsedGeometryTest"};
  auto parsed = ParsedGeometry::fromWktLiteral(testLiterals[5].first, sweeper);
  auto restored = ParsedGeometry::fromBytes(parsed.value().toBytes());

  // The restored geometry yields the same batch as the original.
  sj::WriteBatch batch, restoredBatch;
  parsed.value().addToBatch(sweeper, 7, true, batch);
  restored.addToBatch(sweeper, 7, true, restoredBatch);
  ASSERT_EQ(restoredBatch.areas.size(), 2);
  expectEqualBatches(restoredBatch, batch);
}

}  // namespace

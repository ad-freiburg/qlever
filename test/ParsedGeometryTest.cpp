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

// The configuration of a spatial join, for a `WITHIN_DIST` join with the given
// distance in meters (for no such join if negative).
sj::SpatialJoinCfg config(double withinDist = -1) {
  auto cfg = ParsedGeometry::spatialJoinConfig();
  cfg.withinDist = withinDist;
  return cfg;
}

// A store as the spatial join uses it, with the given configuration.
sj::GeometryStore makeStore(const sj::SpatialJoinCfg& cfg) {
  return sj::GeometryStore{cfg, ".", "ParsedGeometryTest", false};
}

// The batch that `libspatialjoin` produces when it parses `literal` at query
// time as the geometry with the given `row`, and the bounding box that it
// reports for it. This is the reference for `ParsedGeometry::addToBatch`.
std::pair<sj::WriteBatch, ::util::geo::I32Box> parseAtQueryTime(
    std::string_view literal, sj::GeometryStore& store, size_t row) {
  struct Parser : public sj::WKTParserBase<sj::ParseJob> {
    explicit Parser(sj::GeometryStore* store)
        : sj::WKTParserBase<sj::ParseJob>(store, sj::PreparedGeomMetaCb{}, 1) {}
    void processQueue(size_t) override {}
    using sj::WKTParserBase<sj::ParseJob>::parseWKTGeometry;
    ::util::geo::I32Box boundingBox() const { return _bboxes[0]; }
  };
  std::string wkt{literal};
  sj::WriteBatch batch;
  Parser parser{&store};
  parser.parseWKTGeometry(wkt.c_str(), std::to_string(row), 0, batch, true);
  return {std::move(batch), parser.boundingBox()};
}

// Compare two bounding boxes.
void expectEqualBoxes(const ::util::geo::I32Box& a,
                      const ::util::geo::I32Box& b) {
  EXPECT_EQ(a.getLowerLeft(), b.getLowerLeft());
  EXPECT_EQ(a.getUpperRight(), b.getUpperRight());
}

// Compare two serialized geometries by their size and the id at their
// beginning (its length as two bytes, followed by the id). The rest is not
// compared byte by byte, because `libspatialjoin` writes the padding bytes of
// its structs (points of the rings, box ids) uninitialized. That the stored
// geometry works in a spatial join is tested end to end in
// `SpatialJoinAlgorithmsTest` (`LibspatialJoinWithParsedGeometries`).
void expectEqualRaw(const std::string& a, const std::string& b,
                    const std::string& id) {
  EXPECT_EQ(a.size(), b.size());
  size_t prefixSize = sizeof(uint16_t) + id.size();
  ASSERT_GE(a.size(), prefixSize);
  EXPECT_EQ(a.substr(0, prefixSize), b.substr(0, prefixSize));
  EXPECT_EQ(a.substr(sizeof(uint16_t), id.size()), id);
}

// Compare two batches field by field.
void expectEqualBatches(const sj::WriteBatch& a, const sj::WriteBatch& b) {
  auto expectEqualCands = [](const std::vector<sj::WriteCand>& a,
                             const std::vector<sj::WriteCand>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
      expectEqualRaw(a[i].raw, b[i].raw, b[i].gid);
      EXPECT_EQ(a[i].gid, b[i].gid);
      EXPECT_EQ(a[i].subid, b[i].subid);
      expectEqualBoxes(a[i].meta.bbox, b[i].meta.bbox);
      expectEqualBoxes(a[i].meta.box45, b[i].meta.box45);
      EXPECT_EQ(a[i].meta.numAnchors, b[i].meta.numAnchors);
      EXPECT_EQ(a[i].type, b[i].type);
      // A point has no right point.
      if (a[i].type != sj::POINT) {
        EXPECT_EQ(a[i].rightPoint, b[i].rightPoint);
      }
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
  ASSERT_EQ(a.multiGeoms.size(), b.multiGeoms.size());
  for (size_t i = 0; i < a.multiGeoms.size(); ++i) {
    EXPECT_EQ(a.multiGeoms[i].gid, b.multiGeoms[i].gid);
    expectEqualBoxes(a.multiGeoms[i].bbox, b.multiGeoms[i].bbox);
    expectEqualBatches(a.multiGeoms[i].members, b.multiGeoms[i].members);
  }
}

// The literals of the tests, one for each of the geometry types of
// `libspatialjoin` that a parsed geometry can have (a point, a line, a tiny
// polygon with fewer than ten points, a polygon with more), and a
// multipolygon. The tiny polygon lies in a single box of the box ids, which
// makes it "simple".
const std::vector<std::pair<std::string, sj::GeomType>> testLiterals{
    {lit("POINT(7.8 48.0)"), sj::POINT},
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
// produces when it parses the literal at query time, without and with a
// `WITHIN_DIST` join (which pads the bounding box that is returned).
TEST(ParsedGeometryTest, addToBatch) {
  // The store of the index build, and the stores of a query without and with a
  // `WITHIN_DIST` join.
  auto indexStore = makeStore(config());
  auto queryStore = makeStore(config());
  auto withinDistStore = makeStore(config(500));

  // A row with more than six digits, so that `libspatialjoin` does not fold
  // the small geometries of the reference into their events (a parsed
  // geometry is never folded, because it is stored with a long placeholder
  // id, so the batches would differ only in that).
  constexpr size_t row = 12345678;
  for (const auto& [literal, type] : testLiterals) {
    auto parsed = ParsedGeometry::fromWktLiteral(literal, indexStore);
    ASSERT_TRUE(parsed.has_value()) << literal;
    EXPECT_EQ(parsed.value().parts()[0].type_, type) << literal;
    for (auto* store : {&queryStore, &withinDistStore}) {
      sj::WriteBatch batch;
      auto box = parsed.value().addToBatch(*store, row, batch);
      auto [expectedBatch, expectedBox] =
          parseAtQueryTime(literal, *store, row);
      expectEqualBatches(batch, expectedBatch);
      expectEqualBoxes(box, expectedBox);
    }
  }
}

// Test that the parts of a parsed geometry are those of the literal: one per
// geometry, with the sub ids of a multi geometry.
TEST(ParsedGeometryTest, parts) {
  auto store = makeStore(config());

  // A single geometry has one part with sub id 0.
  auto polygon = ParsedGeometry::fromWktLiteral(testLiterals[3].first, store);
  ASSERT_EQ(polygon.value().parts().size(), 1);
  EXPECT_EQ(polygon.value().parts()[0].subid_, 0);
  EXPECT_FALSE(polygon.value().isMulti());

  // A multipolygon with two polygons has two parts with sub ids 1 and 2.
  auto multi = ParsedGeometry::fromWktLiteral(testLiterals[4].first, store);
  ASSERT_EQ(multi.value().parts().size(), 2);
  EXPECT_EQ(multi.value().parts()[0].subid_, 1);
  EXPECT_EQ(multi.value().parts()[1].subid_, 2);
  EXPECT_TRUE(multi.value().isMulti());

  // A line with two points has no parsed geometry.
  EXPECT_FALSE(ParsedGeometry::fromWktLiteral(
                   lit("LINESTRING(7.804 48.0008, 7.80402 48.00082)"), store)
                   .has_value());

  // A literal that is not a valid WKT geometry has no parsed geometry.
  EXPECT_FALSE(ParsedGeometry::fromWktLiteral("\"foo\"", store).has_value());
  EXPECT_FALSE(ParsedGeometry::fromWktLiteral(lit("POLYGON((7.8 48.0))"), store)
                   .has_value());
}

// Test that a parsed geometry survives the round trip through its byte
// representation.
TEST(ParsedGeometryTest, toBytesAndFromBytes) {
  auto store = makeStore(config());
  auto parsed = ParsedGeometry::fromWktLiteral(testLiterals[4].first, store);
  auto restored = ParsedGeometry::fromBytes(parsed.value().toBytes());

  // The restored geometry yields the same batch as the original.
  sj::WriteBatch batch, restoredBatch;
  parsed.value().addToBatch(store, 7, batch);
  restored.addToBatch(store, 7, restoredBatch);
  ASSERT_EQ(restoredBatch.multiGeoms.size(), 1);
  ASSERT_EQ(restoredBatch.multiGeoms[0].members.areas.size(), 2);
  expectEqualBatches(restoredBatch, batch);
}

}  // namespace

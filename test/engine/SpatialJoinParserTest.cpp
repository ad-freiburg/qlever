// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <spatialjoin/GeometryStore.h>
#include <spatialjoin/SweepEventList.h>
#include <spatialjoin/WKTParse.h>
#include <util/geo/Geo.h>

#include "../util/LocalVocabEntryTestHelpers.h"
#include "./SpatialJoinPrefilterTestHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/SpatialJoinConfig.h"
#include "engine/SpatialJoinParser.h"
#include "index/LocalVocab.h"
#include "rdfTypes/GeometryInfo.h"

// _____________________________________________________________________________
namespace {

using namespace SpatialJoinPrefilterTestHelpers;
using namespace ad_utility::detail::parallel_wkt_parser;

// Test that `addValueIdToQueue` parses or prefilters the geometries and that
// the bounding box of the parsed geometries is computed.
TEST(SpatialJoinParser, AddValueIdToQueue) {
  using enum SpatialJoinType::Enum;

  // Prepare test index
  auto kg = buildLibSJTestDataset();
  auto qec = buildQec(kg, true);
  const auto& index = qec->getIndex();

  auto [vMap, nMap] = resolveValIdTable(qec, 6);
  auto idxUni = getValId(nMap, "uni");
  auto idxLondon = getValId(nMap, "london");
  auto idxNewYork = getValId(nMap, "lib");

  // Prepare geometry store and event list
  SweeperResult results;
  SweeperDistResult resultDists;
  auto cfg = makeSweeperCfg(LibSpatialJoinConfig{INTERSECTS}, results,
                            resultDists, -1);
  std::string sweeperPath = qec->getIndex().getOnDiskBase() + ".spatialjoin";
  sj::GeometryStore store{cfg, ".", sweeperPath, false};
  sj::SweepEventList events{cfg, ".", sweeperPath};

  // Left side without prefilter box
  std::optional<util::geo::DBox> prefilterBox = std::nullopt;
  WKTParser parser1{&store, events.addCb(0), 5, true, prefilterBox, index};
  EXPECT_EQ(parser1.getParseCounter(), 0);
  EXPECT_EQ(parser1.getPrefilterCounter(), 0);
  parser1.addValueIdToQueue(idxUni, 0, std::nullopt);
  parser1.addValueIdToQueue(idxUni, 1, std::nullopt);
  parser1.addValueIdToQueue(idxLondon, 2, std::nullopt);
  parser1.done();
  EXPECT_EQ(parser1.getParseCounter(), 3);
  EXPECT_EQ(parser1.getPrefilterCounter(), 0);
  checkPrefilterBox(ad_utility::detail::projectInt32WebMercToDoubleLatLng(
                        parser1.getBoundingBox()),
                    boundingBoxUniAndLondon);

  // Right side with prefilter box
  auto newYorkBox =
      ad_utility::GeometryInfo::getBoundingBox(areaStatueOfLiberty);
  ASSERT_TRUE(newYorkBox.has_value());
  std::optional<util::geo::DBox> newYorkUtilBox =
      ad_utility::detail::boundingBoxToUtilBox(newYorkBox.value());

  WKTParser parser2{&store, events.addCb(0), 5, true, newYorkUtilBox, index};
  EXPECT_EQ(parser2.getParseCounter(), 0);
  EXPECT_EQ(parser2.getPrefilterCounter(), 0);
  parser2.addValueIdToQueue(idxUni, 0, std::nullopt);
  parser2.addValueIdToQueue(idxUni, 1, std::nullopt);
  parser2.addValueIdToQueue(idxLondon, 2, std::nullopt);
  parser2.addValueIdToQueue(idxNewYork, 3, std::nullopt);
  // Also test prefiltering using an explicitly provided bounding box.
  auto uniGeoInfo = ad_utility::GeometryInfo::fromWktLiteral(areaUniFreiburg);
  ASSERT_TRUE(uniGeoInfo.has_value());
  parser2.addValueIdToQueue(idxUni, 4, uniGeoInfo.value().getBoundingBox());
  parser2.done();

  // New York is parsed, 2x Uni and 1x London get filtered out
  EXPECT_EQ(parser2.getParseCounter(), 1);
  EXPECT_EQ(parser2.getPrefilterCounter(), 4);
  auto actualBox = ad_utility::detail::projectInt32WebMercToDoubleLatLng(
      parser2.getBoundingBox());
  checkPrefilterBox(actualBox, newYorkUtilBox.value());

  // Code coverage for queue clearing after 10'000 entries
  WKTParser parser3{&store, events.addCb(0),         5,
                    true,   boundingBoxUniAndLondon, index};
  EXPECT_EQ(parser3.getParseCounter(), 0);
  EXPECT_EQ(parser3.getPrefilterCounter(), 0);
  for (size_t i = 0; i < 25'000; ++i) {
    parser3.addValueIdToQueue(idxNewYork, i, std::nullopt);
  }
  parser3.addValueIdToQueue(idxLondon, 25'000, std::nullopt);
  parser3.addValueIdToQueue(idxUni, 25'001, std::nullopt);
  parser3.done();

  // Uni and London get parsed, 25'000x New York gets filtered out
  EXPECT_EQ(parser3.getParseCounter(), 2);
  EXPECT_EQ(parser3.getPrefilterCounter(), 25'000);
  auto actualBox2 = ad_utility::detail::projectInt32WebMercToDoubleLatLng(
      parser3.getBoundingBox());
  checkPrefilterBox(actualBox2, boundingBoxUniAndLondon);
}

// Test that a parser can be destroyed without `done()` while its worker
// threads still have work, as happens when a query is cancelled during the
// parsing. The threads must stop before the members of the parser that they
// use are destroyed (otherwise they write to freed memory, which only a
// sanitizer build detects reliably).
TEST(SpatialJoinParser, DestroyWithoutDone) {
  using enum SpatialJoinType::Enum;
  auto kg = buildLibSJTestDataset();
  auto qec = buildQec(kg, true);
  const auto& index = qec->getIndex();
  auto [vMap, nMap] = resolveValIdTable(qec, 6);
  auto idxUni = getValId(nMap, "uni");

  SweeperResult results;
  SweeperDistResult resultDists;
  auto cfg = makeSweeperCfg(LibSpatialJoinConfig{INTERSECTS}, results,
                            resultDists, -1);
  std::string sweeperPath = qec->getIndex().getOnDiskBase() + ".spatialjoin";
  sj::GeometryStore store{cfg, ".", sweeperPath, false};
  sj::SweepEventList events{cfg, ".", sweeperPath};

  for (size_t numBatches : {0, 1, 5}) {
    WKTParser parser{&store, events.addCb(0), 4, true, std::nullopt, index};
    for (size_t i = 0; i < numBatches * WKT_PARSER_BATCH_SIZE; ++i) {
      parser.addValueIdToQueue(idxUni, i, std::nullopt);
    }
    // The parser goes out of scope here without `done()`.
  }
}

// Test that invalid WKT literals from the index and from the local vocabulary
// are skipped by the strict parsing and counted by `getNumInvalid`.
TEST(SpatialJoinParser, InvalidWktIsSkipped) {
  auto lit = [](std::string_view wkt) {
    return absl::StrCat("\"", wkt, "\"^^<", GEO_WKT_LITERAL, ">");
  };
  // Literals from the index (`VocabIndex`). Not points, as they would be
  // folded into `GeoPoint` IDs.
  std::vector<std::string> indexWkts{"LINESTRING(1 1, 3 3)",
                                     "LINESTRING(1 1, 3 3, 4 a)"};
  std::string kg;
  for (const auto& wkt : indexWkts) {
    absl::StrAppend(&kg, "<s> <p> ", lit(wkt), " .\n");
  }
  auto qec = ad_utility::testing::getQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  SweeperResult results;
  SweeperDistResult resultDists;
  auto cfg =
      makeSweeperCfg(LibSpatialJoinConfig{SpatialJoinType::Enum::INTERSECTS},
                     results, resultDists, -1);
  std::string sweeperPath = qec->getIndex().getOnDiskBase() + ".spatialjoin";
  sj::GeometryStore store{cfg, ".", sweeperPath, false};
  sj::SweepEventList events{cfg, ".", sweeperPath};
  WKTParser parser{&store, events.addCb(0), 2,
                   false,  std::nullopt,    qec->getIndex()};

  size_t row = 0;
  for (const auto& wkt : indexWkts) {
    auto id = getId(lit(wkt));
    ASSERT_EQ(id.getDatatype(), Datatype::VocabIndex);
    parser.addValueIdToQueue(id, row++, std::nullopt);
  }
  // Literals from the query (`LocalVocabIndex`): The first two are valid
  // (exponent notation and a leading `+` are allowed in the strict mode). The
  // last one has an unsupported CRS.
  LocalVocab localVocab;
  for (auto wkt : {"POINT(1 2)", "POINT(1e1 +2)", "POINT(1 2, 3 4)",
                   "LINESTRING(1 1, 3 3, 4 a)", "not wkt",
                   "<http://example.com/crs> POINT(1 2)"}) {
    auto idx = localVocab.getIndexAndAddIfNotContained(
        ad_utility::testing::localVocabEntryFromStringRepresentation(
            lit(wkt), qec->getLocalVocabContext()));
    parser.addValueIdToQueue(Id::makeFromLocalVocabIndex(idx), row++,
                             std::nullopt);
  }
  parser.done();

  EXPECT_EQ(parser.getParseCounter(), 8);
  EXPECT_EQ(parser.getNumInvalid(), 5);
  EXPECT_EQ(events.numObjects(), 3);
}

// Test that `SpatialJoinParseJob`s are compared by their `ValueId` and row,
// but not by their bounding box.
TEST(SpatialJoinParser, SpatialJoinTaskOperatorEq) {
  // Jobs that differ in the row, the `ValueId` or the bounding box.
  auto point = ValueId::makeFromGeoPoint({1, 1});
  auto undef = ValueId::makeUndefined();
  SpatialJoinParseJob job1{point, 5, "", std::nullopt};
  SpatialJoinParseJob job2{point, 7, "", std::nullopt};
  SpatialJoinParseJob job3{undef, 5, "", std::nullopt};
  SpatialJoinParseJob job4{point, 5, "",
                           ad_utility::BoundingBox{{1, 1}, {1, 1}}};

  // A different row or `ValueId` makes the jobs different.
  EXPECT_EQ(job1, job1);
  EXPECT_NE(job1, job2);
  EXPECT_NE(job1, job3);
  EXPECT_NE(job2, job3);

  // The bounding box is ignored.
  EXPECT_EQ(job1, job4);
}

}  // namespace

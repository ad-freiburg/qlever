// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <absl/strings/str_join.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <spatialjoin/Sweeper.h>
#include <spatialjoin/WKTParse.h>
#include <util/geo/Geo.h>

#include "./SpatialJoinPrefilterTestHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/SpatialJoinConfig.h"
#include "engine/SpatialJoinParser.h"

// _____________________________________________________________________________
namespace {

using namespace SpatialJoinPrefilterTestHelpers;
using namespace ad_utility::detail::parallel_wkt_parser;

// _____________________________________________________________________________
TEST(SpatialJoinParser, AddValueIdToQueue) {
  using enum SpatialJoinType;

  // Prepare test index
  auto kg = buildLibSJTestDataset();
  auto qec = buildQec(kg, true);
  const auto& index = qec->getIndex();

  auto [vMap, nMap] = resolveValIdTable(qec, 6);
  auto idxUni = getValId(nMap, "uni");
  auto idxLondon = getValId(nMap, "london");
  auto idxNewYork = getValId(nMap, "lib");

  // Prepare sweeper
  SweeperResult results;
  SweeperDistResult resultDists;
  auto cfg = makeSweeperCfg({INTERSECTS}, results, resultDists, -1);
  std::string sweeperPath = qec->getIndex().getOnDiskBase() + ".spatialjoin";
  sj::Sweeper sweeper{cfg, ".", sweeperPath};

  // Left side without prefilter box
  std::optional<util::geo::DBox> prefilterBox = std::nullopt;
  WKTParser parser1{&sweeper, 5, true, prefilterBox, index};
  EXPECT_EQ(parser1.getParseCounter(), 0);
  EXPECT_EQ(parser1.getPrefilterCounter(), 0);
  parser1.addValueIdToQueue(idxUni, 0, false);
  parser1.addValueIdToQueue(idxUni, 1, false);
  parser1.addValueIdToQueue(idxLondon, 2, false);
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

  WKTParser parser2{&sweeper, 5, true, newYorkUtilBox, index};
  EXPECT_EQ(parser2.getParseCounter(), 0);
  EXPECT_EQ(parser2.getPrefilterCounter(), 0);
  parser2.addValueIdToQueue(idxUni, 0, true);
  parser2.addValueIdToQueue(idxUni, 1, true);
  parser2.addValueIdToQueue(idxLondon, 2, true);
  parser2.addValueIdToQueue(idxNewYork, 3, true);
  parser2.done();

  // New York is parsed, 2x Uni and 1x London get filtered out
  EXPECT_EQ(parser2.getParseCounter(), 1);
  EXPECT_EQ(parser2.getPrefilterCounter(), 3);
  auto actualBox = ad_utility::detail::projectInt32WebMercToDoubleLatLng(
      parser2.getBoundingBox());
  checkPrefilterBox(actualBox, newYorkUtilBox.value());

  // Code coverage for queue clearing after 10'000 entries
  WKTParser parser3{&sweeper, 5, true, boundingBoxUniAndLondon, index};
  EXPECT_EQ(parser3.getParseCounter(), 0);
  EXPECT_EQ(parser3.getPrefilterCounter(), 0);
  for (size_t i = 0; i < 25'000; ++i) {
    parser3.addValueIdToQueue(idxNewYork, i, true);
  }
  parser3.addValueIdToQueue(idxLondon, 25'000, true);
  parser3.addValueIdToQueue(idxUni, 25'001, true);
  parser3.done();

  // Uni and London get parsed, 25'000x New York gets filtered out
  EXPECT_EQ(parser3.getParseCounter(), 2);
  EXPECT_EQ(parser3.getPrefilterCounter(), 25'000);
  auto actualBox2 = ad_utility::detail::projectInt32WebMercToDoubleLatLng(
      parser3.getBoundingBox());
  checkPrefilterBox(actualBox2, boundingBoxUniAndLondon);

  // The sweeper object is empty and flushing provides no value for this test.
  // However the object needs to be flushed anyway to prevent a memory leak,
  // because memory manually allocated in the sweeper's constructor is freed in
  // this function.
  sweeper.flush();
}

// _____________________________________________________________________________
TEST(SpatialJoinParser, SpatialJoinTaskOperatorEq) {
  // Test equality operator of `SpatialJoinParseJob` helper struct
  auto point = ValueId::makeFromGeoPoint({1, 1});
  auto undef = ValueId::makeUndefined();

  SpatialJoinParseJob job1{point, 5, true, ""};
  SpatialJoinParseJob job1Copy = job1;
  SpatialJoinParseJob job2{point, 7, true, ""};
  SpatialJoinParseJob job3{point, 5, false, ""};
  SpatialJoinParseJob job4{undef, 5, true, ""};

  EXPECT_EQ(job1, job1);
  EXPECT_EQ(job2, job2);
  EXPECT_EQ(job3, job3);
  EXPECT_EQ(job4, job4);
  EXPECT_EQ(job1, job1Copy);

  EXPECT_NE(job1, job2);
  EXPECT_NE(job1, job3);
  EXPECT_NE(job1, job4);
  EXPECT_NE(job2, job3);
  EXPECT_NE(job2, job4);
  EXPECT_NE(job3, job4);
}

}  // namespace

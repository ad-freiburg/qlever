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

#include <random>

#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "rdfTypes/GeoCellGrid.h"

namespace {

using ad_utility::GeoCellGrid;
using ad_utility::GeoCellGridScheme;

// Build a full WKT literal (with quotes and datatype suffix) from the given
// content.
std::string wkt(std::string_view content) {
  return absl::StrCat("\"", content, "\"", GEO_LITERAL_SUFFIX);
}

TEST(GeoCellGrid, basics) {
  GeoCellGrid grid{10};
  EXPECT_EQ(grid.level(), 10);
  EXPECT_EQ(grid.scheme(), GeoCellGridScheme::Flat);
  EXPECT_EQ(grid.numCellsPerDimension(), 1024u);
  EXPECT_EQ(grid.numCellBits(), 21u);
  EXPECT_EQ(grid.sentinelCell(), (uint64_t{1} << 21) - 1);
  EXPECT_EQ(grid.numPositionBits(), ValueId::numDataBits - 22);
  EXPECT_EQ(grid.maxNumWords(), uint64_t{1} << (ValueId::numDataBits - 22));

  // Level 0 is invalid, as are levels where no position bits remain.
  EXPECT_ANY_THROW(GeoCellGrid{0});
  EXPECT_ANY_THROW(GeoCellGrid{40});
}

TEST(GeoCellGrid, schemeStringConversion) {
  for (auto scheme : ad_utility::allGeoCellGridSchemes) {
    auto parsed =
        ad_utility::geoCellGridSchemeFromString(ad_utility::toString(scheme));
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed.value(), scheme);
  }
  EXPECT_EQ(ad_utility::toString(GeoCellGridScheme::Flat), "flat");
  EXPECT_FALSE(ad_utility::geoCellGridSchemeFromString("nope").has_value());
}

TEST(GeoCellGrid, cellIndexFromPoint) {
  GeoCellGrid grid{1};
  // Level 1 divides the earth into 2 x 2 cells; cell = (cellY << 1) | cellX.
  EXPECT_EQ(grid.cellIndexFromPoint(-90.0, -45.0), 0u);
  EXPECT_EQ(grid.cellIndexFromPoint(90.0, -45.0), 1u);
  EXPECT_EQ(grid.cellIndexFromPoint(-90.0, 45.0), 2u);
  EXPECT_EQ(grid.cellIndexFromPoint(90.0, 45.0), 3u);
  // The boundary values are clamped into the valid grid range.
  EXPECT_EQ(grid.cellIndexFromPoint(180.0, 90.0), 3u);
  EXPECT_EQ(grid.cellIndexFromPoint(-180.0, -90.0), 0u);

  // Spot check on a fine grid against the closed formula.
  GeoCellGrid fine{10};
  uint64_t cx = static_cast<uint64_t>((13.405 + 180.0) / 360.0 * 1024.0);
  uint64_t cy = static_cast<uint64_t>((52.52 + 90.0) / 180.0 * 1024.0);
  EXPECT_EQ(fine.cellIndexFromPoint(13.405, 52.52), (cy << 10) | cx);
}

TEST(GeoCellGrid, cellIndexFromBoundingBoxAndWktLiteral) {
  GeoCellGrid grid{1};
  // A geometry entirely inside one cell gets that cell.
  EXPECT_EQ(grid.cellIndexFromWktLiteral(wkt("POINT(90 45)")), 3u);
  EXPECT_EQ(grid.cellIndexFromWktLiteral(wkt("LINESTRING(10 10, 20 20)")), 3u);
  // A geometry whose bounding box crosses a cell border gets the sentinel.
  EXPECT_EQ(grid.cellIndexFromWktLiteral(wkt("LINESTRING(-10 10, 20 20)")),
            grid.sentinelCell());
  // Unparsable literals also get the sentinel.
  EXPECT_EQ(grid.cellIndexFromWktLiteral(wkt("NOTAGEOMETRY(1 2)")),
            grid.sentinelCell());
}

TEST(GeoCellGrid, annotateIndexRoundtrip) {
  GeoCellGrid grid{4};
  uint64_t annotated = grid.annotateIndex(5, 7);
  EXPECT_EQ(grid.cellOfIndex(annotated), 5u);
  EXPECT_EQ(grid.positionOfIndex(annotated), 7u);
  EXPECT_EQ(annotated, (uint64_t{5} << grid.numPositionBits()) | 7);
}

TEST(GeoCellGrid, vocabIndexRangeForCells) {
  GeoCellGrid grid{2};
  auto [first, last] = grid.vocabIndexRangeForCells(3, 5);
  EXPECT_EQ(first, GeoCellGrid::geoVocabMarkerBit +
                       (uint64_t{3} << grid.numPositionBits()));
  EXPECT_EQ(last, GeoCellGrid::geoVocabMarkerBit +
                      (uint64_t{6} << grid.numPositionBits()));
  // For the largest cell number the exclusive upper bound must carry past
  // the marker bit instead of wrapping into it.
  auto [sFirst, sLast] =
      grid.vocabIndexRangeForCells(grid.sentinelCell(), grid.sentinelCell());
  EXPECT_GT(sLast, sFirst);
  EXPECT_EQ(sLast, GeoCellGrid::geoVocabMarkerBit +
                       ((grid.sentinelCell() + 1) << grid.numPositionBits()));
}

TEST(GeoCellGrid, coveringCellRanges) {
  GeoCellGrid grid{2};
  // Level 2 divides the earth into 4 x 4 cells of 90 x 45 degrees. The box
  // below covers grid columns 1-2 and grid rows 1-2, i.e. one range per row,
  // plus the sentinel range at the end.
  auto ranges = grid.coveringCellRanges(-10.0, -10.0, 10.0, 10.0);
  using P = std::pair<uint64_t, uint64_t>;
  EXPECT_THAT(ranges,
              ::testing::ElementsAre(
                  P{(1 << 2) | 1, (1 << 2) | 2}, P{(2 << 2) | 1, (2 << 2) | 2},
                  P{grid.sentinelCell(), grid.sentinelCell()}));

  // A tiny box within a single cell: one cell plus the sentinel.
  auto small = grid.coveringCellRanges(10.0, 10.0, 11.0, 11.0);
  EXPECT_THAT(small, ::testing::ElementsAre(
                         P{(2 << 2) | 2, (2 << 2) | 2},
                         P{grid.sentinelCell(), grid.sentinelCell()}));
}

TEST(GeoCellGrid, isWktLiteral) {
  for (const std::string& word : {wkt("POINT(1 2)"), wkt("NOTAGEOMETRY")}) {
    EXPECT_TRUE(GeoCellGrid::isWktLiteral(word)) << word;
  }
  for (const std::string& word :
       {std::string{"\"foo\""}, std::string{"<http://example.org>"},
        std::string{"\"foo\"@en"}}) {
    EXPECT_FALSE(GeoCellGrid::isWktLiteral(word)) << word;
  }
}

// The `GeoCellGrid` encodes the assumption that the WKT region of the
// vocabulary index space is marked by the top payload bit, which must match
// the marker layout of the `SplitGeoVocabulary`.
TEST(GeoCellGrid, markerBitConsistentWithSplitGeoVocabulary) {
  using SGV = SplitGeoVocabulary<VocabularyInMemory>;
  EXPECT_EQ(SGV::markerShift, ValueId::numDataBits - 1);
  EXPECT_EQ(SGV::addMarker(0, 1), GeoCellGrid::geoVocabMarkerBit);
  EXPECT_TRUE(GeoCellGrid::isGeoVocabIndex(SGV::addMarker(42, 1)));
  EXPECT_FALSE(GeoCellGrid::isGeoVocabIndex(SGV::addMarker(42, 0)));
}

// Every cell number must fit into `numCellBits()` bits.
TEST(GeoCellGrid, cellNumbersFitTheField) {
  std::mt19937_64 gen{42};
  std::uniform_real_distribution<double> lngDist{-180.0, 180.0};
  std::uniform_real_distribution<double> latDist{-90.0, 90.0};
  std::uniform_real_distribution<double> sizeDist{0.0, 5.0};
  for (auto scheme : ad_utility::allGeoCellGridSchemes) {
    GeoCellGrid grid{6, scheme};
    for (int i = 0; i < 2000; ++i) {
      double lng = lngDist(gen);
      double lat = latDist(gen);
      double w = sizeDist(gen);
      double h = sizeDist(gen);
      ad_utility::BoundingBox box{GeoPoint{std::clamp(lat, -90.0, 90.0),
                                           std::clamp(lng, -180.0, 180.0)},
                                  GeoPoint{std::clamp(lat + h, -90.0, 90.0),
                                           std::clamp(lng + w, -180.0, 180.0)}};
      auto cell = grid.cellIndexFromBoundingBox(box);
      EXPECT_LT(cell, uint64_t{1} << grid.numCellBits());
    }
  }
}

// The central conservativeness property, checked for every scheme: if a
// geometry's bounding box intersects a query rectangle, then the geometry's
// cell is contained in the covering cell ranges of the rectangle.
TEST(GeoCellGrid, coverIsConservativeForAllSchemes) {
  std::mt19937_64 gen{4711};
  std::uniform_real_distribution<double> lngDist{-180.0, 179.0};
  std::uniform_real_distribution<double> latDist{-90.0, 89.0};
  std::uniform_real_distribution<double> geomSize{0.0, 8.0};
  std::uniform_real_distribution<double> querySize{0.01, 30.0};

  auto contains = [](const GeoCellGrid::CellRanges& ranges,
                     GeoCellGrid::CellIndex cell) {
    for (const auto& [first, last] : ranges) {
      if (cell >= first && cell <= last) {
        return true;
      }
    }
    return false;
  };

  for (auto scheme : ad_utility::allGeoCellGridSchemes) {
    GeoCellGrid grid{5, scheme};
    size_t numChecked = 0;
    for (int q = 0; q < 60; ++q) {
      double qLng = lngDist(gen);
      double qLat = latDist(gen);
      double qLng2 = std::min(qLng + querySize(gen), 180.0);
      double qLat2 = std::min(qLat + querySize(gen), 90.0);
      auto ranges = grid.coveringCellRanges(qLng, qLat, qLng2, qLat2);
      // The ranges must be ascending and non-overlapping.
      for (size_t i = 1; i < ranges.size(); ++i) {
        EXPECT_GT(ranges[i].first, ranges[i - 1].second + 1);
      }
      for (int g = 0; g < 300; ++g) {
        double lng = lngDist(gen);
        double lat = latDist(gen);
        double lng2 = std::min(lng + geomSize(gen), 180.0);
        double lat2 = std::min(lat + geomSize(gen), 90.0);
        bool intersects =
            lng <= qLng2 && lng2 >= qLng && lat <= qLat2 && lat2 >= qLat;
        if (!intersects) {
          continue;
        }
        ad_utility::BoundingBox box{GeoPoint{lat, lng}, GeoPoint{lat2, lng2}};
        auto cell = grid.cellIndexFromBoundingBox(box);
        EXPECT_TRUE(contains(ranges, cell))
            << ad_utility::toString(scheme) << " geometry [" << lng << ", "
            << lat << ", " << lng2 << ", " << lat2 << "] query [" << qLng
            << ", " << qLat << ", " << qLng2 << ", " << qLat2 << "] cell "
            << cell;
        ++numChecked;
      }
    }
    // Make sure the test actually exercised intersecting pairs.
    EXPECT_GT(numChecked, 50u) << ad_utility::toString(scheme);
  }
}

}  // namespace

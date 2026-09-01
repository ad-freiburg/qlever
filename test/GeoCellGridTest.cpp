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

// Test correctness of the basic methods of `GeoCellGrid`.
TEST(GeoCellGrid, basics) {
  GeoCellGrid grid{10};

  // A grid of level 10 has 1024 x 1024 cells. A cell index for that
  // grid has 21 bits: 10 bits per dimension, plus one extra bit so that the
  // all-ones sentinel is larger than every regular cell index.
  EXPECT_EQ(grid.level(), 10);
  EXPECT_EQ(grid.scheme(), GeoCellGridScheme::Flat);
  static_assert(std::is_same_v<decltype(grid.scheme()), GeoCellGridScheme>);
  EXPECT_EQ(grid.numCellsPerDimension(), 1024u);
  EXPECT_EQ(grid.numCellBits(), 21u);
  EXPECT_EQ(grid.sentinelCell(), (uint64_t{1} << 21) - 1);

  // The bits of a vocabulary index below the marker bit and the cell bits hold
  // the position of a word inside the geo vocabulary.
  EXPECT_EQ(grid.numPositionBits(), ValueId::numDataBits - 22);
  EXPECT_EQ(grid.maxNumWords(), uint64_t{1} << (ValueId::numDataBits - 22));

  // Level 0 is invalid, as are levels where no position bits remain. The
  // largest valid level leaves exactly one bit for the position: a vocabulary
  // index has one marker bit, `2 * level + 1` cell bits, and at least one
  // position bit.
  uint8_t maxLevel = (ValueId::numDataBits - 3) / 2;
  EXPECT_NO_THROW(GeoCellGrid{maxLevel});
  EXPECT_GE(GeoCellGrid{maxLevel}.numPositionBits(), 1u);
  EXPECT_ANY_THROW(GeoCellGrid{0});
  EXPECT_ANY_THROW(GeoCellGrid{static_cast<uint8_t>(maxLevel + 1)});
  EXPECT_ANY_THROW(GeoCellGrid{40});
}

// Test the conversion of the schemes to and from their parameter names.
TEST(GeoCellGrid, schemeStringConversion) {
  // `toString` and `fromString` are inverse to each other.
  for (const auto& [scheme, name] : GeoCellGridScheme::descriptions_) {
    EXPECT_EQ(GeoCellGridScheme{scheme}.toString(), name);
    EXPECT_EQ(GeoCellGridScheme::fromString(name), scheme);
  }

  // The name of `Flat` is "flat". An unknown name throws.
  EXPECT_EQ(GeoCellGridScheme::Flat.toString(), "flat");
  EXPECT_ANY_THROW(GeoCellGridScheme::fromString("nope"));
}

// Test `cellIndexFromPoint` on two simple grids.
TEST(GeoCellGrid, cellIndexFromPoint) {
  // A simple 2 x 2 grid, where the cell indices are easy to compute by hand.
  // Points on the boundary of the coordinate domain are clamped into the grid.
  GeoCellGrid grid{1};

  // Level 1 divides the earth into 2 x 2 cells, with
  // cellIndex = (cellY << 1) | cellX.
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

// Test `cellIndexFromWktLiteral` on a 2 x 2 grid.
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

// Test the round trip between a pair of cell index and position and the
// annotated vocabulary index.
TEST(GeoCellGrid, annotateIndexRoundtrip) {
  GeoCellGrid grid{4};
  uint64_t annotated = grid.annotateIndex(5, 7);

  // `cellOfIndex` and `positionOfIndex` are the two inverses of
  // `annotateIndex`.
  EXPECT_EQ(grid.cellOfIndex(annotated), 5u);
  EXPECT_EQ(grid.positionOfIndex(annotated), 7u);

  // The annotated index has the documented bit layout, with the cell index
  // above the position.
  EXPECT_EQ(annotated, (uint64_t{5} << grid.numPositionBits()) | 7);
}

// Test `vocabIndexRangeForCells` for a range of regular cells and for the
// sentinel cell.
TEST(GeoCellGrid, vocabIndexRangeForCells) {
  GeoCellGrid grid{2};

  // A range of regular cells.
  auto [lower, upper] = grid.vocabIndexRangeForCells(3, 5);
  EXPECT_EQ(lower, GeoCellGrid::geoVocabMarkerBit +
                       (uint64_t{3} << grid.numPositionBits()));
  EXPECT_EQ(upper, GeoCellGrid::geoVocabMarkerBit +
                       (uint64_t{6} << grid.numPositionBits()));

  // The sentinel cell is the largest cell index. Its exclusive upper bound
  // is `(sentinelCell() + 1) << numPositionBits()`, which is the same value
  // as the marker bit. The method adds the two, so the result is larger than
  // every vocabulary index, which is correct for an exclusive bound. With a
  // bitwise or instead of the addition, the result would be just the marker
  // bit, which is smaller than the lower bound.
  auto [sLower, sUpper] =
      grid.vocabIndexRangeForCells(grid.sentinelCell(), grid.sentinelCell());
  EXPECT_GT(sUpper, sLower);
  EXPECT_EQ(sUpper, GeoCellGrid::geoVocabMarkerBit +
                        ((grid.sentinelCell() + 1) << grid.numPositionBits()));
}

// Test `coveringCellRanges` on a 4 x 4 grid, where the covers are easy to
// compute by hand.
TEST(GeoCellGrid, coveringCellRanges) {
  GeoCellGrid grid{2};

  // Level 2 divides the earth into 4 x 4 cells of 90 x 45 degrees. The box
  // below covers grid columns 1-2 and grid rows 1-2. The cell indices of one
  // grid row are contiguous, so the cover is one range per row, followed by
  // the range that contains only the sentinel cell.
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

// Check that `GeoCellGrid::geoVocabMarkerBit` matches the marker layout of
// the `SplitGeoVocabulary`.
TEST(GeoCellGrid, markerBitConsistentWithSplitGeoVocabulary) {
  using SGV = SplitGeoVocabulary<VocabularyInMemory>;

  // The `SplitGeoVocabulary` marks the words of its geo vocabulary with the
  // topmost data bit of the vocabulary index. These checks fail if one of
  // the two sides changes its layout.
  EXPECT_EQ(SGV::markerShift, ValueId::numDataBits - 1);
  EXPECT_EQ(SGV::addMarker(0, 1), GeoCellGrid::geoVocabMarkerBit);
  EXPECT_TRUE(GeoCellGrid::isGeoVocabIndex(SGV::addMarker(42, 1)));
  EXPECT_FALSE(GeoCellGrid::isGeoVocabIndex(SGV::addMarker(42, 0)));
}

// Test that the cell index of a random bounding box always fits into
// `numCellBits()` bits, for every scheme.
TEST(GeoCellGrid, cellIndicesFitTheField) {
  std::mt19937_64 gen{42};
  std::uniform_real_distribution<double> lngDist{-180.0, 180.0};
  std::uniform_real_distribution<double> latDist{-90.0, 90.0};
  std::uniform_real_distribution<double> sizeDist{0.0, 5.0};

  // This is the invariant that makes it safe to store the cell index in the
  // bit field above the position.
  for (const auto& [scheme, name] : GeoCellGridScheme::descriptions_) {
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
      auto cellIndex = grid.cellIndexFromBoundingBox(box);
      EXPECT_LT(cellIndex, uint64_t{1} << grid.numCellBits());
    }
  }
}

// Test the central guarantee of the class, for random rectangles and
// geometries and for every scheme: if the bounding box of a geometry
// intersects a rectangle, then the cell index of that geometry is contained
// in the covering cell ranges of the rectangle.
TEST(GeoCellGrid, coverForAllSchemesContainsEveryIntersectingGeometry) {
  // Random rectangles of up to 30 x 30 degrees and random geometries of up
  // to 8 x 8 degrees, anywhere on earth.
  std::mt19937_64 gen{4711};
  std::uniform_real_distribution<double> lngDist{-180.0, 179.0};
  std::uniform_real_distribution<double> latDist{-90.0, 89.0};
  std::uniform_real_distribution<double> geomSize{0.0, 8.0};
  std::uniform_real_distribution<double> querySize{0.01, 30.0};

  // Helper lambda that computes whether `cellIndex` is contained in one of
  // the `ranges`.
  auto contains = [](const GeoCellGrid::CellRanges& ranges,
                     GeoCellGrid::CellIndex cellIndex) {
    for (const auto& [first, last] : ranges) {
      if (cellIndex >= first && cellIndex <= last) {
        return true;
      }
    }
    return false;
  };

  // Do this for every scheme.
  for (const auto& [scheme, name] : GeoCellGridScheme::descriptions_) {
    GeoCellGrid grid{5, scheme};
    size_t numChecked = 0;

    // For each of 60 random rectangles, compute the covering cell ranges.
    for (int q = 0; q < 60; ++q) {
      double qLng = lngDist(gen);
      double qLat = latDist(gen);
      double qLng2 = std::min(qLng + querySize(gen), 180.0);
      double qLat2 = std::min(qLat + querySize(gen), 90.0);
      auto ranges = grid.coveringCellRanges(qLng, qLat, qLng2, qLat2);

      // The ranges must be ascending. Adjacent ranges must have been merged,
      // so there must be a gap between consecutive ranges.
      for (size_t i = 1; i < ranges.size(); ++i) {
        EXPECT_GT(ranges[i].first, ranges[i - 1].second + 1);
      }

      // Draw 300 random geometries and skip those that do not intersect
      // the rectangle.
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

        // The cell index of an intersecting geometry must be in one of the
        // ranges. On failure, print the geometry and the rectangle.
        ad_utility::BoundingBox box{GeoPoint{lat, lng}, GeoPoint{lat2, lng2}};
        auto cellIndex = grid.cellIndexFromBoundingBox(box);
        EXPECT_TRUE(contains(ranges, cellIndex))
            << name << " geometry [" << lng << ", " << lat << ", " << lng2
            << ", " << lat2 << "] query [" << qLng << ", " << qLat << ", "
            << qLng2 << ", " << qLat2 << "] cell index " << cellIndex;
        ++numChecked;
      }
    }

    // Make sure the random choices actually produced enough intersecting
    // pairs to test something.
    EXPECT_GT(numChecked, 50u) << name;
  }
}

}  // namespace

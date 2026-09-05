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

#include <limits>
#include <random>

#include "index/vocabulary/GeoVocabulary.h"
#include "index/vocabulary/SplitVocabulary.h"
#include "index/vocabulary/StringSortComparator.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "rdfTypes/GeoCellGrid.h"

namespace {

using ad_utility::GeoCellGrid;
using ad_utility::GeoCellGridScheme;
using ad_utility::GeoCellIdPrefilter;

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

  // NaN is rejected, the clamping would not catch it.
  double nan = std::numeric_limits<double>::quiet_NaN();
  EXPECT_ANY_THROW(grid.cellIndexFromPoint(nan, 0.0));
  EXPECT_ANY_THROW(grid.cellIndexFromPoint(0.0, nan));

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

  // A cell index above the sentinel or a position that does not fit are
  // caught by the expensive checks.
  if (ad_utility::areExpensiveChecksEnabled) {
    EXPECT_ANY_THROW(grid.annotateIndex(grid.sentinelCell() + 1, 0));
    EXPECT_ANY_THROW(grid.annotateIndex(0, grid.maxNumWords()));
  }
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

  // The range must be ascending and end at the sentinel cell at the latest.
  EXPECT_ANY_THROW(grid.vocabIndexRangeForCells(5, 3));
  EXPECT_ANY_THROW(grid.vocabIndexRangeForCells(0, grid.sentinelCell() + 1));
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

// _____________________________________________________________________________
TEST(GeoCellGrid, equality) {
  // Two grids are equal iff their level and scheme agree. There is currently
  // only one scheme, so only the level can differ.
  GeoCellGrid grid1{2};
  GeoCellGrid grid2{2};
  GeoCellGrid grid3{3};
  EXPECT_TRUE(grid1 == grid2);
  EXPECT_FALSE(grid1 == grid3);
}

// _____________________________________________________________________________
TEST(GeoCellGrid, coveringCellRangesRequiresOrderedBounds) {
  // The bounds of the rectangle must be ordered in both dimensions.
  GeoCellGrid grid{2};
  EXPECT_THROW(grid.coveringCellRanges(10.0, -10.0, -10.0, 10.0),
               ad_utility::Exception);
  EXPECT_THROW(grid.coveringCellRanges(-10.0, 10.0, 10.0, -10.0),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(GeoCellGrid, coveringCellRangesMergesAdjacentRanges) {
  // A box that spans the full longitude range covers each of its grid rows
  // completely. The cell range of a full row is adjacent to that of the next
  // row, so the per-row ranges merge into a single range.
  GeoCellGrid grid{2};
  auto ranges = grid.coveringCellRanges(-180.0, -10.0, 180.0, 10.0);
  using P = std::pair<uint64_t, uint64_t>;
  EXPECT_THAT(ranges, ::testing::ElementsAre(
                          P{(1 << 2) | 0, (2 << 2) | 3},
                          P{grid.sentinelCell(), grid.sentinelCell()}));
}

// _____________________________________________________________________________
TEST(GeoCellGrid, unknownSchemeIsDefendedAgainst) {
  // The switches over the scheme end in `AD_FAIL()` as a defense against a
  // future scheme that is not yet handled everywhere. Neither the constructor
  // of `GeoCellGridScheme` nor that of `GeoCellGrid` inspects the scheme
  // value, so these paths can be tested with an artificial invalid scheme.
  GeoCellGrid grid{2,
                   GeoCellGridScheme{static_cast<GeoCellGridScheme::Enum>(99)}};
  EXPECT_THROW(grid.cellIndexFromPoint(0.0, 0.0), ad_utility::Exception);
  EXPECT_THROW(grid.cellIndexFromWktLiteral(wkt("POINT(90 45)")),
               ad_utility::Exception);

  EXPECT_THROW(grid.coveringCellRanges(-10.0, -10.0, 10.0, 10.0),
               ad_utility::Exception);
}

// The following tests exercise the three schemes beyond `Flat`, the geo-cell
// order of the `TripleComponentComparator`, the cell-annotated indices of the
// `GeoVocabulary`, and the `GeoCellIdPrefilter`.

// _____________________________________________________________________________
TEST(GeoCellGrid, cellBitsPerScheme) {
  EXPECT_EQ(GeoCellGrid(10, GeoCellGridScheme::Flat).numCellBits(), 21u);
  EXPECT_EQ(GeoCellGrid(10, GeoCellGridScheme::Flat4Shifts).numCellBits(), 23u);
  EXPECT_EQ(GeoCellGrid(10, GeoCellGridScheme::Hierarchical).numCellBits(),
            21u);
  EXPECT_EQ(
      GeoCellGrid(10, GeoCellGridScheme::Hierarchical3Shifts).numCellBits(),
      23u);
  // The hierarchical schemes have no separate sentinel; the root of the
  // first copy takes its role.
  EXPECT_EQ(GeoCellGrid(10, GeoCellGridScheme::Hierarchical).sentinelCell(),
            uint64_t{1} << 20);
  EXPECT_EQ(GeoCellGrid(10, GeoCellGridScheme::Flat).sentinelCell(),
            (uint64_t{1} << 21) - 1);
}

// _____________________________________________________________________________
TEST(GeoCellGrid, hierarchicalEncoding) {
  GeoCellGrid grid{3, GeoCellGridScheme::Hierarchical};
  // The root is the middle of the ID space of 2 * 3 + 1 = 7 bits.
  EXPECT_EQ(grid.sentinelCell(), 64u);
  // A point geometry gets a leaf cell (odd cell number).
  ad_utility::BoundingBox point{GeoPoint{10.0, 10.0}, GeoPoint{10.0, 10.0}};
  auto leaf = grid.cellIndexFromBoundingBox(point);
  EXPECT_EQ(leaf & 1, 1u);
  // A geometry spanning the whole world gets the root.
  ad_utility::BoundingBox world{GeoPoint{-90.0, -180.0}, GeoPoint{90.0, 180.0}};
  EXPECT_EQ(grid.cellIndexFromBoundingBox(world), grid.sentinelCell());
  // The cover of a tiny rectangle consists of one leaf (or few leaves) plus
  // all their ancestors, root included.
  auto ranges = grid.coveringCellRanges(10.0, 10.0, 10.1, 10.1);
  bool containsRoot = false;
  bool containsLeaf = false;
  for (auto [first, last] : ranges) {
    containsRoot |=
        (grid.sentinelCell() >= first && grid.sentinelCell() <= last);
    containsLeaf |= (leaf >= first && leaf <= last);
  }
  EXPECT_TRUE(containsRoot);
  EXPECT_TRUE(containsLeaf);
}

// _____________________________________________________________________________
TEST(GeoCellGrid, shiftedSchemesBoundTheSentinelPopulation) {
  std::mt19937_64 gen{815};
  std::uniform_real_distribution<double> lngDist{-170.0, 160.0};
  std::uniform_real_distribution<double> latDist{-80.0, 70.0};

  // Flat-4-shifts: every geometry of at most half a cell in both dimensions
  // fits a regular cell of one of the four copies.
  {
    GeoCellGrid grid{5, GeoCellGridScheme::Flat4Shifts};
    double cellLng = 360.0 / 32.0;
    double cellLat = 180.0 / 32.0;
    for (int i = 0; i < 3000; ++i) {
      double lng = lngDist(gen);
      double lat = latDist(gen);
      ad_utility::BoundingBox box{
          GeoPoint{lat, lng},
          GeoPoint{lat + 0.49 * cellLat, lng + 0.49 * cellLng}};
      EXPECT_NE(grid.cellIndexFromBoundingBox(box), grid.sentinelCell());
    }
  }

  // Hierarchical-3-shifts: every geometry is stored at a cell of side at
  // most ~6 times its own extent (Chan's guarantee; we allow a factor of 8
  // for the quantization at the borders), so nothing escalates towards the
  // root by more than a constant number of levels.
  {
    uint8_t level = 8;
    GeoCellGrid grid{level, GeoCellGridScheme::Hierarchical3Shifts};
    for (int i = 0; i < 3000; ++i) {
      double lng = lngDist(gen);
      double lat = latDist(gen);
      // Extent: 1/64 of the domain, i.e. natural level 6 of 8.
      double w = 360.0 / 64.0;
      double h = 180.0 / 64.0;
      ad_utility::BoundingBox box{GeoPoint{lat, lng},
                                  GeoPoint{lat + h, lng + w}};
      auto cell = grid.cellIndexFromBoundingBox(box);
      // Depth below the leaves is encoded in the trailing zeros of the cell
      // number; the stored cell has side length 2^depthBelow leaves.
      uint64_t cellWithoutShift =
          cell & ((uint64_t{1} << (2 * uint64_t{level} + 1)) - 1);
      uint64_t depthBelow = 0;
      while (((cellWithoutShift >> depthBelow) & 1) == 0) {
        ++depthBelow;
      }
      AD_CORRECTNESS_CHECK(depthBelow % 2 == 0);
      depthBelow /= 2;
      // Natural level 6 -> stored level >= 3 (cell side <= 8x extent).
      EXPECT_LE(depthBelow, uint64_t{5})
          << "geometry at [" << lng << ", " << lat << "]";
    }
  }
}

// _____________________________________________________________________________
TEST(GeoCellGrid, comparatorAppliesGeoCellOrder) {
  using Level = TripleComponentComparator::Level;
  TripleComponentComparator cmp;
  cmp.setGeoCellGrid(GeoCellGrid{2});

  // Level 2: POINT(-170 80) is in cell (3 << 2) | 0 = 12, POINT(170 -80) in
  // cell (0 << 2) | 3 = 3.
  std::string cell12 = wkt("POINT(-170 80)");
  std::string cell3 = wkt("POINT(170 -80)");
  ASSERT_LT(cmp.compareLexicographically(cell12, cell3, Level::TOTAL), 0);

  // Non-WKT words have geo sort key 0, WKT literals a key ordered by cell.
  EXPECT_EQ(cmp.geoSortKey("<http://example.org>"), 0u);
  EXPECT_EQ(cmp.geoSortKey("\"POINT(1 1)\""), 0u);  // no datatype, not WKT
  EXPECT_EQ(cmp.geoSortKey(cell12), (uint64_t{1} << 63) | 12);
  EXPECT_EQ(cmp.geoSortKey(cell3), (uint64_t{1} << 63) | 3);

  // WKT literals sort after all other words ...
  EXPECT_LT(cmp.compare("<http://example.org>", cell3, Level::TOTAL), 0);
  EXPECT_LT(cmp.compare("\"zzz\"", cell3, Level::TOTAL), 0);
  // ... and by cell first among each other, even against the lexicographic
  // order.
  EXPECT_GT(cmp.compare(cell12, cell3, Level::TOTAL), 0);
  // Within one cell the order is lexicographic.
  std::string cell12b = wkt("POINT(-171 80)");
  EXPECT_LT(cmp.compare(cell12, cell12b, Level::TOTAL), 0);

  // The variant with precomputed keys is consistent with `compare`.
  EXPECT_TRUE(cmp.isLessInTotalWithExternalFlagAndGeoSortKeys(
      cell3, false, cmp.geoSortKey(cell3), cell12, false,
      cmp.geoSortKey(cell12)));
  EXPECT_FALSE(cmp.isLessInTotalWithExternalFlagAndGeoSortKeys(
      cell12, false, cmp.geoSortKey(cell12), cell3, false,
      cmp.geoSortKey(cell3)));

  // Without a grid the behavior is the plain lexicographic one.
  TripleComponentComparator plainCmp;
  EXPECT_EQ(plainCmp.geoSortKey(cell12), 0u);
  EXPECT_LT(plainCmp.compare(cell12, "<http://example.org>", Level::TOTAL), 0);
  EXPECT_LT(plainCmp.compare(cell12, cell3, Level::TOTAL), 0);
}

// _____________________________________________________________________________
TEST(GeoCellIdPrefilter, canBeSkipped) {
  GeoCellGrid grid{2};
  // Query box entirely inside cell (2 << 2) | 2 = 10.
  GeoCellIdPrefilter prefilter{grid, 10.0, 10.0, 11.0, 11.0};

  auto geoId = [&grid](uint64_t cell, uint64_t position) {
    return GeoCellGrid::geoVocabMarkerBit | grid.annotateIndex(cell, position);
  };

  // Indices outside the WKT region can never be skipped.
  EXPECT_FALSE(prefilter.canBeSkipped(42));
  // The covered cell and the sentinel cell are kept.
  EXPECT_FALSE(prefilter.canBeSkipped(geoId(10, 0)));
  EXPECT_FALSE(prefilter.canBeSkipped(geoId(10, 12345)));
  EXPECT_FALSE(prefilter.canBeSkipped(geoId(grid.sentinelCell(), 3)));
  // All other cells are skipped.
  EXPECT_TRUE(prefilter.canBeSkipped(geoId(0, 0)));
  EXPECT_TRUE(prefilter.canBeSkipped(geoId(9, 7)));
  EXPECT_TRUE(prefilter.canBeSkipped(geoId(11, 7)));
  EXPECT_TRUE(prefilter.canBeSkipped(geoId(grid.sentinelCell() - 1, 0)));
}

// _____________________________________________________________________________
TEST(GeoVocabulary, cellAnnotatedIndices) {
  using GV = GeoVocabulary<VocabularyInMemory>;
  GeoCellGrid grid{2};
  const std::string fn = "geocellvocab-test.dat";

  // Words in (cell, lexicographic) order: cell 3, cell 12 (twice), sentinel.
  std::string w0 = wkt("POINT(170 -80)");  // cell 3
  std::string w1 = wkt("POINT(-170 80)");  // cell 12
  std::string w2 = wkt("POINT(-171 80)");  // cell 12
  std::string w3 = wkt("NOTAGEOMETRY");    // sentinel (invalid)
  std::vector<std::string> words{w0, w1, w2, w3};
  std::vector<uint64_t> expectedIndices{
      grid.annotateIndex(3, 0), grid.annotateIndex(12, 1),
      grid.annotateIndex(12, 2), grid.annotateIndex(grid.sentinelCell(), 3)};

  {
    GV writeVocab;
    writeVocab.setGeoCellGrid(grid);
    auto ww = writeVocab.makeDiskWriterPtr(fn);
    ww->readableName() = "test";
    for (size_t i = 0; i < words.size(); ++i) {
      EXPECT_EQ((*ww)(words[i], false), expectedIndices[i]);
    }
    ww->finish();
  }

  GV geoVocab;
  geoVocab.open(fn);
  ASSERT_TRUE(geoVocab.getGeoCellGrid().has_value());
  EXPECT_EQ(geoVocab.getGeoCellGrid().value(), grid);
  EXPECT_EQ(geoVocab.size(), words.size());

  // Retrieval by annotated index.
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(geoVocab[expectedIndices[i]], words[i]);
    EXPECT_EQ(geoVocab.toPosition(expectedIndices[i]), i);
    EXPECT_EQ(geoVocab.toAnnotatedIndex(i), expectedIndices[i]);
  }
  EXPECT_TRUE(geoVocab.getGeoInfo(expectedIndices[0]).has_value());
  EXPECT_FALSE(geoVocab.getGeoInfo(expectedIndices[3]).has_value());

  // The past-the-end index is larger than every valid index.
  EXPECT_EQ(geoVocab.endIndex(),
            grid.annotateIndex(grid.sentinelCell(), words.size()));

  // `scanAll` yields the annotated indices.
  std::vector<uint64_t> scannedIndices;
  for (const auto& indexAndWord : geoVocab.scanAll()) {
    scannedIndices.push_back(indexAndWord.index_);
  }
  EXPECT_THAT(scannedIndices, ::testing::ElementsAreArray(expectedIndices));

  // Binary search with the geo-cell-aware comparator returns annotated
  // indices.
  TripleComponentComparator cmp;
  cmp.setGeoCellGrid(grid);
  auto comparator = [&cmp](const auto& a, const auto& b) {
    return cmp(a, b, TripleComponentComparator::Level::TOTAL);
  };
  for (size_t i = 0; i < words.size(); ++i) {
    auto wordAndIndex = geoVocab.lower_bound(words[i], comparator);
    ASSERT_FALSE(wordAndIndex.isEnd());
    EXPECT_EQ(wordAndIndex.index(), expectedIndices[i]);
    EXPECT_EQ(wordAndIndex.word(), words[i]);
  }

  // Feeding words out of cell order must fail.
  {
    GV badVocab;
    badVocab.setGeoCellGrid(grid);
    auto ww = badVocab.makeDiskWriterPtr("geocellvocab-test-bad.dat");
    ww->readableName() = "test";
    (*ww)(w1, false);
    EXPECT_ANY_THROW((*ww)(w0, false));
    ww->finish();
  }
}

// _____________________________________________________________________________
TEST(GeoVocabulary, cellAnnotatedIndicesThroughSplitVocabulary) {
  using SGV = SplitGeoVocabulary<VocabularyInMemory>;
  GeoCellGrid grid{2};
  const std::string fn = "geocellsplitvocab-test.dat";

  std::string iri = "<http://example.org/a>";
  std::string wkt3 = wkt("POINT(170 -80)");   // cell 3
  std::string wkt12 = wkt("POINT(-170 80)");  // cell 12

  TripleComponentComparator cmp;
  cmp.setGeoCellGrid(grid);

  {
    SGV writeVocab;
    writeVocab.setGeoCellGrid(grid);
    auto ww = writeVocab.makeDiskWriterPtr(fn);
    ww->readableName() = "test";
    // Words in the comparator's order: all non-WKT words first, then the WKT
    // literals by cell.
    EXPECT_EQ((*ww)(iri, false), 0u);
    EXPECT_EQ((*ww)(wkt3, false), SGV::addMarker(grid.annotateIndex(3, 0), 1));
    EXPECT_EQ((*ww)(wkt12, false),
              SGV::addMarker(grid.annotateIndex(12, 1), 1));
    ww->finish();
  }

  SGV vocab;
  vocab.setGeoCellGrid(grid);
  vocab.open(fn);

  auto comparator = [&cmp](const auto& a, const auto& b) {
    return cmp(a, b, TripleComponentComparator::Level::TOTAL);
  };

  // Retrieval and exact lookup by marked, cell-annotated index.
  EXPECT_EQ(vocab[SGV::addMarker(grid.annotateIndex(3, 0), 1)], wkt3);
  EXPECT_EQ(vocab[SGV::addMarker(grid.annotateIndex(12, 1), 1)], wkt12);
  EXPECT_EQ(vocab[0], iri);

  auto [lo3, hi3] = vocab.getPositionOfWord(wkt3, comparator);
  EXPECT_EQ(lo3, SGV::addMarker(grid.annotateIndex(3, 0), 1));
  EXPECT_EQ(hi3, lo3 + 1);

  // A WKT literal that is not in the vocabulary and larger than all entries
  // gets past-the-end bounds that are larger than every valid index.
  std::string wktMissing = wkt("NOTAGEOMETRY");  // sentinel cell
  auto [loM, hiM] = vocab.getPositionOfWord(wktMissing, comparator);
  EXPECT_EQ(loM, hiM);
  EXPECT_GT(loM, SGV::addMarker(grid.annotateIndex(12, 1), 1));
}

}  // namespace

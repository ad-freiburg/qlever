// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

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

TEST(GeoCellGrid, basics) {
  GeoCellGrid grid{10};
  EXPECT_EQ(grid.level(), 10);
  EXPECT_EQ(grid.numCellsPerDimension(), 1024u);
  EXPECT_EQ(grid.numCellBits(), 21u);
  EXPECT_EQ(grid.sentinelCell(), (uint64_t{1} << 21) - 1);
  EXPECT_EQ(grid.numPositionBits(), ValueId::numDataBits - 22);
  EXPECT_EQ(grid.maxNumWords(), uint64_t{1} << (ValueId::numDataBits - 22));

  // Level 0 is invalid, as are levels where no position bits remain.
  EXPECT_ANY_THROW(GeoCellGrid{0});
  EXPECT_ANY_THROW(GeoCellGrid{40});
}

TEST(GeoCellGrid, cellFromPoint) {
  GeoCellGrid grid{1};
  // Level 1 divides the earth into 2 x 2 cells; cell = (cellY << 1) | cellX.
  EXPECT_EQ(grid.cellFromPoint(-90.0, -45.0), 0u);
  EXPECT_EQ(grid.cellFromPoint(90.0, -45.0), 1u);
  EXPECT_EQ(grid.cellFromPoint(-90.0, 45.0), 2u);
  EXPECT_EQ(grid.cellFromPoint(90.0, 45.0), 3u);
  // The boundary values are clamped into the valid grid range.
  EXPECT_EQ(grid.cellFromPoint(180.0, 90.0), 3u);
  EXPECT_EQ(grid.cellFromPoint(-180.0, -90.0), 0u);

  // Spot check on a fine grid against the closed formula.
  GeoCellGrid fine{10};
  uint64_t cx = static_cast<uint64_t>((13.405 + 180.0) / 360.0 * 1024.0);
  uint64_t cy = static_cast<uint64_t>((52.52 + 90.0) / 180.0 * 1024.0);
  EXPECT_EQ(fine.cellFromPoint(13.405, 52.52), (cy << 10) | cx);
}

TEST(GeoCellGrid, cellFromBoundingBoxAndWktLiteral) {
  GeoCellGrid grid{1};
  // A geometry entirely inside one cell gets that cell.
  EXPECT_EQ(grid.cellFromWktLiteral(wkt("POINT(90 45)")), 3u);
  EXPECT_EQ(grid.cellFromWktLiteral(wkt("LINESTRING(10 10, 20 20)")), 3u);
  // A geometry whose bounding box crosses a cell border gets the sentinel.
  EXPECT_EQ(grid.cellFromWktLiteral(wkt("LINESTRING(-10 10, 20 20)")),
            grid.sentinelCell());
  // Unparseable literals also get the sentinel.
  EXPECT_EQ(grid.cellFromWktLiteral(wkt("NOTAGEOMETRY(1 2)")),
            grid.sentinelCell());
}

TEST(GeoCellGrid, annotateIndexRoundtrip) {
  GeoCellGrid grid{4};
  uint64_t annotated = grid.annotateIndex(5, 7);
  EXPECT_EQ(grid.cellOfIndex(annotated), 5u);
  EXPECT_EQ(grid.positionOfIndex(annotated), 7u);
  EXPECT_EQ(annotated, (uint64_t{5} << grid.numPositionBits()) | 7);
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

TEST(GeoCellGrid, isWktLiteralMatchesGeoSplitFunc) {
  detail::splitVocabulary::GeoSplitFunc splitFunc;
  for (const std::string& word :
       {wkt("POINT(1 2)"), wkt("NOTAGEOMETRY"), std::string{"\"foo\""},
        std::string{"<http://example.org>"}, std::string{"\"foo\"@en"}}) {
    EXPECT_EQ(GeoCellGrid::isWktLiteral(word),
              static_cast<bool>(splitFunc(word)))
        << word;
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

// Regression test: a `SplitVocabulary` must accept the cell-annotated indices
// of its `GeoVocabulary` (which exceed the geo vocabulary's size by
// construction) in `operator[]` and `getPositionOfWord`.
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

TEST(GeoCellGrid, schemeStringConversion) {
  using enum GeoCellGridScheme;
  for (auto scheme : {Flat, Flat4Shifts, Hierarchical, Hierarchical3Shifts}) {
    auto parsed =
        ad_utility::geoCellGridSchemeFromString(ad_utility::toString(scheme));
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed.value(), scheme);
  }
  EXPECT_FALSE(ad_utility::geoCellGridSchemeFromString("nope").has_value());
}

TEST(GeoCellGrid, cellBitsPerScheme) {
  using enum GeoCellGridScheme;
  EXPECT_EQ(GeoCellGrid(10, Flat).numCellBits(), 21u);
  EXPECT_EQ(GeoCellGrid(10, Flat4Shifts).numCellBits(), 23u);
  EXPECT_EQ(GeoCellGrid(10, Hierarchical).numCellBits(), 21u);
  EXPECT_EQ(GeoCellGrid(10, Hierarchical3Shifts).numCellBits(), 23u);
  // The hierarchical schemes have no separate sentinel; the root of the
  // first copy takes its role.
  EXPECT_EQ(GeoCellGrid(10, Hierarchical).sentinelCell(), uint64_t{1} << 20);
  EXPECT_EQ(GeoCellGrid(10, Flat).sentinelCell(), (uint64_t{1} << 21) - 1);
}

// Every cell number must fit into `numCellBits()` bits.
TEST(GeoCellGrid, cellNumbersFitTheField) {
  using enum GeoCellGridScheme;
  std::mt19937_64 gen{42};
  std::uniform_real_distribution<double> lngDist{-180.0, 180.0};
  std::uniform_real_distribution<double> latDist{-90.0, 90.0};
  std::uniform_real_distribution<double> sizeDist{0.0, 5.0};
  for (auto scheme : {Flat, Flat4Shifts, Hierarchical, Hierarchical3Shifts}) {
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
      auto cell = grid.cellFromBoundingBox(box);
      EXPECT_LT(cell, uint64_t{1} << grid.numCellBits());
    }
  }
}

// The central conservativeness property for all four schemes: if a
// geometry's bounding box intersects a query rectangle, then the geometry's
// cell is contained in the covering cell ranges of the rectangle.
TEST(GeoCellGrid, coverIsConservativeForAllSchemes) {
  using enum GeoCellGridScheme;
  std::mt19937_64 gen{4711};
  std::uniform_real_distribution<double> lngDist{-180.0, 179.0};
  std::uniform_real_distribution<double> latDist{-90.0, 89.0};
  std::uniform_real_distribution<double> geomSize{0.0, 8.0};
  std::uniform_real_distribution<double> querySize{0.01, 30.0};

  auto contains = [](const GeoCellGrid::CellRanges& ranges,
                     GeoCellGrid::Cell cell) {
    for (const auto& [first, last] : ranges) {
      if (cell >= first && cell <= last) {
        return true;
      }
    }
    return false;
  };

  for (auto scheme : {Flat, Flat4Shifts, Hierarchical, Hierarchical3Shifts}) {
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
        auto cell = grid.cellFromBoundingBox(box);
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

// Scheme-specific guarantees about which geometries avoid the "no
// information" cell.
TEST(GeoCellGrid, shiftedSchemesBoundTheSentinelPopulation) {
  using enum GeoCellGridScheme;
  std::mt19937_64 gen{815};
  std::uniform_real_distribution<double> lngDist{-170.0, 160.0};
  std::uniform_real_distribution<double> latDist{-80.0, 70.0};

  // Flat-4-shifts: every geometry of at most half a cell in both dimensions
  // fits a regular cell of one of the four copies.
  {
    GeoCellGrid grid{5, Flat4Shifts};
    double cellLng = 360.0 / 32.0;
    double cellLat = 180.0 / 32.0;
    for (int i = 0; i < 3000; ++i) {
      double lng = lngDist(gen);
      double lat = latDist(gen);
      ad_utility::BoundingBox box{
          GeoPoint{lat, lng},
          GeoPoint{lat + 0.49 * cellLat, lng + 0.49 * cellLng}};
      EXPECT_NE(grid.cellFromBoundingBox(box), grid.sentinelCell());
    }
  }

  // Hierarchical-3-shifts: every geometry is stored at a cell of side at
  // most ~6 times its own extent (Chan's guarantee; we allow a factor of 8
  // for the quantization at the borders), so nothing escalates towards the
  // root by more than a constant number of levels.
  {
    uint8_t level = 8;
    GeoCellGrid grid{level, Hierarchical3Shifts};
    for (int i = 0; i < 3000; ++i) {
      double lng = lngDist(gen);
      double lat = latDist(gen);
      // Extent: 1/64 of the domain, i.e. natural level 6 of 8.
      double w = 360.0 / 64.0;
      double h = 180.0 / 64.0;
      ad_utility::BoundingBox box{GeoPoint{lat, lng},
                                  GeoPoint{lat + h, lng + w}};
      auto cell = grid.cellFromBoundingBox(box);
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

// The S2-style encoding of the hierarchical scheme: parents sort within the
// range of their subtree, and the subtree ranges of siblings are disjoint.
TEST(GeoCellGrid, hierarchicalEncoding) {
  GeoCellGrid grid{3, GeoCellGridScheme::Hierarchical};
  // The root is the middle of the ID space of 2 * 3 + 1 = 7 bits.
  EXPECT_EQ(grid.sentinelCell(), 64u);
  // A point geometry gets a leaf cell (odd cell number).
  ad_utility::BoundingBox point{GeoPoint{10.0, 10.0}, GeoPoint{10.0, 10.0}};
  auto leaf = grid.cellFromBoundingBox(point);
  EXPECT_EQ(leaf & 1, 1u);
  // A geometry spanning the whole world gets the root.
  ad_utility::BoundingBox world{GeoPoint{-90.0, -180.0}, GeoPoint{90.0, 180.0}};
  EXPECT_EQ(grid.cellFromBoundingBox(world), grid.sentinelCell());
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

}  // namespace

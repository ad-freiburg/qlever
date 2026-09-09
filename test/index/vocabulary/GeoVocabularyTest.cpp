// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include "../../GeometryInfoTestHelpers.h"
#include "VocabularyTestHelpers.h"
#include "gmock/gmock.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/GeoVocabulary.h"
#include "index/vocabulary/Vocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/File.h"

namespace {

using namespace geoInfoTestHelpers;
using namespace ad_utility;
using AnyGeoVocab = GeoVocabulary<VocabularyInMemory>;

// Define a typed test suite to test the `GeoVocabulary` on different types of
// underlying vocabularies.
using GeoVocabularyUnderlyingVocabTypes =
    ::testing::Types<VocabularyInMemory,
                     CompressedVocabulary<VocabularyInternalExternal>>;
template <typename T>
class GeoVocabularyUnderlyingVocabTypedTest : public ::testing::Test {
 public:
  // The base filename of the `GeoVocabulary` that the tests below write, and an
  // `absl::Cleanup` that deletes all the files of that vocabulary.
  static std::string filename() {
    return absl::StrCat(gtestCurrentTestName(), ".dat");
  }
  static auto getFileCleanup() {
    return vocabulary_test::makeVocabFileCleanup<GeoVocabulary<T>>(filename());
  }

  // A function to test that a `GeoVocabulary` can correctly insert and
  // lookup literals and precompute geometry information. This test is
  // generic on the type of the underlying vocabulary, because the
  // `GeoVocabulary` should behave exactly the same no matter which underlying
  // vocabulary implementation is used.
  void testGeoVocabulary() {
    using GV = GeoVocabulary<T>;
    auto cleanup = getFileCleanup();
    GV geoVocab;
    const std::string fn = filename();
    auto ww = geoVocab.makeDiskWriterPtr(fn);
    ww->readableName() = "test";

    std::vector<std::string> testLiterals{
        // Invalid literal
        "\"Example non-geometry literal\"@en",
        "\"BLABLIBLU(1 2, 3 4, 5 6, 7 8, 9 0)\""
        "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
        // Out of range literal
        "\"POLYGON((1 1, 2 2, 3 450))\""
        "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
        // Valid WKT literals
        "\"GEOMETRYCOLLECTION(LINESTRING(2 2, 4 4), "
        "POLYGON((2 4, 4 4, 4 2, 2 2)))\""
        "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
        "\"LINESTRING(1 1, 2 2, 3 3)\""
        "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
        "\"POLYGON((1 1, 2 2, 3 3))\""
        "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
    };
    ql::ranges::sort(testLiterals);

    for (size_t i = 0; i < testLiterals.size(); i++) {
      auto lit = testLiterals[i];
      auto idx = (*ww)(lit, true);
      ASSERT_EQ(i, idx);
    }

    ww->finish();

    geoVocab.open(fn);

    auto checkGeoVocabContents = [&testLiterals](GV& geoVocab) {
      ASSERT_EQ(geoVocab.size(), testLiterals.size());
      for (size_t i = 0; i < testLiterals.size(); i++) {
        ASSERT_EQ(geoVocab[i], testLiterals[i]);
        ASSERT_EQ(geoVocab.getUnderlyingVocabulary()[i], testLiterals[i]);
        EXPECT_GEOMETRYINFO(geoVocab.getGeoInfo(i),
                            GeometryInfo::fromWktLiteral(testLiterals[i]));
      }
    };

    checkGeoVocabContents(geoVocab);

    // Test further methods
    ASSERT_EQ(geoVocab.size(), testLiterals.size());
    ASSERT_EQ(geoVocab.getUnderlyingVocabulary().size(), testLiterals.size());
    const auto& geoVocabConstRef = geoVocab;
    ASSERT_EQ(geoVocabConstRef.getUnderlyingVocabulary().size(),
              testLiterals.size());

    auto wI = geoVocab.lower_bound("\"LINE", ql::ranges::less{});
    ASSERT_EQ(wI.index(), 3);
    ASSERT_EQ(wI.word(),
              "\"LINESTRING(1 1, 2 2, 3 3)\""
              "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

    wI = geoVocab.upper_bound("\"XYZ", ql::ranges::less{});
    ASSERT_TRUE(wI.isEnd());

    geoVocab.close();
  };

  // Build a `GeoVocabulary` on disk , fill it with a small set of WKT literals.
  GeoVocabulary<T> setupGeoVocab() {
    GeoVocabulary<T> geoVocab;
    auto ww = geoVocab.makeDiskWriterPtr(filename());
    ww->readableName() = "test";
    std::vector<std::string> testLiterals{
        "\"LINESTRING(1 1, 2 2, 3 3)\""
        "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
        "\"POINT(1 1)\""
        "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
        "\"POLYGON((1 1, 2 2, 3 3))\""
        "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
        "\"POINT(2 2)\""
        "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
    };
    // The underlying vocabularies require sorted input at write time.
    ql::ranges::sort(testLiterals);
    for (const auto& [i, literal] : ::ranges::views::enumerate(testLiterals)) {
      EXPECT_EQ(static_cast<uint64_t>(i), (*ww)(literal, true));
    }
    ww->finish();
    geoVocab.open(filename());
    EXPECT_GE(geoVocab.size(), 4u);
    return geoVocab;
  }

  // `lookupBatch` must yield exactly the same strings as looking each index up
  // individually via `operator[]`.
  void testLookupBatch() {
    auto cleanup = getFileCleanup();
    auto geoVocab = setupGeoVocab();
    std::array<size_t, 5> indices{2, 0, 3, 1, 0};
    auto result = geoVocab.lookupBatch(indices);
    vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
        geoVocab, result, indices);
  }

  // `lookupBatchesStreamed` must yield, for each batch, exactly the same
  // strings as the individual `operator[]` lookups for that batch's indices.
  void testLookupBatchesStreamed() {
    auto cleanup = getFileCleanup();
    auto geoVocab = setupGeoVocab();

    std::vector<std::vector<size_t>> batches{{2, 0, 3}, {1}, {0, 0}};
    // `VocabLookupInput` takes ownership of the batches, so keep a copy of the
    // indices to compare against.
    const auto expectedBatches = batches;
    auto streamedResults =
        geoVocab.lookupBatchesStreamed(VocabLookupInput{std::move(batches)});
    vocabulary_test::assertStreamedLookupMatchesVocabularyAtIndices(
        geoVocab, streamedResults, expectedBatches);
  }
};

TYPED_TEST_SUITE(GeoVocabularyUnderlyingVocabTypedTest,
                 GeoVocabularyUnderlyingVocabTypes);

// An `absl::Cleanup` that deletes all the files that an `RdfsVocabulary` of the
// given `type` with the given base `filename` consists of.
auto getFileCleanup(VocabularyType type, const std::string& filename) {
  return vocabulary_test::makeVocabFileCleanup(
      filename, PolymorphicVocabulary::fileSuffixes(type));
}

// _____________________________________________________________________________
TYPED_TEST(GeoVocabularyUnderlyingVocabTypedTest, TypedTest) {
  this->testGeoVocabulary();
}

// _____________________________________________________________________________
TYPED_TEST(GeoVocabularyUnderlyingVocabTypedTest, LookupBatch) {
  this->testLookupBatch();
}

// _____________________________________________________________________________
TYPED_TEST(GeoVocabularyUnderlyingVocabTypedTest, LookupBatchesStreamed) {
  this->testLookupBatchesStreamed();
}

// _____________________________________________________________________________
TEST(GeoVocabularyTest, VocabularyGetGeoInfoFromUnderlyingGeoVocab) {
  const VocabularyType geoSplitVocabType{
      VocabularyType::Enum::OnDiskCompressedGeoSplit};
  const VocabularyType nonGeoVocabType{VocabularyType::Enum::OnDiskCompressed};

  // Generate test vocabulary
  const std::string filename = absl::StrCat(gtestCurrentTestName(), ".geo");
  auto cleanup = getFileCleanup(geoSplitVocabType, filename);
  RdfsVocabulary vocabulary;
  vocabulary.resetToType(geoSplitVocabType);
  ASSERT_TRUE(vocabulary.isGeoInfoAvailable());
  auto wordCallback = vocabulary.makeWordWriterPtr(filename);
  auto nonGeoIdx = (*wordCallback)("<http://example.com/abc>", true);
  static constexpr std::string_view exampleGeoLit =
      "\"LINESTRING(2 2, 4 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  auto geoIdx = (*wordCallback)(exampleGeoLit, true);
  wordCallback->finish();

  // Load test vocabulary and try to retrieve precomputed `GeometryInfo`
  vocabulary.readFromFile(filename);
  ASSERT_TRUE(vocabulary.isGeoInfoAvailable());
  ASSERT_FALSE(vocabulary.getGeoInfo(VocabIndex::make(nonGeoIdx)).has_value());
  auto gi = vocabulary.getGeoInfo(VocabIndex::make(geoIdx));
  ASSERT_TRUE(gi.has_value());
  GeometryInfo exp{2,
                   {{2, 2}, {4, 4}},
                   {3, 3},
                   {1},
                   getLengthForTesting(exampleGeoLit),
                   getAreaForTesting(exampleGeoLit)};
  EXPECT_GEOMETRYINFO(gi.value(), exp);

  // Cannot get `GeometryInfo` from `PolymorphicVocabulary` with no underlying
  // `GeoVocabulary`
  const std::string nonGeoFilename =
      absl::StrCat(gtestCurrentTestName(), ".nonGeo");
  auto nonGeoCleanup = getFileCleanup(nonGeoVocabType, nonGeoFilename);
  RdfsVocabulary nonGeoVocab;
  nonGeoVocab.resetToType(nonGeoVocabType);
  ASSERT_FALSE(nonGeoVocab.isGeoInfoAvailable());
  auto ngWordCallback = nonGeoVocab.makeWordWriterPtr(nonGeoFilename);
  (*ngWordCallback)("<http://example.com/abc>", true);
  ngWordCallback->finish();
  nonGeoVocab.readFromFile(nonGeoFilename);
  ASSERT_FALSE(nonGeoVocab.getGeoInfo(VocabIndex::make(0)).has_value());
}

// _____________________________________________________________________________
TEST(GeoVocabularyTest, InvalidGeometryInfoVersion) {
  const VocabularyType geoSplitVocabType{
      VocabularyType::Enum::OnDiskCompressedGeoSplit};

  // Generate test vocabulary
  const std::string filename = absl::StrCat(gtestCurrentTestName(), ".geo");
  auto cleanup = getFileCleanup(geoSplitVocabType, filename);
  RdfsVocabulary vocabulary;
  vocabulary.resetToType(geoSplitVocabType);
  auto wordCallback = vocabulary.makeWordWriterPtr(filename);
  (*wordCallback)("\"test\"@en", true);
  wordCallback->finish();

  // Overwrite the geoinfo file with an invalid header
  const std::string geometryFilename = absl::StrCat(filename, ".geometry");
  ad_utility::File geoInfoFile{
      AnyGeoVocab::getGeoInfoFilename(geometryFilename), "w"};
  uint64_t fakeHeader = 0;
  geoInfoFile.write(&fakeHeader, 8);
  geoInfoFile.close();

  // Opening the vocabulary should throw an exception
  AD_EXPECT_THROW_WITH_MESSAGE(
      vocabulary.readFromFile(filename),
      ::testing::HasSubstr(
          absl::StrCat("The geometry info version of ",
                       AnyGeoVocab::getGeoInfoFilename(geometryFilename),
                       " is 0, which is incompatible")));
}

// _____________________________________________________________________________
TEST(GeoVocabularyTest, WordWriterDestructor) {
  const std::string lit =
      "\"LINESTRING(1 1, 2 2, 3 3)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

  // Create a `GeoVocabulary::WordWriter` and destruct it without a call to
  // `finish()`.
  const std::string filename1 = absl::StrCat(gtestCurrentTestName(), ".1");
  auto cleanup1 = vocabulary_test::makeVocabFileCleanup<AnyGeoVocab>(filename1);
  AnyGeoVocab sv1;
  auto wordWriter1 = sv1.makeDiskWriterPtr(filename1);
  (*wordWriter1)(lit, true);
  ASSERT_FALSE(wordWriter1->finishWasCalled());
  wordWriter1.reset();

  // Create a `GeoVocabulary::WordWriter` and destruct it after an explicit
  // call to `finish()`.
  const std::string filename2 = absl::StrCat(gtestCurrentTestName(), ".2");
  auto cleanup2 = vocabulary_test::makeVocabFileCleanup<AnyGeoVocab>(filename2);
  AnyGeoVocab sv2;
  auto wordWriter2 = sv2.makeDiskWriterPtr(filename2);
  (*wordWriter2)(lit, true);
  wordWriter2->finish();
  ASSERT_TRUE(wordWriter2->finishWasCalled());
  wordWriter2.reset();
}

// Test that a `GeoVocabulary` with a geo cell grid hands out indices with the
// cell index in the upper bits and translates between indices and positions
// in all its operations. Templated on the underlying vocabulary, see the
// `TEST` below.
template <typename UnderlyingVocabulary>
void testGeoCellGridIndices(const std::string& fn) {
  using GV = GeoVocabulary<UnderlyingVocabulary>;
  GeoCellGrid grid{2};
  auto wkt = [](std::string_view content) {
    return absl::StrCat("\"", content, GEO_LITERAL_SUFFIX);
  };

  // Words in (cell index, lexicographic) order: cell 3, cell 12 (twice), the
  // sentinel cell (for the unparsable literal).
  std::string w0 = wkt("POINT(170 -80)");  // cell 3
  std::string w1 = wkt("POINT(-170 80)");  // cell 12
  std::string w2 = wkt("POINT(-171 80)");  // cell 12
  std::string w3 = wkt("NOTAGEOMETRY");    // sentinel (invalid)
  std::vector<std::string> words{w0, w1, w2, w3};
  std::vector<uint64_t> expectedIndices{
      grid.indexFromCellAndPosition(3, 0), grid.indexFromCellAndPosition(12, 1),
      grid.indexFromCellAndPosition(12, 2),
      grid.indexFromCellAndPosition(grid.sentinelCell(), 3)};

  // The writer assigns the indices.
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

  // The grid is not stored with the vocabulary, it has to be set before
  // opening (the index configuration does this).
  GV geoVocab;
  geoVocab.setGeoCellGrid(grid);
  geoVocab.open(fn);
  ASSERT_TRUE(geoVocab.getGeoCellGrid().has_value());
  EXPECT_EQ(geoVocab.getGeoCellGrid().value(), grid);
  EXPECT_EQ(geoVocab.size(), words.size());

  // Retrieval by index, and the translation between index and position in
  // both directions (the cell is recomputed from the stored bounding box, or
  // from the word for the invalid geometry).
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(geoVocab[expectedIndices[i]], words[i]);
    EXPECT_EQ(geoVocab.positionFromIndex(expectedIndices[i]), i);
    EXPECT_EQ(geoVocab.indexFromPosition(i, words[i]), expectedIndices[i]);
  }
  EXPECT_TRUE(geoVocab.getGeoInfo(expectedIndices[0]).has_value());
  EXPECT_FALSE(geoVocab.getGeoInfo(expectedIndices[3]).has_value());
  // NOTE: `lookupBatch` takes `size_t` indices, which is not the same type as
  // `uint64_t` on all platforms.
  std::vector<size_t> batchIndices(expectedIndices.begin(),
                                   expectedIndices.end());
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
      geoVocab, geoVocab.lookupBatch(batchIndices), batchIndices);

  // An index whose position part is out of range is rejected.
  EXPECT_ANY_THROW(geoVocab[grid.indexFromCellAndPosition(3, words.size())]);
  EXPECT_ANY_THROW(
      geoVocab.getGeoInfo(grid.indexFromCellAndPosition(3, words.size())));

  // The grid of an opened vocabulary cannot be changed anymore.
  geoVocab.setGeoCellGrid(std::nullopt);
  EXPECT_EQ(geoVocab.getGeoCellGrid(), std::optional{grid});

  // The past-the-end index is larger than every valid index.
  EXPECT_EQ(geoVocab.endIndex(),
            grid.indexFromCellAndPosition(grid.sentinelCell(), words.size()));

  // `scanAll` yields the indices.
  std::vector<uint64_t> scannedIndices;
  for (const auto& indexAndWord : geoVocab.scanAll()) {
    scannedIndices.push_back(indexAndWord.index_);
  }
  EXPECT_THAT(scannedIndices, ::testing::ElementsAreArray(expectedIndices));

  // Binary search returns the indices. The comparator orders WKT literals by
  // cell index first; here it is written by hand, in a follow-up change the
  // `TripleComponentComparator` produces this order.
  auto comparator = [&grid](std::string_view a, std::string_view b) {
    auto key = [&grid](std::string_view w) {
      return std::pair{grid.cellIndexFromWktLiteral(w), w};
    };
    return key(a) < key(b);
  };
  for (size_t i = 0; i < words.size(); ++i) {
    auto wordAndIndex = geoVocab.lower_bound(words[i], comparator);
    ASSERT_FALSE(wordAndIndex.isEnd());
    EXPECT_EQ(wordAndIndex.index(), expectedIndices[i]);
    EXPECT_EQ(wordAndIndex.word(), words[i]);
  }
  // A word larger than all words (same sentinel cell, but lexicographically
  // larger) yields the past-the-end result.
  EXPECT_TRUE(geoVocab.lower_bound(wkt("ZZZ"), comparator).isEnd());

  // Without the grid, the same files are read with plain positions as
  // indices.
  GV plainVocab;
  plainVocab.open(fn);
  EXPECT_FALSE(plainVocab.getGeoCellGrid().has_value());
  EXPECT_EQ(plainVocab.endIndex(), words.size());
  for (size_t i = 0; i < words.size(); ++i) {
    EXPECT_EQ(plainVocab[i], words[i]);
    EXPECT_EQ(plainVocab.indexFromPosition(i, words[i]), i);
  }

  // Feeding words out of cell order must fail.
  {
    GV badVocab;
    badVocab.setGeoCellGrid(grid);
    auto ww = badVocab.makeDiskWriterPtr(fn + ".bad");
    ww->readableName() = "test";
    (*ww)(w1, false);
    EXPECT_ANY_THROW((*ww)(w0, false));
    ww->finish();
  }

  // The past-the-end index of an empty vocabulary with a grid is 0.
  {
    GV emptyVocab;
    emptyVocab.setGeoCellGrid(grid);
    emptyVocab.makeDiskWriterPtr(fn + ".empty")->finish();
    emptyVocab.open(fn + ".empty");
    EXPECT_EQ(emptyVocab.endIndex(), 0u);
  }

  // The finest grid leaves only two position bits, so the fourth word does
  // not fit anymore (one position stays free for the past-the-end index).
  {
    GV fullVocab;
    fullVocab.setGeoCellGrid(GeoCellGrid{28});
    auto ww = fullVocab.makeDiskWriterPtr(fn + ".full");
    ww->readableName() = "test";
    for (size_t i = 0; i < 3; ++i) {
      (*ww)(w0, false);
    }
    AD_EXPECT_THROW_WITH_MESSAGE((*ww)(w0, false),
                                 ::testing::HasSubstr("Too many WKT literals"));
    ww->finish();
  }
}

// Run the test above for both underlying vocabularies of a `GeoVocabulary`.
TEST(GeoVocabulary, geoCellGridIndices) {
  testGeoCellGridIndices<VocabularyInMemory>("geocellvocab-test-inmemory.dat");
  testGeoCellGridIndices<CompressedVocabulary<VocabularyInternalExternal>>(
      "geocellvocab-test-compressed.dat");
}

}  // namespace

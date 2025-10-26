// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../../GeometryInfoTestHelpers.h"
#include "gmock/gmock.h"
#include "index/Vocabulary.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/GeoVocabulary.h"
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
  // A function to test that a `GeoVocabulary` can correctly insert and
  // lookup literals and precompute geometry information. This test is
  // generic on the type of the underlying vocabulary, because the
  // `GeoVocabulary` should behave exactly the same no matter which underlying
  // vocabulary implementation is used.
  void testGeoVocabulary() {
    using GV = GeoVocabulary<T>;
    GV geoVocab;
    const std::string fn = "geovocab-test1.dat";
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
    std::sort(testLiterals.begin(), testLiterals.end());

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
        checkGeoInfo(geoVocab.getGeoInfo(i),
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
};

TYPED_TEST_SUITE(GeoVocabularyUnderlyingVocabTypedTest,
                 GeoVocabularyUnderlyingVocabTypes);

// _____________________________________________________________________________
TYPED_TEST(GeoVocabularyUnderlyingVocabTypedTest, TypedTest) {
  this->testGeoVocabulary();
}

// _____________________________________________________________________________
TEST(GeoVocabularyTest, VocabularyGetGeoInfoFromUnderlyingGeoVocab) {
  const VocabularyType geoSplitVocabType{
      VocabularyType::Enum::OnDiskCompressedGeoSplit};
  const VocabularyType nonGeoVocabType{VocabularyType::Enum::OnDiskCompressed};

  // Generate test vocabulary
  RdfsVocabulary vocabulary;
  vocabulary.resetToType(geoSplitVocabType);
  ASSERT_TRUE(vocabulary.isGeoInfoAvailable());
  auto wordCallback = vocabulary.makeWordWriterPtr("geoVocabTest.dat");
  auto nonGeoIdx = (*wordCallback)("<http://example.com/abc>", true);
  static constexpr std::string_view exampleGeoLit =
      "\"LINESTRING(2 2, 4 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  auto geoIdx = (*wordCallback)(exampleGeoLit, true);
  wordCallback->finish();

  // Load test vocabulary and try to retrieve precomputed `GeometryInfo`
  vocabulary.readFromFile("geoVocabTest.dat");
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
  checkGeoInfo(gi.value(), exp);

  // Cannot get `GeometryInfo` from `PolymorphicVocabulary` with no underlying
  // `GeoVocabulary`
  RdfsVocabulary nonGeoVocab;
  nonGeoVocab.resetToType(nonGeoVocabType);
  ASSERT_FALSE(nonGeoVocab.isGeoInfoAvailable());
  auto ngWordCallback = vocabulary.makeWordWriterPtr("nonGeoVocabTest.dat");
  (*ngWordCallback)("<http://example.com/abc>", true);
  ngWordCallback->finish();
  nonGeoVocab.readFromFile("nonGeoVocabTest.dat");
  ASSERT_FALSE(nonGeoVocab.getGeoInfo(VocabIndex::make(0)).has_value());
}

// _____________________________________________________________________________
TEST(GeoVocabularyTest, InvalidGeometryInfoVersion) {
  const VocabularyType geoSplitVocabType{
      VocabularyType::Enum::OnDiskCompressedGeoSplit};

  // Generate test vocabulary
  RdfsVocabulary vocabulary;
  vocabulary.resetToType(geoSplitVocabType);
  auto wordCallback = vocabulary.makeWordWriterPtr("geoVocabTest2.dat");
  (*wordCallback)("\"test\"@en", true);
  wordCallback->finish();

  // Overwrite the geoinfo file with an invalid header
  ad_utility::File geoInfoFile{
      AnyGeoVocab::getGeoInfoFilename("geoVocabTest2.dat.geometry"), "w"};
  uint64_t fakeHeader = 0;
  geoInfoFile.write(&fakeHeader, 8);
  geoInfoFile.close();

  // Opening the vocabulary should throw an exception
  AD_EXPECT_THROW_WITH_MESSAGE(
      vocabulary.readFromFile("geoVocabTest2.dat"),
      ::testing::HasSubstr(
          "The geometry info version of geoVocabTest2.dat.geometry.geoinfo is "
          "0, which is incompatible"));
}

// _____________________________________________________________________________
TEST(GeoVocabularyTest, WordWriterDestructor) {
  const std::string lit =
      "\"LINESTRING(1 1, 2 2, 3 3)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

  // Create a `GeoVocabulary::WordWriter` and destruct it without a call to
  // `finish()`.
  AnyGeoVocab sv1;
  auto wordWriter1 =
      sv1.makeDiskWriterPtr("GeoVocabularyWordWriterDestructor1.dat");
  (*wordWriter1)(lit, true);
  ASSERT_FALSE(wordWriter1->finishWasCalled());
  wordWriter1.reset();

  // Create a `GeoVocabulary::WordWriter` and destruct it after an explicit
  // call to `finish()`.
  AnyGeoVocab sv2;
  auto wordWriter2 =
      sv2.makeDiskWriterPtr("GeoVocabularyWordWriterDestructor2.dat");
  (*wordWriter2)(lit, true);
  wordWriter2->finish();
  ASSERT_TRUE(wordWriter2->finishWasCalled());
  wordWriter2.reset();
}

}  // namespace

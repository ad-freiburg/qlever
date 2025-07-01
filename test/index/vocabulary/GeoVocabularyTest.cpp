// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../../GeometryInfoTestHelpers.h"
#include "index/Vocabulary.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/GeoVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/File.h"

namespace {

using namespace geoInfoTestHelpers;
using namespace ad_utility;

// A function to test that a GeoVocabulary can correctly insert and
// lookup literals and precompute geometry information. This function is generic
// because the GeoVocabulary should behave exactly the same no matter which
// underlying vocabulary implementation is used: see the following uses of this
// function in the TEST(...) blocks.
template <typename T>
void testGeoVocabulary() {
  using GV = GeoVocabulary<T>;
  GV geoVocab;
  const std::string fn = "geovocab-test1.dat";
  auto ww = geoVocab.makeDiskWriterPtr(fn);
  ww->readableName() = "test";

  std::vector<std::string> testLiterals{
      "\"GEOMETRYCOLLECTION(LINESTRING(2 2, 4 4), POLYGON((2 4, 4 4, 4 2, 2 "
      "2)))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      "\"LINESTRING(1 1, 2 2, 3 "
      "3)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      "\"POLYGON((1 1, 2 2, 3 "
      "3))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};

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

  geoVocab.close();
};

// _____________________________________________________________________________
TEST(GeoVocabularyTest, GeoVocabInMem) {
  testGeoVocabulary<VocabularyInMemory>();
}

// _____________________________________________________________________________
TEST(GeoVocabularyTest, GeoVocabInternalExternal) {
  testGeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>();
}

// _____________________________________________________________________________
TEST(GeoVocabularyTest, VocabularyGetGeoInfoFromUnderlyingGeoVocab) {
  // Generate test vocabulary
  RdfsVocabulary vocabulary;
  const VocabularyType geoSplitVocabType{
      VocabularyType::Enum::OnDiskCompressedGeoSplit};
  vocabulary.resetToType(geoSplitVocabType);
  auto wordCallback = vocabulary.makeWordWriterPtr("geoVocabTest.dat");
  auto nonGeoIdx = (*wordCallback)("<http://example.com/abc>", true);
  auto geoIdx = (*wordCallback)(
      "\"LINESTRING(2 2, 4 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      true);
  wordCallback->finish();

  // Load test vocabulary and try to retrieve precomputed `GeometryInfo`
  vocabulary.readFromFile("geoVocabTest.dat");
  ASSERT_FALSE(vocabulary.getGeoInfo(VocabIndex::make(nonGeoIdx)).has_value());
  auto gi = vocabulary.getGeoInfo(VocabIndex::make(geoIdx));
  ASSERT_TRUE(gi.has_value());
  GeometryInfo exp{2, {{2, 2}, {4, 4}}, {3, 3}};
  checkGeoInfo(gi.value(), exp);
}

}  // namespace

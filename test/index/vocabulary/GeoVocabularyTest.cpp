// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../../GeometryInfoTestHelpers.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/GeoVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/File.h"

namespace {

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
  auto ww = geoVocab.makeWordWriter(fn);
  ww.readableName() = "test";

  std::vector<std::string> testLiterals{
      "\"GEOMETRYCOLLECTION(LINESTRING(2 2, 4 4), POLYGON((2 4, 4 4, 4 2, 2 "
      "2)))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      "\"LINESTRING(1 1, 2 2, 3 "
      "3)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      "\"POLYGON((1 1, 2 2, 3 "
      "3))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"};

  for (size_t i = 0; i < testLiterals.size(); i++) {
    auto lit = testLiterals[i];
    auto idx = ww(lit, true);
    ASSERT_EQ(i, idx);
  }

  ww.finish();

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

  const std::string fn2 = "geovocab-test2.dat";
  geoVocab.close();

  GV geoVocab2;
  geoVocab2.build(testLiterals, fn2);

  checkGeoVocabContents(geoVocab2);

  geoVocab2.close();
};

TEST(GeoVocabularyTest, GeoVocabInMem) {
  testGeoVocabulary<VocabularyInMemory>();
}

TEST(GeoVocabularyTest, GeoVocabInternalExternal) {
  testGeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>();
}

}  // namespace

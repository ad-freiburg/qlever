// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "index/vocabulary/GeoVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "util/File.h"

// TODO<ullingerc> Unit tests

TEST(GeoVocabularyTest, GeoVocabInMem) {
  GeoVocabulary<VocabularyInMemory> geoVocab;
  const std::string fn = "geovocab-test1.dat";
  auto ww = geoVocab.makeWordWriter(fn);
  ww.readableName() = "test";

  std::vector<std::string> testLiterals{
      "\"GEOMETRYCOLLECTION(LINESTRING(1 1, 2 2, 3 "
      "3), POLYGON((1 1, 2 2, 3 "
      "3)))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
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

  auto checkGeoVocabContents = [&testLiterals, &geoVocab]() {
    ASSERT_EQ(geoVocab.size(), testLiterals.size());
    for (size_t i = 0; i < testLiterals.size(); i++) {
      ASSERT_EQ(geoVocab[i], testLiterals[i]);
      ASSERT_EQ(geoVocab.getUnderlyingVocabulary()[i], testLiterals[i]);

      auto gi = geoVocab.getGeoInfo(i);
      // TODO<ullingerc> In PR #1957 actually test content of GeometryInfo
      ASSERT_EQ(gi.dummy_, 1);
    }
  };

  checkGeoVocabContents();

  const std::string fn2 = "geovocab-test2.dat";
  geoVocab.close();
  geoVocab.build(testLiterals, fn2);
  geoVocab.open(fn2);

  checkGeoVocabContents();

  geoVocab.close();

  ad_utility::deleteFile(fn);
  ad_utility::deleteFile(fn + ".geoinfo");
  ad_utility::deleteFile(fn2);
  ad_utility::deleteFile(fn2 + ".geoinfo");
}

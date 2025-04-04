// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include <cstdio>
#include <vector>

#include "../src/index/Vocabulary.h"
#include "../src/util/json.h"
#include "global/IndexTypes.h"

using json = nlohmann::json;
using std::string;

TEST(VocabularyTest, getIdForWordTest) {
  std::vector<TextVocabulary> vec(2);

  ad_utility::HashSet<std::string> s{"a", "ab", "ba", "car"};
  for (auto& v : vec) {
    auto filename = "vocTest1.dat";
    v.createFromSet(s, filename, std::nullopt);
    WordVocabIndex idx;
    ASSERT_TRUE(v.getId("ba", &idx));
    ASSERT_EQ(2u, idx.get());
    ASSERT_TRUE(v.getId("a", &idx));
    ASSERT_EQ(0u, idx.get());
    ASSERT_FALSE(v.getId("foo", &idx));
    ad_utility::deleteFile(filename);
  }

  // with case insensitive ordering
  TextVocabulary voc;
  voc.setLocale("en", "US", false);
  ad_utility::HashSet<string> s2{"a", "A", "Ba", "car"};
  auto filename = "vocTest2.dat";
  voc.createFromSet(s2, filename, std::nullopt);
  WordVocabIndex idx;
  ASSERT_TRUE(voc.getId("Ba", &idx));
  ASSERT_EQ(2u, idx.get());
  ASSERT_TRUE(voc.getId("a", &idx));
  ASSERT_EQ(0u, idx.get());
  // getId only gets exact matches;
  ASSERT_FALSE(voc.getId("ba", &idx));
  ad_utility::deleteFile(filename);
}

TEST(VocabularyTest, getIdRangeForFullTextPrefixTest) {
  TextVocabulary v;
  ad_utility::HashSet<string> s{"wordA0", "wordA1", "wordB2", "wordB3",
                                "wordB4"};
  auto filename = "vocTest3.dat";
  v.createFromSet(s, filename, std::nullopt);

  uint64_t word0 = 0;
  // Match exactly one
  auto retVal = v.getIdRangeForFullTextPrefix("wordA1*");
  ASSERT_TRUE(retVal.has_value());
  ASSERT_EQ(word0 + 1, retVal.value().first().get());
  ASSERT_EQ(word0 + 1, retVal.value().last().get());

  // Match all
  retVal = v.getIdRangeForFullTextPrefix("word*");
  ASSERT_TRUE(retVal.has_value());
  ASSERT_EQ(word0, retVal.value().first().get());
  ASSERT_EQ(word0 + 4, retVal.value().last().get());

  // Match first two
  retVal = v.getIdRangeForFullTextPrefix("wordA*");
  ASSERT_TRUE(retVal.has_value());
  ASSERT_EQ(word0, retVal.value().first().get());
  ASSERT_EQ(word0 + 1, retVal.value().last().get());

  // Match last three
  retVal = v.getIdRangeForFullTextPrefix("wordB*");
  ASSERT_TRUE(retVal.has_value());
  ASSERT_EQ(word0 + 2, retVal.value().first().get());
  ASSERT_EQ(word0 + 4, retVal.value().last().get());

  retVal = v.getIdRangeForFullTextPrefix("foo*");
  ASSERT_FALSE(retVal.has_value());
  ad_utility::deleteFile(filename);
}

TEST(VocabularyTest, createFromSetTest) {
  ad_utility::HashSet<string> s;
  s.insert("a");
  s.insert("ab");
  s.insert(
      "\"POLYGON((1 2, 3 "
      "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  s.insert("ba");
  s.insert("car");
  s.insert(
      "\"LINESTRING(1 2, 3 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  TextVocabulary v;
  auto filename = "vocTest4.dat";
  auto geoFilename = "vocTest4-geo.dat";
  v.createFromSet(s, filename, geoFilename);

  WordVocabIndex idx;
  ASSERT_TRUE(v.getId("ba", &idx));
  ASSERT_EQ(2u, idx.get());

  ASSERT_TRUE(v.getId("a", &idx));
  ASSERT_EQ(0u, idx.get());

  ASSERT_FALSE(v.getId("foo", &idx));

  ASSERT_TRUE(
      v.getId("\"LINESTRING(1 2, 3 "
              "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
              &idx));
  ASSERT_EQ(static_cast<uint64_t>(1) << 59, idx.get());
  ASSERT_TRUE(
      v.getId("\"POLYGON((1 2, 3 "
              "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
              &idx));
  ASSERT_EQ((static_cast<uint64_t>(1) << 59) | 1, idx.get());

  ad_utility::deleteFile(filename);
  ad_utility::deleteFile(geoFilename);
}

TEST(VocabularyTest, IncompleteLiterals) {
  TripleComponentComparator comp("en", "US", false);

  ASSERT_TRUE(comp("\"fieldofwork", "\"GOLD\"@en"));
}

TEST(Vocabulary, PrefixFilter) {
  RdfsVocabulary vocabulary;
  vocabulary.setLocale("en", "US", true);
  ad_utility::HashSet<string> words;

  words.insert("\"exa\"");
  words.insert("\"exp\"");
  words.insert("\"ext\"");
  words.insert(R"("["Ex-vivo" renal artery revascularization]"@en)");
  auto filename = "vocTest5.dat";
  vocabulary.createFromSet(words, filename, std::nullopt);

  // Found in internal but not in external vocabulary.
  auto ranges = vocabulary.prefixRanges("\"exp");
  RdfsVocabulary::PrefixRanges expectedRanges{
      {std::pair{VocabIndex::make(1u), VocabIndex::make(2u)}}};
  ASSERT_EQ(ranges, expectedRanges);
  ad_utility::deleteFile(filename);
}

TEST(VocabularyTest, ItemAt) {
  ad_utility::HashSet<string> s;
  s.insert("a");
  s.insert("ab");
  s.insert(
      "\"POLYGON((1 2, 3 "
      "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  s.insert("ba");
  s.insert("car");
  s.insert(
      "\"LINESTRING(1 2, 3 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  TextVocabulary v;
  auto filename = "vocTest6.dat";
  auto geoFilename = "vocTest6-geo.dat";
  v.createFromSet(s, filename, geoFilename);

  ASSERT_EQ(v[WordVocabIndex::make(0)], "a");
  ASSERT_EQ(v[WordVocabIndex::make(1)], "ab");
  ASSERT_EQ(v[WordVocabIndex::make(2)], "ba");
  ASSERT_EQ(v[WordVocabIndex::make(3)], "car");

  WordVocabIndex idx = WordVocabIndex::make(static_cast<uint64_t>(1) << 59);
  ASSERT_EQ(v[idx],
            "\"LINESTRING(1 2, 3 "
            "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  idx = WordVocabIndex::make(static_cast<uint64_t>(1) << 59 | 1);
  ASSERT_EQ(v[idx],
            "\"POLYGON((1 2, 3 "
            "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  ad_utility::deleteFile(filename);
  ad_utility::deleteFile(geoFilename);
}

TEST(Vocabulary, GeoLiteral) {
  ASSERT_TRUE(RdfsVocabulary::stringIsGeoLiteral(
      "\"POLYGON((1 2, 3 "
      "4))\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"));
  ASSERT_TRUE(RdfsVocabulary::stringIsGeoLiteral(
      "\"LINESTRING(1 2, 3 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"));
  ASSERT_FALSE(RdfsVocabulary::stringIsGeoLiteral(""));
  ASSERT_FALSE(RdfsVocabulary::stringIsGeoLiteral("\"abc\""));
  ASSERT_FALSE(
      RdfsVocabulary::stringIsGeoLiteral("\"\"^^<http://example.com>"));
  //
  ;
}

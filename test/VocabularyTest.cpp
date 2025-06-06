// Copyright 2011 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include <cstdio>
#include <vector>

#include "global/IndexTypes.h"
#include "index/Vocabulary.h"
#include "util/json.h"

// Anonymous namespace for the normal tests
namespace {

using json = nlohmann::json;
using std::string;

TEST(VocabularyTest, getIdForWordTest) {
  std::vector<TextVocabulary> vec(2);

  ad_utility::HashSet<std::string> s{"a", "ab", "ba", "car"};
  for (auto& v : vec) {
    auto filename = "vocTest1.dat";
    v.createFromSet(s, filename);
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
  voc.createFromSet(s2, filename);
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
  v.createFromSet(s, filename);

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
  v.createFromSet(s, filename);

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
  vocabulary.createFromSet(words, filename);

  // Found in internal but not in external vocabulary.
  auto ranges = vocabulary.prefixRanges("\"exp");
  RdfsVocabulary::PrefixRanges expectedRanges{
      {std::pair{VocabIndex::make(1u), VocabIndex::make(2u)}}};
  ASSERT_EQ(ranges, expectedRanges);
  ad_utility::deleteFile(filename);
}

}  // namespace

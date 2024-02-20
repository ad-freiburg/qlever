// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include <cstdio>
#include <vector>

#include "../src/index/Vocabulary.h"
#include "../src/util/json.h"

using json = nlohmann::json;
using std::string;

TEST(VocabularyTest, getIdForWordTest) {
  std::vector<TextVocabulary> vec(2);

  ad_utility::HashSet<std::string> s{"a", "ab", "ba", "car"};
  for (auto& v : vec) {
    v.createFromSet(s);
    WordVocabIndex idx;
    ASSERT_TRUE(v.getId("ba", &idx));
    ASSERT_EQ(2u, idx.get());
    ASSERT_TRUE(v.getId("a", &idx));
    ASSERT_EQ(0u, idx.get());
    ASSERT_FALSE(v.getId("foo", &idx));
  }

  // with case insensitive ordering
  TextVocabulary voc;
  voc.setLocale("en", "US", false);
  ad_utility::HashSet<string> s2{"a", "A", "Ba", "car"};
  voc.createFromSet(s2);
  WordVocabIndex idx;
  ASSERT_TRUE(voc.getId("Ba", &idx));
  ASSERT_EQ(2u, idx.get());
  ASSERT_TRUE(voc.getId("a", &idx));
  ASSERT_EQ(0u, idx.get());
  // getId only gets exact matches;
  ASSERT_FALSE(voc.getId("ba", &idx));
}

TEST(VocabularyTest, getIdRangeForFullTextPrefixTest) {
  TextVocabulary v;
  ad_utility::HashSet<string> s{"wordA0", "wordA1", "wordB2", "wordB3",
                                "wordB4"};
  v.createFromSet(s);

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
}

TEST(VocabularyTest, readWriteTest) {
  {
    TextVocabulary v;
    ad_utility::HashSet<string> s{"wordA0", "wordA1", "wordB2", "wordB3",
                                  "wordB4"};
    v.createFromSet(s);
    ASSERT_EQ(size_t(5), v.size());

    v.writeToFile("_testtmp_vocfile");
  }

  TextVocabulary v2;
  v2.readFromFile("_testtmp_vocfile");
  ASSERT_EQ(size_t(5), v2.size());
  remove("_testtmp_vocfile");
}

TEST(VocabularyTest, createFromSetTest) {
  ad_utility::HashSet<string> s;
  s.insert("a");
  s.insert("ab");
  s.insert("ba");
  s.insert("car");
  TextVocabulary v;
  v.createFromSet(s);
  WordVocabIndex idx;
  ASSERT_TRUE(v.getId("ba", &idx));
  ASSERT_EQ(2u, idx.get());
  ASSERT_TRUE(v.getId("a", &idx));
  ASSERT_EQ(0u, idx.get());
  ASSERT_FALSE(v.getId("foo", &idx));
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
  vocabulary.createFromSet(words);

  // Found in internal but not in external vocabulary.
  auto ranges = vocabulary.prefixRanges("\"exp");
  auto firstIndexExternal =
      VocabIndex::make(vocabulary.getInternalVocab().size());
  RdfsVocabulary::PrefixRanges expectedRanges{
      {std::pair{VocabIndex::make(1u), VocabIndex::make(2u)},
       {std::pair{firstIndexExternal, firstIndexExternal}}}};
  ASSERT_EQ(ranges, expectedRanges);
}

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
  IdRange retVal;
  // Match exactly one
  ASSERT_TRUE(v.getIdRangeForFullTextPrefix("wordA1*", &retVal));
  ASSERT_EQ(word0 + 1, retVal.first().get());
  ASSERT_EQ(word0 + 1, retVal.last().get());

  // Match all
  ASSERT_TRUE(v.getIdRangeForFullTextPrefix("word*", &retVal));
  ASSERT_EQ(word0, retVal.first().get());
  ASSERT_EQ(word0 + 4, retVal.last().get());

  // Match first two
  ASSERT_TRUE(v.getIdRangeForFullTextPrefix("wordA*", &retVal));
  ASSERT_EQ(word0, retVal.first().get());
  ASSERT_EQ(word0 + 1, retVal.last().get());

  // Match last three
  ASSERT_TRUE(v.getIdRangeForFullTextPrefix("wordB*", &retVal));
  ASSERT_EQ(word0 + 2, retVal.first().get());
  ASSERT_EQ(word0 + 4, retVal.last().get());

  ASSERT_FALSE(v.getIdRangeForFullTextPrefix("foo*", &retVal));
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
  RdfsVocabulary voc;
  voc.setLocale("en", "US", true);
  ad_utility::HashSet<string> s;

  s.insert("\"exa\"");
  s.insert("\"exp\"");
  s.insert("\"ext\"");
  s.insert(R"("["Ex-vivo" renal artery revascularization]"@en)");
  voc.createFromSet(s);

  auto x = voc.prefix_range("\"exp");
  ASSERT_EQ(x.first.get(), 1u);
  ASSERT_EQ(x.second.get(), 2u);
}

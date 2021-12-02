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

  for (auto& v : vec) {
    v.push_back("a");
    v.push_back("ab");
    v.push_back("ba");
    v.push_back("car");
    Id id;
    ASSERT_TRUE(v.getId("ba", &id));
    ASSERT_EQ(Id(2), id);
    ASSERT_TRUE(v.getId("a", &id));
    ASSERT_EQ(Id(0), id);
    ASSERT_FALSE(v.getId("foo", &id));
  }

  // with case insensitive ordering
  TextVocabulary voc;
  voc.setLocale("en", "US", false);
  voc.push_back("a");
  voc.push_back("A");
  voc.push_back("Ba");
  voc.push_back("car");
  Id id;
  ASSERT_TRUE(voc.getId("Ba", &id));
  ASSERT_EQ(Id(2), id);
  ASSERT_TRUE(voc.getId("a", &id));
  ASSERT_EQ(Id(0), id);
  // getId only gets exact matches;
  ASSERT_FALSE(voc.getId("ba", &id));
};

TEST(VocabularyTest, getIdRangeForFullTextPrefixTest) {
  TextVocabulary v;
  v.push_back("wordA0");
  v.push_back("wordA1");
  v.push_back("wordB2");
  v.push_back("wordB3");
  v.push_back("wordB4");

  Id word0 = 0;
  IdRange retVal;
  // Match exactly one
  ASSERT_TRUE(v.getIdRangeForFullTextPrefix("wordA1*", &retVal));
  ASSERT_EQ(word0 + 1, retVal._first);
  ASSERT_EQ(word0 + 1, retVal._last);

  // Match all
  ASSERT_TRUE(v.getIdRangeForFullTextPrefix("word*", &retVal));
  ASSERT_EQ(word0, retVal._first);
  ASSERT_EQ(word0 + 4, retVal._last);

  // Match first two
  ASSERT_TRUE(v.getIdRangeForFullTextPrefix("wordA*", &retVal));
  ASSERT_EQ(word0, retVal._first);
  ASSERT_EQ(word0 + 1, retVal._last);

  // Match last three
  ASSERT_TRUE(v.getIdRangeForFullTextPrefix("wordB*", &retVal));
  ASSERT_EQ(word0 + 2, retVal._first);
  ASSERT_EQ(word0 + 4, retVal._last);

  ASSERT_FALSE(v.getIdRangeForFullTextPrefix("foo*", &retVal));
}

TEST(VocabularyTest, readWriteTest) {
  TextVocabulary v;
  v.push_back("wordA0");
  v.push_back("wordA1");
  v.push_back("wordB2");
  v.push_back("wordB3");
  v.push_back("wordB4");
  ASSERT_EQ(size_t(5), v.size());

  v.writeToFile("_testtmp_vocfile");
  v.push_back("foo");
  ASSERT_EQ(size_t(6), v.size());
  v.readFromFile("_testtmp_vocfile");
  ASSERT_EQ(size_t(5), v.size());
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
  Id id;
  ASSERT_TRUE(v.getId("ba", &id));
  ASSERT_EQ(Id(2), id);
  ASSERT_TRUE(v.getId("a", &id));
  ASSERT_EQ(Id(0), id);
  ASSERT_FALSE(v.getId("foo", &id));
};

TEST(VocabularyTest, IncompleteLiterals) {
  TripleComponentComparator comp("en", "US", false);

  ASSERT_TRUE(comp("\"fieldofwork", "\"GOLD\"@en"));
}

TEST(Vocabulary, PrefixFilter) {
  RdfsVocabulary voc;
  voc.push_back("\"exa\"");
  voc.push_back("\"exp\"");
  voc.push_back("\"ext\"");
  voc.push_back("\"[\"Ex-vivo\" renal artery revascularization]\"@en");

  auto x = voc.prefix_range("\"exp");
  ASSERT_EQ(x.first, 1u);
  ASSERT_EQ(x.second, 2u);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

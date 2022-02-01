// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include "../src/index/ExternalVocabulary.h"

TEST(ExternalVocabularyTest, getWordbyIdTest) {
  vector<string> v;
  v.push_back("a");
  v.push_back("ab");
  v.push_back("ba");
  v.push_back("car");
  {
    ExternalVocabulary<SimpleStringComparator> ev;
    ev.buildFromVector(v, "__tmp.evtest");
    ASSERT_EQ("a", ev[0]);
    ASSERT_EQ("ba", ev[2]);
    ASSERT_EQ("car", ev[3]);
  }
  remove("__tmo.evtest");
};

TEST(ExternalVocabularyTest, getIdForWordTest) {
  vector<string> v;
  v.push_back("a");
  v.push_back("ab");
  v.push_back("ba");
  v.push_back("car");
  {
    ExternalVocabulary<SimpleStringComparator> ev;
    ev.buildFromVector(v, "__tmp.evtest");
    Id id;
    ASSERT_TRUE(ev.getId("ba", &id));
    ASSERT_EQ(Id(2), id);
    ASSERT_TRUE(ev.getId("a", &id));
    ASSERT_EQ(Id(0), id);
    ASSERT_TRUE(ev.getId("car", &id));
    ASSERT_EQ(Id(3), id);
    ASSERT_FALSE(ev.getId("foo", &id));
  }
  remove("__tmo.evtest");
};

TEST(VocabularyTest, readWriteTest) {
  vector<string> v;
  v.push_back("wordA0");
  v.push_back("wordA1");
  v.push_back("wordB2");
  v.push_back("wordB3");
  v.push_back("wordB4");
  ASSERT_EQ(size_t(5), v.size());
  {
    ExternalVocabulary<SimpleStringComparator> ev;
    ev.buildFromVector(v, "__tmp.evtest");
  }
  {
    ExternalVocabulary<SimpleStringComparator> ev;
    ev.initFromFile("__tmp.evtest");
    ASSERT_EQ(size_t(5), ev.size());
  }
  remove("__tmp.evtest");
}

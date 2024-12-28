// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "parser/WordsAndDocsFileParser.h"

TEST(WordsAndDocsFileParserTest, wordsFileGetLineTest) {
  char* locale = setlocale(LC_CTYPE, "");
  std::cout << "Set locale LC_CTYPE to: " << locale << std::endl;

  std::fstream f("_testtmp.contexts.tsv", std::ios_base::out);
  f << "Foo\t0\t0\t2\n"
       "foo\t0\t0\t2\n"
       "Bär\t1\t0\t1\n"
       "Äü\t0\t0\t1\n"
       "X\t0\t1\t1\n";

  f.close();
  WordsFileParser p("_testtmp.contexts.tsv", LocaleManager("en", "US", false));
  size_t i = 0;
  for (auto line : p) {
    switch (i) {
      case 0:
        ASSERT_EQ("foo", line.word_);
        ASSERT_FALSE(line.isEntity_);
        ASSERT_EQ(0u, line.contextId_.get());
        ASSERT_EQ(2u, line.score_);
        break;
      case 1:
        ASSERT_EQ("foo", line.word_);
        ASSERT_FALSE(line.isEntity_);
        ASSERT_EQ(0u, line.contextId_.get());
        ASSERT_EQ(2u, line.score_);
        break;
      case 2:
        ASSERT_EQ("Bär", line.word_);
        ASSERT_TRUE(line.isEntity_);
        ASSERT_EQ(0u, line.contextId_.get());
        ASSERT_EQ(1u, line.score_);
        break;
      case 3:
        ASSERT_EQ("äü", line.word_);
        ASSERT_FALSE(line.isEntity_);
        ASSERT_EQ(0u, line.contextId_.get());
        ASSERT_EQ(1u, line.score_);
        break;
      case 4:
        ASSERT_EQ("x", line.word_);
        ASSERT_FALSE(line.isEntity_);
        ASSERT_EQ(1u, line.contextId_.get());
        ASSERT_EQ(1u, line.score_);
        break;
      default:
        // shouldn't be reached
        ASSERT_TRUE(false);
    }
    ++i;
  }
  ASSERT_EQ(i, 5);
  remove("_testtmp.contexts.tsv");
};

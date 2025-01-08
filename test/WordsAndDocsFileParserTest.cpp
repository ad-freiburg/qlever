// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "./WordsAndDocsFileLineCreator.h"
#include "parser/WordsAndDocsFileParser.h"

TEST(WordsAndDocsFileParserTest, wordsFileParserTest) {
  char* locale = setlocale(LC_CTYPE, "");
  std::cout << "Set locale LC_CTYPE to: " << locale << std::endl;

  std::fstream f("_testtmp.contexts.tsv", std::ios_base::out);
  f << createWordsFileLine("Foo", false, 0, 2)
    << createWordsFileLine("foo", false, 0, 2)
    << createWordsFileLine("Bär", true, 0, 1)
    << createWordsFileLine("Äü", false, 0, 1)
    << createWordsFileLine("X", false, 1, 1);

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

TEST(WordsAndDocsFileParser, docsFileParserTest) {
  char* locale = setlocale(LC_CTYPE, "");
  std::cout << "Set locale LC_CTYPE to: " << locale << std::endl;

  std::fstream f("_testtmp.documents.tsv", std::ios_base::out);
  f << createDocsFileLine(4, "This TeSt is OnlyCharcters")
    << createDocsFileLine(7, "Wh4t h4pp3ns t0 num83rs")
    << createDocsFileLine(8, "An( sp@ci*l ch.ar,:act=_er+s")
    << createDocsFileLine(190293, "Large docId");
  f.close();
  DocsFileParser p("_testtmp.documents.tsv", LocaleManager("en", "US", false));
  size_t i = 0;
  for (auto line : p) {
    switch (i) {
      case 0:
        ASSERT_EQ(4u, line.docId_.get());
        ASSERT_EQ("This TeSt is OnlyCharcters", line.docContent_);
        break;
      case 1:
        ASSERT_EQ(7u, line.docId_.get());
        ASSERT_EQ("Wh4t h4pp3ns t0 num83rs", line.docContent_);
        break;
      case 2:
        ASSERT_EQ(8u, line.docId_.get());
        ASSERT_EQ("An( sp@ci*l ch.ar,:act=_er+s", line.docContent_);
        break;
      case 3:
        ASSERT_EQ(190293u, line.docId_.get());
        ASSERT_EQ("Large docId", line.docContent_);
        break;
      default:
        // shouldn't be reached
        ASSERT_TRUE(false);
    }
    ++i;
  }
  ASSERT_EQ(i, 4);
  remove("_testtmp.documents.tsv");
}

TEST(TokenizeAndNormalizeText, tokenizeAndNormalizeTextTest) {
  char* locale = setlocale(LC_CTYPE, "");
  std::cout << "Set locale LC_CTYPE to: " << locale << std::endl;
  using TestVec = std::vector<std::string>;
  char separator = ' ';
  auto testVecToText = [separator](TestVec input) {
    std::string output = "";
    for (auto word : input) {
      output.append(word + separator);
    }
    if (output.size() >= 1) {
      output.pop_back();
    }
    return output;
  };

  TestVec wordsTest1 = {"already", "normalized", "text"};
  TestVec normalizedWordsTest1 = wordsTest1;

  std::string textTest1 = testVecToText(wordsTest1);

  TokenizeAndNormalizeText test1(textTest1, LocaleManager("en", "US", false));
  size_t i1 = 0;
  for (auto normalizedWord : test1) {
    ASSERT_EQ(normalizedWordsTest1.at(i1), normalizedWord);
    ++i1;
  }
  ASSERT_EQ(i1, 3);

  TestVec wordsTest2 = {"TeXt", "WITH", "UpperCASe"};
  TestVec normalizedWordsTest2 = {"text", "with", "uppercase"};

  std::string textTest2 = testVecToText(wordsTest2);

  TokenizeAndNormalizeText test2(textTest2, LocaleManager("en", "US", false));
  size_t i2 = 0;
  for (auto normalizedWord : test2) {
    ASSERT_EQ(normalizedWordsTest2.at(i2), normalizedWord);
    ++i2;
  }
  ASSERT_EQ(i2, 3);

  TestVec wordsTest3 = {"41ph4num3r1c", "t3xt"};
  TestVec normalizedWordsTest3 = {"41ph4num3r1c", "t3xt"};

  std::string textTest3 = testVecToText(wordsTest3);

  TokenizeAndNormalizeText test3(textTest3, LocaleManager("en", "US", false));
  size_t i3 = 0;
  for (auto normalizedWord : test3) {
    ASSERT_EQ(normalizedWordsTest3.at(i3), normalizedWord);
    ++i3;
  }
  ASSERT_EQ(i3, 2);

  TestVec wordsTest4 = {"test\t",      "with\n", "different,",
                        "separators.", "here",   ",.\t"};
  TestVec normalizedWordsTest4 = {"test", "with", "different", "separators",
                                  "here"};

  std::string textTest4 = testVecToText(wordsTest4);

  TokenizeAndNormalizeText test4(textTest4, LocaleManager("en", "US", false));
  size_t i4 = 0;
  for (auto normalizedWord : test4) {
    ASSERT_EQ(normalizedWordsTest4.at(i4), normalizedWord);
    ++i4;
  }
  ASSERT_EQ(i4, 5);
}

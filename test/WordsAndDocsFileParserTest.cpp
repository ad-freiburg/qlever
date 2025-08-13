// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "./WordsAndDocsFileLineCreator.h"
#include "./util/GTestHelpers.h"
#include "parser/WordsAndDocsFileParser.h"

// All lambdas and type aliases used in this file contained here
namespace {

/// Type aliases

// Word, isEntity, contextId, score
using WordLine = std::tuple<std::string, bool, size_t, size_t>;
using WordLineVec = std::vector<WordLine>;

// docId, docContent
using DocLine = std::tuple<size_t, std::string>;
using DocLineVec = std::vector<DocLine>;

using StringVec = std::vector<std::string>;

/// Lambdas

auto getLocaleManager = []() -> LocaleManager {
  return LocaleManager("en", "US", false);
};

auto wordsFileLineToWordLine =
    [](const WordsFileLine& wordsFileLine) -> WordLine {
  return std::make_tuple(wordsFileLine.word_, wordsFileLine.isEntity_,
                         static_cast<size_t>(wordsFileLine.contextId_.get()),
                         static_cast<size_t>(wordsFileLine.score_));
};

// Lambda that takes in a path to wordsFile to initialize the Parser and an
// expectedResult that is compared against the parsers outputs.
auto testWordsFileParser = [](const std::string& wordsFilePath,
                              const WordLineVec& expectedResult) {
  size_t i = 0;
  LocaleManager localeManager = getLocaleManager();
  for (auto wordsFileLine : WordsFileParser{wordsFilePath, localeManager}) {
    ASSERT_TRUE(i < expectedResult.size());
    WordLine testLine = wordsFileLineToWordLine(wordsFileLine);

    // Not testing the whole tuples against each other to have a cleaner
    // indication what exactly caused the assertion to fail
    ASSERT_EQ(std::get<0>(testLine), std::get<0>(expectedResult.at(i)));
    ASSERT_EQ(std::get<1>(testLine), std::get<1>(expectedResult.at(i)));
    ASSERT_EQ(std::get<2>(testLine), std::get<2>(expectedResult.at(i)));
    ASSERT_EQ(std::get<3>(testLine), std::get<3>(expectedResult.at(i)));

    ++i;
  }
  ASSERT_EQ(i, expectedResult.size());
};

auto docsFileLineToDocLine = [](const DocsFileLine& docsFileLine) -> DocLine {
  return std::make_tuple(static_cast<size_t>(docsFileLine.docId_.get()),
                         docsFileLine.docContent_);
};

// Same as testWordsFileParser but for docsFile
auto testDocsFileParser = [](const std::string& docsFilePath,
                             const DocLineVec& expectedResult) {
  size_t i = 0;
  LocaleManager localeManager = getLocaleManager();
  for (auto docsFileLine : DocsFileParser{docsFilePath, localeManager}) {
    ASSERT_TRUE(i < expectedResult.size());
    DocLine testLine = docsFileLineToDocLine(docsFileLine);

    // Not testing the whole tuples against each other to have a cleaner
    // indication what exactly caused the assertion to fail
    ASSERT_EQ(std::get<0>(testLine), std::get<0>(expectedResult.at(i)));
    ASSERT_EQ(std::get<1>(testLine), std::get<1>(expectedResult.at(i)));

    ++i;
  }
};

// Passing the testText as copy to make sure it stays alive during the usage of
// tokenizer
auto testTokenizeAndNormalizeText =
    [](std::string testText, const StringVec& normalizedTextAsVec,
       ad_utility::source_location loc =
           ad_utility::source_location::current()) {
      auto t = generateLocationTrace(loc);
      size_t i = 0;
      LocaleManager localeManager = getLocaleManager();
      for (auto normalizedWord :
           tokenizeAndNormalizeText(testText, localeManager)) {
        ASSERT_TRUE(i < normalizedTextAsVec.size());
        ASSERT_EQ(normalizedWord, normalizedTextAsVec.at(i));

        ++i;
      }
      ASSERT_EQ(i, normalizedTextAsVec.size());
    };

}  // namespace

TEST(WordsAndDocsFileParserTest, wordsFileParserTest) {
  char* locale = setlocale(LC_CTYPE, "");
  std::cout << "Set locale LC_CTYPE to: " << locale << std::endl;

  std::fstream f("_testtmp.contexts.tsv", std::ios_base::out);
  f << createWordsFileLineAsString("Foo", false, 0, 2)
    << createWordsFileLineAsString("foo", false, 0, 2)
    << createWordsFileLineAsString("Bär", true, 0, 1)
    << createWordsFileLineAsString("Äü", false, 0, 1)
    << createWordsFileLineAsString("X", false, 1, 1);
  f.close();

  WordLineVec expected = {{"foo", false, 0, 2},
                          {"foo", false, 0, 2},
                          {"Bär", true, 0, 1},
                          {"äü", false, 0, 1},
                          {"x", false, 1, 1}};

  testWordsFileParser("_testtmp.contexts.tsv", expected);
  remove("_testtmp.contexts.tsv");
};

TEST(WordsAndDocsFileParser, docsFileParserTest) {
  char* locale = setlocale(LC_CTYPE, "");
  std::cout << "Set locale LC_CTYPE to: " << locale << std::endl;

  std::fstream f("_testtmp.documents.tsv", std::ios_base::out);
  f << createDocsFileLineAsString(4, "This TeSt is OnlyCharcters")
    << createDocsFileLineAsString(7, "Wh4t h4pp3ns t0 num83rs")
    << createDocsFileLineAsString(8, "An( sp@ci*l ch.ar,:act=_er+s")
    << createDocsFileLineAsString(190293, "Large docId");
  f.close();

  DocLineVec expected = {{4, "This TeSt is OnlyCharcters"},
                         {7, "Wh4t h4pp3ns t0 num83rs"},
                         {8, "An( sp@ci*l ch.ar,:act=_er+s"},
                         {190293, "Large docId"}};

  testDocsFileParser("_testtmp.documents.tsv", expected);
  remove("_testtmp.documents.tsv");
}

TEST(TokenizeAndNormalizeText, tokenizeAndNormalizeTextTest) {
  char* locale = setlocale(LC_CTYPE, "");
  std::cout << "Set locale LC_CTYPE to: " << locale << std::endl;

  // Test 1
  testTokenizeAndNormalizeText("already normalized text",
                               {"already", "normalized", "text"});

  // Test 2
  testTokenizeAndNormalizeText("TeXt WITH UpperCASe",
                               {"text", "with", "uppercase"});

  // Test 3
  testTokenizeAndNormalizeText("41ph4num3r1c t3xt", {"41ph4num3r1c", "t3xt"});

  // Test 4
  testTokenizeAndNormalizeText(
      "test\twith\ndifferent,separators.here ,.\t",
      {"test", "with", "different", "separators", "here"});

  // Regression test for https://github.com/ad-freiburg/qlever/issues/2244
  testTokenizeAndNormalizeText("�", {});
}

// _____________________________________________________________________________
TEST(TokenizeAndNormalizeText, Unicode) {
  testTokenizeAndNormalizeText(
      "Äpfel über,affen\U0001F600háusen, ääädä\U0001F600blubä",
      {"äpfel", "über", "affen", "háusen", "ääädä", "blubä"});

  AD_EXPECT_THROW_WITH_MESSAGE(testTokenizeAndNormalizeText("\255", {}),
                               ::testing::HasSubstr("Invalid UTF-8"));
}

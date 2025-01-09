// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>

#include "./WordsAndDocsFileLineCreator.h"
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

auto getLocaleManager = []() { return LocaleManager("en", "US", false); };

auto wordsFileLineToWordLine = [](WordsFileLine wordsFileLine) -> WordLine {
  return std::make_tuple(wordsFileLine.word_, wordsFileLine.isEntity_,
                         static_cast<size_t>(wordsFileLine.contextId_.get()),
                         static_cast<size_t>(wordsFileLine.score_));
};

// Lambda that takes in a path to wordsFile to initialize the Parser and an
// expectedResult that is compared against the parsers outputs.
auto testWordsFileParser = [](std::string wordsFilePath,
                              WordLineVec expectedResult) {
  WordsFileParser p(wordsFilePath, getLocaleManager());
  size_t i = 0;
  for (auto wordsFileLine : p) {
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

auto docsFileLineToDocLine = [](DocsFileLine docsFileLine) -> DocLine {
  return std::make_tuple(static_cast<size_t>(docsFileLine.docId_.get()),
                         docsFileLine.docContent_);
};

// Same as testWordsFileParser but for docsFile
auto testDocsFileParser = [](std::string docsFilePath,
                             DocLineVec expectedResult) {
  DocsFileParser p(docsFilePath, getLocaleManager());
  size_t i = 0;
  for (auto docsFileLine : p) {
    ASSERT_TRUE(i < expectedResult.size());
    DocLine testLine = docsFileLineToDocLine(docsFileLine);

    // Not testing the whole tuples against each other to have a cleaner
    // indication what exactly caused the assertion to fail
    ASSERT_EQ(std::get<0>(testLine), std::get<0>(expectedResult.at(i)));
    ASSERT_EQ(std::get<1>(testLine), std::get<1>(expectedResult.at(i)));

    ++i;
  }
};

auto testTokenizeAndNormalizeText = [](std::string testText,
                                       StringVec normalizedTextAsVec) {
  TokenizeAndNormalizeText testTokenizer(testText, getLocaleManager());
  size_t i = 0;
  for (auto normalizedWord : testTokenizer) {
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

  WordLineVec expected = {
      std::make_tuple("foo", false, 0, 2), std::make_tuple("foo", false, 0, 2),
      std::make_tuple("Bär", true, 0, 1), std::make_tuple("äü", false, 0, 1),
      std::make_tuple("x", false, 1, 1)};

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

  DocLineVec expected = {std::make_pair(4, "This TeSt is OnlyCharcters"),
                         std::make_pair(7, "Wh4t h4pp3ns t0 num83rs"),
                         std::make_pair(8, "An( sp@ci*l ch.ar,:act=_er+s"),
                         std::make_pair(190293, "Large docId")};

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
}

//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/iostreams/filter/zlib.hpp>

#include "../WordsAndDocsFileLineCreator.h"
#include "../printers/VariablePrinters.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "./TextIndexScanTestHelpers.h"
#include "engine/IndexScan.h"
#include "engine/TextIndexScanForWord.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;
using ad_utility::source_location;
namespace h = textIndexScanTestHelpers;
using qlever::TextScoringMetric;

namespace {

std::string kg =
    "<a> <p> \"he failed the test\" . <a> <p> \"testing can help\" . <a> <p> "
    "\"some other sentence\" . <b> <p> \"the test on friday was really hard\" "
    ". <b> <x2> <x> . <b> <x2> <xb2> . <Astronomer> <is-a> <job> .";

std::string wordsFileContent =
    createWordsFileLineAsString("astronomer", false, 1, 1) +
    createWordsFileLineAsString("<Astronomer>", true, 1, 0) +
    createWordsFileLineAsString("scientist", false, 1, 1) +
    createWordsFileLineAsString("field", false, 1, 1) +
    createWordsFileLineAsString("astronomy", false, 1, 1) +
    createWordsFileLineAsString("astronomer", false, 2, 0) +
    createWordsFileLineAsString("<Astronomer>", true, 2, 0) +
    createWordsFileLineAsString(":s:firstsentence", false, 2, 0) +
    createWordsFileLineAsString("scientist", false, 2, 0) +
    createWordsFileLineAsString("field", false, 2, 0) +
    createWordsFileLineAsString("astronomy", false, 2, 0) +
    createWordsFileLineAsString("astronomy", false, 3, 1) +
    createWordsFileLineAsString("concentrates", false, 3, 1) +
    createWordsFileLineAsString("studies", false, 3, 1) +
    createWordsFileLineAsString("specific", false, 3, 1) +
    createWordsFileLineAsString("question", false, 3, 1) +
    createWordsFileLineAsString("outside", false, 3, 1) +
    createWordsFileLineAsString("scope", false, 3, 1) +
    createWordsFileLineAsString("earth", false, 3, 1) +
    createWordsFileLineAsString("astronomy", false, 4, 1) +
    createWordsFileLineAsString("concentrates", false, 4, 1) +
    createWordsFileLineAsString("studies", false, 4, 1) +
    createWordsFileLineAsString("field", false, 4, 1) +
    createWordsFileLineAsString("outside", false, 4, 1) +
    createWordsFileLineAsString("scope", false, 4, 1) +
    createWordsFileLineAsString("earth", false, 4, 1) +
    createWordsFileLineAsString("tester", false, 5, 1) +
    createWordsFileLineAsString("rockets", false, 5, 1) +
    createWordsFileLineAsString("astronomer", false, 5, 1) +
    createWordsFileLineAsString("<Astronomer>", true, 5, 0) +
    createWordsFileLineAsString("although", false, 5, 1) +
    createWordsFileLineAsString("astronomer", false, 6, 0) +
    createWordsFileLineAsString("<Astronomer>", true, 6, 0) +
    createWordsFileLineAsString("although", false, 6, 0) +
    createWordsFileLineAsString("<Astronomer>", true, 6, 0) +
    createWordsFileLineAsString("space", false, 6, 1) +
    createWordsFileLineAsString("<Astronomer>", true, 7, 0) +
    createWordsFileLineAsString("space", false, 7, 0) +
    createWordsFileLineAsString("earth", false, 7, 1);

std::string firstDocText =
    "An astronomer is a scientist in the field of "
    "astronomy who concentrates their studies on a "
    "specific question or field outside of the scope of "
    "Earth.";

std::string secondDocText =
    "The Tester of the rockets can be an astronomer "
    "too although they might not be in space but on "
    "earth.";

std::string docsFileContent = createDocsFileLineAsString(4, firstDocText) +
                              createDocsFileLineAsString(7, secondDocText);

std::pair<std::string, std::string> contentsOfWordsFileAndDocsFile = {
    wordsFileContent, docsFileContent};

// Combines the given string with first doc text
auto withFirst(const std::string& s) {
  return h::combineToString(firstDocText, s);
}

// Same but with second
auto withSecond(const std::string& s) {
  return h::combineToString(secondDocText, s);
}

// Struct to reduce code duplication
struct TextResult {
  QueryExecutionContext* qec_;
  const Result& result_;
  bool isPrefixSearch_ = true;
  bool scoreIsInt_ = true;

  auto getRow(size_t row) const {
    return h::combineToString(
        h::getTextRecordFromResultTable(qec_, result_, row),
        h::getWordFromResultTable(qec_, result_, row));
  }

  auto getId(size_t row) const {
    return h::getTextRecordIdFromResultTable(qec_, result_, row);
  }

  auto getTextRecord(size_t row) const {
    return h::getTextRecordFromResultTable(qec_, result_, row);
  }

  auto getWord(size_t row) const {
    return h::getWordFromResultTable(qec_, result_, row);
  }

  auto getScore(size_t row) const {
    return h::getScoreFromResultTable(qec_, result_, row, isPrefixSearch_,
                                      scoreIsInt_);
  }

  void checkListOfWords(const std::vector<std::string>& expectedWords,
                        size_t& startingIndex) const {
    for (const auto& word : expectedWords) {
      ASSERT_EQ(word, getWord(startingIndex));
      ++startingIndex;
    }
  }
};

// Return a `QueryExecutionContext` from the turtle `kg` (see above) that has a
// text index that contains the literals from the `kg` as well as the
// `contentsOfWordsFileAndDocsFile` (also above). The metrics used for the text
// scores can be specified.
auto getQecWithTextIndex(
    std::optional<TextScoringMetric> textScoring = std::nullopt) {
  using namespace ad_utility::testing;
  TestIndexConfig config{kg};
  config.createTextIndex = true;
  config.contentsOfWordsFileAndDocsfile = contentsOfWordsFileAndDocsFile;
  if (textScoring.has_value()) {
    config.scoringMetric = textScoring;
  }
  return getQec(std::move(config));
}

TEST(TextIndexScanForWord, TextScoringMetric) {
  using enum TextScoringMetric;
  using namespace qlever;
  ASSERT_EQ(getTextScoringMetricAsString(EXPLICIT), "explicit");
  ASSERT_EQ(getTextScoringMetricAsString(TFIDF), "tf-idf");
  ASSERT_EQ(getTextScoringMetricAsString(BM25), "bm25");
  ASSERT_EQ(getTextScoringMetricAsString(static_cast<TextScoringMetric>(999)),
            "explicit");

  ASSERT_EQ(getTextScoringMetricFromString("explicit"), EXPLICIT);
  ASSERT_EQ(getTextScoringMetricFromString("tf-idf"), TFIDF);
  ASSERT_EQ(getTextScoringMetricFromString("bm25"), BM25);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      getTextScoringMetricFromString("fail"),
      ::testing::StrEq(R"(Faulty text scoring metric given: "fail".)"),
      std::runtime_error);
}

TEST(TextIndexScanForWord, WordScanPrefix) {
  auto qec = getQecWithTextIndex();

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "test*"};
  TextIndexScanForWord s2{qec, Variable{"?text2"}, "test*"};

  // Test if size calculations are right
  ASSERT_EQ(s1.getResultWidth(), 3);

  auto result = s1.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTable().numColumns(), 3);
  ASSERT_EQ(result.idTable().size(), 4);
  s2.getExternallyVisibleVariableColumns();

  // Test if all columns are there and correct
  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?text2"}, {0, AlwaysDefined}},
      {Variable{"?ql_matchingword_text2_test"}, {1, AlwaysDefined}},
      {Variable{"?ql_score_prefix_text2_test"}, {2, AlwaysDefined}}};
  EXPECT_THAT(s2.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));

  // Tests if the correct texts are retrieved from a mix of non literal and
  // literal texts
  TextResult tr{qec, result};
  ASSERT_EQ(withSecond("tester"), tr.getRow(0));
  ASSERT_EQ(h::combineToString("\"he failed the test\"", "test"), tr.getRow(1));
  ASSERT_EQ(h::combineToString("\"testing can help\"", "testing"),
            tr.getRow(2));
  ASSERT_EQ(
      h::combineToString("\"the test on friday was really hard\"", "test"),
      tr.getRow(3));

  // Tests if the correct texts are retrieved from the non literal texts
  TextIndexScanForWord t1{qec, Variable{"?t1"}, "astronom*"};
  result = t1.computeResultOnlyForTesting();
  ASSERT_EQ(TextRecordIndex::make(1), tr.getId(0));
  ASSERT_EQ(firstDocText, tr.getTextRecord(0));
  ASSERT_EQ(TextRecordIndex::make(1), tr.getId(1));
  ASSERT_EQ(firstDocText, tr.getTextRecord(1));
  ASSERT_EQ(TextRecordIndex::make(2), tr.getId(2));
  ASSERT_EQ(firstDocText, tr.getTextRecord(2));
  ASSERT_EQ(TextRecordIndex::make(2), tr.getId(3));
  ASSERT_EQ(firstDocText, tr.getTextRecord(3));
  ASSERT_EQ(TextRecordIndex::make(3), tr.getId(4));
  ASSERT_EQ(firstDocText, tr.getTextRecord(4));
  ASSERT_EQ(TextRecordIndex::make(4), tr.getId(5));
  ASSERT_EQ(firstDocText, tr.getTextRecord(5));
  ASSERT_EQ(TextRecordIndex::make(5), tr.getId(6));
  ASSERT_EQ(secondDocText, tr.getTextRecord(6));
  ASSERT_EQ(TextRecordIndex::make(6), tr.getId(7));
  ASSERT_EQ(secondDocText, tr.getTextRecord(7));

  // Tests if correct words are deducted from prefix
  ASSERT_EQ("astronomer", tr.getWord(0));
  ASSERT_EQ("astronomy", tr.getWord(1));
  ASSERT_EQ("astronomer", tr.getWord(2));
  ASSERT_EQ("astronomy", tr.getWord(3));
  ASSERT_EQ("astronomy", tr.getWord(4));
  ASSERT_EQ("astronomy", tr.getWord(5));
  ASSERT_EQ("astronomer", tr.getWord(6));
  ASSERT_EQ("astronomer", tr.getWord(7));

  // Tests if the correct scores are retrieved from the non literal texts for
  // Explicit scores
  qec = getQecWithTextIndex(TextScoringMetric::EXPLICIT);

  TextIndexScanForWord score1{qec, Variable{"?t1"}, "astronom*"};
  auto scoreResultCount = score1.computeResultOnlyForTesting();
  auto tr1 = TextResult{qec, scoreResultCount, true};
  ASSERT_EQ(1, tr1.getScore(0));
  ASSERT_EQ(1, tr1.getScore(1));
  ASSERT_EQ(0, tr1.getScore(2));
  ASSERT_EQ(0, tr1.getScore(3));
  ASSERT_EQ(1, tr1.getScore(4));
  ASSERT_EQ(1, tr1.getScore(5));
  ASSERT_EQ(1, tr1.getScore(6));
  ASSERT_EQ(0, tr1.getScore(7));

  // Tests if the correct scores are retrieved from the non-literal texts for
  // TFIDF
  qec = getQecWithTextIndex(TextScoringMetric::TFIDF);
  TextIndexScanForWord score2{qec, Variable{"?t1"}, "astronom*"};
  auto scoreResultTFIDF = score2.computeResultOnlyForTesting();
  auto tr2 = TextResult{qec, scoreResultTFIDF, true, false};
  float tfidfWord1Doc4 = h::calculateTFIDFFromParameters(1, 2, 6);
  float tfidfWord1Doc7 = h::calculateTFIDFFromParameters(1, 2, 6);
  float tfidfWord2Doc4 = h::calculateTFIDFFromParameters(1, 1, 6);
  ASSERT_EQ(tfidfWord1Doc4, tr2.getScore(0));
  ASSERT_EQ(tfidfWord2Doc4, tr2.getScore(1));
  ASSERT_EQ(tfidfWord1Doc4, tr2.getScore(2));
  ASSERT_EQ(tfidfWord2Doc4, tr2.getScore(3));
  ASSERT_EQ(tfidfWord2Doc4, tr2.getScore(4));
  ASSERT_EQ(tfidfWord2Doc4, tr2.getScore(5));
  ASSERT_EQ(tfidfWord1Doc7, tr2.getScore(6));
  ASSERT_EQ(tfidfWord1Doc7, tr2.getScore(7));

  // Tests if the correct scores are retrieved from the non literal texts for
  // BM25
  qec = getQecWithTextIndex(TextScoringMetric::BM25);
  TextIndexScanForWord score3{qec, Variable{"?t1"}, "astronom*"};
  auto scoreResultBM25 = score3.computeResultOnlyForTesting();
  auto tr3 = TextResult{qec, scoreResultBM25, true, false};
  float bm25Word1Doc4 =
      h::calculateBM25FromParameters(1, 2, 6, 7, 15, 0.75, 1.75);
  float bm25Word1Doc7 =
      h::calculateBM25FromParameters(1, 2, 6, 7, 10, 0.75, 1.75);
  float bm25Word2Doc4 =
      h::calculateBM25FromParameters(1, 1, 6, 7, 15, 0.75, 1.75);
  ASSERT_EQ(bm25Word1Doc4, tr3.getScore(0));
  ASSERT_EQ(bm25Word2Doc4, tr3.getScore(1));
  ASSERT_EQ(bm25Word1Doc4, tr3.getScore(2));
  ASSERT_EQ(bm25Word2Doc4, tr3.getScore(3));
  ASSERT_EQ(bm25Word2Doc4, tr3.getScore(4));
  ASSERT_EQ(bm25Word2Doc4, tr3.getScore(5));
  ASSERT_EQ(bm25Word1Doc7, tr3.getScore(6));
  ASSERT_EQ(bm25Word1Doc7, tr3.getScore(7));
}

TEST(TextIndexScanForWord, WordScanShortPrefix) {
  auto qec = getQecWithTextIndex();

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "a*"};
  // Test if size calculations are right
  ASSERT_EQ(s1.getResultWidth(), 3);

  auto result = s1.computeResultOnlyForTesting();
  auto tr = TextResult{qec, result};
  ASSERT_EQ(result.idTable().numColumns(), 3);
  ASSERT_EQ(result.idTable().size(), 10);

  // Check if word and text are correctly retrieved
  ASSERT_EQ(withFirst("astronomer"), tr.getRow(0));
  ASSERT_EQ(withFirst("astronomy"), tr.getRow(1));
  ASSERT_EQ(withFirst("astronomer"), tr.getRow(2));
  ASSERT_EQ(withFirst("astronomy"), tr.getRow(3));
  ASSERT_EQ(withFirst("astronomy"), tr.getRow(4));
  ASSERT_EQ(withFirst("astronomy"), tr.getRow(5));
  ASSERT_EQ(withSecond("although"), tr.getRow(6));
  ASSERT_EQ(withSecond("astronomer"), tr.getRow(7));
  ASSERT_EQ(withSecond("although"), tr.getRow(8));
  ASSERT_EQ(withSecond("astronomer"), tr.getRow(9));
}

TEST(TextIndexScanForWord, WordScanStarPrefix) {
  auto qec = getQecWithTextIndex();

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "*"};
  ASSERT_EQ(s1.getResultWidth(), 3);

  auto result = s1.computeResultOnlyForTesting();
  auto tr = TextResult{qec, result};
  ASSERT_EQ(result.idTable().numColumns(), 3);
  ASSERT_EQ(result.idTable().size(), 50);

  // Things to look out for: Documents come before Literals. Literals are
  // Sorted wrt. the StringSortComparator. The both of them are classified as
  // contexts. In one context the words are ordered wrt. the
  // StringSortComparator.
  std::vector<std::string> words = {"astronomer",
                                    "astronomy",
                                    "field",
                                    "scientist",
                                    ":s:firstsentence",
                                    "astronomer",
                                    "astronomy",
                                    "field",
                                    "scientist",
                                    "astronomy",
                                    "concentrates",
                                    "earth",
                                    "outside",
                                    "question",
                                    "scope",
                                    "specific",
                                    "studies",
                                    "astronomy",
                                    "concentrates",
                                    "earth",
                                    "field",
                                    "outside",
                                    "scope",
                                    "studies",
                                    "although",
                                    "astronomer",
                                    "rockets",
                                    "tester",
                                    "although",
                                    "astronomer",
                                    "space",
                                    "earth",
                                    "space",
                                    "failed",
                                    "he",
                                    "test",
                                    "the",
                                    "other",
                                    "sentence",
                                    "some",
                                    "can",
                                    "help",
                                    "testing",
                                    "friday",
                                    "hard",
                                    "on",
                                    "really",
                                    "test",
                                    "the",
                                    "was"};
  size_t startingIndex = 0;
  tr.checkListOfWords(words, startingIndex);
}

TEST(TextIndexScanForWord, WordScanBasic) {
  auto qec = getQecWithTextIndex();

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "test"};

  ASSERT_EQ(s1.getResultWidth(), 2);

  auto result = s1.computeResultOnlyForTesting();
  auto tr1 = TextResult{qec, result, false};
  ASSERT_EQ(result.idTable().numColumns(), 2);
  ASSERT_EQ(result.idTable().size(), 2);

  ASSERT_EQ("\"he failed the test\"", tr1.getTextRecord(0));
  ASSERT_EQ("\"the test on friday was really hard\"", tr1.getTextRecord(1));

  TextIndexScanForWord s2{qec, Variable{"?text1"}, "testing"};

  ASSERT_EQ(s2.getResultWidth(), 2);

  result = s2.computeResultOnlyForTesting();
  auto tr2 = TextResult{qec, result, false};
  ASSERT_EQ(result.idTable().numColumns(), 2);
  ASSERT_EQ(result.idTable().size(), 1);

  ASSERT_EQ("\"testing can help\"", tr2.getTextRecord(0));

  TextIndexScanForWord s3{qec, Variable{"?text1"}, "tester"};

  ASSERT_EQ(s3.getResultWidth(), 2);

  result = s3.computeResultOnlyForTesting();
  auto tr3 = TextResult{qec, result, false};
  ASSERT_EQ(result.idTable().numColumns(), 2);
  ASSERT_EQ(result.idTable().size(), 1);

  ASSERT_EQ(secondDocText, tr3.getTextRecord(0));
}

TEST(TextIndexScanForWord, CacheKey) {
  auto qec = getQecWithTextIndex();

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "test*"};
  TextIndexScanForWord s2{qec, Variable{"?text2"}, "test*"};
  // Different text variables, same word (both with prefix)
  ASSERT_EQ(s1.getCacheKeyImpl(), s2.getCacheKeyImpl());

  TextIndexScanForWord s3{qec, Variable{"?text1"}, "test"};
  // Same text variable, different words (one with, one without prefix)
  ASSERT_NE(s1.getCacheKeyImpl(), s3.getCacheKeyImpl());

  TextIndexScanForWord s4{qec, Variable{"?text1"}, "tests"};
  // Same text variable, different words (both without prefix)
  ASSERT_NE(s3.getCacheKeyImpl(), s4.getCacheKeyImpl());

  TextIndexScanForWord s5{qec, Variable{"?text2"}, "tests"};
  // Different text variables, different words (both without prefix)
  ASSERT_NE(s3.getCacheKeyImpl(), s5.getCacheKeyImpl());
  // Different text variables, same words (both without prefix)
  ASSERT_EQ(s4.getCacheKeyImpl(), s5.getCacheKeyImpl());
}

TEST(TextIndexScanForWord, KnownEmpty) {
  auto qec = getQecWithTextIndex();

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "nonExistentWord*"};
  ASSERT_TRUE(s1.knownEmptyResult());

  TextIndexScanForWord s2{qec, Variable{"?text1"}, "nonExistentWord"};
  ASSERT_TRUE(s2.knownEmptyResult());

  TextIndexScanForWord s3{qec, Variable{"?text1"}, "test"};
  ASSERT_TRUE(!s3.knownEmptyResult());

  TextIndexScanForWord s4{qec, Variable{"?text1"}, "test*"};
  ASSERT_TRUE(!s4.knownEmptyResult());

  TextIndexScanForWord s5{qec, Variable{"?text1"}, "testing"};
  ASSERT_TRUE(!s5.knownEmptyResult());
}

// _____________________________________________________________________________
TEST(TextIndexScanForWord, clone) {
  auto qec = getQec();

  TextIndexScanForWord scan{qec, Variable{"?text1"}, "nonExistentWord*"};

  auto clone = scan.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(scan, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), scan.getDescriptor());
}
}  // namespace

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

TEST(TextIndexScanForWord, TextScoringMetric) {
  using enum TextScoringMetric;
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
  auto qec = getQec(kg, true, true, true, 16_B, true, true,
                    contentsOfWordsFileAndDocsFile);

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
  ASSERT_EQ(h::combineToString(secondDocText, "tester"),
            h::combineToString(h::getTextRecordFromResultTable(qec, result, 0),
                               h::getWordFromResultTable(qec, result, 0)));

  ASSERT_EQ(h::combineToString("\"he failed the test\"", "test"),
            h::combineToString(h::getTextRecordFromResultTable(qec, result, 1),
                               h::getWordFromResultTable(qec, result, 1)));
  ASSERT_EQ(h::combineToString("\"testing can help\"", "testing"),
            h::combineToString(h::getTextRecordFromResultTable(qec, result, 2),
                               h::getWordFromResultTable(qec, result, 2)));
  ASSERT_EQ(
      h::combineToString("\"the test on friday was really hard\"", "test"),
      h::combineToString(h::getTextRecordFromResultTable(qec, result, 3),
                         h::getWordFromResultTable(qec, result, 3)));

  // Tests if the correct texts are retrieved from the non literal texts
  TextIndexScanForWord t1{qec, Variable{"?t1"}, "astronom*"};
  auto tresult = t1.computeResultOnlyForTesting();
  ASSERT_EQ(TextRecordIndex::make(1),
            h::getTextRecordIdFromResultTable(qec, tresult, 0));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 0));
  ASSERT_EQ(TextRecordIndex::make(1),
            h::getTextRecordIdFromResultTable(qec, tresult, 1));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 1));
  ASSERT_EQ(TextRecordIndex::make(2),
            h::getTextRecordIdFromResultTable(qec, tresult, 2));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 2));
  ASSERT_EQ(TextRecordIndex::make(2),
            h::getTextRecordIdFromResultTable(qec, tresult, 3));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 3));
  ASSERT_EQ(TextRecordIndex::make(3),
            h::getTextRecordIdFromResultTable(qec, tresult, 4));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 4));
  ASSERT_EQ(TextRecordIndex::make(4),
            h::getTextRecordIdFromResultTable(qec, tresult, 5));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 5));
  ASSERT_EQ(TextRecordIndex::make(5),
            h::getTextRecordIdFromResultTable(qec, tresult, 6));
  ASSERT_EQ(secondDocText, h::getTextRecordFromResultTable(qec, tresult, 6));
  ASSERT_EQ(TextRecordIndex::make(6),
            h::getTextRecordIdFromResultTable(qec, tresult, 7));
  ASSERT_EQ(secondDocText, h::getTextRecordFromResultTable(qec, tresult, 7));

  // Tests if correct words are deducted from prefix
  ASSERT_EQ("astronomer", h::getWordFromResultTable(qec, tresult, 0));
  ASSERT_EQ("astronomy", h::getWordFromResultTable(qec, tresult, 1));
  ASSERT_EQ("astronomer", h::getWordFromResultTable(qec, tresult, 2));
  ASSERT_EQ("astronomy", h::getWordFromResultTable(qec, tresult, 3));
  ASSERT_EQ("astronomy", h::getWordFromResultTable(qec, tresult, 4));
  ASSERT_EQ("astronomy", h::getWordFromResultTable(qec, tresult, 5));
  ASSERT_EQ("astronomer", h::getWordFromResultTable(qec, tresult, 6));
  ASSERT_EQ("astronomer", h::getWordFromResultTable(qec, tresult, 7));

  // Tests if the correct scores are retrieved from the non literal texts for
  // Explicit scores
  qec =
      getQec(kg, true, true, true, 16_B, true, true,
             contentsOfWordsFileAndDocsFile, 1_kB, TextScoringMetric::EXPLICIT);

  TextIndexScanForWord score1{qec, Variable{"?t1"}, "astronom*"};
  auto scoreResultCount = score1.computeResultOnlyForTesting();
  ASSERT_EQ(1, h::getScoreFromResultTable(qec, scoreResultCount, 0, true));
  ASSERT_EQ(1, h::getScoreFromResultTable(qec, scoreResultCount, 1, true));
  ASSERT_EQ(0, h::getScoreFromResultTable(qec, scoreResultCount, 2, true));
  ASSERT_EQ(0, h::getScoreFromResultTable(qec, scoreResultCount, 3, true));
  ASSERT_EQ(1, h::getScoreFromResultTable(qec, scoreResultCount, 4, true));
  ASSERT_EQ(1, h::getScoreFromResultTable(qec, scoreResultCount, 5, true));
  ASSERT_EQ(1, h::getScoreFromResultTable(qec, scoreResultCount, 6, true));
  ASSERT_EQ(0, h::getScoreFromResultTable(qec, scoreResultCount, 7, true));

  // Tests if the correct scores are retrieved from the non literal texts for
  // TFIDF
  qec = getQec(kg, true, true, true, 16_B, true, true,
               contentsOfWordsFileAndDocsFile, 1_kB, TextScoringMetric::TFIDF);
  TextIndexScanForWord score2{qec, Variable{"?t1"}, "astronom*"};
  auto scoreResultTFIDF = score2.computeResultOnlyForTesting();
  float tfidfWord1Doc4 = h::calculateTFIDFFromParameters(1, 2, 6);
  float tfidfWord1Doc7 = h::calculateTFIDFFromParameters(1, 2, 6);
  float tfidfWord2Doc4 = h::calculateTFIDFFromParameters(1, 1, 6);
  ASSERT_EQ(tfidfWord1Doc4,
            h::getScoreFromResultTable(qec, scoreResultTFIDF, 0, true, false));
  ASSERT_EQ(tfidfWord2Doc4,
            h::getScoreFromResultTable(qec, scoreResultTFIDF, 1, true, false));
  ASSERT_EQ(tfidfWord1Doc4,
            h::getScoreFromResultTable(qec, scoreResultTFIDF, 2, true, false));
  ASSERT_EQ(tfidfWord2Doc4,
            h::getScoreFromResultTable(qec, scoreResultTFIDF, 3, true, false));
  ASSERT_EQ(tfidfWord2Doc4,
            h::getScoreFromResultTable(qec, scoreResultTFIDF, 4, true, false));
  ASSERT_EQ(tfidfWord2Doc4,
            h::getScoreFromResultTable(qec, scoreResultTFIDF, 5, true, false));
  ASSERT_EQ(tfidfWord1Doc7,
            h::getScoreFromResultTable(qec, scoreResultTFIDF, 6, true, false));
  ASSERT_EQ(tfidfWord1Doc7,
            h::getScoreFromResultTable(qec, scoreResultTFIDF, 7, true, false));

  // Tests if the correct scores are retrieved from the non literal texts for
  // BM25
  qec = getQec(kg, true, true, true, 16_B, true, true,
               contentsOfWordsFileAndDocsFile, 1_kB, TextScoringMetric::BM25);
  TextIndexScanForWord score3{qec, Variable{"?t1"}, "astronom*"};
  auto scoreResultBM25 = score3.computeResultOnlyForTesting();
  float bm25Word1Doc4 =
      h::calculateBM25FromParameters(1, 2, 6, 7, 15, 0.75, 1.75);
  float bm25Word1Doc7 =
      h::calculateBM25FromParameters(1, 2, 6, 7, 10, 0.75, 1.75);
  float bm25Word2Doc4 =
      h::calculateBM25FromParameters(1, 1, 6, 7, 15, 0.75, 1.75);
  ASSERT_EQ(bm25Word1Doc4,
            h::getScoreFromResultTable(qec, scoreResultBM25, 0, true, false));
  ASSERT_EQ(bm25Word2Doc4,
            h::getScoreFromResultTable(qec, scoreResultBM25, 1, true, false));
  ASSERT_EQ(bm25Word1Doc4,
            h::getScoreFromResultTable(qec, scoreResultBM25, 2, true, false));
  ASSERT_EQ(bm25Word2Doc4,
            h::getScoreFromResultTable(qec, scoreResultBM25, 3, true, false));
  ASSERT_EQ(bm25Word2Doc4,
            h::getScoreFromResultTable(qec, scoreResultBM25, 4, true, false));
  ASSERT_EQ(bm25Word2Doc4,
            h::getScoreFromResultTable(qec, scoreResultBM25, 5, true, false));
  ASSERT_EQ(bm25Word1Doc7,
            h::getScoreFromResultTable(qec, scoreResultBM25, 6, true, false));
  ASSERT_EQ(bm25Word1Doc7,
            h::getScoreFromResultTable(qec, scoreResultBM25, 7, true, false));
}

TEST(TextIndexScanForWord, WordScanBasic) {
  auto qec = getQec(kg, true, true, true, 16_B, true, true,
                    contentsOfWordsFileAndDocsFile);

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "test"};

  ASSERT_EQ(s1.getResultWidth(), 2);

  auto result = s1.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTable().numColumns(), 2);
  ASSERT_EQ(result.idTable().size(), 2);

  ASSERT_EQ("\"he failed the test\"",
            h::getTextRecordFromResultTable(qec, result, 0));
  ASSERT_EQ("\"the test on friday was really hard\"",
            h::getTextRecordFromResultTable(qec, result, 1));

  TextIndexScanForWord s2{qec, Variable{"?text1"}, "testing"};

  ASSERT_EQ(s2.getResultWidth(), 2);

  result = s2.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTable().numColumns(), 2);
  ASSERT_EQ(result.idTable().size(), 1);

  ASSERT_EQ("\"testing can help\"",
            h::getTextRecordFromResultTable(qec, result, 0));

  TextIndexScanForWord s3{qec, Variable{"?text1"}, "tester"};

  ASSERT_EQ(s3.getResultWidth(), 2);

  result = s3.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTable().numColumns(), 2);
  ASSERT_EQ(result.idTable().size(), 1);

  ASSERT_EQ(secondDocText, h::getTextRecordFromResultTable(qec, result, 0));
}

TEST(TextIndexScanForWord, CacheKey) {
  auto qec = getQec(kg, true, true, true, 16_B, true, true,
                    contentsOfWordsFileAndDocsFile);

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
  auto qec = getQec(kg, true, true, true, 16_B, true, true,
                    contentsOfWordsFileAndDocsFile);

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

//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../printers/VariablePrinters.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
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

TEST(TextIndexScanForWord, WordScanPrefix) {
  std::string wordsFileContent;
  wordsFileContent =
      "astronomer\t0\t1\t1\n"
      "<Astronomer>\t1\t1\t0\n"
      "scientist\t0\t1\t1\n"
      "field\t0\t1\t1\n"
      "astronomy\t0\t1\t1\n"
      "astronomer\t0\t2\t0\n"
      "<Astronomer>\t1\t2\t0\n"
      ":s:firstsentence\t0\t2\t0\n"
      "scientist\t0\t2\t0\n"
      "field\t0\t2\t0\n"
      "astronomy\t0\t2\t0\n"
      "astronomy\t0\t3\t1\n"
      "concentrates\t0\t3\t1\n"
      "studies\t0\t3\t1\n"
      "specific\t0\t3\t1\n"
      "question\t0\t3\t1\n"
      "outside\t0\t3\t1\n"
      "scope\t0\t3\t1\n"
      "earth\t0\t3\t1\n"
      "astronomy\t0\t4\t1\n"
      "concentrates\t0\t4\t1\n"
      "studies\t0\t4\t1\n"
      "field\t0\t4\t1\n"
      "outside\t0\t4\t1\n"
      "scope\t0\t4\t1\n"
      "earth\t0\t4\t1\n";
  std::string docsFileContent;
  docsFileContent =
      "4\tAn astronomer is a scientist in the field of astronomy who "
      "concentrates their studies on a specific question or field outside of "
      "the scope of Earth.\n";
  auto qec = getQec(kg, true, true, true, 16_B, true, true, wordsFileContent,
                    docsFileContent);

  TextIndexScanForWord t1{qec, Variable{"?t1"}, "astronom*"};
  auto tresult = t1.computeResultOnlyForTesting();
  ASSERT_EQ(
      "An astronomer is a scientist in the field of astronomy who concentrates "
      "their studies on a specific question or field outside of the scope of "
      "Earth.",
      h::getTextExcerptFromResultTable(qec, tresult, 0));

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "test*"};
  TextIndexScanForWord s2{qec, Variable{"?text2"}, "test*"};

  ASSERT_EQ(s1.getResultWidth(), 3);

  auto result = s1.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTable().numColumns(), 3);
  ASSERT_EQ(result.idTable().size(), 3);
  s2.getExternallyVisibleVariableColumns();

  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?text2"}, {0, AlwaysDefined}},
      {Variable{"?ql_matchingword_text2_test"}, {1, AlwaysDefined}},
      {Variable{"?ql_score_prefix_text2_test"}, {2, AlwaysDefined}}};
  EXPECT_THAT(s2.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));

  ASSERT_EQ(h::combineToString("\"he failed the test\"", "test"),
            h::combineToString(h::getTextRecordFromResultTable(qec, result, 0),
                               h::getWordFromResultTable(qec, result, 0)));
  ASSERT_EQ(h::combineToString("\"testing can help\"", "testing"),
            h::combineToString(h::getTextRecordFromResultTable(qec, result, 1),
                               h::getWordFromResultTable(qec, result, 1)));
  ASSERT_EQ(
      h::combineToString("\"the test on friday was really hard\"", "test"),
      h::combineToString(h::getTextRecordFromResultTable(qec, result, 2),
                         h::getWordFromResultTable(qec, result, 2)));
}

TEST(TextIndexScanForWord, WordScanBasic) {
  auto qec = getQec(kg, true, true, true, 16_B, true);

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
}

TEST(TextIndexScanForWord, CacheKey) {
  auto qec = getQec(kg, true, true, true, 16_B, true);

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
  auto qec = getQec(kg, true, true, true, 16_B, true);

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
}  // namespace

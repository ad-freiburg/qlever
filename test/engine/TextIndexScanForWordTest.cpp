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

std::string wordsFileContent =
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
    "earth\t0\t4\t1\n"
    "tester\t0\t5\t1\n"
    "rockets\t0\t5\t1\n"
    "astronomer\t0\t5\t1\n"
    "<Astronomer>\t1\t5\t0\n"
    "although\t0\t5\t1\n"
    "astronomer\t0\t6\t0\n"
    "<Astronomer>\t1\t6\t0\n"
    "although\t0\t6\t0\n"
    "<Astronomer>\t1\t6\t0\n"
    "space\t0\t6\t1\n"
    "<Astronomer>\t1\t7\t0\n"
    "space\t0\t7\t0\n"
    "earth\t0\t7\t1\n";

std::string docsFileContent =
    "4\tAn astronomer is a scientist in the field of astronomy who "
    "concentrates their studies on a specific question or field outside of "
    "the scope of Earth.\n"
    "7\tThe Tester of the rockets can be an astronomer too although they "
    "might not be in space but on earth.\n";

std::string firstDocText =
    "An astronomer is a scientist in the field of "
    "astronomy who concentrates their studies on a "
    "specific question or field outside of the scope of "
    "Earth.";

std::string secondDocText =
    "The Tester of the rockets can be an astronomer "
    "too although they might not be in space but on "
    "earth.";

TEST(TextIndexScanForWord, WordScanPrefix) {
  auto qec = getQec(kg, true, true, true, 16_B, true, true, wordsFileContent,
                    docsFileContent);

  TextIndexScanForWord t1{qec, Variable{"?t1"}, "astronom*"};
  auto tresult = t1.computeResultOnlyForTesting();
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 0));
  ASSERT_EQ(TextRecordIndex::make(1),
            h::getTextRecordIdFromResultTable(qec, tresult, 0));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 1));
  ASSERT_EQ(TextRecordIndex::make(1),
            h::getTextRecordIdFromResultTable(qec, tresult, 1));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 2));
  ASSERT_EQ(TextRecordIndex::make(2),
            h::getTextRecordIdFromResultTable(qec, tresult, 2));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 3));
  ASSERT_EQ(TextRecordIndex::make(2),
            h::getTextRecordIdFromResultTable(qec, tresult, 3));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 4));
  ASSERT_EQ(TextRecordIndex::make(3),
            h::getTextRecordIdFromResultTable(qec, tresult, 4));
  ASSERT_EQ(firstDocText, h::getTextRecordFromResultTable(qec, tresult, 5));
  ASSERT_EQ(TextRecordIndex::make(4),
            h::getTextRecordIdFromResultTable(qec, tresult, 5));
  ASSERT_EQ(secondDocText, h::getTextRecordFromResultTable(qec, tresult, 6));
  ASSERT_EQ(TextRecordIndex::make(5),
            h::getTextRecordIdFromResultTable(qec, tresult, 6));
  ASSERT_EQ(secondDocText, h::getTextRecordFromResultTable(qec, tresult, 7));
  ASSERT_EQ(TextRecordIndex::make(6),
            h::getTextRecordIdFromResultTable(qec, tresult, 7));

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "test*"};
  TextIndexScanForWord s2{qec, Variable{"?text2"}, "test*"};

  ASSERT_EQ(s1.getResultWidth(), 3);

  auto result = s1.computeResultOnlyForTesting();
  ASSERT_EQ(result.idTable().numColumns(), 3);
  ASSERT_EQ(result.idTable().size(), 4);
  s2.getExternallyVisibleVariableColumns();

  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?text2"}, {0, AlwaysDefined}},
      {Variable{"?ql_matchingword_text2_test"}, {1, AlwaysDefined}},
      {Variable{"?ql_score_prefix_text2_test"}, {2, AlwaysDefined}}};
  EXPECT_THAT(s2.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));

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
}

TEST(TextIndexScanForWord, WordScanBasic) {
  auto qec = getQec(kg, true, true, true, 16_B, true, true, wordsFileContent,
                    docsFileContent);

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
  auto qec = getQec(kg, true, true, true, 16_B, true, true, wordsFileContent,
                    docsFileContent);

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
  auto qec = getQec(kg, true, true, true, 16_B, true, true, wordsFileContent,
                    docsFileContent);

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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../IndexTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
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
    ". <b> <x2> <x> . <b> <x2> <xb2> .";

TEST(TextIndexScanForWord, WordScanPrefix) {
  auto qec = getQec(kg, true, true, true, 16_B, true);

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "test*"};
  TextIndexScanForWord s2{qec, Variable{"?text2"}, "test*"};

  ASSERT_EQ(s1.getResultWidth(), 2);

  auto result = s1.computeResultOnlyForTesting();
  ASSERT_EQ(result.width(), 2);
  ASSERT_EQ(result.size(), 3);
  s2.getExternallyVisibleVariableColumns();

  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?text2"}, {0, AlwaysDefined}},
      {Variable{"?ql_matchingword_text2_test"}, {1, AlwaysDefined}}};
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

  ASSERT_EQ(s1.getResultWidth(), 1);

  auto result = s1.computeResultOnlyForTesting();
  ASSERT_EQ(result.width(), 1);
  ASSERT_EQ(result.size(), 2);

  ASSERT_EQ("\"he failed the test\"",
            h::getTextRecordFromResultTable(qec, result, 0));
  ASSERT_EQ("\"the test on friday was really hard\"",
            h::getTextRecordFromResultTable(qec, result, 1));
}

TEST(TextIndexScanForWord, CacheKey) {
  auto qec = getQec(kg, true, true, true, 16_B, true);

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "test*"};
  TextIndexScanForWord s2{qec, Variable{"?text2"}, "test*"};
  ASSERT_EQ(s1.getCacheKeyImpl(), s2.getCacheKeyImpl());

  TextIndexScanForWord s3{qec, Variable{"?text1"}, "test"};
  ASSERT_TRUE(s1.getCacheKeyImpl() != s3.getCacheKeyImpl());
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
}
}  // namespace

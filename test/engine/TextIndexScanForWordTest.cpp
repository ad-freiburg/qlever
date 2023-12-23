#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../IndexTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "engine/IndexScan.h"
#include "engine/TextIndexScanForWord.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;
using ad_utility::source_location;

// NOTE: this function exploits a "lucky accindent" that allows us to
// obtain the textRecord using idToOptionalString.
// TODO: Implement a more elegant/stable version
string getTextRecordFromResultTable(const QueryExecutionContext* qec,
                                    const ResultTable& result,
                                    const size_t& rowIndex) {
  return qec->getIndex()
      .idToOptionalString(
          result.idTable().getColumn(0)[rowIndex].getVocabIndex())
      .value();
}

string getWordFromResultTable(const QueryExecutionContext* qec,
                              const ResultTable& result,
                              const size_t& rowIndex) {
  return qec->getIndex()
      .idToOptionalString(
          result.idTable().getColumn(1)[rowIndex].getWordVocabIndex())
      .value();
}

string combineToString(const string& text, const string& word) {
  std::stringstream ss;
  ss << "Text: " << text << ", Word: " << word << endl;
  return ss.str();
}

TEST(TextIndexScanForWord, WordScanBasic) {
  auto qec = getQec(
      "<a> <p> \"he failed the test\" . <a> <p> \"testing can help\" . <a> <p> "
      "\"some other sentence\" . <b> <p> \"the test on friday was really "
      "hard\" . <b> <x2> <x> . <b> <x2> <xb2> .",
      true, true, true, 16_B, true);

  TextIndexScanForWord s1{qec, Variable{"?text1"}, "test*"};
  TextIndexScanForWord s2{qec, Variable{"?text2"}, "test*"};

  ASSERT_EQ(s1.getCacheKeyImpl(), s2.getCacheKeyImpl());
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

  ASSERT_EQ(combineToString("\"he failed the test\"", "test"),
            combineToString(getTextRecordFromResultTable(qec, result, 0),
                            getWordFromResultTable(qec, result, 0)));
  ASSERT_EQ(combineToString("\"testing can help\"", "testing"),
            combineToString(getTextRecordFromResultTable(qec, result, 1),
                            getWordFromResultTable(qec, result, 1)));
  ASSERT_EQ(combineToString("\"the test on friday was really hard\"", "test"),
            combineToString(getTextRecordFromResultTable(qec, result, 2),
                            getWordFromResultTable(qec, result, 2)));

  TextIndexScanForWord s3{qec, Variable{"?text1"}, "test"};

  ASSERT_TRUE(s1.getCacheKeyImpl() != s3.getCacheKeyImpl());
  ASSERT_EQ(s3.getResultWidth(), 1);

  result = s3.computeResultOnlyForTesting();
  ASSERT_EQ(result.width(), 1);
  ASSERT_EQ(result.size(), 2);

  ASSERT_EQ("\"he failed the test\"",
            getTextRecordFromResultTable(qec, result, 0));
  ASSERT_EQ("\"the test on friday was really hard\"",
            getTextRecordFromResultTable(qec, result, 1));
}

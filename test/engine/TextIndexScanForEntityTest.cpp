#include <gtest/gtest.h>

#include "../IndexTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "engine/IndexScan.h"
#include "engine/TextIndexScanForEntity.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;
using ad_utility::source_location;

string getTextFromResultTable(const QueryExecutionContext* qec,
                              const ResultTable& result,
                              const size_t& rowIndex) {
  return qec->getIndex()
      .idToOptionalString(
          result.idTable().getColumn(0)[rowIndex].getVocabIndex())
      .value();
}

string getEntityFromResultTable(const QueryExecutionContext* qec,
                                const ResultTable& result,
                                const size_t& rowIndex) {
  return qec->getIndex()
      .idToOptionalString(
          result.idTable().getColumn(1)[rowIndex].getVocabIndex())
      .value();
}

TEST(TextIndexScanForEntity, EntityScanBasic) {
  auto qec = getQec(
      "<a> <p> \"he failed the test\" . <b> <p> \"some other "
      "sentence\" . <a> <p> \"testing can help\" . <b> <p> \"the test on "
      "friday was really hard\" . <b> <x2> <x> . <b> <x2> <xb2> .",
      true, true, true, 16_B, true);

  TextIndexScanForEntity s1{qec, Variable{"?text"}, Variable{"?entityVar"},
                            "test*"};
  TextIndexScanForEntity s2{qec, Variable{"?text2"}, Variable{"?entityVar2"},
                            "test*"};
  ASSERT_EQ(s1.getCacheKeyImpl(), s2.getCacheKeyImpl());
  ASSERT_EQ(s1.getResultWidth(), 3);

  auto result = s1.computeResultOnlyForTesting();
  ASSERT_EQ(result.width(), 3);
  ASSERT_EQ(result.size(), 3);

  // NOTE: because of the way the graph above is constructed, the entities are
  // texts
  ASSERT_EQ("\"he failed the test\"", getEntityFromResultTable(qec, result, 0));
  ASSERT_EQ("\"testing can help\"", getEntityFromResultTable(qec, result, 1));
  ASSERT_EQ("\"the test on friday was really hard\"",
            getEntityFromResultTable(qec, result, 2));

  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?text2"}, {0, AlwaysDefined}},
      {Variable{"?entityVar2"}, {1, AlwaysDefined}},
      {Variable{"?ql_score_text2_var_entityVar2"}, {2, AlwaysDefined}}};
  EXPECT_THAT(s2.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));

  // fixed entity case
  string fixedEntity = "\"some other sentence\"";
  TextIndexScanForEntity s3{qec, Variable{"?text3"}, fixedEntity, "sentence"};

  result = s3.computeResultOnlyForTesting();
  ASSERT_EQ(s3.getResultWidth(), 2);
  ASSERT_EQ(result.width(), 2);
  ASSERT_EQ(result.size(), 1);

  ASSERT_TRUE(s1.getCacheKeyImpl() != s3.getCacheKeyImpl());

  expectedVariables = {
      {Variable{"?text3"}, {0, AlwaysDefined}},
      {Variable{
           "?ql_score_text3_fixedEntity__34_some_32_other_32_sentence_34_"},
       {1, AlwaysDefined}}};
  EXPECT_THAT(s3.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));

  ASSERT_EQ(fixedEntity, getTextFromResultTable(qec, result, 0));

  fixedEntity = "\"new entity\"";
  TextIndexScanForEntity s4{qec, Variable{"?text4"}, fixedEntity, "sentence"};
  ASSERT_TRUE(s3.getCacheKeyImpl() != s4.getCacheKeyImpl());

  fixedEntity = "\"he failed the test\"";
  TextIndexScanForEntity s5{qec, Variable{"?text5"}, fixedEntity, "test*"};
  result = s5.computeResultOnlyForTesting();
  ASSERT_EQ(result.width(), 2);
  ASSERT_EQ(result.size(), 1);

  ASSERT_EQ(fixedEntity, getTextFromResultTable(qec, result, 0));
}

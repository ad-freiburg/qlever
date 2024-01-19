//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../IndexTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "./TextIndexScanTestHelpers.h"
#include "engine/IndexScan.h"
#include "engine/TextIndexScanForEntity.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;
using ad_utility::source_location;
namespace h = textIndexScanTestHelpers;

namespace {
std::string kg =
    "<a> <p> \"he failed the test\" . <a> <p> \"testing can help\" . <a> <p> "
    "\"some other sentence\" . <b> <p> \"the test on friday was really hard\" "
    ". <b> <x2> <x> . <b> <x2> <xb2> .";

TEST(TextIndexScanForEntity, EntityScanBasic) {
  auto qec = getQec(kg, true, true, true, 16_B, true);

  TextIndexScanForEntity s1{qec, Variable{"?text"}, Variable{"?entityVar"},
                            "test*"};
  TextIndexScanForEntity s2{qec, Variable{"?text2"}, Variable{"?entityVar2"},
                            "test*"};
  ASSERT_EQ(s1.getResultWidth(), 3);

  auto result = s1.computeResultOnlyForTesting();
  ASSERT_EQ(result.width(), 3);
  ASSERT_EQ(result.size(), 3);

  // NOTE: because of the way the graph above is constructed, the entities are
  // texts
  ASSERT_EQ("\"he failed the test\"",
            h::getEntityFromResultTable(qec, result, 0));
  ASSERT_EQ("\"testing can help\"",
            h::getEntityFromResultTable(qec, result, 1));
  ASSERT_EQ("\"the test on friday was really hard\"",
            h::getEntityFromResultTable(qec, result, 2));

  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?text2"}, {0, AlwaysDefined}},
      {Variable{"?entityVar2"}, {1, AlwaysDefined}},
      {Variable{"?ql_score_text2_var_entityVar2"}, {2, AlwaysDefined}}};
  EXPECT_THAT(s2.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
}

TEST(TextIndexScanForEntity, FixedEntityScan) {
  auto qec = getQec(kg, true, true, true, 16_B, true);

  string fixedEntity = "\"some other sentence\"";
  TextIndexScanForEntity s3{qec, Variable{"?text3"}, fixedEntity, "sentence"};

  auto result = s3.computeResultOnlyForTesting();
  ASSERT_EQ(s3.getResultWidth(), 2);
  ASSERT_EQ(result.width(), 2);
  ASSERT_EQ(result.size(), 1);

  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables = {
      {Variable{"?text3"}, {0, AlwaysDefined}},
      {Variable{
           "?ql_score_text3_fixedEntity__34_some_32_other_32_sentence_34_"},
       {1, AlwaysDefined}}};
  EXPECT_THAT(s3.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));

  ASSERT_EQ(fixedEntity, h::getTextRecordFromResultTable(qec, result, 0));

  fixedEntity = "\"he failed the test\"";
  TextIndexScanForEntity s4{qec, Variable{"?text4"}, fixedEntity, "test*"};
  result = s4.computeResultOnlyForTesting();
  ASSERT_EQ(result.width(), 2);
  ASSERT_EQ(result.size(), 1);

  ASSERT_EQ(fixedEntity, h::getTextRecordFromResultTable(qec, result, 0));
}

TEST(TextIndexScanForEntity, CacheKeys) {
  auto qec = getQec(kg, true, true, true, 16_B, true);

  TextIndexScanForEntity s1{qec, Variable{"?text"}, Variable{"?entityVar"},
                            "test*"};
  TextIndexScanForEntity s2{qec, Variable{"?text2"}, Variable{"?entityVar2"},
                            "test*"};
  // Different text vars, different entity vars, same word (both with prefix)
  ASSERT_EQ(s1.getCacheKeyImpl(), s2.getCacheKeyImpl());

  TextIndexScanForEntity s3{qec, Variable{"?text3"}, Variable{"?entityVar"},
                            "test"};
  // Different text vars, same entity var, different words (one with, one
  // without prefix)
  ASSERT_NE(s1.getCacheKeyImpl(), s3.getCacheKeyImpl());

  TextIndexScanForEntity s4{qec, Variable{"?text4"}, Variable{"?entityVar"},
                            "sentence*"};
  // Different text vars, same entity var, different words (both with prefix)
  ASSERT_NE(s1.getCacheKeyImpl(), s4.getCacheKeyImpl());

  // fixed entity case
  string fixedEntity = "\"some other sentence\"";
  TextIndexScanForEntity s5{qec, Variable{"?text3"}, fixedEntity, "sentence"};
  // Same text var, different entities (one entity var, one fixed entity), same
  // word
  ASSERT_NE(s3.getCacheKeyImpl(), s5.getCacheKeyImpl());

  TextIndexScanForEntity s6{qec, Variable{"?text6"}, fixedEntity, "sentence"};
  // Different text vars, same fixed entity, same word
  ASSERT_EQ(s5.getCacheKeyImpl(), s6.getCacheKeyImpl());

  string newFixedEntity = "\"he failed the test\"";
  TextIndexScanForEntity s7{qec, Variable{"?text7"}, newFixedEntity,
                            "sentence"};
  // Different text vars, different fixed entities, same word
  ASSERT_NE(s5.getCacheKeyImpl(), s7.getCacheKeyImpl());

  TextIndexScanForEntity s8{qec, Variable{"?text7"}, newFixedEntity,
                            "sentences"};
  // Same text var, same fixed entitiy, different words
  ASSERT_NE(s7.getCacheKeyImpl(), s8.getCacheKeyImpl());
}

TEST(TextIndexScanForEntity, KnownEmpty) {
  auto qec = getQec(kg, true, true, true, 16_B, true);

  TextIndexScanForEntity s1{qec, Variable{"?text"}, Variable{"?entityVar"},
                            "nonExistentWord*"};
  ASSERT_TRUE(s1.knownEmptyResult());

  string fixedEntity = "\"non existent entity\"";
  AD_EXPECT_THROW_WITH_MESSAGE(
      TextIndexScanForEntity(qec, Variable{"?text"}, fixedEntity, "test*"),
      ::testing::ContainsRegex(absl::StrCat(
          "The entity ", fixedEntity,
          " is not part of the underlying knowledge graph and can therefore "
          "not be used as the object of ql:contains-entity")));

  TextIndexScanForEntity s2{qec, Variable{"?text"}, Variable{"?entityVar"},
                            "test*"};
  ASSERT_TRUE(!s2.knownEmptyResult());

  TextIndexScanForEntity s3{qec, Variable{"?text"}, Variable{"?entityVar"},
                            "test"};
  ASSERT_TRUE(!s3.knownEmptyResult());
}

}  // namespace

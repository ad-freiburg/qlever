//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "./TextIndexScanTestHelpers.h"
#include "engine/IndexScan.h"
#include "engine/TextIndexScanForEntity.h"
#include "parser/ParsedQuery.h"

using namespace ad_utility::testing;
using ad_utility::source_location;
namespace h = textIndexScanTestHelpers;
using TextScoringMetric = qlever::TextScoringMetric;

namespace {
std::string kg =
    "<a> <p> \"he failed the test\" . <a> <p> \"testing can help\" . <a> <p> "
    "\"some other sentence\" . <b> <p> \"the test on friday was really hard\" "
    ". <b> <x2> <x> . <b> <x2> <xb2> .";

std::string wordsFileContent =
    h::createWordsFileLineAsString("astronomer", false, 1, 1) +
    h::createWordsFileLineAsString("<Astronomer>", true, 1, 0) +
    h::createWordsFileLineAsString("scientist", false, 1, 1) +
    h::createWordsFileLineAsString("field", false, 1, 1) +
    h::createWordsFileLineAsString("astronomy", false, 1, 1) +
    h::createWordsFileLineAsString("astronomer", false, 2, 0) +
    h::createWordsFileLineAsString("<Astronomer>", true, 2, 0) +
    h::createWordsFileLineAsString(":s:firstsentence", false, 2, 0) +
    h::createWordsFileLineAsString("scientist", false, 2, 0) +
    h::createWordsFileLineAsString("field", false, 2, 0) +
    h::createWordsFileLineAsString("astronomy", false, 2, 0) +
    h::createWordsFileLineAsString("astronomy", false, 3, 1) +
    h::createWordsFileLineAsString("concentrates", false, 3, 1) +
    h::createWordsFileLineAsString("studies", false, 3, 1) +
    h::createWordsFileLineAsString("specific", false, 3, 1) +
    h::createWordsFileLineAsString("question", false, 3, 1) +
    h::createWordsFileLineAsString("outside", false, 3, 1) +
    h::createWordsFileLineAsString("scope", false, 3, 1) +
    h::createWordsFileLineAsString("earth", false, 3, 1) +
    h::createWordsFileLineAsString("astronomy", false, 4, 1) +
    h::createWordsFileLineAsString("concentrates", false, 4, 1) +
    h::createWordsFileLineAsString("studies", false, 4, 1) +
    h::createWordsFileLineAsString("field", false, 4, 1) +
    h::createWordsFileLineAsString("outside", false, 4, 1) +
    h::createWordsFileLineAsString("scope", false, 4, 1) +
    h::createWordsFileLineAsString("earth", false, 4, 1) +
    h::createWordsFileLineAsString("tester", false, 5, 1) +
    h::createWordsFileLineAsString("rockets", false, 5, 1) +
    h::createWordsFileLineAsString("astronomer", false, 5, 1) +
    h::createWordsFileLineAsString("<Astronomer>", true, 5, 0) +
    h::createWordsFileLineAsString("although", false, 5, 1) +
    h::createWordsFileLineAsString("astronomer", false, 6, 0) +
    h::createWordsFileLineAsString("<Astronomer>", true, 6, 0) +
    h::createWordsFileLineAsString("although", false, 6, 0) +
    h::createWordsFileLineAsString("<Astronomer>", true, 6, 0) +
    h::createWordsFileLineAsString("space", false, 6, 1) +
    h::createWordsFileLineAsString("<Astronomer>", true, 7, 0) +
    h::createWordsFileLineAsString("space", false, 7, 0) +
    h::createWordsFileLineAsString("earth", false, 7, 1);

std::string firstDocText =
    "An astronomer is a scientist in the field of "
    "astronomy who concentrates their studies on a "
    "specific question or field outside of the scope of "
    "Earth.";

std::string secondDocText =
    "The Tester of the rockets can be an astronomer "
    "too although they might not be in space but on "
    "earth.";

std::string docsFileContent = h::createDocsFileLineAsString(4, firstDocText) +
                              h::createDocsFileLineAsString(7, secondDocText);

std::pair<std::string, std::string> contentsOfWordsFileAndDocsFile = {
    wordsFileContent, docsFileContent};

// Return a `QueryExecutionContext` from the given `kg`(see above) that has a
// text index for the literals in the `kg`.
auto qecWithOnlyLiteralTextIndex = []() {
  TestIndexConfig config{kg};
  config.createTextIndex = true;
  return getQec(std::move(config));
};

// Return a `QueryExecutionContext` from the turtle `kg` (see above) that has a
// text index that contains the literals from the `kg` as well as the
// `contentsOfWordsFileAndDocsFile` (also above). The metrics used for the text
// scores can be specified.
auto getQecWithTextIndex(
    std::optional<TextScoringMetric> textScoring = std::nullopt) {
  using namespace ad_utility::testing;
  auto kg2 =
      "<a> <p> \"he failed the test\" . <a> <p> \"testing can help\" . <a> <p> "
      "\"some other sentence\" . <b> <p> \"the test on friday was really "
      "hard\" "
      ". <b> <x2> <x> . <b> <x2> <xb2> . <Astronomer> <is-a> <job> .";
  TestIndexConfig config{kg2};
  config.createTextIndex = true;
  config.contentsOfWordsFileAndDocsfile = contentsOfWordsFileAndDocsFile;
  if (textScoring.has_value()) {
    config.scoringMetric = textScoring;
  }
  return getQec(std::move(config));
}

TEST(TextIndexScanForEntity, ShortPrefixWord) {
  auto qec = qecWithOnlyLiteralTextIndex();
  TextIndexScanForEntity s1{qec, Variable{"?text"}, Variable{"?entityVar"},
                            "t*"};
  ASSERT_EQ(s1.getResultWidth(), 3);
  auto result = s1.computeResultOnlyForTesting();
  auto tr = h::TextResult{qec, result, true};
  ASSERT_EQ(result.idTable().numColumns(), 3);
  ASSERT_EQ(result.idTable().size(), 4);
  ASSERT_EQ("\"he failed the test\"", tr.getEntity(0));
  ASSERT_EQ("\"some other sentence\"", tr.getEntity(1));
  ASSERT_EQ("\"testing can help\"", tr.getEntity(2));
  ASSERT_EQ("\"the test on friday was really hard\"", tr.getEntity(3));
}

TEST(TextIndexScanForEntity, EntityScanBasic) {
  auto qec = qecWithOnlyLiteralTextIndex();

  TextIndexScanForEntity s1{qec, Variable{"?text"}, Variable{"?entityVar"},
                            "test*"};
  TextIndexScanForEntity s2{qec, Variable{"?text2"}, Variable{"?entityVar2"},
                            "test*"};
  ASSERT_EQ(s1.getResultWidth(), 3);

  auto result = s1.computeResultOnlyForTesting();
  auto tr = h::TextResult{qec, result, true};
  ASSERT_EQ(result.idTable().numColumns(), 3);
  ASSERT_EQ(result.idTable().size(), 4);

  // NOTE: because of the way the graph above is constructed, the entities are
  // texts
  // Note the Text Block size while testing is 2, so both blocks are retrieved
  // and merged. The texts appear in order of their textRecordIndex which in
  // this case is alphabetically.
  ASSERT_EQ("\"he failed the test\"", tr.getEntity(0));
  ASSERT_EQ("\"some other sentence\"", tr.getEntity(1));
  ASSERT_EQ("\"testing can help\"", tr.getEntity(2));
  ASSERT_EQ("\"the test on friday was really hard\"", tr.getEntity(3));

  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?text2"}, {0, AlwaysDefined}},
      {Variable{"?entityVar2"}, {1, AlwaysDefined}},
      {Variable{"?ql_score_text2_var_entityVar2"}, {2, AlwaysDefined}}};
  EXPECT_THAT(s2.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
}

TEST(TextIndexScanForEntity, FixedEntityScan) {
  auto qec = qecWithOnlyLiteralTextIndex();

  std::string fixedEntity = "\"some other sentence\"";
  TextIndexScanForEntity s3{qec, Variable{"?text3"}, fixedEntity, "sentence"};

  auto result = s3.computeResultOnlyForTesting();
  ASSERT_EQ(s3.getResultWidth(), 2);
  ASSERT_EQ(result.idTable().numColumns(), 2);
  ASSERT_EQ(result.idTable().size(), 1);

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
  ASSERT_EQ(result.idTable().numColumns(), 2);
  ASSERT_EQ(result.idTable().size(), 1);

  ASSERT_EQ(fixedEntity, h::getTextRecordFromResultTable(qec, result, 0));
}

// Test if entity scans work not only for literal text index but also if text
// index is build using words- and docsfile.
TEST(TextIndexScanForEntity, FulltextIndexEntityScan) {
  auto qec = getQecWithTextIndex();

  TextIndexScanForEntity s1{qec, Variable{"?text"}, Variable{"?entityVar"},
                            "astronomer*"};
  ASSERT_EQ(s1.getResultWidth(), 3);

  auto result = s1.computeResultOnlyForTesting();
  auto tr = h::TextResult{qec, result, true};
  ASSERT_EQ(result.idTable().numColumns(), 3);
  ASSERT_EQ(result.idTable().size(), 4);

  ASSERT_EQ("<Astronomer>", tr.getEntity(0));
  ASSERT_EQ("<Astronomer>", tr.getEntity(1));
  ASSERT_EQ("<Astronomer>", tr.getEntity(2));
  ASSERT_EQ("<Astronomer>", tr.getEntity(3));
}

TEST(TextIndexScanForEntity, CacheKeys) {
  auto qec = qecWithOnlyLiteralTextIndex();

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
  std::string fixedEntity = "\"some other sentence\"";
  TextIndexScanForEntity s5{qec, Variable{"?text3"}, fixedEntity, "sentence"};
  // Same text var, different entities (one entity var, one fixed entity), same
  // word
  ASSERT_NE(s3.getCacheKeyImpl(), s5.getCacheKeyImpl());

  TextIndexScanForEntity s6{qec, Variable{"?text6"}, fixedEntity, "sentence"};
  // Different text vars, same fixed entity, same word
  ASSERT_EQ(s5.getCacheKeyImpl(), s6.getCacheKeyImpl());

  std::string newFixedEntity = "\"he failed the test\"";
  TextIndexScanForEntity s7{qec, Variable{"?text7"}, newFixedEntity,
                            "sentence"};
  // Different text vars, different fixed entities, same word
  ASSERT_NE(s5.getCacheKeyImpl(), s7.getCacheKeyImpl());

  TextIndexScanForEntity s8{qec, Variable{"?text7"}, newFixedEntity,
                            "sentences"};
  // Same text var, same fixed entity, different words
  ASSERT_NE(s7.getCacheKeyImpl(), s8.getCacheKeyImpl());
}

TEST(TextIndexScanForEntity, KnownEmpty) {
  auto qec = qecWithOnlyLiteralTextIndex();

  TextIndexScanForEntity s1{qec, Variable{"?text"}, Variable{"?entityVar"},
                            "nonExistentWord*"};
  ASSERT_TRUE(s1.knownEmptyResult());

  std::string fixedEntity = "\"non existent entity\"";
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

// _____________________________________________________________________________
TEST(TextIndexScanForEntity, clone) {
  auto qec = getQec();

  TextIndexScanForEntity scan{qec, Variable{"?text"}, Variable{"?entityVar"},
                              "nonExistentWord*"};

  auto clone = scan.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(scan, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), scan.getDescriptor());
}

}  // namespace

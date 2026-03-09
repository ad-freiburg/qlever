// Copyright 2026 The QLever Authors, in particular
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "engine/IndexScan.h"
#include "index/ExportIds.h"
#include "parser/LiteralOrIri.h"
#include "parser/NormalizedString.h"
#include "rdfTypes/Literal.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/ParseableDuration.h"

using namespace std::string_literals;
using namespace std::chrono_literals;
using ::testing::ElementsAre;
using ::testing::EndsWith;
using ::testing::Eq;
using ::testing::HasSubstr;

namespace {
// _____________________________________________________________________________
TEST(ExportIds, idToLiteralFunctionality) {
  std::string kg =
      "<s> <p> \"something\" . <s> <p> 1 . <s> <p> "
      "\"some\"^^<http://www.w3.org/2001/XMLSchema#string> . <s> <p> "
      "\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype> .";
  auto qec = ad_utility::testing::getQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());
  using enum Datatype;

  // Helper function that takes an ID and a vector of test cases and checks
  // if the ID is correctly converted. A more detailed explanation of the test
  // logic is below with the test cases.
  auto checkIdToLiteral =
      [&](Id id,
          const std::vector<std::tuple<bool, std::optional<std::string>>>&
              cases) {
        for (const auto& [onlyLiteralsWithXsdString, expected] : cases) {
          auto result = ql::exportIds::idToLiteral(
              qec->getIndex(), id, LocalVocab{}, onlyLiteralsWithXsdString);
          if (expected) {
            EXPECT_THAT(result,
                        ::testing::Optional(::testing::ResultOf(
                            [](const auto& literalOrIri) {
                              return literalOrIri.toStringRepresentation();
                            },
                            ::testing::StrEq(*expected))));
          } else {
            EXPECT_EQ(result, std::nullopt);
          }
        }
      };

  // Test cases: Each tuple describes one test case.
  // The first element is the ID of the element to test.
  // The second element is a list of 2 configurations:
  // 1. for literals all datatypes are removed, IRIs
  // are converted to literals
  // 2. only literals with no datatype or`xsd:string` are returned.
  // In the last case the datatype is removed.
  std::vector<
      std::tuple<Id, std::vector<std::tuple<bool, std::optional<std::string>>>>>
      testCases = {
          // Case: Literal without datatype
          {getId("\"something\""),
           {{false, "\"something\""}, {true, "\"something\""}}},

          // Case: Literal with datatype `xsd:string`
          {getId("\"some\"^^<http://www.w3.org/2001/XMLSchema#string>"),
           {{false, "\"some\""}, {true, "\"some\""}}},

          // Case: Literal with unknown datatype
          {getId("\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype>"),
           {{false, "\"dadudeldu\""}, {true, std::nullopt}}},

          // Case: IRI
          {getId("<s>"), {{false, "\"s\""}, {true, std::nullopt}}},

          // Case: datatype `Int`
          {ad_utility::testing::IntId(1),
           {{false, "\"1\""}, {true, std::nullopt}}},

          // Case: Undefined ID
          {ad_utility::testing::UndefId(),
           {{false, std::nullopt}, {true, std::nullopt}}}};

  for (const auto& [id, cases] : testCases) {
    checkIdToLiteral(id, cases);
  }
}

// _____________________________________________________________________________
TEST(ExportIds, idToLiteralOrIriFunctionality) {
  std::string kg =
      "<s> <p> \"something\" . <s> <p> 1 . <s> <p> "
      "\"some\"^^<http://www.w3.org/2001/XMLSchema#string> . <s> <p> "
      "\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype> . <s> <p> "
      "<http://example.com/> .";
  auto qec = ad_utility::testing::getQec(kg);
  auto getId = ad_utility::testing::makeGetId(qec->getIndex());

  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  using Literal = ad_utility::triple_component::Literal;
  using Iri = ad_utility::triple_component::Iri;

  std::vector<std::pair<ValueId, std::optional<LiteralOrIri>>> expected{
      {getId("\"something\""),
       LiteralOrIri{Literal::fromStringRepresentation("\"something\"")}},
      {getId("\"some\"^^<http://www.w3.org/2001/XMLSchema#string>"),
       LiteralOrIri{Literal::fromStringRepresentation(
           "\"some\"^^<http://www.w3.org/2001/XMLSchema#string>")}},
      {getId("\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype>"),
       LiteralOrIri{Literal::fromStringRepresentation(
           "\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype>")}},
      {getId("<http://example.com/>"),
       LiteralOrIri{Iri::fromIriref("<http://example.com/>")}},
      {ValueId::makeFromBool(true),
       LiteralOrIri{Literal::fromStringRepresentation(
           "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>")}},
      {ValueId::makeUndefined(), std::nullopt}};
  for (const auto& [valueId, expRes] : expected) {
    ASSERT_EQ(
        ql::exportIds::idToLiteralOrIri(qec->getIndex(), valueId, LocalVocab{}),
        expRes);
  }
}

// _____________________________________________________________________________
TEST(ExportIds, getLiteralOrNullopt) {
  using LiteralOrIri = ql::exportIds::LiteralOrIri;
  using Literal = ad_utility::triple_component::Literal;
  using Iri = ad_utility::triple_component::Iri;

  auto litOrNulloptTestHelper = [](std::optional<LiteralOrIri> input,
                                   std::optional<std::string> expectedRes) {
    auto res = ql::exportIds::getLiteralOrNullopt(input);
    ASSERT_EQ(res.has_value(), expectedRes.has_value());
    if (res.has_value()) {
      ASSERT_EQ(expectedRes.value(), res.value().toStringRepresentation());
    }
  };

  auto lit = LiteralOrIri{Literal::fromStringRepresentation("\"test\"")};
  litOrNulloptTestHelper(lit, "\"test\"");

  auto litWithType = LiteralOrIri{Literal::fromStringRepresentation(
      "\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype>")};
  litOrNulloptTestHelper(
      litWithType, "\"dadudeldu\"^^<http://www.dadudeldu.com/NoSuchDatatype>");

  litOrNulloptTestHelper(std::nullopt, std::nullopt);

  auto iri = LiteralOrIri{Iri::fromIriref("<https://example.com/>")};
  litOrNulloptTestHelper(iri, std::nullopt);
}

// _____________________________________________________________________________
TEST(ExportIds, ReplaceAnglesByQuotes) {
  std::string input = "<s>";
  std::string expected = "\"s\"";
  EXPECT_EQ(ql::exportIds::replaceAnglesByQuotes(input), expected);
  input = "s>";
  EXPECT_THROW(ql::exportIds::replaceAnglesByQuotes(input),
               ad_utility::Exception);
  input = "<s";
  EXPECT_THROW(ql::exportIds::replaceAnglesByQuotes(input),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(ExportIds, blankNodeIrisAreProperlyFormatted) {
  using ad_utility::triple_component::Iri;
  std::string_view input = "_:test";
  EXPECT_THAT(
      ql::exportIds::blankNodeIriToString(Iri::fromStringRepresentation(
          absl::StrCat(QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX, input, ">"))),
      ::testing::Optional(::testing::Eq(input)));
  EXPECT_EQ(ql::exportIds::blankNodeIriToString(
                Iri::fromStringRepresentation("<some_iri>")),
            std::nullopt);
}

// _____________________________________________________________________________
TEST(ExportIds, GetLiteralOrIriFromVocabIndexWithEncodedIris) {
  // Test the getLiteralOrIriFromVocabIndex function specifically with encoded
  // IRIs

  // Create an EncodedIriManager with test prefixes
  std::vector<std::string> prefixes = {"http://example.org/",
                                       "http://test.com/"};
  EncodedIriManager encodedIriManager{prefixes};

  // Create a test index config with the encoded IRI manager
  using namespace ad_utility::testing;
  TestIndexConfig config;
  config.encodedIriManager = encodedIriManager;
  auto qec = getQec(std::move(config));

  // Test driver lambda to reduce code duplication
  LocalVocab emptyLocalVocab;
  auto testEncodedIri = [&](const std::string& iri) {
    // Encode the IRI
    auto encodedIdOpt = encodedIriManager.encode(iri);
    ASSERT_TRUE(encodedIdOpt.has_value()) << "Failed to encode IRI: " << iri;

    Id encodedId = *encodedIdOpt;
    EXPECT_EQ(encodedId.getDatatype(), Datatype::EncodedVal);

    // Test getLiteralOrIriFromVocabIndex with the encoded ID
    auto result = ql::exportIds::getLiteralOrIriFromVocabIndex(
        qec->getIndex(), encodedId, emptyLocalVocab);

    // The result should be the original IRI
    EXPECT_TRUE(result.isIri());
    EXPECT_EQ(result.toStringRepresentation(), iri);
  };

  // Test multiple encoded IRIs
  testEncodedIri("<http://example.org/123>");
  testEncodedIri("<http://test.com/456>");

  // Test that non-encodable IRIs fall back to VocabIndex handling
  // (This test assumes the test index has some vocabulary entries)
  if (!qec->getIndex().getVocab().size()) {
    VocabIndex vocabIndex = VocabIndex::make(0);  // First vocab entry
    Id vocabId = Id::makeFromVocabIndex(vocabIndex);

    auto vocabResult = ql::exportIds::getLiteralOrIriFromVocabIndex(
        qec->getIndex(), vocabId, emptyLocalVocab);

    // Should successfully return some IRI or literal from vocabulary
    EXPECT_FALSE(vocabResult.toStringRepresentation().empty());
  }
}
}  // namespace

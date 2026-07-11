// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "engine/IndexScan.h"
#include "index/ExportIds.h"
#include "index/LocalVocabEntry.h"
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
using ::testing::ElementsAreArray;
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

using IriTypes = ::testing::Types<ad_utility::triple_component::Iri,
                                  ad_utility::triple_component::IriView>;

template <typename IriType>
class ExportIdsBlankNodeTest : public ::testing::Test {};

TYPED_TEST_SUITE(ExportIdsBlankNodeTest, IriTypes);

// _____________________________________________________________________________
TYPED_TEST(ExportIdsBlankNodeTest, blankNodeIrisAreProperlyFormatted) {
  using IriType = TypeParam;
  std::string_view input = "_:test";

  // Keep the string representations in named locals: `IriView` stores a view,
  // so passing the `absl::StrCat` temporary directly would dangle.
  std::string blankNodeRepresentation =
      absl::StrCat(QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX, input, ">");
  auto blankNodeIri =
      IriType::fromStringRepresentation(blankNodeRepresentation);
  EXPECT_THAT(ql::exportIds::blankNodeIriToString(blankNodeIri),
              ::testing::Optional(::testing::Eq(input)));

  std::string nonBlankNodeRepresentation = "<some_iri>";
  auto nonBlankNodeIri =
      IriType::fromStringRepresentation(nonBlankNodeRepresentation);
  EXPECT_EQ(ql::exportIds::blankNodeIriToString(nonBlankNodeIri), std::nullopt);
}

// _____________________________________________________________________________
TEST(ExportIds, GetLiteralOrIriFromVocabIndexWithEncodedIris) {
  // Test the getLiteralOrIriFromVocabIndex function specifically with encoded
  // IRIs

  // Create a test index config with the encoded IRI manager
  using namespace ad_utility::testing;
  TestIndexConfig config;
  config.encodedPrefixesWithoutAngleBrackets = {"http://example.org/",
                                                "http://test.com/"};
  auto qec = getQec(std::move(config));
  const auto& encodedIriManager = qec->getIndex().encodedIriManager();

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
  if (qec->getIndex().getVocab().size()) {
    VocabIndex vocabIndex = VocabIndex::make(0);  // First vocab entry
    Id vocabId = Id::makeFromVocabIndex(vocabIndex);

    auto vocabResult = ql::exportIds::getLiteralOrIriFromVocabIndex(
        qec->getIndex(), vocabId, emptyLocalVocab);

    // Should successfully return some IRI or literal from vocabulary
    EXPECT_FALSE(vocabResult.toStringRepresentation().empty());
  }
}

// _____________________________________________________________________________
// Verify that `idsToStringAndType` produces identical results to calling
// `idToStringAndType` for each ID individually.
TEST(ExportIds, idsToStringAndTypeBatchMatchesIndividualLookups) {
  // Build a small index with IRIs and literals of various types.
  std::string kg =
      "<s> <p> <o> . "
      "<s> <q> \"hello\" . "
      "<s> <p> 42 . "
      "<s> <p> 3.14 .";
  auto qec = ad_utility::testing::getQec(kg);
  const Index& index = qec->getIndex();
  LocalVocab localVocab{};
  auto getId = ad_utility::testing::makeGetId(index);

  // Collect a mix of VocabIndex IDs (IRIs, literal) and non-VocabIndex IDs
  // (integer, double, undefined).
  std::vector<Id> ids{
      getId("<s>"),
      getId("<p>"),
      getId("<o>"),
      getId("<q>"),
      getId("\"hello\""),
      Id::makeFromInt(42),
      Id::makeFromDouble(3.14),
      Id::makeUndefined(),
  };

  // `idsToStringAndType` requires the input to be sorted by `ValueId`.
  ql::ranges::sort(ids);

  auto batchResults = ql::exportIds::idsToStringAndType(
      index, ql::span<const Id>{ids}, localVocab);

  ASSERT_EQ(batchResults.size(), ids.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(batchResults[i],
              ql::exportIds::idToStringAndType(index, ids[i], localVocab))
        << "Mismatch at index " << i;
  }
}

// _____________________________________________________________________________
// Empty span returns an empty vector.
TEST(ExportIds, idsToStringAndTypeEmptyInput) {
  auto qec = ad_utility::testing::getQec("<s> <p> <o>");
  LocalVocab localVocab{};
  auto result = ql::exportIds::idsToStringAndType(
      qec->getIndex(), ql::span<const Id>{}, localVocab);
  EXPECT_TRUE(result.empty());
}

// _____________________________________________________________________________
template <bool RemoveQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity>
void expectResolveVocabMatchesOracle(const Index& index, ql::span<const Id> ids,
                                     ql::span<const size_t> positions,
                                     const EscapeFunction& escape) {
  SCOPED_TRACE(absl::StrCat(
      "removeQuotesAndAngleBrackets=", RemoveQuotesAndAngleBrackets,
      " returnOnlyLiterals=", returnOnlyLiterals));
  std::vector<std::optional<std::pair<std::string, const char*>>> results(
      ids.size());

  ql::exportIds::resolveVocabIndexIds<RemoveQuotesAndAngleBrackets,
                                      returnOnlyLiterals, EscapeFunction>(
      index, ids, positions, results, escape);
  std::set<size_t> resolved(positions.begin(), positions.end());
  LocalVocab emptyLocalVocab{};
  for (size_t i = 0; i < ids.size(); ++i) {
    if (resolved.count(i) != 0) {
      // The batched result must equal the non-batched per-ID path.
      auto expected =
          ql::exportIds::idToStringAndType<RemoveQuotesAndAngleBrackets,
                                           returnOnlyLiterals, EscapeFunction>(
              index, ids[i], emptyLocalVocab, escape);
      EXPECT_EQ(results[i], expected);
    } else {
      EXPECT_EQ(results[i], std::nullopt);
    }
  }
}

// _____________________________________________________________________________
// Mirror of `expectResolveVocabMatchesOracle` for `resolveNonVocabIndexIds`:
// resolve the (non-`VocabIndex`) IDs at `positions` and check that every
// resolved slot matches the per-ID `idToStringAndType` path, while every other
// slot is left untouched (`std::nullopt`).
template <bool RemoveQuotesAndAngleBrackets = false,
          bool returnOnlyLiterals = false,
          typename EscapeFunction = ql::identity>
void expectResolveNonVocabMatchesOracle(const Index& index,
                                        ql::span<const Id> ids,
                                        const LocalVocab& localVocab,
                                        ql::span<const size_t> positions,
                                        const EscapeFunction& escape) {
  SCOPED_TRACE(absl::StrCat(
      "removeQuotesAndAngleBrackets=", RemoveQuotesAndAngleBrackets,
      " returnOnlyLiterals=", returnOnlyLiterals));
  std::vector<std::optional<std::pair<std::string, const char*>>> results(
      ids.size());

  ql::exportIds::resolveNonVocabIndexIds<RemoveQuotesAndAngleBrackets,
                                         returnOnlyLiterals, EscapeFunction>(
      index, ids, localVocab, positions, results, escape);
  std::set<size_t> resolved(positions.begin(), positions.end());
  for (size_t i = 0; i < ids.size(); ++i) {
    if (resolved.count(i) != 0) {
      // The result must equal the per-ID path (which itself may be
      // `std::nullopt`, e.g. for `Undefined` or a filtered-out non-literal).
      auto expected =
          ql::exportIds::idToStringAndType<RemoveQuotesAndAngleBrackets,
                                           returnOnlyLiterals, EscapeFunction>(
              index, ids[i], localVocab, escape);
      EXPECT_EQ(results[i], expected);
    } else {
      EXPECT_EQ(results[i], std::nullopt);
    }
  }
}

// _____________________________________________________________________________
// An escape function that visibly transforms its input. Using `ql::identity`
// here would make "the escape function was applied" indistinguishable from
// "the escape function was silently dropped".
const auto escapeWithMarker = [](std::string s) {
  return absl::StrCat("X:", s);
};

// The expected result of `literalOrIriToStringAndType` for one input, for each
// of the four combinations of the two template flags. `std::nullopt` means
// that the function is expected to return `std::nullopt`.
struct ExpectedForFlags {
  std::optional<std::string> plain_;
  std::optional<std::string> removeQuotesAndAngleBrackets_;
  std::optional<std::string> returnOnlyLiterals_;
  std::optional<std::string> both_;
};

// _____________________________________________________________________________
// Run `literalOrIriToStringAndType` on `word` with the given flags and the
// `escapeWithMarker` escape function, and check that the result is `expected`
// (where `std::nullopt` means that the function must return `std::nullopt`).
template <bool removeQuotesAndAngleBrackets, bool returnOnlyLiterals,
          typename LiteralOrIriType>
void expectLiteralOrIriToStringAndType(
    const LiteralOrIriType& word, const std::optional<std::string>& expected) {
  SCOPED_TRACE(absl::StrCat(
      "removeQuotesAndAngleBrackets=", removeQuotesAndAngleBrackets,
      " returnOnlyLiterals=", returnOnlyLiterals));
  auto result =
      ql::exportIds::literalOrIriToStringAndType<removeQuotesAndAngleBrackets,
                                                 returnOnlyLiterals>(
          word, escapeWithMarker);
  if (!expected.has_value()) {
    EXPECT_EQ(result, std::nullopt);
    return;
  }
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, expected.value());
  // This function never reports an XSD datatype; that is the job of
  // `idToStringAndTypeForEncodedValue`.
  EXPECT_EQ(result->second, nullptr);
}

using LiteralOrIriTypes =
    ::testing::Types<ad_utility::triple_component::LiteralOrIri,
                     ad_utility::triple_component::LiteralOrIriView>;

template <typename LiteralOrIriType>
class ExportIdsLiteralOrIriToStringAndTypeTest : public ::testing::Test {
 protected:
  // Run `literalOrIriToStringAndType` on the `LiteralOrIri[View]` parsed from
  // `stringRepresentation` for all four flag combinations.
  static void checkAllFlagCombinations(std::string_view stringRepresentation,
                                       const ExpectedForFlags& expected) {
    SCOPED_TRACE(stringRepresentation);
    // Keep the string representation in a named local: `LiteralOrIriView`
    // stores views, so a temporary would dangle.
    std::string owned{stringRepresentation};
    auto word = LiteralOrIriType::fromStringRepresentation(owned);
    expectLiteralOrIriToStringAndType<false, false>(word, expected.plain_);
    expectLiteralOrIriToStringAndType<true, false>(
        word, expected.removeQuotesAndAngleBrackets_);
    expectLiteralOrIriToStringAndType<false, true>(
        word, expected.returnOnlyLiterals_);
    expectLiteralOrIriToStringAndType<true, true>(word, expected.both_);
  }
};

TYPED_TEST_SUITE(ExportIdsLiteralOrIriToStringAndTypeTest, LiteralOrIriTypes);

// _____________________________________________________________________________
TYPED_TEST(ExportIdsLiteralOrIriToStringAndTypeTest, literals) {
  // A plain literal is returned as-is; `removeQuotesAndAngleBrackets` strips
  // the quotes. `returnOnlyLiterals` never rejects a literal.
  TestFixture::checkAllFlagCombinations(
      "\"hello\"", {.plain_ = "X:\"hello\"",
                    .removeQuotesAndAngleBrackets_ = "X:hello",
                    .returnOnlyLiterals_ = "X:\"hello\"",
                    .both_ = "X:hello"});

  // With a datatype, the plain path keeps the `^^<...>` suffix, while
  // `removeQuotesAndAngleBrackets` reduces the literal to its content.
  std::string_view withDatatype =
      "\"42\"^^<http://www.w3.org/2001/XMLSchema#int>";
  TestFixture::checkAllFlagCombinations(
      withDatatype, {.plain_ = absl::StrCat("X:", withDatatype),
                     .removeQuotesAndAngleBrackets_ = "X:42",
                     .returnOnlyLiterals_ = absl::StrCat("X:", withDatatype),
                     .both_ = "X:42"});

  // Same for a language tag.
  TestFixture::checkAllFlagCombinations(
      "\"hallo\"@de", {.plain_ = "X:\"hallo\"@de",
                       .removeQuotesAndAngleBrackets_ = "X:hallo",
                       .returnOnlyLiterals_ = "X:\"hallo\"@de",
                       .both_ = "X:hallo"});
}

// _____________________________________________________________________________
TYPED_TEST(ExportIdsLiteralOrIriToStringAndTypeTest, iris) {
  // `returnOnlyLiterals` rejects IRIs. Otherwise, the angle brackets are kept
  // unless `removeQuotesAndAngleBrackets` is set.
  TestFixture::checkAllFlagCombinations(
      "<http://example.org/x>",
      {.plain_ = "X:<http://example.org/x>",
       .removeQuotesAndAngleBrackets_ = "X:http://example.org/x",
       .returnOnlyLiterals_ = std::nullopt,
       .both_ = std::nullopt});
}

// _____________________________________________________________________________
TYPED_TEST(ExportIdsLiteralOrIriToStringAndTypeTest, blankNodeIris) {
  std::string blankNode =
      absl::StrCat(QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX, "_:b0", ">");

  // A blank node is an IRI internally, so `returnOnlyLiterals` rejects it
  // before the blank-node check is ever reached. Otherwise, it is returned as
  // its `_:` representation, independent of `removeQuotesAndAngleBrackets`
  // and, notably, *without* the escape function being applied.
  TestFixture::checkAllFlagCombinations(
      blankNode, {.plain_ = "_:b0",
                  .removeQuotesAndAngleBrackets_ = "_:b0",
                  .returnOnlyLiterals_ = std::nullopt,
                  .both_ = std::nullopt});
}

// _____________________________________________________________________________
TEST(ExportIds, partitionIdPositions) {
  using namespace ad_utility::testing;

  auto expectPartition =
      [](const std::vector<Id>& ids,
         const std::vector<size_t>& expectedVocabIndices,
         const std::vector<size_t>& expectedNonVocabIndices) {
        auto positions = ql::exportIds::partitionIdPositions(ids);
        EXPECT_THAT(positions.vocabIndexIndices_,
                    ElementsAreArray(expectedVocabIndices));
        EXPECT_THAT(positions.nonVocabIndexIndices_,
                    ElementsAreArray(expectedNonVocabIndices));
      };

  // Empty input yields two empty partitions.
  expectPartition({}, {}, {});

  // All `VocabIndex`, respectively no `VocabIndex` at all.
  expectPartition({VocabId(0), VocabId(5)}, {0, 1}, {});
  expectPartition({IntId(1), UndefId(), BlankNodeId(3)}, {}, {0, 1, 2});

  // The interesting case: `LocalVocabIndex` and encoded values interspersed
  // between the `VocabIndex` IDs, so the partition cannot be a range split.
  expectPartition({VocabId(0), LocalVocabId(1), VocabId(2), IntId(7),
                   VocabId(3), DoubleId(1.5)},
                  {0, 2, 4}, {1, 3, 5});

  // Both partitions are stable, i.e. the positions are ascending even when the
  // underlying vocabulary indices are not. `resolveVocabIndexIds` relies on
  // this to scatter the batched lookup results back to the right slots.
  expectPartition({VocabId(9), IntId(0), VocabId(1)}, {0, 2}, {1});
}

// _____________________________________________________________________________
// Resolve `VocabIndex` IDs and verify the batched lookup against the per-ID
// path for every combination of the two template flags.
TEST(ExportIds, resolveVocabIndexIds) {
  using namespace ad_utility::testing;
  std::string kg = "<s> <p> <o> . <s> <q> \"hello\" . <s> <p> \"world\"@en .";
  auto qec = getQec(kg);
  const Index& index = qec->getIndex();
  auto getId = makeGetId(index);

  std::vector<Id> ids{getId("<s>"), getId("<p>"), getId("<o>"),
                      getId("\"hello\""), getId("\"world\"@en")};

  auto check = [&](const std::vector<size_t>& positions) {
    expectResolveVocabMatchesOracle<false, false>(index, ids, positions,
                                                  escapeWithMarker);
    expectResolveVocabMatchesOracle<true, false>(index, ids, positions,
                                                 escapeWithMarker);
    expectResolveVocabMatchesOracle<false, true>(index, ids, positions,
                                                 escapeWithMarker);
    expectResolveVocabMatchesOracle<true, true>(index, ids, positions,
                                                escapeWithMarker);
  };

  // Empty positions: early return, every slot stays `std::nullopt`.
  check({});
  // All positions resolved.
  check({0, 1, 2, 3, 4});
  // A subset: the untouched slots must remain `std::nullopt`.
  check({1, 3});
  // Positions given out of order (and not in underlying-vocab-index order) to
  // exercise the scatter back to the correct result slot.
  check({4, 0, 2});
}

// _____________________________________________________________________________
// Resolve non-`VocabIndex` IDs (encoded values and a `LocalVocabIndex`) and
// verify against the per-ID path for every combination of the two template
// flags.
TEST(ExportIds, resolveNonVocabIndexIds) {
  using namespace ad_utility::testing;
  auto qec = getQec("<s> <p> <o>");
  const Index& index = qec->getIndex();

  // Populate a `LocalVocab` with a literal so the `LocalVocabIndex` resolution
  // path is exercised, not just the values encoded directly in the ID bits.
  LocalVocab localVocab{};
  auto localVocabIndex = localVocab.getIndexAndAddIfNotContained(
      LocalVocabEntry::literalWithoutQuotes("localLit",
                                            qec->getLocalVocabContext()));
  Id localVocabId = Id::makeFromLocalVocabIndex(localVocabIndex);

  std::vector<Id> ids{
      Id::makeFromInt(42),
      Id::makeFromDouble(3.14),
      Id::makeUndefined(),
      localVocabId,
  };

  auto check = [&](const std::vector<size_t>& positions) {
    expectResolveNonVocabMatchesOracle<false, false>(
        index, ids, localVocab, positions, escapeWithMarker);
    expectResolveNonVocabMatchesOracle<true, false>(
        index, ids, localVocab, positions, escapeWithMarker);
    expectResolveNonVocabMatchesOracle<false, true>(
        index, ids, localVocab, positions, escapeWithMarker);
    expectResolveNonVocabMatchesOracle<true, true>(index, ids, localVocab,
                                                   positions, escapeWithMarker);
  };

  // Empty positions: nothing is resolved.
  check({});
  // All positions: encoded int/double, `Undefined` (resolves to `nullopt`) and
  // the local-vocab literal.
  check({0, 1, 2, 3});
  // A subset (`Undefined` + local literal); untouched slots stay `nullopt`.
  check({2, 3});
}

}  // namespace

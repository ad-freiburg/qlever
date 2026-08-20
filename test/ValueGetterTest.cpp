//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: @DuDaAG,
//           Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "../test/printers/UnitOfMeasurementPrinters.h"
#include "./ValueGetterTestHelpers.h"
#include "GeometryInfoTestHelpers.h"
#include "rdfTypes/GeometryInfo.h"

namespace {

using namespace valueGetterTestHelpers;
using namespace unitVGTestHelpers;
using namespace geoInfoVGTestHelpers;

// One input for the two `LiteralValueGetter`s below, together with the content
// that each of them is expected to extract from it. The two getters see exactly
// the same inputs and differ only in those expectations, so they share this
// table.
struct LiteralGetterTestCase {
  // The word, and whether it is a word of the auxiliary vocabulary (as opposed
  // to one of the vocabulary of the main index).
  std::string word_;
  bool isFromAuxVocab_;
  std::optional<std::string> expectedWithStrFunction_;
  std::optional<std::string> expectedWithoutStrFunction_;
};

// The words of the auxiliary vocabulary are deliberately handled just like the
// words of the vocabulary of the main index.
const std::vector<LiteralGetterTestCase> literalGetterTestCases{
    {"\"noType\"", false, "noType", "noType"},
    {"\"someType\"^^<someType>", false, "someType", std::nullopt},
    {"\"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>", false,
     "anXsdString", "anXsdString"},
    {"<x>", false, "x", std::nullopt},
    {auxPlainLiteral, true, "noAuxType", "noAuxType"},
    {auxTypedLiteral, true, "someAuxType", std::nullopt},
    {auxIri, true, "https://example.com/aux", std::nullopt}};

// Run all of `literalGetterTestCases` against `getter`, where `getExpected`
// selects the expectation that belongs to that getter.
void checkAllLiteralGetterTestCases(LiteralValueGetterVariant getter,
                                    const auto& getExpected) {
  for (const auto& testCase : literalGetterTestCases) {
    SCOPED_TRACE(testCase.word_);
    const auto& expected = getExpected(testCase);
    if (testCase.isFromAuxVocab_) {
      checkLiteralContentAndDatatypeFromAuxVocabId(testCase.word_, expected,
                                                   std::nullopt, getter);
    } else {
      checkLiteralContentAndDatatypeFromId(testCase.word_, expected,
                                           std::nullopt, getter);
    }
  }
}

// _____________________________________________________________________________
TEST(LiteralValueGetterWithStrFunction, OperatorWithId) {
  checkAllLiteralGetterTestCases(
      sparqlExpression::detail::LiteralValueGetterWithStrFunction{},
      [](const LiteralGetterTestCase& testCase) {
        return testCase.expectedWithStrFunction_;
      });
}

// _____________________________________________________________________________
TEST(LiteralValueGetterWithStrFunction, OperatorWithLiteralOrIri) {
  using Iri = ad_utility::triple_component::Iri;
  sparqlExpression::detail::LiteralValueGetterWithStrFunction
      literalValueGetter;
  checkLiteralContentAndDatatypeFromLiteralOrIri("noType", std::nullopt, false,
                                                 "noType", std::nullopt,
                                                 literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "someType", Iri::fromIriref("<someType>"), false, "someType",
      std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "anXsdString",
      Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"), false,
      "anXsdString", std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "<x>", std::nullopt, true, "x", std::nullopt, literalValueGetter);
}

// _____________________________________________________________________________
TEST(LiteralValueGetterWithoutStrFunction, OperatorWithId) {
  checkAllLiteralGetterTestCases(
      sparqlExpression::detail::LiteralValueGetterWithoutStrFunction{},
      [](const LiteralGetterTestCase& testCase) {
        return testCase.expectedWithoutStrFunction_;
      });
}

// _____________________________________________________________________________
TEST(LiteralValueGetterWithoutStrFunction, OperatorWithLiteralOrIri) {
  using Iri = ad_utility::triple_component::Iri;
  sparqlExpression::detail::LiteralValueGetterWithoutStrFunction
      literalValueGetter;
  checkLiteralContentAndDatatypeFromLiteralOrIri("noType", std::nullopt, false,
                                                 "noType", std::nullopt,
                                                 literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "someType", Iri::fromIriref("<someType>"), false, std::nullopt,
      std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "anXsdString",
      Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"), false,
      "anXsdString", std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri("<x>", std::nullopt, true,
                                                 std::nullopt, std::nullopt,
                                                 literalValueGetter);
}

// _____________________________________________________________________________
TEST(UnitOfMeasurementValueGetter, OperatorWithId) {
  sparqlExpression::detail::UnitOfMeasurementValueGetter unitValueGetter;
  checkUnitValueGetterFromId("<http://qudt.org/vocab/unit/M>",
                             UnitOfMeasurement::METERS, unitValueGetter);
  checkUnitValueGetterFromId("<http://qudt.org/vocab/unit/KiloM>",
                             UnitOfMeasurement::KILOMETERS, unitValueGetter);
  checkUnitValueGetterFromId("<http://qudt.org/vocab/unit/MI>",
                             UnitOfMeasurement::MILES, unitValueGetter);
  checkUnitValueGetterFromId(
      "\"http://qudt.org/vocab/unit/M\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>",
      UnitOfMeasurement::METERS, unitValueGetter);
  checkUnitValueGetterFromId(
      "\"http://qudt.org/vocab/unit/KiloM\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>",
      UnitOfMeasurement::KILOMETERS, unitValueGetter);
  checkUnitValueGetterFromId(
      "\"http://qudt.org/vocab/unit/MI\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>",
      UnitOfMeasurement::MILES, unitValueGetter);

  checkUnitValueGetterFromId(
      "\"http://qudt.org/vocab/unit/example\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>",
      UnitOfMeasurement::UNKNOWN, unitValueGetter);

  checkUnitValueGetterFromId(
      "\"http://example.com\"^^<http://www.w3.org/2001/XMLSchema#anyURI>",
      UnitOfMeasurement::UNKNOWN, unitValueGetter);
  checkUnitValueGetterFromId("\"x\"", UnitOfMeasurement::UNKNOWN,
                             unitValueGetter);
  checkUnitValueGetterFromId("\"1.5\"^^<http://example.com>",
                             UnitOfMeasurement::UNKNOWN, unitValueGetter);
  checkUnitValueGetterFromId("\"http://qudt.org/vocab/unit/MI\"",
                             UnitOfMeasurement::UNKNOWN, unitValueGetter);

  // A word of an auxiliary vocabulary is resolved just like a word of the
  // vocabulary of the main index.
  checkUnitValueGetterFromAuxVocabId(auxUnitIri, UnitOfMeasurement::METERS,
                                     unitValueGetter);
  checkUnitValueGetterFromAuxVocabId(auxIri, UnitOfMeasurement::UNKNOWN,
                                     unitValueGetter);
  checkUnitValueGetterFromAuxVocabId(
      auxPlainLiteral, UnitOfMeasurement::UNKNOWN, unitValueGetter);
}

// _____________________________________________________________________________
TEST(UnitOfMeasurementValueGetter, OperatorWithIdSkipEncodedValue) {
  sparqlExpression::detail::UnitOfMeasurementValueGetter getter;
  checkUnitValueGetterFromIdEncodedValue(ValueId::makeFromBool(true), getter);
  checkUnitValueGetterFromIdEncodedValue(ValueId::makeFromBool(false), getter);
  checkUnitValueGetterFromIdEncodedValue(ValueId::makeFromInt(-50), getter);
  checkUnitValueGetterFromIdEncodedValue(ValueId::makeFromInt(1000), getter);
  checkUnitValueGetterFromIdEncodedValue(ValueId::makeFromDouble(1000.5),
                                         getter);
  checkUnitValueGetterFromIdEncodedValue(
      ValueId::makeFromGeoPoint(GeoPoint{20.0, 20.0}), getter);
}

// _____________________________________________________________________________
TEST(UnitOfMeasurementValueGetter, OperatorWithLiteralOrIri) {
  sparqlExpression::detail::UnitOfMeasurementValueGetter unitValueGetter;
  checkUnitValueGetterFromLiteralOrIri("http://qudt.org/vocab/unit/M",
                                       UnitOfMeasurement::METERS,
                                       unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri("http://qudt.org/vocab/unit/MI",
                                       UnitOfMeasurement::MILES,
                                       unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri("http://qudt.org/vocab/unit/KiloM",
                                       UnitOfMeasurement::KILOMETERS,
                                       unitValueGetter);

  checkUnitValueGetterFromLiteralOrIri("http://qudt.org/vocab/unit/m",
                                       UnitOfMeasurement::UNKNOWN,
                                       unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri("http://qudt.org/vocab/unit/",
                                       UnitOfMeasurement::UNKNOWN,
                                       unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri(
      "http://example.com/", UnitOfMeasurement::UNKNOWN, unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri("", UnitOfMeasurement::UNKNOWN,
                                       unitValueGetter);
  checkUnitValueGetterFromLiteralOrIri("x", UnitOfMeasurement::UNKNOWN,
                                       unitValueGetter);
}

// _____________________________________________________________________________
TEST(GeometryInfoValueGetterTest, OperatorWithVocabIdOrLiteral) {
  GeoInfoTester t;
  auto noGeoInfo = geoInfoMatcher(std::nullopt);
  static constexpr std::string_view line =
      "\"LINESTRING(2 2, 4 4)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  t.checkFromLocalAndNormalVocabAndLiteral(
      std::string{line},
      geoInfoMatcher(ad_utility::GeometryInfo{2,
                                              {{2, 2}, {4, 4}},
                                              {3, 3},
                                              {1},
                                              getLengthForTesting(line),
                                              MetricArea{0}}));
  static constexpr std::string_view polygon =
      "\"POLYGON((2 4, 4 4, 4 2, 2 2))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  t.checkFromLocalAndNormalVocabAndLiteral(
      std::string{polygon},
      geoInfoMatcher(ad_utility::GeometryInfo{3,
                                              {{2, 2}, {4, 4}},
                                              {3, 3},
                                              {1},
                                              getLengthForTesting(polygon),
                                              getAreaForTesting(polygon)}));
  t.checkFromLocalAndNormalVocabAndLiteral("\"someType\"^^<someType>",
                                           noGeoInfo);
  t.checkFromLocalAndNormalVocabAndLiteral("\"noType\"", noGeoInfo);
  t.checkFromLocalAndNormalVocabAndLiteral("<https://example.com/test>",
                                           noGeoInfo);

  // The WKT literals of an auxiliary vocabulary are parsed just like those of
  // the vocabulary of the main index. NOTE: The placeholder `AuxVocabulary`
  // stores no precomputed `GeometryInfo`, so it is computed from the string.
  t.checkFromAuxVocab(auxWktLiteral, geoInfoMatcher(ad_utility::GeometryInfo{
                                         2,
                                         {{6, 6}, {8, 8}},
                                         {7, 7},
                                         {1},
                                         getLengthForTesting(auxWktLiteral),
                                         MetricArea{0}}));
  t.checkFromAuxVocab(auxPlainLiteral, noGeoInfo);
  t.checkFromAuxVocab(auxIri, noGeoInfo);
}

// _____________________________________________________________________________
TEST(GeometryInfoValueGetterTest, OperatorWithIdGeoPoint) {
  GeoInfoTester t;
  auto noGeoInfo = geoInfoMatcher(std::nullopt);
  t.checkFromValueId(ValueId::makeFromGeoPoint({3, 2}),
                     geoInfoMatcher(GeometryInfo{1,
                                                 {{3, 2}, {3, 2}},
                                                 {3, 2},
                                                 {1},
                                                 ad_utility::MetricLength{0},
                                                 MetricArea{0}}));
  t.checkFromValueId(ValueId::makeUndefined(), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromBool(true), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromInt(42), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromDouble(42.01), noGeoInfo);
}

// _____________________________________________________________________________
TEST(GeometryInfoValueGetterTest, OperatorWithUnrelatedId) {
  GeoInfoTester t;
  auto noGeoInfo = geoInfoMatcher(std::nullopt);
  t.checkFromValueId(ValueId::makeUndefined(), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromBool(true), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromInt(42), noGeoInfo);
  t.checkFromValueId(ValueId::makeFromDouble(42.01), noGeoInfo);
}

// _____________________________________________________________________________
TEST(GeoPointOrWktValueGetterTest, OperatorWithIdGeoPoint) {
  GeoPointOrWktTester t;

  GeoPoint p1{20, 30};
  auto p1id = ValueId::makeFromGeoPoint(p1);
  t.checkFromValueId(p1id, geoPointOrWktMatcher(p1));

  GeoPoint p2{0, 0};
  auto p2id = ValueId::makeFromGeoPoint(p2);
  t.checkFromValueId(p2id, geoPointOrWktMatcher(p2));

  auto noGeoInfoOrWkt = geoPointOrWktMatcher(std::nullopt);
  t.checkFromValueId(ValueId::makeUndefined(), noGeoInfoOrWkt);
  t.checkFromValueId(ValueId::makeFromBool(true), noGeoInfoOrWkt);
  t.checkFromValueId(ValueId::makeFromInt(42), noGeoInfoOrWkt);
  t.checkFromValueId(ValueId::makeFromDouble(42.01), noGeoInfoOrWkt);
}

// _____________________________________________________________________________
TEST(GeoPointOrWktValueGetterTest, OperatorWithLit) {
  checkGeoPointOrWktFromLocalAndNormalVocabAndLiteralForValid(
      "\"LINESTRING(2 2, 4 4)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  checkGeoPointOrWktFromLocalAndNormalVocabAndLiteralForValid(
      "\"POLYGON((2 4, 4 4, 4 2, 2 2))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");

  GeoPointOrWktTester t;
  auto noGeoInfoOrWkt = geoPointOrWktMatcher(std::nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"someType\"^^<someType>",
                                           noGeoInfoOrWkt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"noType\"", noGeoInfoOrWkt);
  t.checkFromLocalAndNormalVocabAndLiteral("<https://example.com/test>",
                                           noGeoInfoOrWkt);

  // A WKT literal of an auxiliary vocabulary is returned as it is, just like
  // one of the vocabulary of the main index.
  t.checkFromAuxVocab(auxWktLiteral, geoPointOrWktMatcher(auxWktLiteral));
  t.checkFromAuxVocab(auxPlainLiteral, noGeoInfoOrWkt);
  t.checkFromAuxVocab(auxIri, noGeoInfoOrWkt);
}

// _____________________________________________________________________________
TEST(IntValueGetterTest, OperatorWithId) {
  IntValueGetterTester t;
  t.checkFromValueId(ValueId::makeFromInt(42), Eq(42));
  t.checkFromValueId(ValueId::makeFromInt(-500), Eq(-500));
  t.checkFromValueId(ValueId::makeFromBool(true), Eq(std::nullopt));
  t.checkFromValueId(ValueId::makeUndefined(), Eq(std::nullopt));
  t.checkFromValueId(ValueId::makeFromDouble(4.5), Eq(std::nullopt));
  t.checkFromValueId(ValueId::makeFromGeoPoint({3, 4}), Eq(std::nullopt));
  // No word of a vocabulary is an integer, not even a word of an auxiliary
  // vocabulary.
  t.checkFromAllAuxVocabWords(Eq(std::nullopt));
}

// _____________________________________________________________________________
TEST(IntValueGetterTest, OperatorWithLit) {
  IntValueGetterTester t;
  auto noInt = Eq(std::nullopt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"someType\"^^<someType>", noInt);
  t.checkFromLocalAndNormalVocabAndLiteral("\"noType\"", noInt);
  t.checkFromLocalAndNormalVocabAndLiteral("<https://example.com/test>", noInt);
}

// _____________________________________________________________________________
TEST(NumericOrDateValueGetterTest, OperatorWithId) {
  NumericOrDateValueGetterTester t;
  auto expectDouble = [](double value)
      -> Matcher<std::optional<sparqlExpression::detail::NumericOrDateValue>> {
    return Optional(VariantWith<double>(DoubleNear(value, 0.01)));
  };
  auto expectInt = [](int64_t value)
      -> Matcher<std::optional<sparqlExpression::detail::NumericOrDateValue>> {
    return Optional(VariantWith<int64_t>(Eq(value)));
  };
  auto expectDateYearOrDuration = [](DateYearOrDuration value)
      -> Matcher<std::optional<sparqlExpression::detail::NumericOrDateValue>> {
    return Optional(VariantWith<DateYearOrDuration>(Eq(value)));
  };

  t.checkFromValueId(ValueId::makeFromInt(-42), expectInt(-42));
  t.checkFromValueId(ValueId::makeFromDouble(50.2), expectDouble(50.2));
  t.checkFromValueId(ValueId::makeFromBool(true), expectInt(1));
  t.checkFromValueId(
      ValueId::makeFromDate(DateYearOrDuration(Date(2013, 5, 16))),
      expectDateYearOrDuration(DateYearOrDuration(Date(2013, 5, 16))));
  t.checkFromValueId(
      ValueId::makeFromDate(DateYearOrDuration(
          DayTimeDuration(DayTimeDuration::Type::Positive, 102))),
      expectDateYearOrDuration(DateYearOrDuration(
          DayTimeDuration(DayTimeDuration::Type::Positive, 102))));
  auto isNotNumeric =
      Optional(VariantWith<sparqlExpression::detail::NotNumeric>(_));
  t.checkFromValueId(ValueId::makeUndefined(), isNotNumeric);
  t.checkFromValueId(ValueId::makeFromGeoPoint({3, 4}), isNotNumeric);

  // `LiteralOrIri`.
  t.checkFromLocalAndNormalVocabAndLiteral("\"someType\"^^<someType>",
                                           isNotNumeric);
  t.checkFromLocalAndNormalVocabAndLiteral("\"noType\"", isNotNumeric);
  t.checkFromLocalAndNormalVocabAndLiteral("<https://example.com/test>",
                                           isNotNumeric);

  // No word of an auxiliary vocabulary is numeric either.
  t.checkFromAllAuxVocabWords(isNotNumeric);
}

// The value getters below had no test of their own in this file yet. Each of
// them is now tested for an `Id` of every `Datatype` (including the
// `AuxVocabIndex` of an auxiliary vocabulary), using the fixture below.

// A test fixture that provides an `Id` of every `Datatype` that a value getter
// can encounter, together with the context in which those `Id`s are valid (see
// `AuxVocabTestContext`). The `Id`s are created once per test, such that the
// tests below can simply use them.
class ValueGetterFixture : public ::testing::Test {
 protected:
  AuxVocabTestContext context_;

  // The `Id`s that encode their value directly in the `Id`.
  Id undefined_ = Id::makeUndefined();
  Id boolTrue_ = Id::makeFromBool(true);
  Id boolFalse_ = Id::makeFromBool(false);
  Id int_ = Id::makeFromInt(42);
  Id intZero_ = Id::makeFromInt(0);
  Id double_ = Id::makeFromDouble(4.5);
  Id doubleZero_ = Id::makeFromDouble(0.0);
  Id date_ = Id::makeFromDate(DateYearOrDuration{Date{2013, 5, 16}});
  Id geoPoint_ = Id::makeFromGeoPoint(GeoPoint{3, 4});

  // The `Id`s that point into a data structure of the index, but that denote
  // neither a literal nor an IRI of one of its vocabularies. NOTE: The index
  // has no text index (see `AuxVocabTestContext::makeConfig`), so the last two
  // of these must only be used with a value getter that does not resolve them,
  // which is the case for every getter below: all of them either ignore those
  // two datatypes outright, or resolve them via `idToStringAndType` with
  // `returnOnlyLiterals`, which discards them before the lookup.
  Id blankNode_ = Id::makeFromBlankNodeIndex(BlankNodeIndex::make(0));
  Id textRecord_ = Id::makeFromTextRecordIndex(TextRecordIndex::make(0));
  Id wordVocab_ = Id::makeFromWordVocabIndex(WordVocabIndex::make(0));

  // An IRI that is encoded directly in the `Id`.
  Id encodedIri_ = context_.encodedIriId();

  // The words of the vocabulary of the main index.
  Id plainLiteral_ = context_.getId(mainPlainLiteral);
  Id typedLiteral_ = context_.getId(mainTypedLiteral);
  Id langLiteral_ = context_.getId(mainLangLiteral);
  Id wktLiteral_ = context_.getId(mainWktLiteral);
  Id iri_ = context_.getId(mainIri);

  // The same kinds of word, but from the auxiliary vocabulary.
  Id auxEmptyLiteral_ = context_.auxId(auxEmptyLiteral);
  Id auxPlainLiteral_ = context_.auxId(auxPlainLiteral);
  Id auxTypedLiteral_ = context_.auxId(auxTypedLiteral);
  Id auxLangLiteral_ = context_.auxId(auxLangLiteral);
  Id auxWktLiteral_ = context_.auxId(auxWktLiteral);
  Id auxIri_ = context_.auxId(auxIri);

  // And from the local vocabulary, which holds the words that are contained in
  // neither vocabulary of the index.
  Id localPlainLiteral_ = context_.localVocabId("\"noLocalType\"");
  Id localTypedLiteral_ =
      context_.localVocabId("\"someLocalType\"^^<someType>");
  Id localLangLiteral_ = context_.localVocabId("\"withLocalLang\"@en");
  Id localIri_ = context_.localVocabId("<https://example.com/local>");

  // The `Id`s of the plain literals of the three vocabularies that store
  // strings, together with the content of the literal.
  std::vector<std::pair<Id, std::string>> plainLiterals_{
      {plainLiteral_, "noMainType"},
      {localPlainLiteral_, "noLocalType"},
      {auxPlainLiteral_, "noAuxType"}};
  // The same for the literals with a datatype resp. a language tag, and for the
  // IRIs.
  std::vector<std::pair<Id, std::string>> typedLiterals_{
      {typedLiteral_, "someMainType"},
      {localTypedLiteral_, "someLocalType"},
      {auxTypedLiteral_, "someAuxType"}};
  std::vector<std::pair<Id, std::string>> langLiterals_{
      {langLiteral_, "withMainLang"},
      {localLangLiteral_, "withLocalLang"},
      {auxLangLiteral_, "withAuxLang"}};
  std::vector<std::pair<Id, std::string>> iris_{
      {iri_, "https://example.com/main"},
      {localIri_, "https://example.com/local"},
      {auxIri_, "https://example.com/aux"}};

  // Apply the `ValueGetter` to `id` in the context of this fixture.
  template <typename ValueGetter>
  auto get(Id id) {
    return ValueGetter{}(id, &context_.context);
  }

  // Check that the `ValueGetter` maps every `Id` in `ids` to a value that
  // matches `expected`.
  template <typename ValueGetter, typename Matcher>
  void expectAll(std::initializer_list<Id> ids, const Matcher& expected,
                 ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(loc);
    for (Id id : ids) {
      EXPECT_THAT(get<ValueGetter>(id), expected) << id;
    }
  }

  // Same as `expectAll` above, but for the `Id`s of the given words (see
  // `plainLiterals_` and friends below), where the expected value depends on
  // the content of the word: `makeMatcher` is called with that content.
  template <typename ValueGetter, typename MatcherFactory>
  void expectAllWords(
      const std::vector<std::pair<Id, std::string>>& words,
      const MatcherFactory& makeMatcher,
      ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(loc);
    for (const auto& [id, content] : words) {
      EXPECT_THAT(get<ValueGetter>(id), makeMatcher(content)) << content;
    }
  }
};

// One fixture per value getter, such that each test suite below is named after
// the getter that it tests.
class NumericValueGetterTest : public ValueGetterFixture {};
class EffectiveBooleanValueGetterTest : public ValueGetterFixture {};
class DatatypeValueGetterTest : public ValueGetterFixture {};
class LanguageTagValueGetterTest : public ValueGetterFixture {};
class ToNumericValueGetterTest : public ValueGetterFixture {};
class IsIriAndIsLiteralValueGetterTest : public ValueGetterFixture {};
class IriOrUriValueGetterTest : public ValueGetterFixture {};

// _____________________________________________________________________________
TEST_F(NumericValueGetterTest, OperatorWithId) {
  using namespace sparqlExpression::detail;
  using Getter = NumericValueGetter;
  EXPECT_THAT(get<Getter>(int_), VariantWith<int64_t>(42));
  EXPECT_THAT(get<Getter>(double_), VariantWith<double>(DoubleNear(4.5, 0.01)));
  EXPECT_THAT(get<Getter>(boolTrue_), VariantWith<int64_t>(1));
  EXPECT_THAT(get<Getter>(boolFalse_), VariantWith<int64_t>(0));
  // Nothing else is numeric, in particular no word of any of the three
  // vocabularies that store strings, and no encoded IRI.
  expectAll<Getter>(
      {undefined_, date_, geoPoint_, blankNode_, textRecord_, wordVocab_,
       encodedIri_, plainLiteral_, localPlainLiteral_, auxPlainLiteral_,
       wktLiteral_, auxWktLiteral_, iri_, auxIri_},
      VariantWith<NotNumeric>(_));
}

// _____________________________________________________________________________
TEST_F(EffectiveBooleanValueGetterTest, OperatorWithId) {
  using Getter = sparqlExpression::detail::EffectiveBooleanValueGetter;
  using Result = Getter::Result;
  // Numbers are `true` iff they are not zero, booleans are themselves. A word
  // is `true` iff its content is not empty, no matter which vocabulary it comes
  // from. The remaining datatypes are unconditionally `true`, except for the
  // ones that have no truth value at all.
  expectAll<Getter>({int_,
                     double_,
                     boolTrue_,
                     plainLiteral_,
                     typedLiteral_,
                     langLiteral_,
                     wktLiteral_,
                     iri_,
                     localPlainLiteral_,
                     localIri_,
                     auxPlainLiteral_,
                     auxTypedLiteral_,
                     auxLangLiteral_,
                     auxWktLiteral_,
                     auxIri_,
                     encodedIri_,
                     date_,
                     geoPoint_,
                     textRecord_,
                     wordVocab_},
                    Eq(Result::True));
  expectAll<Getter>({intZero_, doubleZero_, boolFalse_,
                     Id::makeFromDouble(std::nan("")), auxEmptyLiteral_},
                    Eq(Result::False));
  expectAll<Getter>({undefined_, blankNode_}, Eq(Result::Undef));
}

// _____________________________________________________________________________
TEST_F(DatatypeValueGetterTest, OperatorWithId) {
  using Getter = sparqlExpression::detail::DatatypeValueGetter;
  using Iri = ad_utility::triple_component::Iri;
  auto iriMatcher = [](std::string_view iri) {
    return Optional(Iri::fromIrirefWithoutBrackets(iri));
  };
  // The datatypes of the values that are encoded directly in the `Id`.
  EXPECT_THAT(get<Getter>(boolTrue_), iriMatcher(XSD_BOOLEAN_TYPE));
  EXPECT_THAT(get<Getter>(int_), iriMatcher(XSD_INT_TYPE));
  EXPECT_THAT(get<Getter>(double_), iriMatcher(XSD_DOUBLE_TYPE));
  EXPECT_THAT(get<Getter>(geoPoint_), iriMatcher(GEO_WKT_LITERAL));
  EXPECT_THAT(get<Getter>(date_),
              iriMatcher(date_.getDate().toStringAndType().second));

  // For a word of one of the three vocabularies that store strings, the
  // datatype is the one of the literal, `xsd:string` if it has none, or
  // `rdf:langString` if it has a language tag.
  // The datatype is the same for all the words of one group, so ignore the
  // content that `expectAllWords` passes to the matcher factory.
  auto ignoreContent = [](auto matcher) {
    return [matcher](const std::string&) { return matcher; };
  };
  expectAllWords<Getter>(plainLiterals_, ignoreContent(iriMatcher(XSD_STRING)));
  expectAllWords<Getter>(typedLiterals_, ignoreContent(iriMatcher("someType")));
  expectAllWords<Getter>(langLiterals_,
                         ignoreContent(iriMatcher(RDF_LANGTAG_STRING)));
  expectAll<Getter>({wktLiteral_, auxWktLiteral_}, iriMatcher(GEO_WKT_LITERAL));

  // An IRI has no datatype, and neither have the remaining datatypes.
  expectAllWords<Getter>(iris_, ignoreContent(Eq(std::nullopt)));
  expectAll<Getter>(
      {encodedIri_, undefined_, blankNode_, textRecord_, wordVocab_},
      Eq(std::nullopt));
}

// _____________________________________________________________________________
// NOTE: There are additional tests for this getter in the `LanguageTagGetter`
// suite in `LanguageExpressionsTest.cpp`, which covers the words of the
// vocabulary of the main index and of a local vocabulary.
TEST_F(LanguageTagValueGetterTest, OperatorWithId) {
  using Getter = sparqlExpression::detail::LanguageTagValueGetter;
  // Only a literal with a language tag has one.
  expectAllWords<Getter>(langLiterals_, [](const std::string&) {
    return Optional(std::string{"en"});
  });
  // For a literal without a language tag the standard requires the empty
  // string, and the values that are encoded directly in the `Id` all are
  // literals.
  expectAll<Getter>(
      {boolTrue_, int_, double_, date_, geoPoint_, plainLiteral_, typedLiteral_,
       wktLiteral_, localPlainLiteral_, localTypedLiteral_, auxPlainLiteral_,
       auxTypedLiteral_, auxWktLiteral_, auxEmptyLiteral_},
      Optional(::testing::IsEmpty()));
  // An IRI is not a literal, so it has no language tag at all, and neither have
  // the remaining datatypes.
  expectAll<Getter>({iri_, localIri_, auxIri_, encodedIri_, undefined_,
                     blankNode_, textRecord_, wordVocab_},
                    Eq(std::nullopt));
}

// _____________________________________________________________________________
TEST_F(ToNumericValueGetterTest, OperatorWithId) {
  using Getter = sparqlExpression::detail::ToNumericValueGetter;
  EXPECT_THAT(get<Getter>(int_), VariantWith<int64_t>(42));
  EXPECT_THAT(get<Getter>(double_), VariantWith<double>(DoubleNear(4.5, 0.01)));
  EXPECT_THAT(get<Getter>(boolTrue_), VariantWith<int64_t>(1));
  EXPECT_THAT(get<Getter>(geoPoint_),
              VariantWith<std::string>(
                  geoPoint_.getGeoPoint().toStringRepresentation()));

  // For a literal of one of the three vocabularies that store strings, the
  // getter falls back to its content.
  for (const auto& literals : {plainLiterals_, typedLiterals_, langLiterals_}) {
    expectAllWords<Getter>(literals, [](const std::string& content) {
      return VariantWith<std::string>(content);
    });
  }

  // An IRI is not a literal, so there is nothing to convert, and the remaining
  // datatypes have no string representation that this getter uses.
  expectAll<Getter>({iri_, localIri_, auxIri_, encodedIri_, undefined_, date_,
                     blankNode_, textRecord_, wordVocab_},
                    VariantWith<std::monostate>(_));
}

// _____________________________________________________________________________
TEST_F(IsIriAndIsLiteralValueGetterTest, OperatorWithId) {
  using namespace sparqlExpression::detail;
  auto expectIsIriAndIsLiteral = [this](std::initializer_list<Id> ids,
                                        bool isIri, bool isLiteral) {
    expectAll<IsIriValueGetter>(ids, Eq(Id::makeFromBool(isIri)));
    expectAll<IsLiteralValueGetter>(ids, Eq(Id::makeFromBool(isLiteral)));
  };
  // The literals of the three vocabularies that store strings.
  //
  // NOTE: `wktLiteral_` (the WKT literal of the vocabulary of the main index)
  // is deliberately absent, because `Vocabulary::isLiteral` misclassifies it as
  // soon as that vocabulary is geo-split (which `makeTestIndex` chooses at
  // random): `Vocabulary::prefixRanges` describes the literals by a single
  // contiguous range of indices, but the indices of a `SplitVocabulary` are
  // marker-encoded, so the words of the geo sub-vocabulary lie far outside that
  // range. The WKT literal of the auxiliary vocabulary below is unaffected,
  // because that vocabulary does not split its words.
  // TODO<joka921> Fix that bug, then add `wktLiteral_` here.
  expectIsIriAndIsLiteral(
      {plainLiteral_, typedLiteral_, langLiteral_, localPlainLiteral_,
       localTypedLiteral_, localLangLiteral_, auxEmptyLiteral_,
       auxPlainLiteral_, auxTypedLiteral_, auxLangLiteral_, auxWktLiteral_},
      false, true);
  // Their IRIs, and the IRIs that are encoded directly in the `Id`.
  expectIsIriAndIsLiteral({iri_, localIri_, auxIri_, encodedIri_}, true, false);
  // The values that are encoded directly in the `Id` are literals.
  expectIsIriAndIsLiteral({boolTrue_, int_, double_, date_, geoPoint_}, false,
                          true);
  // The remaining datatypes are neither.
  expectIsIriAndIsLiteral({undefined_, blankNode_, textRecord_, wordVocab_},
                          false, false);
}

// _____________________________________________________________________________
TEST_F(IriOrUriValueGetterTest, OperatorWithId) {
  using Getter = sparqlExpression::detail::IriOrUriValueGetter;
  // The getter turns a literal or an IRI into an IRI, which is a
  // `LocalVocabEntry` because it does not have to be contained in any
  // vocabulary of the index.
  auto isIriEntry = [](const std::string& expected) {
    return VariantWith<LocalVocabEntry>(
        AD_PROPERTY(LocalVocabEntry, toStringRepresentation,
                    Eq(absl::StrCat("<", expected, ">"))));
  };
  for (const auto& words : {plainLiterals_, typedLiterals_, iris_}) {
    expectAllWords<Getter>(words, isIriEntry);
  }
  EXPECT_THAT(get<Getter>(encodedIri_),
              isIriEntry("https://encoded.example.com/123"));

  // For all the other datatypes there is no IRI, which the getter signals with
  // an undefined `Id`.
  expectAll<Getter>({undefined_, boolTrue_, int_, double_, date_, geoPoint_,
                     blankNode_, textRecord_, wordVocab_},
                    VariantWith<Id>(Eq(Id::makeUndefined())));
}
};  // namespace

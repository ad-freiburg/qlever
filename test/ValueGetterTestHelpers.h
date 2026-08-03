//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: @DuDaAG,
//           Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_VALUEGETTERTESTHELPERS_H
#define QLEVER_TEST_VALUEGETTERTESTHELPERS_H

#include <gtest/gtest.h>

#include <optional>

#include "./GeometryInfoTestHelpers.h"
#include "./SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/Constants.h"
#include "index/IndexImpl.h"
#include "index/LocalVocab.h"
#include "index/LocalVocabEntry.h"
#include "index/vocabulary/AuxVocabulary.h"
#include "index/vocabulary/VocabularyType.h"
#include "parser/LiteralOrIri.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/Literal.h"
#include "util/GTestHelpers.h"
#include "util/TypeTraits.h"

namespace valueGetterTestHelpers {

const std::string ttl = R"(
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
<x> <y> "anXsdString"^^xsd:string, 
        "someType"^^<someType>,
        "noType".
  )";
struct TestContextWithGivenTTl {
  ad_utility::testing::TestIndexConfig config;
  QueryExecutionContext* qec = ad_utility::testing::getQec(config);
  VariableToColumnMap varToColMap;
  LocalVocab localVocab;
  IdTable table{qec->getAllocator()};
  sparqlExpression::EvaluationContext context{
      *qec,
      varToColMap,
      table.asStaticView<0>(),
      qec->getAllocator(),
      localVocab,
      std::make_shared<ad_utility::CancellationHandle<>>(),
      sparqlExpression::EvaluationContext::TimePoint::max()};
  std::function<Id(const std::string&)> getId =
      ad_utility::testing::makeGetId(qec->getIndex());

  // Create a context for an index that is built from the given `config`. Use
  // this overload if the index needs more than a knowledge graph, for example a
  // text index.
  explicit TestContextWithGivenTTl(ad_utility::testing::TestIndexConfig config)
      : config{std::move(config)} {}

  // Create a context for an index that is built from the given knowledge graph.
  TestContextWithGivenTTl(
      std::string turtle,
      std::optional<ad_utility::VocabularyType> vocabularyType = std::nullopt)
      : TestContextWithGivenTTl{[&turtle, &vocabularyType]() {
          ad_utility::testing::TestIndexConfig config{std::move(turtle)};
          config.vocabularyType = vocabularyType;
          return config;
        }()} {}
};

// Helper function to check literal value and datatype
inline void checkLiteralContentAndDatatype(
    const std::optional<ad_utility::triple_component::Literal>& literal,
    const std::optional<std::string>& expectedContent,
    const std::optional<std::string>& expectedDatatype) {
  ASSERT_EQ(literal.has_value(), expectedContent.has_value());
  if (!literal.has_value()) {
    return;
  }
  auto expected = ad_utility::triple_component::Literal::literalWithoutQuotes(
      expectedContent.value());
  if (expectedDatatype.has_value()) {
    expected.addDatatype(
        ad_utility::triple_component::Iri::fromIrirefWithoutBrackets(
            expectedDatatype.value()));
  }
  ASSERT_EQ(literal.value(), expected);
};

// Helper function to get literal from Id and then check its content and
// datatype
inline void checkLiteralContentAndDatatypeFromId(
    const std::string& literalString,
    const std::optional<std::string>& expectedContent,
    const std::optional<std::string>& expectedDatatype,
    std::variant<sparqlExpression::detail::LiteralValueGetterWithStrFunction,
                 sparqlExpression::detail::LiteralValueGetterWithoutStrFunction>
        getter) {
  TestContextWithGivenTTl testContext{ttl};
  auto literal = std::visit(
      [&](auto&& g) {
        return g(testContext.getId(literalString), &testContext.context);
      },
      getter);

  return checkLiteralContentAndDatatype(literal, expectedContent,
                                        expectedDatatype);
};

// Helper function to get literal from LiteralOrIri and then check its content
// and datatype
inline void checkLiteralContentAndDatatypeFromLiteralOrIri(
    const std::string_view& literalContent,
    const std::optional<ad_utility::triple_component::Iri>& literalDescriptor,
    const bool isIri, const std::optional<std::string>& expectedContent,
    const std::optional<std::string>& expectedDatatype,
    std::variant<sparqlExpression::detail::LiteralValueGetterWithStrFunction,
                 sparqlExpression::detail::LiteralValueGetterWithoutStrFunction>
        getter) {
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  using Literal = ad_utility::triple_component::Literal;
  TestContextWithGivenTTl testContext{ttl};

  auto toLiteralOrIri = [](std::string_view content, auto descriptor,
                           bool isIri) {
    if (isIri) {
      return LiteralOrIri::iriref(std::string(content));
    } else {
      return LiteralOrIri{Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(content), descriptor)};
    }
  };
  LiteralOrIri literalOrIri =
      toLiteralOrIri(literalContent, literalDescriptor, isIri);
  auto literal = std::visit(
      [&](auto&& g) { return g(literalOrIri, &testContext.context); }, getter);
  return checkLiteralContentAndDatatype(literal, expectedContent,
                                        expectedDatatype);
};

// The words of the auxiliary vocabulary of an index (see
// `index/vocabulary/AuxVocabulary.h`) that the tests use, in the order in which
// an `AuxVocabulary` stores them. NOTE: That order is the one of `std::string`,
// whereas the actual implementation will use the collation of the vocabulary of
// the main index. These words are deliberately chosen such that the two orders
// agree (verified against the vocabulary of a test index), so that the tests do
// not encode a wrong assumption about the order.
inline const std::vector<std::string> auxVocabWords{
    "\"\"",
    "\"LINESTRING(6 6, 8 8)\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
    "\"noAuxType\"",
    "\"someAuxType\"^^<someType>",
    "\"withAuxLang\"@en",
    "<http://qudt.org/vocab/unit/M>",
    "<https://example.com/aux>"};

// Names for the words of `auxVocabWords`, to be used with the helpers below.
inline const std::string auxEmptyLiteral = auxVocabWords.at(0);
inline const std::string auxWktLiteral = auxVocabWords.at(1);
inline const std::string auxPlainLiteral = auxVocabWords.at(2);
inline const std::string auxTypedLiteral = auxVocabWords.at(3);
inline const std::string auxLangLiteral = auxVocabWords.at(4);
inline const std::string auxUnitIri = auxVocabWords.at(5);
inline const std::string auxIri = auxVocabWords.at(6);

// The words of the vocabulary of the main index of `AuxVocabTestContext`, one
// per kind of literal and IRI that the value getters distinguish. They are
// deliberately disjoint from `auxVocabWords`, because the two vocabularies are.
inline const std::string mainPlainLiteral = "\"noMainType\"";
inline const std::string mainTypedLiteral = "\"someMainType\"^^<someType>";
inline const std::string mainLangLiteral = "\"withMainLang\"@en";
inline const std::string mainWktLiteral =
    "\"LINESTRING(2 2, 4 4)\""
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
inline const std::string mainIri = "<https://example.com/main>";

// The prefix of the IRIs that the index of `AuxVocabTestContext` encodes
// directly in the `Id`, and an IRI that has that prefix.
inline const std::string encodedIriPrefix = "https://encoded.example.com/";
inline const std::string encodedIri = "<https://encoded.example.com/123>";

// A test context whose index has an auxiliary vocabulary that holds
// `auxVocabWords`. Its knowledge graph holds the `main...` words above, and its
// index additionally has a text index and encodes the IRIs that start with
// `encodedIriPrefix` directly in the `Id`, such that an `Id` of every
// `Datatype` can be created for it (see the fixture in `ValueGetterTest.cpp`).
//
// NOTE: The knowledge graph deliberately contains none of the `auxVocabWords`
// (an auxiliary vocabulary is disjoint from the vocabulary of the main index)
// and is used by no other test. The latter matters because `getQec` caches its
// indices by the configuration, so the auxiliary vocabulary that is set here
// would otherwise leak into unrelated tests.
struct AuxVocabTestContext : TestContextWithGivenTTl {
  AuxVocabTestContext() : TestContextWithGivenTTl{makeConfig()} {
    // `getQec` only hands out a `const Index&`, but setting the auxiliary
    // vocabulary is a deliberate test-only mutation of that very index.
    const_cast<Index&>(qec->getIndex())
        .getImpl()
        .setAuxVocabForTesting(std::make_shared<AuxVocabulary>(auxVocabWords));
  }

  // The `Id` of the given word of the auxiliary vocabulary. The word has to be
  // one of `auxVocabWords`.
  Id auxId(const std::string& word) const {
    auto index = qec->getIndex().getImpl().auxVocab()->getId(word);
    AD_CONTRACT_CHECK(index.has_value(),
                      "The given word is not one of `auxVocabWords`");
    return Id::makeFromAuxVocabIndex(index.value());
  }

  // The `Id` of `encodedIri`, which the index encodes directly in the `Id`.
  Id encodedIriId() const {
    auto id = qec->getIndex().encodedIriManager().encode(encodedIri);
    AD_CONTRACT_CHECK(id.has_value(), "The IRI could not be encoded");
    return id.value();
  }

  // The `Id` of `word` in the local vocabulary of this context. The word has to
  // be contained in neither vocabulary of the index, else it would not be
  // stored in a local vocabulary in the first place.
  Id localVocabId(const std::string& word) {
    return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::fromStringRepresentation(
            word, qec->getLocalVocabContext())));
  }

 private:
  // The configuration of the index, see the class comment.
  static ad_utility::testing::TestIndexConfig makeConfig() {
    ad_utility::testing::TestIndexConfig config{absl::StrCat(
        "<x> <y> ", mainPlainLiteral, " , ", mainTypedLiteral, " , ",
        mainLangLiteral, " , ", mainWktLiteral, " , ", mainIri, " .\n")};
    config.createTextIndex = true;
    config.encodedPrefixesWithoutAngleBrackets =
        std::vector<std::string>{encodedIriPrefix};
    return config;
  }
};

// Helper function to get a literal from the `Id` of a word of the auxiliary
// vocabulary and then check its content and datatype. This mirrors
// `checkLiteralContentAndDatatypeFromId` above.
inline void checkLiteralContentAndDatatypeFromAuxVocabId(
    const std::string& word, const std::optional<std::string>& expectedContent,
    const std::optional<std::string>& expectedDatatype,
    std::variant<sparqlExpression::detail::LiteralValueGetterWithStrFunction,
                 sparqlExpression::detail::LiteralValueGetterWithoutStrFunction>
        getter) {
  AuxVocabTestContext testContext;
  auto literal = std::visit(
      [&](auto&& g) {
        return g(testContext.auxId(word), &testContext.context);
      },
      getter);
  return checkLiteralContentAndDatatype(literal, expectedContent,
                                        expectedDatatype);
};

}  // namespace valueGetterTestHelpers

namespace unitVGTestHelpers {

using namespace valueGetterTestHelpers;

// Test turtle for
const std::string unitTtl = R"(
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
<x> <y> "http://example.com"^^xsd:anyURI, 
        "http://qudt.org/vocab/unit/M"^^xsd:anyURI, 
        "http://qudt.org/vocab/unit/KiloM"^^xsd:anyURI, 
        "http://qudt.org/vocab/unit/MI"^^xsd:anyURI, 
        "http://qudt.org/vocab/unit/example"^^xsd:anyURI, 
        "http://qudt.org/vocab/unit/MI", 
        <http://qudt.org/vocab/unit/M>, 
        <http://qudt.org/vocab/unit/KiloM>, 
        <http://qudt.org/vocab/unit/MI>, 
        "1.5"^^<http://example.com>, 
        "x".
  )";

// Helper to test UnitOfMeasurementValueGetter using ValueId input
inline void checkUnitValueGetterFromId(
    const std::string& fullLiteralOrIri, UnitOfMeasurement expectedResult,
    sparqlExpression::detail::UnitOfMeasurementValueGetter getter) {
  TestContextWithGivenTTl testContext{unitTtl};
  auto actualResult =
      getter(testContext.getId(fullLiteralOrIri), &testContext.context);
  ASSERT_EQ(actualResult, expectedResult);
};

// Helper to test UnitOfMeasurementValueGetter using ValueId input where the
// ValueId represents an encoded value
inline void checkUnitValueGetterFromIdEncodedValue(
    ValueId id, sparqlExpression::detail::UnitOfMeasurementValueGetter getter) {
  TestContextWithGivenTTl testContext{unitTtl};
  ASSERT_EQ(getter(id, &testContext.context), UnitOfMeasurement::UNKNOWN);
}

// Helper to test `UnitOfMeasurementValueGetter` on the given word of an
// auxiliary vocabulary, see `checkUnitValueGetterFromId` above. The word has to
// be one of `auxVocabWords`.
inline void checkUnitValueGetterFromAuxVocabId(
    const std::string& word, UnitOfMeasurement expectedResult,
    sparqlExpression::detail::UnitOfMeasurementValueGetter getter) {
  AuxVocabTestContext testContext;
  ASSERT_EQ(getter(testContext.auxId(word), &testContext.context),
            expectedResult);
}

// Helper to test UnitOfMeasurementValueGetter using ValueId input
inline void checkUnitValueGetterFromLiteralOrIri(
    const std::string& unitIriWithoutBrackets, UnitOfMeasurement expectedResult,
    sparqlExpression::detail::UnitOfMeasurementValueGetter getter) {
  TestContextWithGivenTTl testContext{unitTtl};

  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  using Iri = ad_utility::triple_component::Iri;

  auto doTest = [&](const ad_utility::triple_component::LiteralOrIri& litOrIri,
                    bool expectSuccess) {
    auto actualResult = getter(litOrIri, &testContext.context);
    ASSERT_EQ(actualResult,
              expectSuccess ? expectedResult : UnitOfMeasurement::UNKNOWN);
  };

  // Test xsd:anyURI literal method
  auto litTest = [&](const std::string& lit, const std::optional<Iri>& datatype,
                     bool expectSuccess) {
    doTest(LiteralOrIri::literalWithoutQuotes(lit, datatype), expectSuccess);
  };

  litTest(
      unitIriWithoutBrackets,
      Iri::fromIrirefWithoutBrackets("http://www.w3.org/2001/XMLSchema#anyURI"),
      true);
  litTest(unitIriWithoutBrackets, std::nullopt, false);
  litTest(unitIriWithoutBrackets,
          Iri::fromIrirefWithoutBrackets("http://example.com/"), false);

  // Test IRI method
  doTest(LiteralOrIri{Iri::fromIrirefWithoutBrackets(unitIriWithoutBrackets)},
         true);
};

}  // namespace unitVGTestHelpers

namespace geoInfoVGTestHelpers {

using namespace valueGetterTestHelpers;
using namespace geoInfoTestHelpers;

// Helper class to test different value getters
template <typename ValueGetter, typename ReturnType>
class ValueGetterTester {
 private:
  // Test knowledge graph that contains all used literals and iris.
  const std::string testTtl_ =
      "<x> <y> \"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>, "
      " \"someType\"^^<someType>,"
      " <https://example.com/test>,"
      " \"noType\" ,"
      " \"LINESTRING(2 2, 4 4)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>,\n"
      " \"POLYGON((2 4, 4 4, 4 2, 2 2))\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>.\n";

 public:
  // Helper that constructs a local vocab, inserts the literal and passes the
  // `LocalVocabIndex` as a `ValueId` to the `ValueGetter`.
  void checkFromLocalVocab(
      std::string literal,
      ::testing::Matcher<std::optional<ReturnType>> expected,
      Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    ValueGetter getter;
    // Empty knowledge graph, so everything needs to be in the local vocab.
    TestContextWithGivenTTl testContext{""};
    LocalVocab localVocab;
    auto idx = localVocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::fromStringRepresentation(
            std::move(literal), testContext.qec->getLocalVocabContext()));
    auto id = ValueId::makeFromLocalVocabIndex(idx);
    auto res = getter(id, &testContext.context);
    EXPECT_THAT(res, expected);
  }

  // Helper that tests the `ValueGetter` using the `ValueId` of a
  // `VocabIndex` for a given string in the example knowledge graph.
  void checkFromVocab(std::string literal,
                      ::testing::Matcher<std::optional<ReturnType>> expected,
                      Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    ValueGetter getter;
    TestContextWithGivenTTl testContext{testTtl_};
    VocabIndex idx;
    ASSERT_TRUE(testContext.qec->getIndex().getVocab().getId(literal, &idx))
        << "Given test literal is not contained in test dataset";
    auto id = ValueId::makeFromVocabIndex(idx);
    auto res = getter(id, &testContext.context);
    EXPECT_THAT(res, expected);
  }

  // Helper that tests the `ValueGetter` for any custom `ValueId`
  void checkFromValueId(ValueId input,
                        ::testing::Matcher<std::optional<ReturnType>> expected,
                        Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    ValueGetter getter;
    TestContextWithGivenTTl testContext{testTtl_};
    auto res = getter(input, &testContext.context);
    EXPECT_THAT(res, expected);
  }

  // Helper that tests the `ValueGetter` using the `ValueId` of the given word
  // of the auxiliary vocabulary of the index, see `AuxVocabTestContext`. The
  // word has to be one of `auxVocabWords`.
  void checkFromAuxVocab(const std::string& word,
                         ::testing::Matcher<std::optional<ReturnType>> expected,
                         Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    ValueGetter getter;
    AuxVocabTestContext testContext;
    auto res = getter(testContext.auxId(word), &testContext.context);
    EXPECT_THAT(res, expected);
  }

  // Helper that tests the `ValueGetter` for any literal (or IRI) directly
  // passed to it
  void checkFromLiteral(std::string literal,
                        ::testing::Matcher<std::optional<ReturnType>> expected,
                        Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    ValueGetter getter;
    TestContextWithGivenTTl testContext{testTtl_};
    auto litOrIri =
        ad_utility::triple_component::LiteralOrIri::fromStringRepresentation(
            literal);
    auto res = getter(litOrIri, &testContext.context);
    EXPECT_THAT(res, expected);
  }

  // Run the same test case on vocab, local vocab and literal
  void checkFromLocalAndNormalVocabAndLiteral(
      std::string wktInput,
      ::testing::Matcher<std::optional<ReturnType>> expected,
      Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    checkFromVocab(wktInput, expected);
    checkFromLocalVocab(wktInput, expected);
    checkFromLiteral(wktInput, expected);
  }
};

using GeoInfoTester = ValueGetterTester<
    sparqlExpression::detail::GeometryInfoValueGetter<ad_utility::GeometryInfo>,
    ad_utility::GeometryInfo>;
using GeoPointOrWktTester =
    ValueGetterTester<sparqlExpression::detail::GeoPointOrWktValueGetter,
                      GeoPointOrWkt>;
using IntValueGetterTester =
    ValueGetterTester<sparqlExpression::detail::IntValueGetter, int64_t>;
using NumericOrDateValueGetterTester =
    ValueGetterTester<sparqlExpression::detail::NumericOrDateValueGetter,
                      sparqlExpression::detail::NumericOrDateValue>;
using NumericValueGetterTester =
    ValueGetterTester<sparqlExpression::detail::NumericValueGetter,
                      sparqlExpression::detail::NumericValue>;
using EffectiveBooleanValueGetterTester = ValueGetterTester<
    sparqlExpression::detail::EffectiveBooleanValueGetter,
    sparqlExpression::detail::EffectiveBooleanValueGetter::Result>;
using DatatypeValueGetterTester =
    ValueGetterTester<sparqlExpression::detail::DatatypeValueGetter,
                      ad_utility::triple_component::Iri>;
using LanguageTagValueGetterTester =
    ValueGetterTester<sparqlExpression::detail::LanguageTagValueGetter,
                      std::string>;
using ToNumericValueGetterTester =
    ValueGetterTester<sparqlExpression::detail::ToNumericValueGetter,
                      sparqlExpression::detail::IntDoubleStr>;
using IsIriValueGetterTester =
    ValueGetterTester<sparqlExpression::detail::IsIriValueGetter, Id>;
using IsLiteralValueGetterTester =
    ValueGetterTester<sparqlExpression::detail::IsLiteralValueGetter, Id>;
using IriOrUriValueGetterTester =
    ValueGetterTester<sparqlExpression::detail::IriOrUriValueGetter,
                      sparqlExpression::IdOrLocalVocabEntry>;
// _____________________________________________________________________________
inline void checkGeoPointOrWktFromLocalAndNormalVocabAndLiteralForValid(
    std::string wktInput, Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  // We input `wktInput` twice because we expect the value getter to return the
  // wkt string if it is given a plain wkt string.
  GeoPointOrWktTester{}.checkFromLocalAndNormalVocabAndLiteral(
      wktInput, geoPointOrWktMatcher(wktInput));
}

}  // namespace geoInfoVGTestHelpers

#endif  // QLEVER_TEST_VALUEGETTERTESTHELPERS_H

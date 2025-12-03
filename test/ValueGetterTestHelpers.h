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
#include "engine/LocalVocab.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/Constants.h"
#include "index/LocalVocabEntry.h"
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
  std::string turtleInput;
  std::optional<ad_utility::VocabularyType> vocabularyType = std::nullopt;
  QueryExecutionContext* qec =
      ad_utility::testing::getQec(turtleInput, vocabularyType);
  VariableToColumnMap varToColMap;
  LocalVocab localVocab;
  IdTable table{qec->getAllocator()};
  sparqlExpression::EvaluationContext context{
      *qec,
      varToColMap,
      table,
      qec->getAllocator(),
      localVocab,
      std::make_shared<ad_utility::CancellationHandle<>>(),
      sparqlExpression::EvaluationContext::TimePoint::max()};
  std::function<Id(const std::string&)> getId =
      ad_utility::testing::makeGetId(qec->getIndex());
  TestContextWithGivenTTl(
      std::string turtle,
      std::optional<ad_utility::VocabularyType> vocabularyType = std::nullopt)
      : turtleInput{std::move(turtle)}, vocabularyType{vocabularyType} {}
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
    auto litOrIri =
        ad_utility::triple_component::LiteralOrIri::fromStringRepresentation(
            literal);
    auto idx =
        localVocab.getIndexAndAddIfNotContained(LocalVocabEntry{litOrIri});
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

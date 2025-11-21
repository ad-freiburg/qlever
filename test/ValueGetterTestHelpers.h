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

// Helper that constructs a local vocab, inserts the literal and passes the
// LocalVocabIndex as a ValueId to the GeometryInfoValueGetter
inline void checkGeoInfoFromLocalVocab(
    std::string wktInput, std::optional<ad_utility::GeometryInfo> expected,
    Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  sparqlExpression::detail::GeometryInfoValueGetter getter;
  // Not the geoInfoTtl here because the literals should not be contained
  TestContextWithGivenTTl testContext{ttl};
  LocalVocab localVocab;
  auto litOrIri =
      ad_utility::triple_component::LiteralOrIri::fromStringRepresentation(
          wktInput);
  auto idx = localVocab.getIndexAndAddIfNotContained(LocalVocabEntry{litOrIri});
  auto id = ValueId::makeFromLocalVocabIndex(idx);
  auto res = getter(id, &testContext.context);
  EXPECT_GEOMETRYINFO(res, expected);
}

// Test knowledge graph that contains all used literals and iris.
const std::string geoInfoTtl =
    "<x> <y> \"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>, "
    " \"someType\"^^<someType>,"
    " <https://example.com/test>,"
    " \"noType\" ,"
    " \"LINESTRING(2 2, 4 "
    "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>,"
    " \"POLYGON(2 4, 4 4, 4 2, 2 "
    "2)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>.";

// Helper that tests the GeometryInfoValueGetter using the ValueId of a
// VocabIndex for a string in the example knowledge graph.
inline void checkGeoInfoFromVocab(
    std::string wktInput, std::optional<ad_utility::GeometryInfo> expected,
    Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  sparqlExpression::detail::GeometryInfoValueGetter getter;
  TestContextWithGivenTTl testContext{
      geoInfoTtl,
      // Disable vocabulary type fuzzy testing here
      // TODO<ullingerc> Can be re-enabled after merge of #1983
      VocabularyType{VocabularyType::Enum::OnDiskCompressed}};
  VocabIndex idx;
  ASSERT_TRUE(testContext.qec->getIndex().getVocab().getId(wktInput, &idx));
  auto id = ValueId::makeFromVocabIndex(idx);
  auto res = getter(id, &testContext.context);
  EXPECT_GEOMETRYINFO(res, expected);
}

// Helper that tests the GeometryInfoValueGetter using an arbitrary ValueId
inline void checkGeoInfoFromValueId(
    ValueId input, std::optional<ad_utility::GeometryInfo> expected,
    Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  sparqlExpression::detail::GeometryInfoValueGetter getter;
  TestContextWithGivenTTl testContext{geoInfoTtl};
  auto res = getter(input, &testContext.context);
  EXPECT_GEOMETRYINFO(res, expected);
}

// Helper that tests the GeometryInfoValueGetter using a string passed directly
// as LiteralOrIri, not ValueId
inline void checkGeoInfoFromLiteral(
    std::string wktInput, std::optional<ad_utility::GeometryInfo> expected,
    Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  sparqlExpression::detail::GeometryInfoValueGetter getter;
  TestContextWithGivenTTl testContext{geoInfoTtl};
  auto litOrIri =
      ad_utility::triple_component::LiteralOrIri::fromStringRepresentation(
          wktInput);
  auto res = getter(litOrIri, &testContext.context);
  EXPECT_GEOMETRYINFO(res, expected);
}

// Helper that runs each of the tests for GeometryInfoValueGetter using the same
// input
inline void checkGeoInfoFromLocalAndNormalVocabAndLiteral(
    std::string wktInput, std::optional<ad_utility::GeometryInfo> expected,
    Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  checkGeoInfoFromVocab(wktInput, expected);
  checkGeoInfoFromLocalVocab(wktInput, expected);
  checkGeoInfoFromLiteral(wktInput, expected);
}

}  // namespace geoInfoVGTestHelpers

#endif  // QLEVER_TEST_VALUEGETTERTESTHELPERS_H

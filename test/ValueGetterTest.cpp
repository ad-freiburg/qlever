//  Copyright 2022 - 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: @DuDaAG
//           Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./GeometryInfoTestHelpers.h"
#include "./SparqlExpressionTestHelpers.h"
#include "engine/LocalVocab.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "index/LocalVocabEntry.h"
// #include "index/vocabulary/GeoVocabulary.h"
#include "parser/Literal.h"
#include "parser/LiteralOrIri.h"
#include "util/GeometryInfo.h"

// Common things used in multiple tests.
namespace {
const std::string ttl = R"(
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
<x> <y> "anXsdString"^^xsd:string, 
        "someType"^^<someType>,
        "noType".
  )";
struct TestContextWithGivenTTl {
  std::string turtleInput;
  QueryExecutionContext* qec = ad_utility::testing::getQec(turtleInput);
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
  TestContextWithGivenTTl(std::string turtle)
      : turtleInput{std::move(turtle)} {}
};

// Helper function to check literal value and datatype
void checkLiteralContentAndDatatype(
    const std::optional<ad_utility::triple_component::Literal>& literal,
    const std::optional<std::string>& expectedContent,
    const std::optional<std::string>& expectedDatatype) {
  if (literal.has_value()) {
    ASSERT_EQ(asStringViewUnsafe(literal.value().getContent()),
              expectedContent.value_or(""));

    if (literal.value().hasDatatype()) {
      ASSERT_TRUE(expectedDatatype.has_value());
      ASSERT_EQ(asStringViewUnsafe(literal.value().getDatatype()),
                expectedDatatype.value());
    } else {
      ASSERT_FALSE(expectedDatatype.has_value());
    }
  } else {
    ASSERT_FALSE(expectedContent.has_value());
  }
};

// Helper function to get literal from Id and then check its content and
// datatype
void checkLiteralContentAndDatatypeFromId(
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
void checkLiteralContentAndDatatypeFromLiteralOrIri(
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

// Helper that constructs a local vocab, inserts the literal and passes the
// LocalVocabIndex as a ValueId to the GeometryInfoValueGetter
void checkGeoInfoFromLocalVocab(
    std::string wktInput, std::optional<ad_utility::GeometryInfo> expected) {
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
  checkGeoInfo(res, expected);
}

// Test knowledge graph that contains all used literals and iris.
const std::string geoInfoTtl =
    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> "
    "<x> <y> \"anXsdString\"^^xsd:string, "
    " \"someType\"^^<someType>,"
    " <https://example.com/test>,"
    " \"noType\" ,"
    " \"LINESTRING(2 2, 4 "
    "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>,"
    " \"POLYGON(2 4, 4 4, 4 2, 2 "
    "2)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>.";

// Helper that tests the GeometryInfoValueGetter using the ValueId of a
// VocabIndex for a string in the example knowledge graph.
void checkGeoInfoFromVocab(std::string wktInput,
                           std::optional<ad_utility::GeometryInfo> expected) {
  sparqlExpression::detail::GeometryInfoValueGetter getter;
  TestContextWithGivenTTl testContext{geoInfoTtl};
  VocabIndex idx;
  testContext.qec->getIndex().getVocab().getId(wktInput, &idx);
  auto id = ValueId::makeFromVocabIndex(idx);
  auto res = getter(id, &testContext.context);
  checkGeoInfo(res, expected);
}

// Helper that tests the GeometryInfoValueGetter using an arbitrary ValueId
void checkGeoInfoFromValueId(ValueId input,
                             std::optional<ad_utility::GeometryInfo> expected) {
  sparqlExpression::detail::GeometryInfoValueGetter getter;
  TestContextWithGivenTTl testContext{geoInfoTtl};
  auto res = getter(input, &testContext.context);
  checkGeoInfo(res, expected);
}

// Helper that tests the GeometryInfoValueGetter using a string passed directly
// as LiteralOrIri, not ValueId
void checkGeoInfoFromLiteral(std::string wktInput,
                             std::optional<ad_utility::GeometryInfo> expected) {
  sparqlExpression::detail::GeometryInfoValueGetter getter;
  TestContextWithGivenTTl testContext{geoInfoTtl};
  auto litOrIri =
      ad_utility::triple_component::LiteralOrIri::fromStringRepresentation(
          wktInput);
  auto res = getter(litOrIri, &testContext.context);
  checkGeoInfo(res, expected);
}

// Helper that runs each of the tests for GeometryInfoValueGetter using the same
// input
void checkGeoInfoFromLocalAndNormalVocabAndLiteral(
    std::string wktInput, std::optional<ad_utility::GeometryInfo> expected) {
  checkGeoInfoFromVocab(wktInput, expected);
  checkGeoInfoFromLocalVocab(wktInput, expected);
  checkGeoInfoFromLiteral(wktInput, expected);
}

};  // namespace

// namespace

TEST(LiteralValueGetterWithStrFunction, OperatorWithId) {
  sparqlExpression::detail::LiteralValueGetterWithStrFunction
      literalValueGetter;
  checkLiteralContentAndDatatypeFromId("\"noType\"", "noType", std::nullopt,
                                       literalValueGetter);
  checkLiteralContentAndDatatypeFromId("\"someType\"^^<someType>", "someType",
                                       std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromId(
      "\"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>",
      "anXsdString", "http://www.w3.org/2001/XMLSchema#string",
      literalValueGetter);
  checkLiteralContentAndDatatypeFromId("<x>", "x", std::nullopt,
                                       literalValueGetter);
}

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
      "anXsdString", "http://www.w3.org/2001/XMLSchema#string",
      literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri(
      "<x>", std::nullopt, true, "x", std::nullopt, literalValueGetter);
}

TEST(LiteralValueGetterWithoutStrFunction, OperatorWithId) {
  sparqlExpression::detail::LiteralValueGetterWithoutStrFunction
      literalValueGetter;
  checkLiteralContentAndDatatypeFromId("\"noType\"", "noType", std::nullopt,
                                       literalValueGetter);
  checkLiteralContentAndDatatypeFromId("\"someType\"^^<someType>", std::nullopt,
                                       std::nullopt, literalValueGetter);
  checkLiteralContentAndDatatypeFromId(
      "\"anXsdString\"^^<http://www.w3.org/2001/XMLSchema#string>",
      "anXsdString", "http://www.w3.org/2001/XMLSchema#string",
      literalValueGetter);
  checkLiteralContentAndDatatypeFromId("<x>", std::nullopt, std::nullopt,
                                       literalValueGetter);
}

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
      "anXsdString", "http://www.w3.org/2001/XMLSchema#string",
      literalValueGetter);
  checkLiteralContentAndDatatypeFromLiteralOrIri("<x>", std::nullopt, true,
                                                 std::nullopt, std::nullopt,
                                                 literalValueGetter);
}

TEST(GeometryInfoValueGetterTest, OperatorWithVocabIdOrLiteral) {
  checkGeoInfoFromLocalAndNormalVocabAndLiteral(
      "\"LINESTRING(2 2, 4 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      ad_utility::GeometryInfo(
          2, std::pair<GeoPoint, GeoPoint>(GeoPoint(2, 2), GeoPoint(4, 4)),
          GeoPoint(3, 3)));
  checkGeoInfoFromLocalAndNormalVocabAndLiteral(
      "\"POLYGON(2 4, 4 4, 4 "
      "2, 2 2)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
      ad_utility::GeometryInfo(
          3, std::pair<GeoPoint, GeoPoint>(GeoPoint(2, 2), GeoPoint(4, 4)),
          GeoPoint(3, 3)));
  checkGeoInfoFromLocalAndNormalVocabAndLiteral("\"someType\"^^<someType>",
                                                std::nullopt);
  checkGeoInfoFromLocalAndNormalVocabAndLiteral(
      "\"someType\"^^<http://www.w3.org/2001/XMLSchema#string>", std::nullopt);
  checkGeoInfoFromLocalAndNormalVocabAndLiteral("\"noType\"", std::nullopt);
  checkGeoInfoFromLocalAndNormalVocabAndLiteral("<https://example.com/test>",
                                                std::nullopt);
}

TEST(GeometryInfoValueGetterTest, OperatorWithIdGeoPoint) {
  checkGeoInfoFromValueId(
      ValueId::makeFromGeoPoint(GeoPoint(3, 2)),
      ad_utility::GeometryInfo(
          1, std::pair<GeoPoint, GeoPoint>(GeoPoint(3, 2), GeoPoint(3, 2)),
          GeoPoint(3, 2)));
  checkGeoInfoFromValueId(ValueId::makeUndefined(), std::nullopt);
  checkGeoInfoFromValueId(ValueId::makeFromBool(true), std::nullopt);
  checkGeoInfoFromValueId(ValueId::makeFromInt(42), std::nullopt);
  checkGeoInfoFromValueId(ValueId::makeFromDouble(42.01), std::nullopt);
}

TEST(GeometryInfoValueGetterTest, OperatorWithUnrelatedId) {
  checkGeoInfoFromValueId(ValueId::makeUndefined(), std::nullopt);
  checkGeoInfoFromValueId(ValueId::makeFromBool(true), std::nullopt);
  checkGeoInfoFromValueId(ValueId::makeFromInt(42), std::nullopt);
  checkGeoInfoFromValueId(ValueId::makeFromDouble(42.01), std::nullopt);
}

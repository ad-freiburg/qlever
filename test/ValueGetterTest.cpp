//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author:

#include <gtest/gtest.h>

#include "./SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

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

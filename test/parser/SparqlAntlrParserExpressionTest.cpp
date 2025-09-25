// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "../SparqlExpressionTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "./SparqlAntlrParserTestHelpers.h"
#include "engine/sparqlExpressions/BlankNodeExpression.h"
#include "engine/sparqlExpressions/CountStarExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NowDatetimeExpression.h"
#include "engine/sparqlExpressions/RandomExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/UuidExpressions.h"
#include "rdfTypes/GeometryInfo.h"

namespace {
using namespace sparqlParserHelpers;
using namespace sparqlParserTestHelpers;
namespace m = matchers;
using Parser = SparqlAutomaticParser;
using namespace std::literals;
using Var = Variable;
auto iri = ad_utility::testing::iri;

auto lit = ad_utility::testing::tripleComponentLiteral;

// ___________________________________________________________________________
TEST(SparqlParser, primaryExpression) {
  using namespace sparqlExpression;
  using namespace m::builtInCall;
  auto expectPrimaryExpression =
      ExpectCompleteParse<&Parser::primaryExpression>{};
  auto expectFails = ExpectParseFails<&Parser::primaryExpression>{};

  expectPrimaryExpression("<x>", matchLiteralExpression(iri("<x>")));
  expectPrimaryExpression("\"x\"@en",
                          matchLiteralExpression(lit("\"x\"", "@en")));
  expectPrimaryExpression("27", matchLiteralExpression(IntId(27)));
}

// ___________________________________________________________________________
TEST(SparqlParser, builtInCall) {
  using namespace sparqlExpression;
  using namespace m::builtInCall;
  auto expectBuiltInCall = ExpectCompleteParse<&Parser::builtInCall>{};
  auto expectFails = ExpectParseFails<&Parser::builtInCall>{};
  expectBuiltInCall("StrLEN(?x)", matchUnary(&makeStrlenExpression));
  expectBuiltInCall("ucaSe(?x)", matchUnary(&makeUppercaseExpression));
  expectBuiltInCall("lCase(?x)", matchUnary(&makeLowercaseExpression));
  expectBuiltInCall("StR(?x)", matchUnary(&makeStrExpression));
  expectBuiltInCall(
      "iRI(?x)",
      matchNaryWithChildrenMatchers(
          &makeIriOrUriExpression, variableExpressionMatcher(Variable{"?x"}),
          matchLiteralExpression(ad_utility::triple_component::Iri{})));
  expectBuiltInCall(
      "uRI(?x)",
      matchNaryWithChildrenMatchers(
          &makeIriOrUriExpression, variableExpressionMatcher(Variable{"?x"}),
          matchLiteralExpression(ad_utility::triple_component::Iri{})));
  expectBuiltInCall("year(?x)", matchUnary(&makeYearExpression));
  expectBuiltInCall("month(?x)", matchUnary(&makeMonthExpression));
  expectBuiltInCall("tz(?x)", matchUnary(&makeTimezoneStrExpression));
  expectBuiltInCall("timezone(?x)", matchUnary(&makeTimezoneExpression));
  expectBuiltInCall("day(?x)", matchUnary(&makeDayExpression));
  expectBuiltInCall("NOW()", matchPtr<NowDatetimeExpression>());
  expectBuiltInCall("hours(?x)", matchUnary(&makeHoursExpression));
  expectBuiltInCall("minutes(?x)", matchUnary(&makeMinutesExpression));
  expectBuiltInCall("seconds(?x)", matchUnary(&makeSecondsExpression));
  expectBuiltInCall("abs(?x)", matchUnary(&makeAbsExpression));
  expectBuiltInCall("ceil(?x)", matchUnary(&makeCeilExpression));
  expectBuiltInCall("floor(?x)", matchUnary(&makeFloorExpression));
  expectBuiltInCall("round(?x)", matchUnary(&makeRoundExpression));
  expectBuiltInCall("ISIRI(?x)", matchUnary(&makeIsIriExpression));
  expectBuiltInCall("ISUri(?x)", matchUnary(&makeIsIriExpression));
  expectBuiltInCall("ISBLANK(?x)", matchUnary(&makeIsBlankExpression));
  expectBuiltInCall("ISLITERAL(?x)", matchUnary(&makeIsLiteralExpression));
  expectBuiltInCall("ISNUMERIC(?x)", matchUnary(&makeIsNumericExpression));
  expectBuiltInCall("DATATYPE(?x)", matchUnary(&makeDatatypeExpression));
  expectBuiltInCall("BOUND(?x)", matchUnary(&makeBoundExpression));
  expectBuiltInCall("RAND()", matchPtr<RandomExpression>());
  expectBuiltInCall("STRUUID()", matchPtr<StrUuidExpression>());
  expectBuiltInCall("UUID()", matchPtr<UuidExpression>());
  expectBuiltInCall("COALESCE(?x)", matchUnary(makeCoalesceExpressionVariadic));
  expectBuiltInCall("COALESCE()", matchNary(makeCoalesceExpressionVariadic));
  expectBuiltInCall("COALESCE(?x, ?y, ?z)",
                    matchNary(makeCoalesceExpressionVariadic, Var{"?x"},
                              Var{"?y"}, Var{"?z"}));
  expectBuiltInCall("CONCAT(?x)", matchUnary(makeConcatExpressionVariadic));
  expectBuiltInCall("concaT()", matchNary(makeConcatExpressionVariadic));
  expectBuiltInCall(
      "concat(?x, ?y, ?z)",
      matchNary(makeConcatExpressionVariadic, Var{"?x"}, Var{"?y"}, Var{"?z"}));

  auto makeReplaceExpressionThreeArgs = [](auto&& arg0, auto&& arg1,
                                           auto&& arg2) {
    return makeReplaceExpression(AD_FWD(arg0), AD_FWD(arg1), AD_FWD(arg2),
                                 nullptr);
  };

  expectBuiltInCall("replace(?x, ?y, ?z)",
                    matchNary(makeReplaceExpressionThreeArgs, Var{"?x"},
                              Var{"?y"}, Var{"?z"}));
  expectBuiltInCall(
      "replace(?x, ?y, ?z, \"imsU\")",
      matchNaryWithChildrenMatchers(
          makeReplaceExpressionThreeArgs, variableExpressionMatcher(Var{"?x"}),
          matchNaryWithChildrenMatchers(
              &makeMergeRegexPatternAndFlagsExpression,
              variableExpressionMatcher(Var{"?y"}),
              matchLiteralExpression(lit("imsU"))),
          variableExpressionMatcher(Var{"?z"})));
  expectBuiltInCall("IF(?a, ?h, ?c)", matchNary(&makeIfExpression, Var{"?a"},
                                                Var{"?h"}, Var{"?c"}));
  expectBuiltInCall("LANG(?x)", matchUnary(&makeLangExpression));
  expectFails("LANGMATCHES()");
  expectFails("LANGMATCHES(?x)");

  expectBuiltInCall("LANGMATCHES(?x, ?y)", matchNary(&makeLangMatchesExpression,
                                                     Var{"?x"}, Var{"?y"}));
  expectFails("STRDT()");
  expectFails("STRDT(?x)");
  expectBuiltInCall("STRDT(?x, ?y)",
                    matchNary(&makeStrIriDtExpression, Var{"?x"}, Var{"?y"}));
  expectBuiltInCall(
      "STRDT(?x, <http://example/romanNumeral>)",
      matchNaryWithChildrenMatchers(
          &makeStrIriDtExpression, variableExpressionMatcher(Var{"?x"}),
          matchLiteralExpression(iri("<http://example/romanNumeral>"))));

  expectFails("STRLANG()");
  expectFails("STRALANG(?x)");
  expectBuiltInCall("STRLANG(?x, ?y)",
                    matchNary(&makeStrLangTagExpression, Var{"?x"}, Var{"?y"}));
  expectBuiltInCall(
      "STRLANG(?x, \"en\")",
      matchNaryWithChildrenMatchers(&makeStrLangTagExpression,
                                    variableExpressionMatcher(Var{"?x"}),
                                    matchLiteralExpression(lit("en"))));

  // The following three cases delegate to a separate parsing function, so we
  // only perform rather simple checks.
  expectBuiltInCall("COUNT(?x)", matchPtr<CountExpression>());
  auto makeRegexExpressionTwoArgs = [](auto&& arg0, auto&& arg1) {
    return makeRegexExpression(AD_FWD(arg0), AD_FWD(arg1), nullptr);
  };
  expectBuiltInCall(
      "regex(?x, \"ab\")",
      matchNaryWithChildrenMatchers(makeRegexExpressionTwoArgs,
                                    variableExpressionMatcher(Var{"?x"}),
                                    matchLiteralExpression(lit("ab"))));
  expectBuiltInCall(
      "regex(?x, \"ab\", \"imsU\")",
      matchNaryWithChildrenMatchers(
          makeRegexExpressionTwoArgs, variableExpressionMatcher(Var{"?x"}),
          matchNaryWithChildrenMatchers(
              &makeMergeRegexPatternAndFlagsExpression,
              matchLiteralExpression(lit("ab")),
              matchLiteralExpression(lit("imsU")))));

  expectBuiltInCall("MD5(?x)", matchUnary(&makeMD5Expression));
  expectBuiltInCall("SHA1(?x)", matchUnary(&makeSHA1Expression));
  expectBuiltInCall("SHA256(?x)", matchUnary(&makeSHA256Expression));
  expectBuiltInCall("SHA384(?x)", matchUnary(&makeSHA384Expression));
  expectBuiltInCall("SHA512(?x)", matchUnary(&makeSHA512Expression));

  expectBuiltInCall("encode_for_uri(?x)",
                    matchUnary(&makeEncodeForUriExpression));

  const auto& blankNodeExpression = makeUniqueBlankNodeExpression();
  const auto& reference = *blankNodeExpression;
  expectBuiltInCall(
      "bnode()", testing::Pointee(::testing::ResultOf(
                     [](const SparqlExpression& expr) -> const std::type_info& {
                       return typeid(expr);
                     },
                     Eq(std::reference_wrapper(typeid(reference))))));
  expectBuiltInCall("bnode(?x)", matchUnary(&makeBlankNodeExpression));
  // Not implemented yet
  expectFails("sameTerm(?a, ?b)");
}

TEST(SparqlParser, unaryExpression) {
  using namespace sparqlExpression;
  using namespace m::builtInCall;
  auto expectUnary = ExpectCompleteParse<&Parser::unaryExpression>{};

  expectUnary("-?x", matchUnary(&makeUnaryMinusExpression));
  expectUnary("!?x", matchUnary(&makeUnaryNegateExpression));
}

TEST(SparqlParser, multiplicativeExpression) {
  using namespace sparqlExpression;
  using namespace m::builtInCall;
  Variable x{"?x"};
  Variable y{"?y"};
  Variable z{"?z"};
  auto expectMultiplicative =
      ExpectCompleteParse<&Parser::multiplicativeExpression>{};
  expectMultiplicative("?x * ?y", matchNary(&makeMultiplyExpression, x, y));
  expectMultiplicative("?y / ?x", matchNary(&makeDivideExpression, y, x));
  expectMultiplicative(
      "?z * ?y / abs(?x)",
      matchNaryWithChildrenMatchers(&makeDivideExpression,
                                    matchNary(&makeMultiplyExpression, z, y),
                                    matchUnary(&makeAbsExpression)));
  expectMultiplicative(
      "?y / ?z * abs(?x)",
      matchNaryWithChildrenMatchers(&makeMultiplyExpression,
                                    matchNary(&makeDivideExpression, y, z),
                                    matchUnary(&makeAbsExpression)));
}

TEST(SparqlParser, relationalExpression) {
  Variable x{"?x"};
  Variable y{"?y"};
  Variable z{"?z"};
  using namespace sparqlExpression;
  using namespace m::builtInCall;
  auto expectRelational = ExpectCompleteParse<&Parser::relationalExpression>{};
  expectRelational("?x IN (?y, ?z)",
                   matchPtrWithVariables<InExpression>(x, y, z));
  expectRelational("?x NOT IN (?y, ?z)",
                   matchNaryWithChildrenMatchers(
                       &makeUnaryNegateExpression,
                       matchPtrWithVariables<InExpression>(x, y, z)));
  // TODO<joka921> Technically the other relational expressions (=, <, >, etc.)
  // are also untested.
}

// Return a matcher for an `OperatorAndExpression`.
::testing::Matcher<const SparqlQleverVisitor::OperatorAndExpression&>
matchOperatorAndExpression(
    SparqlQleverVisitor::Operator op,
    const ::testing::Matcher<const sparqlExpression::SparqlExpression::Ptr&>&
        expressionMatcher) {
  using OpAndExp = SparqlQleverVisitor::OperatorAndExpression;
  return ::testing::AllOf(AD_FIELD(OpAndExp, operator_, ::testing::Eq(op)),
                          AD_FIELD(OpAndExp, expression_, expressionMatcher));
}

TEST(SparqlParser, multiplicativeExpressionLeadingSignButNoSpaceContext) {
  using namespace sparqlExpression;
  using namespace m::builtInCall;
  Variable x{"?x"};
  Variable y{"?y"};
  Variable z{"?z"};
  using Op = SparqlQleverVisitor::Operator;
  auto expectMultiplicative = ExpectCompleteParse<
      &Parser::multiplicativeExpressionWithLeadingSignButNoSpace>{};
  auto matchVariableExpression = [](Variable var) {
    return matchPtr<VariableExpression>(
        AD_PROPERTY(VariableExpression, value, ::testing::Eq(var)));
  };
  auto matchIdExpression = [](Id id) {
    return matchPtr<IdExpression>(
        AD_PROPERTY(IdExpression, value, ::testing::Eq(id)));
  };

  expectMultiplicative("-3 * ?y",
                       matchOperatorAndExpression(
                           Op::Minus, matchNaryWithChildrenMatchers(
                                          &makeMultiplyExpression,
                                          matchIdExpression(Id::makeFromInt(3)),
                                          matchVariableExpression(y))));
  expectMultiplicative(
      "-3.7 / ?y",
      matchOperatorAndExpression(
          Op::Minus,
          matchNaryWithChildrenMatchers(
              &makeDivideExpression, matchIdExpression(Id::makeFromDouble(3.7)),
              matchVariableExpression(y))));

  expectMultiplicative("+5 * ?y",
                       matchOperatorAndExpression(
                           Op::Plus, matchNaryWithChildrenMatchers(
                                         &makeMultiplyExpression,
                                         matchIdExpression(Id::makeFromInt(5)),
                                         matchVariableExpression(y))));
  expectMultiplicative(
      "+3.9 / ?y", matchOperatorAndExpression(
                       Op::Plus, matchNaryWithChildrenMatchers(
                                     &makeDivideExpression,
                                     matchIdExpression(Id::makeFromDouble(3.9)),
                                     matchVariableExpression(y))));
  expectMultiplicative(
      "-3.2 / abs(?x) * ?y",
      matchOperatorAndExpression(
          Op::Minus, matchNaryWithChildrenMatchers(
                         &makeMultiplyExpression,
                         matchNaryWithChildrenMatchers(
                             &makeDivideExpression,
                             matchIdExpression(Id::makeFromDouble(3.2)),
                             matchUnary(&makeAbsExpression)),
                         matchVariableExpression(y))));
}

TEST(SparqlParser, FunctionCall) {
  using namespace sparqlExpression;
  using namespace m::builtInCall;
  auto expectFunctionCall = ExpectCompleteParse<&Parser::functionCall>{};
  auto expectFunctionCallFails = ExpectParseFails<&Parser::functionCall>{};
  // These prefixes are currently stored without the leading "<", so we have to
  // manually add it when constructing parser inputs.
  auto geof = absl::StrCat("<", GEOF_PREFIX.second);
  auto math = absl::StrCat("<", MATH_PREFIX.second);
  auto xsd = absl::StrCat("<", XSD_PREFIX.second);
  auto ql = absl::StrCat("<", QL_PREFIX.second);

  // Correct function calls. Check that the parser picks the correct expression.
  expectFunctionCall(absl::StrCat(geof, "latitude>(?x)"),
                     matchUnary(&makeLatitudeExpression));
  expectFunctionCall(absl::StrCat(geof, "longitude>(?x)"),
                     matchUnary(&makeLongitudeExpression));
  expectFunctionCall(absl::StrCat(geof, "centroid>(?x)"),
                     matchUnary(&makeCentroidExpression));
  expectFunctionCall(absl::StrCat(ql, "isGeoPoint>(?x)"),
                     matchUnary(&makeIsGeoPointExpression));
  expectFunctionCall(absl::StrCat(geof, "envelope>(?x)"),
                     matchUnary(&makeEnvelopeExpression));
  expectFunctionCall(absl::StrCat(geof, "geometryType>(?x)"),
                     matchUnary(&makeGeometryTypeExpression));

  using enum ad_utility::BoundingCoordinate;
  expectFunctionCall(absl::StrCat(geof, "minX>(?x)"),
                     matchUnary(&makeBoundingCoordinateExpression<MIN_X>));
  expectFunctionCall(absl::StrCat(geof, "minY>(?x)"),
                     matchUnary(&makeBoundingCoordinateExpression<MIN_Y>));
  expectFunctionCall(absl::StrCat(geof, "maxX>(?x)"),
                     matchUnary(&makeBoundingCoordinateExpression<MAX_X>));
  expectFunctionCall(absl::StrCat(geof, "maxY>(?x)"),
                     matchUnary(&makeBoundingCoordinateExpression<MAX_Y>));

  // The different distance functions:
  expectFunctionCall(
      absl::StrCat(geof, "metricDistance>(?a, ?b)"),
      matchNary(&makeMetricDistExpression, Variable{"?a"}, Variable{"?b"}));
  // Compatibility version of geof:distance with two arguments
  expectFunctionCall(
      absl::StrCat(geof, "distance>(?a, ?b)"),
      matchNary(&makeDistExpression, Variable{"?a"}, Variable{"?b"}));
  // geof:distance with IRI as unit in third argument
  expectFunctionCall(
      absl::StrCat(geof, "distance>(?a, ?b, <http://qudt.org/vocab/unit/M>)"),
      matchNaryWithChildrenMatchers(
          &makeDistWithUnitExpression,
          variableExpressionMatcher(Variable{"?a"}),
          variableExpressionMatcher(Variable{"?b"}),
          matchLiteralExpression<ad_utility::triple_component::Iri>(
              ad_utility::triple_component::Iri::fromIriref(
                  "<http://qudt.org/vocab/unit/M>"))));

  // geof:distance with xsd:anyURI literal as unit in third argument
  expectFunctionCall(
      absl::StrCat(geof,
                   "distance>(?a, ?b, "
                   "\"http://qudt.org/vocab/unit/M\"^^<http://www.w3.org/2001/"
                   "XMLSchema#anyURI>)"),
      matchNaryWithChildrenMatchers(
          &makeDistWithUnitExpression,
          variableExpressionMatcher(Variable{"?a"}),
          variableExpressionMatcher(Variable{"?b"}),
          matchLiteralExpression<ad_utility::triple_component::Literal>(
              ad_utility::triple_component::Literal::fromStringRepresentation(
                  "\"http://qudt.org/vocab/unit/M\"^^<http://www.w3.org/2001/"
                  "XMLSchema#anyURI>"))));

  // geof:distance with variable as unit in third argument
  expectFunctionCall(absl::StrCat(geof, "distance>(?a, ?b, ?unit)"),
                     matchNaryWithChildrenMatchers(
                         &makeDistWithUnitExpression,
                         variableExpressionMatcher(Variable{"?a"}),
                         variableExpressionMatcher(Variable{"?b"}),
                         variableExpressionMatcher(Variable{"?unit"})));

  // Geometric relation functions
  expectFunctionCall(
      absl::StrCat(geof, "sfIntersects>(?a, ?b)"),
      matchNary(&makeGeoRelationExpression<SpatialJoinType::INTERSECTS>,
                Variable{"?a"}, Variable{"?b"}));
  expectFunctionCall(
      absl::StrCat(geof, "sfContains>(?a, ?b)"),
      matchNary(&makeGeoRelationExpression<SpatialJoinType::CONTAINS>,
                Variable{"?a"}, Variable{"?b"}));
  expectFunctionCall(
      absl::StrCat(geof, "sfCrosses>(?a, ?b)"),
      matchNary(&makeGeoRelationExpression<SpatialJoinType::CROSSES>,
                Variable{"?a"}, Variable{"?b"}));
  expectFunctionCall(
      absl::StrCat(geof, "sfTouches>(?a, ?b)"),
      matchNary(&makeGeoRelationExpression<SpatialJoinType::TOUCHES>,
                Variable{"?a"}, Variable{"?b"}));
  expectFunctionCall(
      absl::StrCat(geof, "sfEquals>(?a, ?b)"),
      matchNary(&makeGeoRelationExpression<SpatialJoinType::EQUALS>,
                Variable{"?a"}, Variable{"?b"}));
  expectFunctionCall(
      absl::StrCat(geof, "sfOverlaps>(?a, ?b)"),
      matchNary(&makeGeoRelationExpression<SpatialJoinType::OVERLAPS>,
                Variable{"?a"}, Variable{"?b"}));
  expectFunctionCall(
      absl::StrCat(geof, "sfWithin>(?a, ?b)"),
      matchNary(&makeGeoRelationExpression<SpatialJoinType::WITHIN>,
                Variable{"?a"}, Variable{"?b"}));

  // Math functions
  expectFunctionCall(absl::StrCat(math, "log>(?x)"),
                     matchUnary(&makeLogExpression));
  expectFunctionCall(absl::StrCat(math, "exp>(?x)"),
                     matchUnary(&makeExpExpression));
  expectFunctionCall(absl::StrCat(math, "sqrt>(?x)"),
                     matchUnary(&makeSqrtExpression));
  expectFunctionCall(absl::StrCat(math, "sin>(?x)"),
                     matchUnary(&makeSinExpression));
  expectFunctionCall(absl::StrCat(math, "cos>(?x)"),
                     matchUnary(&makeCosExpression));
  expectFunctionCall(absl::StrCat(math, "tan>(?x)"),
                     matchUnary(&makeTanExpression));
  expectFunctionCall(
      absl::StrCat(math, "pow>(?a, ?b)"),
      matchNary(&makePowExpression, Variable{"?a"}, Variable{"?b"}));
  expectFunctionCall(absl::StrCat(xsd, "int>(?x)"),
                     matchUnary(&makeConvertToIntExpression));
  expectFunctionCall(absl::StrCat(xsd, "integer>(?x)"),
                     matchUnary(&makeConvertToIntExpression));
  expectFunctionCall(absl::StrCat(xsd, "double>(?x)"),
                     matchUnary(&makeConvertToDoubleExpression));
  expectFunctionCall(absl::StrCat(xsd, "float>(?x)"),
                     matchUnary(&makeConvertToDoubleExpression));
  expectFunctionCall(absl::StrCat(xsd, "decimal>(?x)"),
                     matchUnary(&makeConvertToDecimalExpression));
  expectFunctionCall(absl::StrCat(xsd, "boolean>(?x)"),
                     matchUnary(&makeConvertToBooleanExpression));
  expectFunctionCall(absl::StrCat(xsd, "date>(?x)"),
                     matchUnary(&makeConvertToDateExpression));
  expectFunctionCall(absl::StrCat(xsd, "dateTime>(?x)"),
                     matchUnary(&makeConvertToDateTimeExpression));

  expectFunctionCall(absl::StrCat(xsd, "string>(?x)"),
                     matchUnary(&makeConvertToStringExpression));

  // Wrong number of arguments.
  expectFunctionCallFails(absl::StrCat(geof, "distance>(?a)"));
  expectFunctionCallFails(absl::StrCat(geof, "distance>()"));
  expectFunctionCallFails(absl::StrCat(geof, "distance>(?a, ?b, ?c, ?d)"));
  expectFunctionCallFails(absl::StrCat(geof, "metricDistance>(?a)"));
  expectFunctionCallFails(absl::StrCat(geof, "metricDistance>(?a, ?b, ?c)"));

  const std::vector<std::string> unaryGeofFunctionNames = {
      "centroid", "envelope", "geometryType", "minX", "minY", "maxX", "maxY"};
  for (const auto& func : unaryGeofFunctionNames) {
    expectFunctionCallFails(absl::StrCat(geof, func, ">()"));
    expectFunctionCallFails(absl::StrCat(geof, func, ">(?a, ?b)"));
    expectFunctionCallFails(absl::StrCat(geof, func, ">(?a, ?b, ?c)"));
  }

  const std::vector<std::string> binaryGeofFunctionNames = {
      "sfIntersects", "sfContains", "sfCovers",   "sfCrosses",
      "sfTouches",    "sfEquals",   "sfOverlaps", "sfWithin"};
  for (const auto& func : binaryGeofFunctionNames) {
    expectFunctionCallFails(absl::StrCat(geof, func, ">()"));
    expectFunctionCallFails(absl::StrCat(geof, func, ">(?a)"));
    expectFunctionCallFails(absl::StrCat(geof, func, ">(?a, ?b, ?c)"));
  }

  expectFunctionCallFails(absl::StrCat(xsd, "date>(?varYear, ?varMonth)"));
  expectFunctionCallFails(absl::StrCat(xsd, "dateTime>(?varYear, ?varMonth)"));

  // Unknown function with `geof:`, `math:`, `xsd:`, or `ql` prefix.
  expectFunctionCallFails(absl::StrCat(geof, "nada>(?x)"));
  expectFunctionCallFails(absl::StrCat(math, "nada>(?x)"));
  expectFunctionCallFails(absl::StrCat(xsd, "nada>(?x)"));
  expectFunctionCallFails(absl::StrCat(ql, "nada>(?x)"));

  // Prefix for which no function is known.
  std::string prefixNexistepas = "<http://nexiste.pas/";
  expectFunctionCallFails(absl::StrCat(prefixNexistepas, "nada>(?x)"));

  // Check that arbitrary nonexisting functions with a single argument silently
  // return an `IdExpression(UNDEF)` in the syntax test mode.
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::syntaxTestMode_>(true);
  expectFunctionCall(
      absl::StrCat(prefixNexistepas, "nada>(?x)"),
      matchPtr<IdExpression>(AD_PROPERTY(IdExpression, value,
                                         ::testing::Eq(Id::makeUndefined()))));
}

// ______________________________________________________________________________
TEST(SparqlParser, substringExpression) {
  using namespace sparqlExpression;
  using namespace m::builtInCall;
  using V = Variable;
  auto expectBuiltInCall = ExpectCompleteParse<&Parser::builtInCall>{};
  auto expectBuiltInCallFails = ExpectParseFails<&Parser::builtInCall>{};
  expectBuiltInCall("SUBSTR(?x, ?y, ?z)", matchNary(&makeSubstrExpression,
                                                    V{"?x"}, V{"?y"}, V{"?z"}));
  // Note: The large number (the default value for the length, which is
  // automatically truncated) is the largest integer that is representable by
  // QLever. Should this ever change, then this test has to be changed
  // accordingly.
  expectBuiltInCall(
      "SUBSTR(?x, 7)",
      matchNaryWithChildrenMatchers(&makeSubstrExpression,
                                    variableExpressionMatcher(V{"?x"}),
                                    idExpressionMatcher(IntId(7)),
                                    idExpressionMatcher(IntId(Id::maxInt))));
  // Too few arguments
  expectBuiltInCallFails("SUBSTR(?x)");
  // Too many arguments
  expectBuiltInCallFails("SUBSTR(?x, 3, 8, 12)");
}

// _________________________________________________________
TEST(SparqlParser, binaryStringExpressions) {
  using namespace sparqlExpression;
  using namespace m::builtInCall;
  using V = Variable;
  auto expectBuiltInCall = ExpectCompleteParse<&Parser::builtInCall>{};
  auto expectBuiltInCallFails = ExpectParseFails<&Parser::builtInCall>{};

  auto makeMatcher = [](auto function) {
    return matchNary(function, V{"?x"}, V{"?y"});
  };

  expectBuiltInCall("STRSTARTS(?x, ?y)", makeMatcher(&makeStrStartsExpression));
  expectBuiltInCall("STRENDS(?x, ?y)", makeMatcher(&makeStrEndsExpression));
  expectBuiltInCall("CONTAINS(?x, ?y)", makeMatcher(&makeContainsExpression));
  expectBuiltInCall("STRAFTER(?x, ?y)", makeMatcher(&makeStrAfterExpression));
  expectBuiltInCall("STRBEFORE(?x, ?y)", makeMatcher(&makeStrBeforeExpression));
}

namespace aggregateTestHelpers {
using namespace sparqlExpression;

// Return a matcher that checks whether a given `SparqlExpression::Ptr` actually
// points to an `AggregateExpr`, that the distinctness and the child variable of
// the aggregate expression match, and that the `AggregateExpr`(via dynamic
// cast) matches all the `additionalMatchers`.
template <typename AggregateExpr>
::testing::Matcher<const SparqlExpression::Ptr&> matchAggregate(
    bool distinct, const Variable& child, const auto&... additionalMatchers) {
  using namespace ::testing;
  using namespace m::builtInCall;
  using Exp = SparqlExpression;

  auto innerMatcher = [&]() -> Matcher<const AggregateExpr&> {
    if constexpr (sizeof...(additionalMatchers) > 0) {
      return AllOf(additionalMatchers...);
    } else {
      return ::testing::_;
    }
  }();
  using enum SparqlExpression::AggregateStatus;
  auto aggregateStatus = distinct ? DistinctAggregate : NonDistinctAggregate;
  return Pointee(AllOf(
      AD_PROPERTY(Exp, isAggregate, Eq(aggregateStatus)),
      AD_PROPERTY(Exp, children, ElementsAre(variableExpressionMatcher(child))),
      WhenDynamicCastTo<const AggregateExpr&>(innerMatcher)));
}

// Return a matcher that checks whether a given `SparqlExpression::Ptr` actually
// points to an `AggregateExpr` and that the distinctness of the aggregate
// expression matches. It does not check the child. This is required to test
// aggregates that implicitly replace their child, like `StdevExpression`.
template <typename AggregateExpr>
::testing::Matcher<const SparqlExpression::Ptr&> matchAggregateWithoutChild(
    bool distinct) {
  using namespace ::testing;
  using namespace m::builtInCall;
  using Exp = SparqlExpression;

  using enum SparqlExpression::AggregateStatus;
  auto aggregateStatus = distinct ? DistinctAggregate : NonDistinctAggregate;
  return Pointee(AllOf(AD_PROPERTY(Exp, isAggregate, Eq(aggregateStatus)),
                       WhenDynamicCastTo<const AggregateExpr&>(testing::_)));
}
}  // namespace aggregateTestHelpers

// ___________________________________________________________
TEST(SparqlParser, aggregateExpressions) {
  using namespace sparqlExpression;
  using namespace m::builtInCall;
  using namespace aggregateTestHelpers;
  using V = Variable;
  auto expectAggregate = ExpectCompleteParse<&Parser::aggregate>{};
  auto expectAggregateFails = ExpectParseFails<&Parser::aggregate>{};

  // For the `COUNT *` expression we have completely hidden the type. So we need
  // to match it via RTTI.
  auto typeIdLambda = [](const auto& ptr) {
    return std::type_index{typeid(ptr)};
  };
  auto typeIdxCountStar = typeIdLambda(*makeCountStarExpression(true));

  // A matcher that matches a `COUNT *` expression with the given distinctness.
  auto matchCountStar =
      [&typeIdLambda, typeIdxCountStar](
          bool distinct) -> ::testing::Matcher<const SparqlExpression::Ptr&> {
    using namespace ::testing;
    using enum SparqlExpression::AggregateStatus;
    auto aggregateStatus = distinct ? DistinctAggregate : NonDistinctAggregate;
    return Pointee(
        AllOf(AD_PROPERTY(SparqlExpression, isAggregate, Eq(aggregateStatus)),
              ResultOf(typeIdLambda, Eq(typeIdxCountStar))));
  };

  expectAggregate("COUNT(*)", matchCountStar(false));
  expectAggregate("COUNT(DISTINCT *)", matchCountStar(true));

  expectAggregate("SAMPLE(?x)",
                  matchAggregate<SampleExpression>(false, V{"?x"}));
  expectAggregate("SAMPLE(DISTINCT ?x)",
                  matchAggregate<SampleExpression>(false, V{"?x"}));

  expectAggregate("Min(?x)", matchAggregate<MinExpression>(false, V{"?x"}));
  expectAggregate("Min(DISTINCT ?x)",
                  matchAggregate<MinExpression>(true, V{"?x"}));

  expectAggregate("Max(?x)", matchAggregate<MaxExpression>(false, V{"?x"}));
  expectAggregate("Max(DISTINCT ?x)",
                  matchAggregate<MaxExpression>(true, V{"?x"}));

  expectAggregate("Count(?x)", matchAggregate<CountExpression>(false, V{"?x"}));
  expectAggregate("Count(DISTINCT ?x)",
                  matchAggregate<CountExpression>(true, V{"?x"}));

  expectAggregate("Avg(?x)", matchAggregate<AvgExpression>(false, V{"?x"}));
  expectAggregate("Avg(DISTINCT ?x)",
                  matchAggregate<AvgExpression>(true, V{"?x"}));

  // A matcher for the separator of `GROUP_CONCAT`.
  auto separator = [](const std::string& sep) {
    return AD_PROPERTY(GroupConcatExpression, getSeparator, Eq(sep));
  };
  expectAggregate("GROUP_CONCAT(?x)", matchAggregate<GroupConcatExpression>(
                                          false, V{"?x"}, separator(" ")));
  expectAggregate(
      "group_concat(DISTINCT ?x)",
      matchAggregate<GroupConcatExpression>(true, V{"?x"}, separator(" ")));

  expectAggregate(
      "GROUP_CONCAT(?x; SEPARATOR= \";\")",
      matchAggregate<GroupConcatExpression>(false, V{"?x"}, separator(";")));
  expectAggregate(
      "group_concat(DISTINCT ?x; SEPARATOR=\";\")",
      matchAggregate<GroupConcatExpression>(true, V{"?x"}, separator(";")));

  // The STDEV expression
  // Here we don't match the child, because StdevExpression replaces it with a
  // DeviationExpression.
  expectAggregate("STDEV(?x)",
                  matchAggregateWithoutChild<StdevExpression>(false));
  expectAggregate("stdev(?x)",
                  matchAggregateWithoutChild<StdevExpression>(false));
  // A distinct stdev is probably not very useful, but should be possible anyway
  expectAggregate("STDEV(DISTINCT ?x)",
                  matchAggregateWithoutChild<StdevExpression>(true));
}
}  // namespace

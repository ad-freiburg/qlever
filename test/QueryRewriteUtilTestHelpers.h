// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_QUERYREWRITEUTILTESTHELPERS_H
#define QLEVER_TEST_QUERYREWRITEUTILTESTHELPERS_H

#include "./PrefilterExpressionTestHelpers.h"
#include "./printers/VariablePrinters.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "global/Constants.h"
#include "parser/Iri.h"
#include "util/GTestHelpers.h"
#include "util/SourceLocation.h"

namespace queryRewriteUtilTestHelpers {

using namespace makeSparqlExpression;
using Loc = ad_utility::source_location;
using V = Variable;
using enum SpatialJoinType;
using enum UnitOfMeasurement;
using ad_utility::triple_component::Iri;
using Ptr = SparqlExpression::Ptr;

using GeoDistanceFilter =
    std::optional<std::pair<sparqlExpression::GeoFunctionCall, double>>;
using DistancePtrAndExpected = std::pair<Ptr, std::optional<GeoDistanceCall>>;

// Test helper for `GeoFunctionCall`
inline void checkGeoFunctionCall(const std::optional<GeoFunctionCall>& a,
                                 const std::optional<GeoFunctionCall>& b,
                                 Loc loc = Loc::current()) {
  auto l = generateLocationTrace(loc);
  ASSERT_EQ(a.has_value(), b.has_value());
  if (!a.has_value()) {
    return;
  }
  ASSERT_EQ(a.value().function_, b.value().function_);
  ASSERT_EQ(a.value().left_, b.value().left_);
  ASSERT_EQ(a.value().right_, b.value().right_);
}

// Test helper for `GeoDistanceCall`
inline void checkGeoDistanceCall(const std::optional<GeoDistanceCall>& a,
                                 const std::optional<GeoDistanceCall>& b,
                                 Loc loc = Loc::current()) {
  auto l = generateLocationTrace(loc);
  checkGeoFunctionCall(a, b);
  if (!a.has_value()) {
    return;
  }
  ASSERT_EQ(a.value().unit_, b.value().unit_);
}

// Test helper for `getGeoDistanceFilter`
inline void checkGeoDistanceFilter(
    const GeoDistanceFilter& result,
    const std::optional<GeoFunctionCall>& expected, double expectedMeters,
    Loc loc = Loc::current()) {
  auto l = generateLocationTrace(loc);
  ASSERT_EQ(result.has_value(), expected.has_value());
  if (!result.has_value()) {
    return;
  }
  checkGeoFunctionCall(result.value().first, expected);
  ASSERT_NEAR(result.value().second, expectedMeters, 0.01);
}

//______________________________________________________________________________
inline DistancePtrAndExpected makeTwoArgumentDist() {
  GeoDistanceCall exp{{WITHIN_DIST, V{"?a"}, V{"?b"}}, KILOMETERS};
  auto ptr = makeDistExpression(getExpr(V{"?a"}), getExpr(V{"?b"}));
  return {std::move(ptr), exp};
}

//______________________________________________________________________________
inline DistancePtrAndExpected makeThreeArgumentDist() {
  GeoDistanceCall exp{{WITHIN_DIST, V{"?a"}, V{"?b"}}, MILES};
  auto ptr = makeDistWithUnitExpression(
      getExpr(V{"?a"}), getExpr(V{"?b"}),
      getExpr(Iri::fromIrirefWithoutBrackets("http://qudt.org/vocab/unit/MI")));
  return {std::move(ptr), exp};
}

//______________________________________________________________________________
inline DistancePtrAndExpected makeMetricDist() {
  GeoDistanceCall exp{{WITHIN_DIST, V{"?a"}, V{"?b"}}, METERS};
  auto ptr = makeMetricDistExpression(getExpr(V{"?a"}), getExpr(V{"?b"}));
  return {std::move(ptr), exp};
}

//______________________________________________________________________________
inline DistancePtrAndExpected makeUnrelated() {
  return {makePowExpression(getExpr(V{"?a"}), getExpr(V{"?b"})), std::nullopt};
}

//______________________________________________________________________________
inline std::shared_ptr<SparqlExpression> makeLessEqualSharedPtr(
    VariantArgs child0, VariantArgs child1) {
  return std::make_shared<LessEqualExpression>(
      std::array<Ptr, 2>{std::visit(getExpr, std::move(child0)),
                         std::visit(getExpr, std::move(child1))});
};

}  // namespace queryRewriteUtilTestHelpers

#endif  // QLEVER_TEST_QUERYREWRITEUTILTESTHELPERS_H

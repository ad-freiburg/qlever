// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./QueryRewriteUtilTestHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/data/SparqlFilter.h"

namespace {

using namespace queryRewriteUtilTestHelpers;

// _____________________________________________________________________________
TEST(QueryRewriteUtilTest, GetGeoDistanceExpressionParameters) {
  auto [expr1, exp1] = makeTwoArgumentDist();
  checkGeoDistanceCall(getGeoDistanceExpressionParameters(*expr1), exp1);

  auto [expr2, exp2] = makeThreeArgumentDist();
  checkGeoDistanceCall(getGeoDistanceExpressionParameters(*expr2), exp2);

  auto [expr3, exp3] = makeMetricDist();
  checkGeoDistanceCall(getGeoDistanceExpressionParameters(*expr3), exp3);

  auto [expr4, exp4] = makeUnrelated();
  checkGeoDistanceCall(getGeoDistanceExpressionParameters(*expr4), exp4);
}

// _____________________________________________________________________________
TEST(QueryRewriteUtilTest, GetGeoDistanceFilter) {
  auto D = &ValueId::makeFromDouble;

  auto [dExpr1, dExp1] = makeTwoArgumentDist();
  auto expr1 = leSprql(std::move(dExpr1), D(10));
  checkGeoDistanceFilter(getGeoDistanceFilter(*expr1), dExp1,
                         // Expect unit converted: 10 km in meters
                         10'000);

  auto [dExpr2, dExp2] = makeThreeArgumentDist();
  auto expr2 = leSprql(std::move(dExpr2), D(10));
  checkGeoDistanceFilter(getGeoDistanceFilter(*expr2), dExp2,
                         // Expect unit converted: 10 miles in meters
                         16'093.44);

  auto [dExpr3, dExp3] = makeMetricDist();
  auto expr3 = leSprql(std::move(dExpr3), D(10));
  checkGeoDistanceFilter(getGeoDistanceFilter(*expr3), dExp3, 10);

  auto [dExpr4, dExp4] = makeUnrelated();
  auto expr4 = leSprql(std::move(dExpr4), D(10));
  checkGeoDistanceFilter(getGeoDistanceFilter(*expr4), dExp4, 10);

  // "<" relation is unsupported
  auto [dExpr5, dExp5] = makeMetricDist();
  auto expr5 = ltSprql(std::move(dExpr5), D(10));
  checkGeoDistanceFilter(getGeoDistanceFilter(*expr5), std::nullopt, 10);
}

// _____________________________________________________________________________
TEST(QueryRewriteUtilTest, RewriteFilterToSpatialJoinConfig) {
  // TODO
  // rewriteFilterToSpatialJoinConfig(SparqlFilter{
  // SparqlExpressionPimpl{shared_ptr<SparqlExpression>}})
}

// TODO<ullingerc> #2140: Add tests for `getGeoFunctionExpressionParameters` +
// `rewriteFilterToSpatialJoinConfig` for geo relation functions

}  // namespace

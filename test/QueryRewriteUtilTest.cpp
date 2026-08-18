// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./QueryRewriteUtilTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/QueryExecutionTree.h"
#include "engine/QueryRewriteUtils.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/data/SparqlFilter.h"

namespace {

using namespace queryRewriteUtilTestHelpers;

// Helper wrapping `rewriteFilterToSpatialJoinConfig` with a test
// `QueryExecutionContext` and a trivial internal-variable generator, so that
// individual test cases can call it with just a `SparqlFilter`.
std::optional<SpatialJoinRewriteResult> rewrite(const SparqlFilter& filter) {
  static size_t count = 0;
  auto* qec = ad_utility::testing::getQec();
  std::function<Variable()> generateUniqueVarName = [] {
    return Variable{absl::StrCat("?_test_internal_", count++)};
  };
  return rewriteFilterToSpatialJoinConfig(filter, qec, generateUniqueVarName);
}

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

  // Non-numeric comparison is unsupported
  auto [dExpr6, dExp6] = makeMetricDist();
  auto expr6 = ltSprql(std::move(dExpr6), ValueId::makeFromGeoPoint({1, 1}));
  checkGeoDistanceFilter(getGeoDistanceFilter(*expr6), std::nullopt, 10);
}

//______________________________________________________________________________

// _____________________________________________________________________________
TEST(QueryRewriteUtilTest, GetDe9imRelationExpressionParameters) {
  auto [expr1, exp1] = makeDe9imRelation();
  checkDe9imRelationCall(getDe9imRelationExpressionParameters(*expr1), exp1);

  // Not a `geof:relate` call
  auto [expr2, exp2] = makeUnrelated();
  checkDe9imRelationCall(getDe9imRelationExpressionParameters(*expr2),
                         std::nullopt);

  // Invalid DE-9IM pattern (wrong length / characters) is rejected
  auto invalidPtr = makeDe9imRelationExpression(
      getExpr(V{"?a"}), getExpr(V{"?b"}),
      getExpr(ad_utility::triple_component::Literal::literalWithoutQuotes(
          "invalid")));
  checkDe9imRelationCall(getDe9imRelationExpressionParameters(*invalidPtr),
                         std::nullopt);

  // The third argument must be a fixed string literal: a variable is
  // rejected, even if it happens to be bound to a valid pattern at runtime.
  auto variablePatternPtr = makeDe9imRelationExpression(
      getExpr(V{"?a"}), getExpr(V{"?b"}), getExpr(V{"?pattern"}));
  checkDe9imRelationCall(
      getDe9imRelationExpressionParameters(*variablePatternPtr), std::nullopt);

  // The left argument must be a variable; a constant is rejected.
  auto nonVarLeftPtr = makeDe9imRelationExpression(
      getExpr(ValueId::makeFromInt(42)), getExpr(V{"?b"}),
      getExpr(ad_utility::triple_component::Literal::literalWithoutQuotes(
          "T*T***T**")));
  checkDe9imRelationCall(getDe9imRelationExpressionParameters(*nonVarLeftPtr),
                         std::nullopt);

  // The right argument must be a variable; a constant is rejected.
  auto nonVarRightPtr = makeDe9imRelationExpression(
      getExpr(V{"?a"}), getExpr(ValueId::makeFromInt(42)),
      getExpr(ad_utility::triple_component::Literal::literalWithoutQuotes(
          "T*T***T**")));
  checkDe9imRelationCall(getDe9imRelationExpressionParameters(*nonVarRightPtr),
                         std::nullopt);

  // A syntactically valid pattern that could still match disjoint geometries
  // is rejected, because `geof:relate` currently only supports patterns that
  // already imply an intersection.
  auto disjointPatternPtr = makeDe9imRelationExpression(
      getExpr(V{"?a"}), getExpr(V{"?b"}),
      getExpr(ad_utility::triple_component::Literal::literalWithoutQuotes(
          "FF*FF****")));
  checkDe9imRelationCall(
      getDe9imRelationExpressionParameters(*disjointPatternPtr), std::nullopt);
}

// _____________________________________________________________________________
TEST(QueryRewriteUtilTest, RewriteFilterToSpatialJoinConfig) {
  auto D = &ValueId::makeFromDouble;

  // Construct `FILTER(geof:metricDistance(?a, ?b) <= 10.0)`
  auto [distExpr, distCall] = makeMetricDist();
  auto exprSharedPtr = makeLessEqualSharedPtr(std::move(distExpr), D(10.0));
  SparqlFilter filter{
      SparqlExpressionPimpl{std::move(exprSharedPtr),
                            "<http://www.opengis.net/def/function/geosparql/"
                            "metricDistance>(?a, ?b) <= 10.0"}};

  // Convert to `SpatialJoinConfiguration`
  auto sjResult = rewrite(filter);
  ASSERT_TRUE(sjResult.has_value());
  const auto& sjConf = sjResult.value().config_;
  ASSERT_EQ(sjConf.left_, V{"?a"});
  ASSERT_EQ(sjConf.right_, V{"?b"});
  ASSERT_EQ(sjConf.joinType_, WITHIN_DIST);
  std::visit([](const auto& task) { ASSERT_EQ(task.maxDist_, 10.0); },
             sjConf.task_);
  // Both sides are variables, so no child is prebuilt.
  ASSERT_FALSE(sjResult.value().childLeft_.has_value());
  ASSERT_FALSE(sjResult.value().childRight_.has_value());

  // Unrelated `FILTER(math:pow(?a, ?b) <= 10.0)` results in `std::nullopt`
  auto [unrelExpr, unrelCall] = makeUnrelated();
  auto unrelExprSharedPtr =
      makeLessEqualSharedPtr(std::move(unrelExpr), D(10.0));
  SparqlFilter unrelFilter{SparqlExpressionPimpl{
      std::move(unrelExprSharedPtr),
      "<http://www.w3.org/2005/xpath-functions/math#pow>(?a, ?b) <= 10.0"}};
  ASSERT_FALSE(rewrite(unrelFilter).has_value());

  // Construct `FILTER(geof:relate(?a, ?b, "T*T***T**"))`
  auto [de9imExpr, de9imCall] = makeDe9imRelation();
  std::shared_ptr<SparqlExpression> de9imSharedPtr = std::move(de9imExpr);
  SparqlFilter de9imFilter{
      SparqlExpressionPimpl{std::move(de9imSharedPtr),
                            "<http://www.opengis.net/def/function/geosparql/"
                            "relate>(?a, ?b, \"T*T***T**\")"}};

  auto de9imSjResult = rewrite(de9imFilter);
  ASSERT_TRUE(de9imSjResult.has_value());
  const auto& de9imSjConf = de9imSjResult.value().config_;
  ASSERT_EQ(de9imSjConf.left_, V{"?a"});
  ASSERT_EQ(de9imSjConf.right_, V{"?b"});
  ASSERT_EQ(de9imSjConf.joinType_, DE9IM);
  const auto& de9imTask = std::get<LibSpatialJoinConfig>(de9imSjConf.task_);
  ASSERT_EQ(de9imTask.de9imFilter_, parseDe9imFilterString("T*T***T**"));
}

// _____________________________________________________________________________
TEST(QueryRewriteUtilTest, RewriteFilterToSpatialJoinConfigWithFixedValue) {
  auto D = &ValueId::makeFromDouble;
  auto point = ValueId::makeFromGeoPoint({1, 1});

  // `FILTER(geof:metricDistance(?a, <fixed point>) <= 10.0)`: the right-hand
  // side is a fixed value, so it must be resolved to a fresh internal
  // variable together with a one-row `VALUES` tree binding it.
  auto distExpr = makeMetricDistExpression(getExpr(V{"?a"}), getExpr(point));
  auto exprSharedPtr = makeLessEqualSharedPtr(std::move(distExpr), D(10.0));
  SparqlFilter filter{
      SparqlExpressionPimpl{std::move(exprSharedPtr),
                            "<http://www.opengis.net/def/function/geosparql/"
                            "metricDistance>(?a, <fixed point>) <= 10.0"}};

  auto sjResult = rewrite(filter);
  ASSERT_TRUE(sjResult.has_value());
  const auto& sjConf = sjResult.value().config_;
  ASSERT_EQ(sjConf.left_, V{"?a"});
  ASSERT_NE(sjConf.right_, V{"?a"});
  ASSERT_FALSE(sjResult.value().childLeft_.has_value());
  ASSERT_TRUE(sjResult.value().childRight_.has_value());

  // The prebuilt child is a one-row `VALUES` clause binding `sjConf.right_`
  // to the fixed point.
  const auto* values = dynamic_cast<const Values*>(
      sjResult.value().childRight_.value()->getRootOperation().get());
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->getResultWidth(), 1u);

  // Both sides fixed: nothing to join on, so this is left to ordinary
  // `FILTER` evaluation.
  auto bothFixedExpr = makeMetricDistExpression(getExpr(point), getExpr(point));
  auto bothFixedSharedPtr =
      makeLessEqualSharedPtr(std::move(bothFixedExpr), D(10.0));
  SparqlFilter bothFixedFilter{SparqlExpressionPimpl{
      std::move(bothFixedSharedPtr),
      "<http://www.opengis.net/def/function/geosparql/"
      "metricDistance>(<fixed point>, <fixed point>) <= 10.0"}};
  ASSERT_FALSE(rewrite(bothFixedFilter).has_value());
}

// TODO<ullingerc> #2140: Add tests for `getGeoFunctionExpressionParameters` +
// `rewriteFilterToSpatialJoinConfig` for geo relation functions

}  // namespace

// Copyright 2022 - 2026 The QLever Authors, in particular:
//
// 2022 - 2024 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <optional>
#include <string>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/PrefixMatchExpression.h"
#include "engine/sparqlExpressions/SampleExpression.h"

using namespace sparqlExpression;
using ad_utility::source_location;

namespace {
auto lit = ad_utility::testing::tripleComponentLiteral;

constexpr auto T = Id::makeFromBool(true);
constexpr auto F = Id::makeFromBool(false);
constexpr Id U = Id::makeUndefined();

// _____________________________________________________________________________
auto literal(const std::string& literal,
             std::string_view langtagOrDatatype = "") {
  return std::make_unique<StringLiteralExpression>(
      lit(literal, langtagOrDatatype));
}

// _____________________________________________________________________________
auto variable(std::string literal) {
  return std::make_unique<VariableExpression>(Variable{std::move(literal)});
}

// _____________________________________________________________________________
bool isPrefixExpression(const SparqlExpression::Ptr& expression) {
  return dynamic_cast<PrefixMatchExpression*>(expression.get());
}

// Make a `ql:prefix-match` expression on `variable` with the given `prefix`.
// The argument `childAsStr` is true iff the variable is enclosed in `STR()`.
SparqlExpression::Ptr makePrefixMatch(std::string variable, std::string prefix,
                                      bool childAsStr = false) {
  SparqlExpression::Ptr child =
      std::make_unique<VariableExpression>(Variable{std::move(variable)});
  if (childAsStr) {
    child = makeStrExpression(std::move(child));
  }
  return makePrefixMatchExpression(std::move(child), literal(prefix));
}

// Evaluate `expression` on the `TestContext` and check that it yields the
// `expected` result.
void testWithExplicitResult(const SparqlExpression& expression,
                            const std::vector<Id>& expected,
                            source_location l = AD_CURRENT_SOURCE_LOC()) {
  TestContext ctx;
  auto trace = generateLocationTrace(std::move(l), "testWithExplicitResult");
  auto resultAsVariant = expression.evaluate(&ctx.context);
  ASSERT_TRUE(
      std::holds_alternative<VectorWithMemoryLimit<Id>>(resultAsVariant));
  const auto& result = std::get<VectorWithMemoryLimit<Id>>(resultAsVariant);

  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}
}  // namespace

namespace sparqlExpression {
// _____________________________________________________________________________
TEST(PrefixMatchExpression, makePrefixMatchExpression) {
  using namespace ::testing;
  auto hasPrefixAndVariableMatcher = [](std::string variableName,
                                        std::string_view prefix) {
    return Pointee(WhenDynamicCastTo<const PrefixMatchExpression&>(
        AllOf(AD_FIELD(PrefixMatchExpression, prefix_, Eq(prefix)),
              AD_FIELD(PrefixMatchExpression, variable_,
                       Eq(Variable{std::move(variableName)})))));
  };
  EXPECT_THAT(makePrefixMatchExpression(variable("?x"), literal("Prefix")),
              hasPrefixAndVariableMatcher("?x", "Prefix"));
  EXPECT_THAT(makePrefixMatchExpression(makeStrExpression(variable("?x")),
                                        literal("Prefix")),
              hasPrefixAndVariableMatcher("?x", "Prefix"));
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      makePrefixMatchExpression(makeStrExpression(variable("?x")),
                                literal("Prefix", "@en")),
      HasSubstr("literals without a language tag or a datatype"),
      std::runtime_error);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      makePrefixMatchExpression(literal("Not a variable"), literal("Prefix")),
      HasSubstr("STR(?var) or ?var"), std::runtime_error);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      makePrefixMatchExpression(variable("?x"), variable("?not_a_constant")),
      HasSubstr("static string literals"), std::runtime_error);
}
}  // namespace sparqlExpression

// _____________________________________________________________________________
auto testPrefixMatchUnorderedColumn =
    [](std::string variable, std::string prefix,
       const std::vector<Id>& expectedResult, bool childAsStr = false,
       source_location l = AD_CURRENT_SOURCE_LOC()) {
      auto trace = generateLocationTrace(l, "testPrefixMatchUnorderedColumn");
      auto expr =
          makePrefixMatch(std::move(variable), std::move(prefix), childAsStr);
      EXPECT_TRUE(isPrefixExpression(expr));
      testWithExplicitResult(*expr, expectedResult);
    };

// _____________________________________________________________________________
TEST(PrefixMatchExpression, unorderedColumn) {
  auto test = testPrefixMatchUnorderedColumn;
  // ?vocab column is `"Beta", "alpha", "älpha"
  // ?mixed column is `1, -0.1, <x>`

  test("?vocab", "Be", {T, F, F});
  // Prefix filters are always case-insensitive.
  test("?vocab", "be", {T, F, F});
  // Prefix filters always work on the primary level, where `a` and `ä` are
  // considered equal.
  test("?vocab", "al", {F, T, T});
  test("?vocab", "äl", {F, T, T});

  test("?vocab", "c", {F, F, F});

  // We explicitly need to pass the STR() function for non-literal entries.
  test("?mixed", "x", {F, F, T}, true);
  test("?mixed", "x", {F, F, F}, false);

  // Unbound input, regression test for
  // https://github.com/ad-freiburg/qlever/issues/2712 .
  {
    auto expr = makePrefixMatch("?doesNotExist", "", false);
    EXPECT_TRUE(isPrefixExpression(expr));
    TestContext ctx;
    auto resultAsVariant = expr->evaluate(&ctx.context);
    EXPECT_THAT(resultAsVariant, ::testing::VariantWith<Id>(U));
  }

  // Input with UNDEF.
  test("?everything", "x", {F, F, U}, false);

  // TODO<joka921> Prefix filters on numbers do not yet work.
}

// _____________________________________________________________________________
auto testPrefixMatchOrderedColumn =
    [](std::string variableAsString, std::string prefix,
       ad_utility::SetOfIntervals expected, bool childAsStr = false,
       source_location l = AD_CURRENT_SOURCE_LOC()) {
      auto trace = generateLocationTrace(l, "testPrefixMatchOrderedColumn");
      auto variable = Variable{variableAsString};
      TestContext ctx = TestContext::sortedBy(variable);
      auto expression = makePrefixMatch(variableAsString, prefix, childAsStr);
      EXPECT_TRUE(isPrefixExpression(expression));
      auto resultAsVariant = expression->evaluate(&ctx.context);
      const auto& result =
          std::get<ad_utility::SetOfIntervals>(resultAsVariant);
      ASSERT_EQ(result, expected);
    };

// _____________________________________________________________________________
TEST(PrefixMatchExpression, orderedColumn) {
  auto test = testPrefixMatchOrderedColumn;
  // Sorted order (by bits of the valueIds):
  // ?vocab column is  "alpha", "älpha", "Beta"
  // ?mixed column is `1, -0.1, <x>`
  test("?vocab", "Be", {{{2, 3}}});
  // Prefix filters are always case-insensitive.
  test("?vocab", "be", {{{2, 3}}});
  // Prefix filters always work on the primary level, where `a` and `ä` are
  // considered equal.
  test("?vocab", "al", {{{0, 2}}});
  test("?vocab", "äl", {{{0, 2}}});
  test("?vocab", "c", {});
  test("?mixed", "x", {{{2, 3}}}, true);

  // Input with UNDEF.
  {
    Variable variable{"?everything"};
    TestContext ctx = TestContext::sortedBy(variable);
    auto expression = makePrefixMatch(variable.name(), "x", false);
    EXPECT_TRUE(isPrefixExpression(expression));
    auto resultAsVariant = expression->evaluate(&ctx.context);
    EXPECT_THAT(resultAsVariant,
                ::testing::VariantWith<VectorWithMemoryLimit<Id>>(
                    ::testing::ElementsAre(U, F, F)));
  }
  // Empty input.
  {
    Variable variable{"?everything"};
    TestContext ctx = TestContext::sortedBy(variable);
    ctx.context._endIndex = 0;
    auto expression = makePrefixMatch(variable.name(), "x", false);
    EXPECT_TRUE(isPrefixExpression(expression));
    auto resultAsVariant = expression->evaluate(&ctx.context);
    EXPECT_THAT(resultAsVariant,
                ::testing::VariantWith<ad_utility::SetOfIntervals>(
                    ad_utility::SetOfIntervals{}));
  }
}

// _____________________________________________________________________________
TEST(PrefixMatchExpression, onGroupedVariableIsConstant) {
  // Evaluate on a single-row "group" in which `?vocab` is constant (`"Beta"`).
  auto setUpGroupedContext = [](TestContext& ctx) {
    ctx.context._groupedVariables = {Variable{"?vocab"}};
    ctx.context._isPartOfGroupBy = true;
    ctx.context._beginIndex = 0;
    ctx.context._endIndex = 1;
  };

  // `"Beta"` matches the prefix `Be` -> constant `true`.
  {
    auto expression = makePrefixMatch("?vocab", "Be");
    ASSERT_TRUE(isPrefixExpression(expression));
    TestContext ctx;
    setUpGroupedContext(ctx);
    EXPECT_THAT(expression->evaluate(&ctx.context),
                ::testing::VariantWith<Id>(T));
  }
  // `"Beta"` does not match the prefix `al` -> constant `false`.
  {
    auto expression = makePrefixMatch("?vocab", "al");
    ASSERT_TRUE(isPrefixExpression(expression));
    TestContext ctx;
    setUpGroupedContext(ctx);
    EXPECT_THAT(expression->evaluate(&ctx.context),
                ::testing::VariantWith<Id>(F));
  }
}

// _____________________________________________________________________________
TEST(PrefixMatchExpression, insideAggregateIsNotFolded) {
  auto prefixMatch = makePrefixMatch("?vocab", "al");
  ASSERT_TRUE(isPrefixExpression(prefixMatch));
  // Wrap the prefix match in an aggregate.
  auto aggregate =
      std::make_unique<SampleExpression>(false, std::move(prefixMatch));
  const auto* child = aggregate->children()[0].get();
  ASSERT_TRUE(child->isInsideAggregate());

  TestContext ctx;
  ctx.context._groupedVariables = {Variable{"?vocab"}};
  ctx.context._isPartOfGroupBy = true;
  // The result is computed per row (a vector), not folded to a single constant.
  EXPECT_THAT(child->evaluate(&ctx.context),
              ::testing::VariantWith<VectorWithMemoryLimit<Id>>(
                  ::testing::ElementsAre(F, T, T)));
}

// _____________________________________________________________________________
TEST(PrefixMatchExpression, onGroupedVariableWithUnexpectedChildResult) {
  // The child of a `PrefixMatchExpression` is always a single variable, so when
  // the variable is grouped, the child evaluates either to a single `ValueId`
  // or (for hash-map/lazy GROUP BY) to a `VectorWithMemoryLimit<ValueId>`. Here
  // we force an unexpected result type by replacing the child with an
  // expression that yields an `IdOrLocalVocabEntry`, which must trigger the
  // `AD_FAIL()` in the otherwise unreachable `else` branch.
  auto expression = makePrefixMatch("?vocab", "al");
  ASSERT_TRUE(isPrefixExpression(expression));
  expression->replaceChild(
      0, std::make_unique<SingleUseExpression>(
             ExpressionResult{IdOrLocalVocabEntry{Id::makeFromBool(true)}}));

  TestContext ctx;
  ctx.context._groupedVariables = {Variable{"?vocab"}};
  ctx.context._isPartOfGroupBy = true;
  ctx.context._beginIndex = 0;
  ctx.context._endIndex = 1;
  AD_EXPECT_THROW_WITH_MESSAGE(expression->evaluate(&ctx.context),
                               ::testing::HasSubstr("unreachable"));
}

// _____________________________________________________________________________
TEST(PrefixMatchExpression, getEstimatesForFilterExpression) {
  using Estimates = SparqlExpressionPimpl::Estimates;
  auto hasEstimate = [](size_t sizeEstimate, size_t costEstimate) {
    using namespace ::testing;
    return AllOf(AD_FIELD(Estimates, sizeEstimate, Eq(sizeEstimate)),
                 AD_FIELD(Estimates, costEstimate, Eq(costEstimate)));
  };
  auto expression = makePrefixMatch("?a", "abc");
  EXPECT_THAT(expression->getEstimatesForFilterExpression(10000, std::nullopt),
              hasEstimate(10, 10010));
  EXPECT_THAT(expression->getEstimatesForFilterExpression(100000, std::nullopt),
              hasEstimate(100, 100100));
  EXPECT_THAT(
      expression->getEstimatesForFilterExpression(10000, Variable{"?b"}),
      hasEstimate(10, 10010));
  EXPECT_THAT(
      expression->getEstimatesForFilterExpression(100000, Variable{"?b"}),
      hasEstimate(100, 100100));
  EXPECT_THAT(
      expression->getEstimatesForFilterExpression(10000, Variable{"?a"}),
      hasEstimate(10, 10));
  EXPECT_THAT(
      expression->getEstimatesForFilterExpression(100000, Variable{"?a"}),
      hasEstimate(100, 100));

  auto longPrefixExpression = makePrefixMatch("?a", "thisisverylong");
  EXPECT_THAT(longPrefixExpression->getEstimatesForFilterExpression(
                  1000000000, std::nullopt),
              hasEstimate(10, 1000000010));
  EXPECT_THAT(longPrefixExpression->getEstimatesForFilterExpression(
                  1000000000, Variable{"?a"}),
              hasEstimate(10, 10));

  auto zeroLengthExpression = makePrefixMatch("?a", "");
  ASSERT_TRUE(isPrefixExpression(zeroLengthExpression));
  EXPECT_THAT(
      zeroLengthExpression->getEstimatesForFilterExpression(100, std::nullopt),
      hasEstimate(100, 200));
  EXPECT_THAT(zeroLengthExpression->getEstimatesForFilterExpression(
                  1000000000, Variable{"?a"}),
              hasEstimate(1000000000, 1000000000));
}

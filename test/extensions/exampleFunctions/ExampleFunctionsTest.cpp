// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Artem <artem@rem.sh>

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Verifies the example extension self-registered and computes correct values.

#include <gtest/gtest.h>

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string_view>
#include <variant>

#include "../../SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/Id.h"
#include "parser/SparqlFunctionRegistry.h"

namespace {
using Registry = parsedQuery::SparqlFunctionRegistry;
constexpr std::string_view kIncrement =
    "<http://qlever.cs.uni-freiburg.de/example/increment>";
constexpr std::string_view kPlus =
    "<http://qlever.cs.uni-freiburg.de/example/plus>";

// An argument list of integer-constant expressions.
Registry::ArgList intArgs(std::initializer_list<int64_t> values) {
  Registry::ArgList args;
  for (int64_t v : values) {
    args.push_back(
        std::make_unique<sparqlExpression::IdExpression>(Id::makeFromInt(v)));
  }
  return args;
}

// Evaluate a constant expression to a single `Id`.
Id evalConstant(Registry::ExpressionPtr expr) {
  sparqlExpression::TestContext testContext;
  return std::get<Id>(expr->evaluate(&testContext.context));
}

TEST(ExampleFunctions, selfRegistered) {
  const auto& registry = Registry::get();
  EXPECT_TRUE(registry.lookup(kIncrement).has_value());
  EXPECT_TRUE(registry.lookup(kPlus).has_value());
}

TEST(ExampleFunctions, computeAndValidateArity) {
  auto increment = Registry::get().lookup(kIncrement);
  auto plus = Registry::get().lookup(kPlus);
  ASSERT_TRUE(increment.has_value());
  ASSERT_TRUE(plus.has_value());

  // increment(41) == 42, plus(2, 3) == 5.
  EXPECT_EQ(evalConstant((*increment)(intArgs({41}))), Id::makeFromInt(42));
  EXPECT_EQ(evalConstant((*plus)(intArgs({2, 3}))), Id::makeFromInt(5));

  // Wrong arity is rejected.
  EXPECT_THROW((*increment)(intArgs({1, 2})),
               parsedQuery::InvalidSparqlFunctionCall);
  EXPECT_THROW((*plus)(intArgs({1})), parsedQuery::InvalidSparqlFunctionCall);
}
}  // namespace

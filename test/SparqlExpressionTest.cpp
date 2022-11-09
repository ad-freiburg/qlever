// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <string>

#include "./SparqlExpressionTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

using namespace sparqlExpression;
using namespace std::literals;

using VD = VectorWithMemoryLimit<double>;
auto checkResultsEqual = [](const auto& a, const auto& b) {
  if constexpr (ad_utility::isSimilar<decltype(a), VD> &&
                ad_utility::isSimilar<decltype(b), VD>) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
      if (std::isnan(a[i])) {
        ASSERT_TRUE(std::isnan(b[i]));
      } else {
        ASSERT_FLOAT_EQ(a[i], b[i]);
      }
    }
  } else if constexpr (ad_utility::isSimilar<decltype(a), decltype(b)>) {
    ASSERT_EQ(a, b);
  } else {
    ASSERT_TRUE(false);
  }
};

template <typename NaryExpression>
auto testNaryExpression = [](const auto& expected, auto&&... operands) {
  QueryExecutionContext* qec = nullptr;
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  LocalVocab localVocab;
  IdTable table{alloc};

  auto getResultSize = []<typename T>(const T& operand) -> size_t {
    if constexpr (isVectorResult<T>) {
      return operand.size();
    }
    return 1;
  };

  auto resultSize = std::max({getResultSize(operands)...});

  sparqlExpression::EvaluationContext context{*qec, map, table, alloc,
                                              localVocab};
  context._endIndex = resultSize;

  auto clone = [](const auto& x) {
    if constexpr (requires { x.clone(); }) {
      return x.clone();
    } else {
      return x;
    }
  };

  std::array<SparqlExpression::Ptr, sizeof...(operands)> children{
      std::make_unique<DummyExpression>(ExpressionResult{clone(operands)})...};

  auto expression = NaryExpression{std::move(children)};

  ExpressionResult res = expression.evaluate(&context);

  ASSERT_TRUE(std::holds_alternative<std::decay_t<decltype(expected)>>(res));

  std::visit(checkResultsEqual, ExpressionResult{clone(expected)}, res);
  // ASSERT_EQ(res, ExpressionResult{clone(expected)});
};

template <typename NaryExpression>
auto testBinaryExpressionCommutative =
    [](const auto& expected, const auto& op1, const auto& op2) {
      testNaryExpression<NaryExpression>(expected, op1, op2);
      testNaryExpression<NaryExpression>(expected, op2, op1);
    };

auto testOr = testBinaryExpressionCommutative<OrExpression>;
auto testAnd = testBinaryExpressionCommutative<AndExpression>;
auto testPlus = testBinaryExpressionCommutative<AddExpression>;
auto testMultiply = testBinaryExpressionCommutative<MultiplyExpression>;
auto testMinus = testNaryExpression<SubtractExpression>;
auto testDivide = testNaryExpression<DivideExpression>;

template <typename T>
using V = VectorWithMemoryLimit<T>;
TEST(SparqlExpression, OrAnd) {
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  V<Bool> b{{false, true, true, false}, alloc};

  V<double> d{{1.0, 2.0, std::numeric_limits<double>::quiet_NaN(), 0.0}, alloc};
  V<Bool> dAsBool{{true, true, false, false}, alloc};

  V<std::string> s{{"true", "", "false", ""}, alloc};
  V<Bool> sAsBool{{true, false, true, false}, alloc};

  V<int64_t> i{{32, -42, 0, 5}, alloc};
  V<Bool> iAsBool{{true, true, false, true}, alloc};

  V<Bool> bOrD{{true, true, true, false}, alloc};
  V<Bool> bAndD{{false, true, false, false}, alloc};

  V<Bool> bOrS{{true, true, true, false}, alloc};
  V<Bool> bAndS{{false, false, true, false}, alloc};

  V<Bool> bOrI{{true, true, true, true}, alloc};
  V<Bool> bAndI{{false, true, false, false}, alloc};

  V<Bool> dOrS{{true, true, true, false}, alloc};
  V<Bool> dAndS{{true, false, false, false}, alloc};

  V<Bool> dOrI{{true, true, false, true}, alloc};
  V<Bool> dAndI{{true, true, false, false}, alloc};

  V<Bool> sOrI{{true, true, true, true}, alloc};
  V<Bool> sAndI{{true, false, false, false}, alloc};

  V<Bool> allTrue{{true, true, true, true}, alloc};
  V<Bool> allFalse{{false, false, false, false}, alloc};

  testOr(b, b, allFalse);
  testOr(allTrue, b, allTrue);
  testOr(dAsBool, d, allFalse);
  testOr(bOrD, d, b);
  testOr(bOrI, b, i);
  testOr(bOrS, b, s);
  testOr(dOrI, d, i);
  testOr(dOrS, d, s);
  testOr(sOrI, i, s);

  testAnd(b, b, allTrue);
  testAnd(dAsBool, d, allTrue);
  testAnd(allFalse, b, allFalse);
  testAnd(bAndD, d, b);
  testAnd(bAndI, b, i);
  testAnd(bAndS, b, s);
  testAnd(dAndI, d, i);
  testAnd(dAndS, d, s);
  testAnd(sAndI, s, i);

  testOr(allTrue, b, true);
  testOr(b, b, false);
  testAnd(allFalse, b, false);
  testAnd(b, b, true);

  testOr(allTrue, b, -42);
  testOr(b, b, 0);
  testAnd(allFalse, b, 0);
  testAnd(b, b, 2839);

  auto nan = std::numeric_limits<double>::quiet_NaN();
  testOr(allTrue, b, -42.32);
  testOr(b, b, 0.0);
  testOr(b, b, nan);
  testAnd(allFalse, b, 0.0);
  testAnd(allFalse, b, nan);
  testAnd(b, b, 2839.123);

  testOr(allTrue, b, std::string("halo"));
  testOr(b, b, std::string(""));
  testAnd(allFalse, b, std::string(""));
  testAnd(b, b, std::string("yellow"));
}

TEST(SparqlExpression, PlusAndMinus) {
  auto nan = std::numeric_limits<double>::quiet_NaN();
  auto inf = std::numeric_limits<double>::infinity();
  auto negInf = -inf;
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};

  V<Bool> b{{true, false, false, true}, alloc};
  V<int64_t> bAsInt{{1, 0, 0, 1}, alloc};

  V<double> d{{1.0, -2.0, nan, 0.0}, alloc};

  V<std::string> s{{"true", "", "false", ""}, alloc};
  V<double> allNan{{nan, nan, nan, nan}, alloc};

  V<int64_t> i{{32, -42, 0, 5}, alloc};
  V<double> iAsDouble{{32.0, -42.0, 0.0, 5.0}, alloc};

  V<double> bPlusD{{2.0, -2.0, nan, 1.0}, alloc};
  V<double> bMinusD{{0, +2.0, nan, 1}, alloc};
  V<double> dMinusB{{0, -2.0, nan, -1}, alloc};
  V<double> bTimesD{{1.0, 0.0, nan, 0.0}, alloc};
  V<double> bByD{{1.0, 0, nan, inf}, alloc};
  V<double> dByB{{1.0, negInf, nan, 0}, alloc};

  testPlus(bPlusD, b, d);
  testMinus(bMinusD, b, d);
  testMinus(dMinusB, d, b);
  testMultiply(bTimesD, b, d);
  testDivide(bByD, b, d);
  testDivide(dByB, d, b);
}

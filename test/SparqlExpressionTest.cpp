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
#include "util/Conversions.h"

// Some useful shortcuts for the tests below.
using namespace sparqlExpression;
using namespace std::literals;
template <typename T>
using V = VectorWithMemoryLimit<T>;

// Test allocator (the inputs to our `SparqlExpression`s are
// `VectorWithMemoryLimit`s, and these require an `AllocatorWithLimit`).
//
// TODO: Can't we define a type here, so that we can easily construct vectors of
// any type that use `alloc` as allocator.
using ad_utility::AllocatorWithLimit;
AllocatorWithLimit<Id> alloc = ad_utility::testing::makeAllocator();

// Assert that the vectors `a` and `b` are equal. The case `V<double>` is
// treated separately because we want to consider two elements that are both
// `NaN` as equal for the test, but they are not equal wrt `==` (by definition,
// a `NaN` is not equal to anything).
auto checkResultsEqual = [](const auto& a, const auto& b) {
  if constexpr (ad_utility::isSimilar<decltype(a), V<double>> &&
                ad_utility::isSimilar<decltype(b), V<double>>) {
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
    ASSERT_TRUE(false) << "Result type does not match";
  }
};

// Assert that the given `NaryExpression` with the given `operands` has the
// `expected` result.
template <typename NaryExpression>
auto testNaryExpression = [](const auto& expected, auto&&... operands) {
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  VariableToColumnMap map;
  LocalVocab localVocab;
  IdTable table{alloc};

  // Get the size of `operand`: size for a vector, 1 otherwise.
  auto getResultSize = []<typename T>(const T& operand) -> size_t {
    if constexpr (isVectorResult<T>) {
      return operand.size();
    }
    return 1;
  };
  auto resultSize = std::max({getResultSize(operands)...});

  sparqlExpression::EvaluationContext context{*ad_utility::testing::getQec(),
                                              map, table, alloc, localVocab};
  context._endIndex = resultSize;

  // NOTE: We need to clone because `VectorWithMemoryLimit` does not have a copy
  // constructor (deliberately).
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

  ExpressionResult result = expression.evaluate(&context);

  ASSERT_TRUE(std::holds_alternative<std::decay_t<decltype(expected)>>(result));

  std::visit(checkResultsEqual, ExpressionResult{clone(expected)}, result);
  // ASSERT_EQ(result, ExpressionResult{clone(expected)});
};

// Assert that the given commutative binary expression has the `expected` result
// in both orders of the operands `op1` and `op2`.
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

// Test `AndExpression` and `OrExpression`.
//
// TODO: Also test `UnaryNegateExpression`.
TEST(SparqlExpression, logicalOperators) {
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

// Test `AddExpression`, `SubtractExpression`, `MultiplyExpression`, and
// `DivideExpression`.
//
// TODO: Also test `UnaryMinusExpression`.
TEST(SparqlExpression, arithemticOperators) {
  auto nan = std::numeric_limits<double>::quiet_NaN();
  auto inf = std::numeric_limits<double>::infinity();
  auto negInf = -inf;

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

// Helper lambda to enable testing a unary expression in one line (see below).
//
// TODO: The tests above could also be simplified (and made much more readable)
// in this vein.
template <typename UnaryExpression, typename OperandType, typename OutputType>
auto testUnaryExpression =
    [](std::vector<OperandType>&& operand, std::vector<OutputType>&& expected) {
      V<OperandType> operandV{std::make_move_iterator(operand.begin()),
                              std::make_move_iterator(operand.end()), alloc};
      V<OutputType> expectedV{std::make_move_iterator(expected.begin()),
                              std::make_move_iterator(expected.end()), alloc};
      testNaryExpression<UnaryExpression>(expectedV, operandV);
    };

// Test `YearExpression`, `MonthExpression`, and `DayExpression`.
TEST(SparqlExpression, dateOperators) {
  // Helper function that asserts that the date operators give the expected
  // result on the given date.
  auto check = [](const string& date, int expectedYear, int expectedMonth,
                  int expectedDay) {
    auto checkYear = testUnaryExpression<YearExpression, std::string, long int>;
    auto checkMonth =
        testUnaryExpression<MonthExpression, std::string, long int>;
    auto checkDay = testUnaryExpression<DayExpression, std::string, long int>;
    checkYear({date}, {expectedYear});
    checkMonth({date}, {expectedMonth});
    checkDay({date}, {expectedDay});
  };

  // Now the checks for dates with varying level of detail.
  check("1970-04-22T11:53:00", 1970, 4, 22);
  check("1970-04-22", 1970, 4, 22);
  check("1970-04-22", 1970, 4, 22);
  check("1970-04-22", 1970, 4, 22);
  check("0042-12-24", 42, 12, 24);
  check("-0099-07-01", -99, 7, 1);
  check("-99-07-01", -99, 7, 1);
  check("-12345699-07-01", -12345699, 7, 1);
  check("321-07-01", 321, 7, 1);
  check("12321-07-01", 12321, 7, 1);
}

// Test `StrlenExpression` and `StrExpression`.
auto checkStrlen = testUnaryExpression<StrlenExpression, std::string, long int>;
template <typename OperandType>
auto checkStr = [](std::vector<OperandType>&& operand,
                   std::vector<std::string>&& expected) {
  testUnaryExpression<StrExpression, OperandType, std::string>(
      std::move(operand), std::move(expected));
};
TEST(SparqlExpression, stringOperators) {
  checkStrlen({"one", "two", "three", ""}, {3, 3, 5, 0});
  checkStr<int64_t>({1, 2, 3}, {"1", "2", "3"});
  checkStr<double>({-1.0, 1.0}, {std::to_string(-1.0), std::to_string(1.0)});
  checkStr<Bool>({true, false, true}, {"1", "0", "1"});
  checkStr<std::string>({"one", "two", "three"}, {"one", "two", "three"});
}

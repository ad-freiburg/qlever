// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <string>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/Conversions.h"

namespace {

using ad_utility::source_location;
// Some useful shortcuts for the tests below.
using namespace sparqlExpression;
using namespace std::literals;
template <typename T>
using V = VectorWithMemoryLimit<T>;

auto naN = std::numeric_limits<double>::quiet_NaN();
auto inf = std::numeric_limits<double>::infinity();
auto negInf = -inf;

auto D = ad_utility::testing::DoubleId;
auto B = ad_utility::testing::BoolId;
auto I = ad_utility::testing::IntId;
auto Voc = ad_utility::testing::VocabId;
auto U = Id::makeUndefined();

using Ids = std::vector<Id>;
using IdOrStrings = std::vector<IdOrString>;

// Test allocator (the inputs to our `SparqlExpression`s are
// `VectorWithMemoryLimit`s, and these require an `AllocatorWithLimit`).
//
// TODO: Can't we define a type here, so that we can easily construct vectors of
// any type that use `alloc` as allocator.
using ad_utility::AllocatorWithLimit;
AllocatorWithLimit<Id> alloc = ad_utility::testing::makeAllocator();

// A matcher for `ValueId`s that also handles floating point comparisons
// (precision issues as well as NaNs) correctly.
::testing::Matcher<const ValueId&> matchId(Id id) {
  if (id.getDatatype() != Datatype::Double) {
    // Everything that is not `Double` must match exactly.
    return ::testing::Eq(id);
  } else {
    // NaN and NaN are considered equal, and we also use `FloatEq` and not
    // `DoubleEq` because the precision of a `ValueId` is less than 64 bits
    // because of the datatype bits.
    testing::Matcher<float> doubleMatcher =
        ::testing::NanSensitiveFloatEq(static_cast<float>(id.getDouble()));
    auto doubleMatcherCast =
        ::testing::MatcherCast<const double&>(doubleMatcher);
    return ::testing::AllOf(
        AD_PROPERTY(Id, getDatatype, ::testing::Eq(Datatype::Double)),
        AD_PROPERTY(Id, getDouble, doubleMatcherCast));
  }
}
// Assert that the vectors `a` and `b` are equal. The case `V<double>` is
// treated separately because we want to consider two elements that are both
// `NaN` as equal for the test, but they are not equal wrt `==` (by definition,
// a `NaN` is not equal to anything).
auto checkResultsEqual = []<SingleExpressionResult A, SingleExpressionResult B>(
                             const A& expected, const B& actual,
                             source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  if constexpr (ad_utility::isSimilar<A, B>) {
    if constexpr (isVectorResult<A>) {
      if constexpr (std::is_same_v<typename A::value_type, Id>) {
        auto matcherVec = ad_utility::transform(
            expected, [](const Id id) { return matchId(id); });
        ASSERT_THAT(actual, ::testing::ElementsAreArray(matcherVec));
      } else {
        ASSERT_EQ(actual, expected);
      }

    } else {
      ASSERT_EQ(actual, expected);
    }
  } else {
    FAIL() << "Result type does not match";
  }
};

// Assert that the given `NaryExpression` with the given `operands` has the
// `expected` result.
auto testNaryExpression = [](auto&& makeExpression,
                             SingleExpressionResult auto&& expected,
                             SingleExpressionResult auto&&... operands) {
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

  auto expressionPtr = std::apply(makeExpression, std::move(children));
  auto& expression = *expressionPtr;

  ExpressionResult result = expression.evaluate(&context);

  ASSERT_TRUE(std::holds_alternative<std::decay_t<decltype(expected)>>(result));

  std::visit(checkResultsEqual, ExpressionResult{clone(expected)}, result);
  // ASSERT_EQ(result, ExpressionResult{clone(expected)});
};

// Assert that the given commutative binary expression has the `expected` result
// in both orders of the operands `op1` and `op2`.
auto testBinaryExpressionCommutative =
    [](auto makeFunction, const SingleExpressionResult auto& expected,
       const SingleExpressionResult auto& op1,
       const SingleExpressionResult auto& op2,
       source_location l = source_location::current()) {
      auto t = generateLocationTrace(l);
      testNaryExpression(makeFunction, expected, op1, op2);
      testNaryExpression(makeFunction, expected, op2, op1);
    };

auto testBinaryExpression = [](auto makeExpression,
                               const SingleExpressionResult auto& expected,
                               const SingleExpressionResult auto& op1,
                               const SingleExpressionResult auto& op2,
                               source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  testNaryExpression(makeExpression, expected, op1, op2);
};

auto testOr =
    std::bind_front(testBinaryExpressionCommutative, &makeOrExpression);
auto testAnd =
    std::bind_front(testBinaryExpressionCommutative, &makeAndExpression);
auto testPlus =
    std::bind_front(testBinaryExpressionCommutative, &makeAddExpression);
auto testMultiply =
    std::bind_front(testBinaryExpressionCommutative, &makeMultiplyExpression);
auto testMinus = std::bind_front(testBinaryExpression, &makeSubtractExpression);
auto testDivide = std::bind_front(testBinaryExpression, &makeDivideExpression);

}  // namespace
// Test `AndExpression` and `OrExpression`.
//
TEST(SparqlExpression, logicalOperators) {
  V<Id> b{{B(false), B(true), B(true), B(false)}, alloc};
  V<Id> d{{D(1.0), D(2.0), D(std::numeric_limits<double>::quiet_NaN()), D(0.0)},
          alloc};
  V<Id> dAsBool{{B(true), B(true), B(false), B(false)}, alloc};

  V<IdOrString> s{{"true", "", "false", ""}, alloc};
  V<Id> sAsBool{{B(true), B(false), B(true), B(false)}, alloc};

  V<Id> i{{I(32), I(-42), I(0), I(5)}, alloc};
  V<Id> iAsId{{B(true), B(true), B(false), B(true)}, alloc};

  V<Id> bOrD{{B(true), B(true), B(true), B(false)}, alloc};
  V<Id> bAndD{{B(false), B(true), B(false), B(false)}, alloc};

  V<Id> bOrS{{B(true), B(true), B(true), B(false)}, alloc};
  V<Id> bAndS{{B(false), B(false), B(true), B(false)}, alloc};

  V<Id> bOrI{{B(true), B(true), B(true), B(true)}, alloc};
  V<Id> bAndI{{B(false), B(true), B(false), B(false)}, alloc};

  V<Id> dOrS{{B(true), B(true), B(true), B(false)}, alloc};
  V<Id> dAndS{{B(true), B(false), B(false), B(false)}, alloc};

  V<Id> dOrI{{B(true), B(true), B(false), B(true)}, alloc};
  V<Id> dAndI{{B(true), B(true), B(false), B(false)}, alloc};

  V<Id> sOrI{{B(true), B(true), B(true), B(true)}, alloc};
  V<Id> sAndI{{B(true), B(false), B(false), B(false)}, alloc};

  V<Id> allTrue{{B(true), B(true), B(true), B(true)}, alloc};
  V<Id> allFalse{{B(false), B(false), B(false), B(false)}, alloc};

  testOr(b, b, allFalse);
  testOr(allTrue, b, allTrue);
  testOr(dAsBool, d, allFalse);
  testOr(bOrD, d, b);
  testOr(bOrI, b, i);
  testOr(bOrS, b, s);
  testOr(dOrI, d, i);
  testOr(dOrS, d, s);
  testOr(sOrI, i, s);

  using S = ad_utility::SetOfIntervals;
  testOr(S{{{0, 6}}}, S{{{0, 4}}}, S{{{3, 6}}});

  testAnd(b, b, allTrue);
  testAnd(dAsBool, d, allTrue);
  testAnd(allFalse, b, allFalse);
  testAnd(bAndD, d, b);
  testAnd(bAndI, b, i);
  testAnd(bAndS, b, s);
  testAnd(dAndI, d, i);
  testAnd(dAndS, d, s);
  testAnd(sAndI, s, i);
  testAnd(S{{{3, 4}}}, S{{{0, 4}}}, S{{{3, 6}}});

  testOr(allTrue, b, B(true));
  testOr(b, b, B(false));
  testAnd(allFalse, b, B(false));
  testAnd(b, b, B(true));

  testOr(allTrue, b, I(-42));
  testOr(b, b, I(0));
  testAnd(allFalse, b, I(0));
  testAnd(b, b, I(2839));

  auto nan = std::numeric_limits<double>::quiet_NaN();
  testOr(allTrue, b, D(-42.32));
  testOr(b, b, D(0.0));
  testOr(b, b, D(nan));
  testAnd(allFalse, b, D(0.0));
  testAnd(allFalse, b, D(nan));
  testAnd(b, b, D(2839.123));

  testOr(allTrue, b, IdOrString{"halo"});
  testOr(b, b, IdOrString(""));
  testAnd(allFalse, b, IdOrString(""));
  testAnd(b, b, IdOrString("yellow"));

  // Test the behavior in the presence of UNDEF values.
  Id t = B(true);
  Id f = B(false);
  {
    V<Id> allValues1{{t, t, t, f, f, f, U, U, U}, alloc};
    V<Id> allValues2{{t, f, U, t, f, U, t, f, U}, alloc};
    V<Id> resultOr{{t, t, t, t, f, U, t, U, U}, alloc};
    V<Id> resultAnd{{t, f, U, f, f, f, U, f, U}, alloc};
    testOr(resultOr, allValues1, allValues2);
    testAnd(resultAnd, allValues1, allValues2);
  }
}

// Test `AddExpression`, `SubtractExpression`, `MultiplyExpression`, and
// `DivideExpression`.
//
// TODO: Also test `UnaryMinusExpression`.
TEST(SparqlExpression, arithmeticOperators) {
  V<Id> b{{B(true), B(false), B(false), B(true)}, alloc};
  V<Id> bAsInt{{I(1), I(0), I(0), I(1)}, alloc};

  V<Id> d{{D(1.0), D(-2.0), D(naN), D(0.0)}, alloc};

  V<std::string> s{{"true", "", "false", ""}, alloc};

  V<Id> allNan{{D(naN), D(naN), D(naN), D(naN)}, alloc};

  V<Id> i{{I(32), I(-42), I(0), I(5)}, alloc};
  V<Id> iAsDouble{{D(32.0), D(-42.0), D(0.0), D(5.0)}, alloc};

  V<Id> bPlusD{{D(2.0), D(-2.0), D(naN), D(1.0)}, alloc};
  V<Id> bMinusD{{D(0), D(2.0), D(naN), D(1)}, alloc};
  V<Id> dMinusB{{D(0), D(-2.0), D(naN), D(-1)}, alloc};
  V<Id> bTimesD{{D(1.0), D(-0.0), D(naN), D(0.0)}, alloc};
  V<Id> bByD{{D(1.0), D(-0.0), D(naN), D(inf)}, alloc};
  V<Id> dByB{{D(1.0), D(negInf), D(naN), D(0)}, alloc};

  testPlus(bPlusD, b, d);
  testMinus(bMinusD, b, d);
  testMinus(dMinusB, d, b);
  testMultiply(bTimesD, b, d);
  testDivide(bByD, b, d);
  testDivide(dByB, d, b);

  V<Id> mixed{{B(true), B(false), I(-4), I(0), I(13), D(-13.2), D(0.0), D(16.3),
               Voc(4)},
              alloc};
  V<Id> plus1{{I(2), I(1), I(-3), I(1), I(14), D(-12.2), D(1.0), D(17.3), U},
              alloc};
  V<Id> minus22{{D(-1.2), D(-2.2), D(-6.2), D(-2.2), D(10.8), D(-15.4), D(-2.2),
                 D(14.1), U},
                alloc};
  V<Id> times2{{I(2), I(0), I(-8), I(0), I(26), D(-26.4), D(0.0), D(32.6), U},
               alloc};
  V<Id> times13{
      {D(1.3), D(0), D(-5.2), D(0), D(16.9), D(-17.16), D(0.0), D(21.19), U},
      alloc};
  V<Id> by2{{D(0.5), D(0), D(-2.0), D(0), D(6.5), D(-6.6), D(0.0), D(8.15), U},
            alloc};

  testPlus(plus1, I(1), mixed);
  testMinus(plus1, mixed, I(-1));

  testMinus(minus22, mixed, D(2.2));
  testPlus(minus22, mixed, D(-2.2));

  testMultiply(times2, mixed, I(2));
  testMultiply(times13, mixed, D(1.3));

  // For division, all results are doubles, so there is no difference between
  // int and double inputs.
  testDivide(by2, mixed, I(2));
  testDivide(by2, mixed, D(2));
  testMultiply(by2, mixed, D(0.5));
  testDivide(times13, mixed, D(1.0 / 1.3));
}

// Helper lambda to enable testing a unary expression in one line (see below).
//
// TODO: The tests above could also be simplified (and made much more readable)
// in this vein.
auto testUnaryExpression =
    []<SingleExpressionResult OperandType, SingleExpressionResult OutputType>(
        auto makeFunction, std::vector<OperandType> operand,
        std::vector<OutputType> expected,
        source_location l = source_location::current()) {
      auto trace = generateLocationTrace(l);
      V<OperandType> operandV{std::make_move_iterator(operand.begin()),
                              std::make_move_iterator(operand.end()), alloc};
      V<OutputType> expectedV{std::make_move_iterator(expected.begin()),
                              std::make_move_iterator(expected.end()), alloc};
      testNaryExpression(makeFunction, expectedV, operandV);
    };

// Test `YearExpression`, `MonthExpression`, and `DayExpression`.
TEST(SparqlExpression, dateOperators) {
  // Helper function that asserts that the date operators give the expected
  // result on the given date.
  auto checkYear = std::bind_front(testUnaryExpression, &makeYearExpression);
  auto checkMonth = std::bind_front(testUnaryExpression, &makeMonthExpression);
  auto checkDay = std::bind_front(testUnaryExpression, &makeDayExpression);
  auto check = [&checkYear, &checkMonth, &checkDay](
                   const DateOrLargeYear& date, std::optional<int> expectedYear,
                   std::optional<int> expectedMonth,
                   std::optional<int> expectedDay,
                   std::source_location l = std::source_location::current()) {
    auto trace = generateLocationTrace(l);
    auto optToId = [](const auto& opt) {
      if (opt.has_value()) {
        return Id::makeFromInt(opt.value());
      } else {
        return Id::makeUndefined();
      }
    };
    checkYear(Ids{Id::makeFromDate(date)}, Ids{optToId(expectedYear)});
    checkMonth(Ids{Id::makeFromDate(date)}, Ids{optToId(expectedMonth)});
    checkDay(Ids{Id::makeFromDate(date)}, Ids{optToId(expectedDay)});
  };

  using D = DateOrLargeYear;
  // Now the checks for dates with varying level of detail.
  check(D::parseXsdDatetime("1970-04-22T11:53:00"), 1970, 4, 22);
  check(D::parseXsdDate("1970-04-22"), 1970, 4, 22);
  check(D::parseXsdDate("1970-04-22"), 1970, 4, 22);
  check(D::parseXsdDate("1970-04-22"), 1970, 4, 22);
  check(D::parseXsdDate("0042-12-24"), 42, 12, 24);
  check(D::parseXsdDate("-0099-07-01"), -99, 7, 1);
  check(D::parseGYear("-1234"), -1234, std::nullopt, std::nullopt);
  check(D::parseXsdDate("0321-07-01"), 321, 7, 1);
  check(D::parseXsdDate("2321-07-01"), 2321, 7, 1);

  // Test behavior of the `largeYear` representation that doesn't store the
  // actual date.
  check(D::parseGYear("123456"), 123456, std::nullopt, std::nullopt);
  check(D::parseGYearMonth("-12345-01"), -12345, 1, std::nullopt);
  check(D::parseGYearMonth("-12345-03"), -12345, 1, std::nullopt);
  check(D::parseXsdDate("-12345-01-01"), -12345, 1, 1);
  check(D::parseXsdDate("-12345-03-04"), -12345, 1, 1);

  // Invalid inputs for date expressions.
  checkYear(Ids{Id::makeFromInt(42)}, Ids{Id::makeUndefined()});
  checkMonth(Ids{Id::makeFromInt(42)}, Ids{Id::makeUndefined()});
  checkDay(Ids{Id::makeFromInt(42)}, Ids{Id::makeUndefined()});
  auto testYear = std::bind_front(testUnaryExpression, &makeYearExpression);
  testYear(Ids{Id::makeFromDouble(42.0)}, Ids{U});
  testYear(Ids{Id::makeFromBool(false)}, Ids{U});
  testYear(IdOrStrings{"noDate"}, Ids{U});
}

// Test `StrlenExpression` and `StrExpression`.
auto checkStrlen = std::bind_front(testUnaryExpression, &makeStrlenExpression);
auto checkStr = std::bind_front(testUnaryExpression, &makeStrExpression);
static auto makeStrlenWithStr = [](auto arg) {
  return makeStrlenExpression(makeStrExpression(std::move(arg)));
};
auto checkStrlenWithStrChild =
    std::bind_front(testUnaryExpression, makeStrlenWithStr);
TEST(SparqlExpression, stringOperators) {
  checkStrlen(IdOrStrings{"one", "two", "three", ""},
              Ids{I(3), I(3), I(5), I(0)});
  checkStrlenWithStrChild(IdOrStrings{"one", "two", "three", ""},
                          Ids{I(3), I(3), I(5), I(0)});

  // Test the different (optimized) behavior depending on whether the STR()
  // function was applied to the argument.
  checkStrlen(IdOrStrings{"one", I(1), D(3.6), ""}, Ids{I(3), U, U, I(0)});
  checkStrlenWithStrChild(IdOrStrings{"one", I(1), D(3.6), ""},
                          Ids{I(3), I(1), I(3), I(0)});
  checkStr(Ids{I(1), I(2), I(3)}, IdOrStrings{"1", "2", "3"});
  checkStr(Ids{D(-1.0), D(1.0), D(2.34)}, IdOrStrings{"-1", "1", "2.34"});
  checkStr(Ids{B(true), B(false), B(true)},
           IdOrStrings{"true", "false", "true"});
  checkStr(IdOrStrings{"one", "two", "three"},
           IdOrStrings{"one", "two", "three"});

  // A simple test for uniqueness of the cache key.
  auto c1a = makeStrlenExpression(std::make_unique<IriExpression>("<bim>"))
                 ->getCacheKey({});
  auto c1b = makeStrlenExpression(std::make_unique<IriExpression>("<bim>"))
                 ->getCacheKey({});
  auto c2a = makeStrExpression(std::make_unique<IriExpression>("<bim>"))
                 ->getCacheKey({});
  auto c2b = makeStrExpression(std::make_unique<IriExpression>("<bim>"))
                 ->getCacheKey({});
  auto c3a = makeStrlenExpression(
                 makeStrExpression(std::make_unique<IriExpression>("<bim>")))
                 ->getCacheKey({});
  auto c3b = makeStrlenExpression(
                 makeStrExpression(std::make_unique<IriExpression>("<bim>")))
                 ->getCacheKey({});

  EXPECT_EQ(c1a, c1b);
  EXPECT_EQ(c2a, c2b);
  EXPECT_EQ(c3a, c3b);

  EXPECT_NE(c1a, c2a);
  EXPECT_NE(c2a, c3a);
  EXPECT_NE(c1a, c3a);
}

// _____________________________________________________________________________________
TEST(SparqlExpression, unaryNegate) {
  auto checkNegate =
      std::bind_front(testUnaryExpression, &makeUnaryNegateExpression);
  // Zero and NaN are considered to be false, so their negation is true

  checkNegate(
      Ids{B(true), B(false), I(0), I(3), D(0), D(12), D(naN), U},
      Ids{B(false), B(true), B(true), B(false), B(true), B(false), B(true), U});
  // Empty strings are considered to be true.
  checkNegate(IdOrStrings{"true", "false", "", "blibb"},
              Ids{B(false), B(false), B(true), B(false)});
}

// _____________________________________________________________________________________
TEST(SparqlExpression, unaryMinus) {
  auto checkMinus =
      std::bind_front(testUnaryExpression, &makeUnaryMinusExpression);
  // Zero and NaN are considered to be false, so their negation is true
  checkMinus(
      Ids{B(true), B(false), I(0), I(3), D(0), D(12.8), D(naN), U, Voc(6)},
      Ids{I(-1), I(0), I(0), I(-3), D(-0.0), D(-12.8), D(-naN), U, U});
  checkMinus(IdOrStrings{"true", "false", "", "<blibb>"}, Ids{U, U, U, U});
}

TEST(SparqlExpression, ceilFloorAbsRound) {
  auto bindUnary = [](auto f) {
    return std::bind_front(testUnaryExpression, f);
  };
  auto checkFloor = bindUnary(&makeFloorExpression);
  auto checkAbs = bindUnary(&makeAbsExpression);
  auto checkRound = bindUnary(&makeRoundExpression);
  auto checkCeil = bindUnary(&makeCeilExpression);

  std::vector<Id> input{B(true),  B(false), I(-3),   I(0),   I(3),
                        D(-13.6), D(-0.5),  D(-0.0), D(0.0), D(0.5),
                        D(1.8),   Voc(6),   U};
  std::vector<Id> abs{I(1),   I(0),   I(3),   I(0),   I(3), D(13.6), D(0.5),
                      D(0.0), D(0.0), D(0.5), D(1.8), U,    U};
  std::vector<Id> floor{I(1),     I(0),    I(-3),   I(0),   I(3),
                        D(-14.0), D(-1.0), D(-0.0), D(0.0), D(0.0),
                        D(1.0),   U,       U};
  std::vector<Id> ceil{I(1),    I(0),   I(-3),  I(0),   I(3), D(-13.0), D(-0.0),
                       D(-0.0), D(0.0), D(1.0), D(2.0), U,    U};
  std::vector<Id> round{I(1),     I(0),    I(-3),   I(0),   I(3),
                        D(-14.0), D(-0.0), D(-0.0), D(0.0), D(1.0),
                        D(2.0),   U,       U};

  checkAbs(input, abs);
  checkFloor(input, floor);
  checkCeil(input, ceil);
  checkRound(input, round);
}

// ________________________________________________________________________________________
TEST(SparqlExpression, geoSparqlExpressions) {
  auto checkLat = std::bind_front(testUnaryExpression, &makeLatitudeExpression);
  auto checkLong =
      std::bind_front(testUnaryExpression, &makeLongitudeExpression);
  auto checkDist = std::bind_front(testNaryExpression, &makeDistExpression);

  checkLat(IdOrStrings{"POINT(24.3 26.8)", "NotAPoint", I(12)},
           Ids{D(26.8), U, U});
  checkLong(IdOrStrings{D(4.2), "POINT(24.3 26.8)", "NotAPoint"},
            Ids{U, D(24.3), U});
  checkDist(D(0.0), IdOrString{"POINT(24.3 26.8)"},
            IdOrString{"POINT(24.3 26.8)"});
  checkDist(U, IdOrString{"POINT(24.3 26.8)"}, IdOrString{I(12)});
  checkDist(U, IdOrString{I(12)}, IdOrString{"POINT(24.3 26.8)"});
  checkDist(U, IdOrString{"POINT(24.3 26.8)"s}, IdOrString{"NotAPoint"});
  checkDist(U, IdOrString{"NotAPoint"}, IdOrString{"POINT(24.3 26.8)"});
}

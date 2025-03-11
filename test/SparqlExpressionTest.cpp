// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <engine/sparqlExpressions/ExistsExpression.h>
#include <gtest/gtest.h>

#include <string>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/CountStarExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/StdevExpression.h"
#include "parser/GeoPoint.h"
#include "util/AllocatorTestHelpers.h"
#include "util/Conversions.h"

namespace {

using ad_utility::source_location;
// Some useful shortcuts for the tests below.
using namespace sparqlExpression;
using namespace std::literals;
using ad_utility::testing::iri;
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
using IdOrLiteralOrIriVec = std::vector<IdOrLiteralOrIri>;

auto lit = [](std::string_view s, std::string_view langtagOrDatatype = "") {
  return ad_utility::triple_component::LiteralOrIri(
      ad_utility::testing::tripleComponentLiteral(s, langtagOrDatatype));
};

auto iriref = [](std::string_view s) {
  return ad_utility::triple_component::LiteralOrIri(iri(s));
};

auto idOrLitOrStringVec =
    [](const std::vector<std::variant<Id, std::string>>& input) {
      IdOrLiteralOrIriVec result;
      for (const auto& el : input) {
        if (std::holds_alternative<Id>(el)) {
          result.push_back(std::get<Id>(el));
        } else {
          result.push_back(lit(std::get<std::string>(el)));
        }
      }
      return result;
    };

// All the helper functions `testUnaryExpression` etc. below internally evaluate
// the given expressions using the `TestContext` class, so it is possible to use
// IDs from the global and local vocab of this class to test expressions. For
// example `testContext().x` is the ID of the entity `<x>` in the vocabulary of
// the index on which the expressions will be evaluated. For details see the
// `TestContext` class. Note: The indirection via a function is necessary
// because otherwise the `gtest_discover_test` step of the CMake build fails for
// some reason.
const auto& testContext() {
  static TestContext ctx{};
  return ctx;
}

// Test allocator (the inputs to our `SparqlExpression`s are
// `VectorWithMemoryLimit`s, and these require an `AllocatorWithLimit`).
//
using ad_utility::AllocatorWithLimit;
AllocatorWithLimit<Id> alloc = ad_utility::testing::makeAllocator();

// A concept for a type that is either a `SingleExpressionResult`, a
// `std::vector<T>` where `T` is a `SingleExpressionResult` or something that
// can be converted to a `string_view`. All those types can be used as input to
// the test helper functions below and are converted to `ExpressionResult`s by
// the test machinery.
template <typename T>
concept VectorOrExpressionResult =
    SingleExpressionResult<T> ||
    (ad_utility::isVector<T> && isConstantResult<typename T::value_type>) ||
    std::convertible_to<T, std::string_view>;

// Convert a `VectorOrExpressionResult` (see above) to a type that is supported
// by the expression module.
CPP_template(typename T)(
    requires VectorOrExpressionResult<T>) auto toExpressionResult(T vec) {
  if constexpr (SingleExpressionResult<T>) {
    return vec;
  } else if constexpr (ranges::convertible_to<T, std::string_view>) {
    // TODO<joka921> Make a generic testing utility for the string case...
    return IdOrLiteralOrIri{lit(vec)};
  } else {
    return VectorWithMemoryLimit<typename T::value_type>{
        std::make_move_iterator(vec.begin()),
        std::make_move_iterator(vec.end()), alloc};
  }
}

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

// Return a matcher that matches a result of an expression that is not a vector.
// If it is an ID (possibly contained in the `IdOrLiteralOrIri` variant), then
// the `matchId` matcher from above is used, else we test for equality.
template <typename T>
requires(!isVectorResult<T>) auto nonVectorResultMatcher(const T& expected) {
  if constexpr (std::is_same_v<T, Id>) {
    return matchId(expected);
  } else if constexpr (std::is_same_v<T, IdOrLiteralOrIri>) {
    return std::visit(
        []<typename U>(const U& el) {
          return ::testing::MatcherCast<const IdOrLiteralOrIri&>(
              ::testing::VariantWith<U>(nonVectorResultMatcher(el)));
        },
        expected);
  } else {
    return ::testing::Eq(expected);
  }
}

// Return a matcher, that matches the `expected` expression result. The case of
// `expected` being a single element or a vector and the case of `expected`
// storing `Ids` which have to be handled using the `matchId` matcher (see
// above) are all handled correctly.
auto sparqlExpressionResultMatcher =
    []<typename Expected>(const Expected& expected) {
      CPP_assert(SingleExpressionResult<Expected>);
      if constexpr (isVectorResult<Expected>) {
        auto matcherVec =
            ad_utility::transform(expected, [](const auto& singleExpected) {
              return nonVectorResultMatcher(singleExpected);
            });
        return ::testing::ElementsAreArray(matcherVec);
      } else {
        return nonVectorResultMatcher(expected);
      }
    };

// A generic function that can copy all kinds of `SingleExpressionResult`s. Note
// that we have disabled the implicit copying of vector results, but for testing
// this is very useful.
auto clone = [](const auto& x) {
  if constexpr (requires { x.clone(); }) {
    return x.clone();
  } else {
    return x;
  }
};

// The implementation of `testNaryExpression` directly below.
auto testNaryExpressionImpl = [](auto&& makeExpression, auto const& expected,
                                 auto const&... operands) {
  CPP_assert(SingleExpressionResult<decltype(expected)> &&
             (SingleExpressionResult<decltype(operands)> && ...));
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::testing::makeAllocator()};
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

  const auto resultSize = [&operands..., &getResultSize]() {
    if constexpr (sizeof...(operands) == 0) {
      (void)getResultSize;
      return 0ul;
    } else {
      return std::max({getResultSize(operands)...});
    }
  }();

  TestContext outerContext;
  sparqlExpression::EvaluationContext& context = outerContext.context;
  context._endIndex = resultSize;

  std::array<SparqlExpression::Ptr, sizeof...(operands)> children{
      std::make_unique<SingleUseExpression>(
          ExpressionResult{clone(operands)})...};

  auto expressionPtr = std::apply(makeExpression, std::move(children));
  auto& expression = *expressionPtr;

  ExpressionResult result = expression.evaluate(&context);

  using ExpectedType = std::decay_t<decltype(expected)>;
  ASSERT_THAT(result, ::testing::VariantWith<ExpectedType>(
                          sparqlExpressionResultMatcher(expected)));
};

// Assert that the given `NaryExpression` with the given `operands` has the
// `expected` result.
auto testNaryExpression = [](auto&& makeExpression,
                             VectorOrExpressionResult auto const& expected,
                             VectorOrExpressionResult auto const&... operands) {
  return testNaryExpressionImpl(makeExpression,
                                toExpressionResult(clone(expected)),
                                toExpressionResult(clone(operands))...);
};

// Assert that the given commutative binary expression has the `expected` result
// in both orders of the operands `op1` and `op2`.
template <auto makeFunction>
auto testBinaryExpressionCommutative =
    [](const auto& expected, const auto& op1, const auto& op2,
       source_location l = source_location::current()) {
      CPP_assert(SingleExpressionResult<decltype(expected)> &&
                 SingleExpressionResult<decltype(op1)> &&
                 SingleExpressionResult<decltype(op2)>);
      auto t = generateLocationTrace(l);
      testNaryExpression(makeFunction, expected, op1, op2);
      testNaryExpression(makeFunction, expected, op2, op1);
    };

// Test an NARY expression, but the operands are passed in as a `std::tuple`.
// This allows us to get the info about the actual point of the test failure via
// the `source_location`. TODO<joka921> Rewrite all the tests to use this
// facility.
template <auto makeFunction>
auto testNaryExpressionVec =
    []<VectorOrExpressionResult Exp, VectorOrExpressionResult... Ops>(
        Exp expected, std::tuple<Ops...> ops,
        source_location l = source_location::current()) {
      auto t = generateLocationTrace(l, "testBinaryExpressionVec");

      std::apply(
          [&](auto&... args) {
            testNaryExpression(makeFunction, expected, args...);
          },
          ops);
    };

auto testOr = testBinaryExpressionCommutative<&makeOrExpression>;
auto testAnd = testBinaryExpressionCommutative<&makeAndExpression>;
auto testPlus = testBinaryExpressionCommutative<&makeAddExpression>;
auto testMultiply = testBinaryExpressionCommutative<&makeMultiplyExpression>;
auto testMinus = std::bind_front(testNaryExpression, &makeSubtractExpression);
auto testDivide = std::bind_front(testNaryExpression, &makeDivideExpression);

}  // namespace

// _____________________________________________________________________________________
TEST(SparqlExpression, logicalOperators) {
  // Test `AndExpression` and `OrExpression`.
  V<Id> b{{B(false), B(true), B(true), B(false)}, alloc};
  V<Id> d{{D(1.0), D(2.0), D(std::numeric_limits<double>::quiet_NaN()), D(0.0)},
          alloc};
  V<Id> dAsBool{{B(true), B(true), B(false), B(false)}, alloc};

  V<IdOrLiteralOrIri> s{{lit("true"), lit(""), lit("false"), lit("")}, alloc};
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

  testOr(allTrue, b, IdOrLiteralOrIri{lit("halo")});
  testOr(b, b, IdOrLiteralOrIri(lit("")));
  testAnd(allFalse, b, IdOrLiteralOrIri(lit("")));
  testAnd(b, b, IdOrLiteralOrIri(lit("yellow")));

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

// _____________________________________________________________________________________
TEST(SparqlExpression, arithmeticOperators) {
  // Test `AddExpression`, `SubtractExpression`, `MultiplyExpression`, and
  // `DivideExpression`.
  //
  // TODO: Also test `UnaryMinusExpression`.
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

// Test that the unary expression that is specified by the `makeFunction` yields
// the `expected` result when being given the `operand`.
template <auto makeFunction>
auto testUnaryExpression = [](VectorOrExpressionResult auto const& operand,
                              VectorOrExpressionResult auto const& expected,
                              source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l);
  testNaryExpression(makeFunction, expected, operand);
};

TEST(SparqlExpression, dateOperators) {
  // Test `YearExpression`, `MonthExpression`, `DayExpression`,
  //  `HoursExpression`, `MinutesExpression`, and `SecondsExpression`.
  // Helper function that asserts that the date operators give the expected
  // result on the given date.
  auto checkYear = testUnaryExpression<&makeYearExpression>;
  auto checkMonth = testUnaryExpression<&makeMonthExpression>;
  auto checkDay = testUnaryExpression<&makeDayExpression>;
  auto checkHours = testUnaryExpression<&makeHoursExpression>;
  auto checkMinutes = testUnaryExpression<&makeMinutesExpression>;
  auto checkSeconds = testUnaryExpression<&makeSecondsExpression>;
  auto check = [&checkYear, &checkMonth, &checkDay, &checkHours, &checkMinutes,
                &checkSeconds](
                   const DateYearOrDuration& date,
                   std::optional<int> expectedYear,
                   std::optional<int> expectedMonth = std::nullopt,
                   std::optional<int> expectedDay = std::nullopt,
                   std::optional<int> expectedHours = std::nullopt,
                   std::optional<int> expectedMinutes = std::nullopt,
                   std::optional<double> expectedSeconds = std::nullopt,
                   std::source_location l = std::source_location::current()) {
    auto trace = generateLocationTrace(l);
    auto optToIdInt = [](const auto& opt) {
      if (opt.has_value()) {
        return Id::makeFromInt(opt.value());
      } else {
        return Id::makeUndefined();
      }
    };
    auto optToIdDouble = [](const auto& opt) {
      if (opt.has_value()) {
        return Id::makeFromDouble(opt.value());
      } else {
        return Id::makeUndefined();
      }
    };
    checkYear(Ids{Id::makeFromDate(date)}, Ids{optToIdInt(expectedYear)});
    checkMonth(Ids{Id::makeFromDate(date)}, Ids{optToIdInt(expectedMonth)});
    checkDay(Ids{Id::makeFromDate(date)}, Ids{optToIdInt(expectedDay)});
    checkHours(Ids{Id::makeFromDate(date)}, Ids{optToIdInt(expectedHours)});
    checkMinutes(Ids{Id::makeFromDate(date)}, Ids{optToIdInt(expectedMinutes)});
    checkSeconds(Ids{Id::makeFromDate(date)},
                 Ids{optToIdDouble(expectedSeconds)});
  };

  using D = DateYearOrDuration;
  // Now the checks for dates with varying level of detail.
  check(D::parseXsdDatetime("1970-04-22T11:53:42.25"), 1970, 4, 22, 11, 53,
        42.25);
  check(D::parseXsdDate("1970-04-22"), 1970, 4, 22);
  check(D::parseXsdDate("1970-04-22"), 1970, 4, 22);
  check(D::parseXsdDate("0042-12-24"), 42, 12, 24);
  check(D::parseXsdDate("-0099-07-01"), -99, 7, 1);
  check(D::parseGYear("-1234"), -1234, std::nullopt, std::nullopt);
  check(D::parseXsdDate("0321-07-01"), 321, 7, 1);
  check(D::parseXsdDate("2321-07-01"), 2321, 7, 1);

  // Test behavior of the `largeYear` representation that doesn't store the
  // actual date.
  check(D::parseGYear("123456"), 123456);
  check(D::parseGYearMonth("-12345-01"), -12345, 1);
  check(D::parseGYearMonth("-12345-03"), -12345, 1);
  check(D::parseXsdDate("-12345-01-01"), -12345, 1, 1);
  check(D::parseXsdDate("-12345-03-04"), -12345, 1, 1);

  // Invalid inputs for date expressions.
  checkYear(Ids{Id::makeFromInt(42)}, Ids{Id::makeUndefined()});
  checkMonth(Ids{Id::makeFromInt(42)}, Ids{Id::makeUndefined()});
  checkDay(Ids{Id::makeFromInt(42)}, Ids{Id::makeUndefined()});
  checkHours(Ids{Id::makeFromInt(42)}, Ids{Id::makeUndefined()});
  checkMinutes(Ids{Id::makeFromInt(84)}, Ids{Id::makeUndefined()});
  checkSeconds(Ids{Id::makeFromDouble(120.0123)}, Ids{Id::makeUndefined()});
  auto testYear = testUnaryExpression<&makeYearExpression>;
  testYear(Ids{Id::makeFromDouble(42.0)}, Ids{U});
  testYear(Ids{Id::makeFromBool(false)}, Ids{U});
  testYear(IdOrLiteralOrIriVec{lit("noDate")}, Ids{U});

  // test makeTimezoneStrExpression / makeTimezoneExpression
  auto positive = DayTimeDuration::Type::Positive;
  auto negative = DayTimeDuration::Type::Negative;
  using Timezone = std::variant<Date::NoTimeZone, Date::TimeZoneZ, int>;
  auto checkStrTimezone = testUnaryExpression<&makeTimezoneStrExpression>;
  auto checkTimezone = testUnaryExpression<&makeTimezoneExpression>;
  Id U = Id::makeUndefined();
  Timezone tz = -5;
  auto d1 = DateYearOrDuration(Date(2011, 1, 10, 14, 45, 13.815, tz));
  auto duration1 = DateYearOrDuration(DayTimeDuration{negative, 0, 5, 0, 0.0});
  checkStrTimezone(Ids{Id::makeFromDate(d1)},
                   IdOrLiteralOrIriVec{lit("-05:00")});
  checkTimezone(Ids{Id::makeFromDate(d1)}, Ids{Id::makeFromDate(duration1)});
  tz = 23;
  auto d2 = DateYearOrDuration(Date(2011, 1, 10, 14, 45, 13.815, tz));
  auto duration2 = DateYearOrDuration(DayTimeDuration{positive, 0, 23, 0, 0.0});
  checkTimezone(Ids{Id::makeFromDate(d2)}, Ids{Id::makeFromDate(duration2)});
  checkStrTimezone(Ids{Id::makeFromDate(d2)},
                   IdOrLiteralOrIriVec{lit("+23:00")});
  tz = Date::TimeZoneZ{};
  auto d3 = DateYearOrDuration(Date(2011, 1, 10, 14, 45, 13.815, tz));
  auto duration3 = DateYearOrDuration(DayTimeDuration{});
  checkTimezone(Ids{Id::makeFromDate(d3)}, Ids{Id::makeFromDate(duration3)});
  checkStrTimezone(Ids{Id::makeFromDate(d3)}, IdOrLiteralOrIriVec{lit("Z")});
  tz = Date::NoTimeZone{};
  DateYearOrDuration d4 =
      DateYearOrDuration(Date(2011, 1, 10, 14, 45, 13.815, tz));
  checkStrTimezone(Ids{Id::makeFromDate(d4)}, IdOrLiteralOrIriVec{lit("")});
  checkTimezone(Ids{Id::makeFromDate(d4)}, Ids{U});
  DateYearOrDuration d5 = DateYearOrDuration(Date(2012, 1, 4, 14, 45));
  checkStrTimezone(Ids{Id::makeFromDate(d5)}, IdOrLiteralOrIriVec{lit("")});
  checkTimezone(Ids{Id::makeFromDate(d5)}, Ids{U});
  checkStrTimezone(IdOrLiteralOrIriVec{lit("2011-01-10T14:")},
                   IdOrLiteralOrIriVec{U});
  checkStrTimezone(Ids{Id::makeFromDouble(120.0123)}, IdOrLiteralOrIriVec{U});
  checkTimezone(Ids{Id::makeFromDouble(2.34)}, Ids{U});
  checkStrTimezone(Ids{Id::makeUndefined()}, IdOrLiteralOrIriVec{U});
  DateYearOrDuration d6 =
      DateYearOrDuration(-1394785, DateYearOrDuration::Type::Year);
  checkTimezone(Ids{Id::makeFromDate(d6)}, Ids{U});
  checkStrTimezone(Ids{Id::makeFromDate(d6)}, IdOrLiteralOrIriVec{lit("")});
  DateYearOrDuration d7 =
      DateYearOrDuration(10000, DateYearOrDuration::Type::Year);
  checkStrTimezone(Ids{Id::makeFromDate(d7)}, IdOrLiteralOrIriVec{lit("")});
  checkTimezone(Ids{Id::makeFromDate(d7)}, Ids{U});
}

// _____________________________________________________________________________________
namespace {
auto checkStrlen = testUnaryExpression<&makeStrlenExpression>;
auto checkStr = testUnaryExpression<&makeStrExpression>;
auto checkIriOrUri = testNaryExpressionVec<&makeIriOrUriExpression>;
auto makeStrlenWithStr = [](auto arg) {
  return makeStrlenExpression(makeStrExpression(std::move(arg)));
};
auto checkStrlenWithStrChild = testUnaryExpression<makeStrlenWithStr>;
}  // namespace
TEST(SparqlExpression, stringOperators) {
  // Test `StrlenExpression` and `StrExpression`.
  checkStrlen(
      IdOrLiteralOrIriVec{lit("one"), lit("two"), lit("three"), lit("")},
      Ids{I(3), I(3), I(5), I(0)});
  checkStrlenWithStrChild(
      IdOrLiteralOrIriVec{lit("one"), lit("two"), lit("three"), lit("")},
      Ids{I(3), I(3), I(5), I(0)});

  // Test the different (optimized) behavior depending on whether the STR()
  // function was applied to the argument.
  checkStrlen(
      IdOrLiteralOrIriVec{lit("one"), lit("tschüss"), I(1), D(3.6), lit("")},
      Ids{I(3), I(7), U, U, I(0)});
  checkStrlenWithStrChild(
      IdOrLiteralOrIriVec{lit("one"), I(1), D(3.6), lit("")},
      Ids{I(3), I(1), I(3), I(0)});
  checkStr(Ids{I(1), I(2), I(3)},
           IdOrLiteralOrIriVec{lit("1"), lit("2"), lit("3")});
  checkStr(Ids{D(-1.0), D(1.0), D(2.34)},
           IdOrLiteralOrIriVec{lit("-1"), lit("1"), lit("2.34")});
  checkStr(Ids{B(true), B(false), B(true)},
           IdOrLiteralOrIriVec{lit("true"), lit("false"), lit("true")});
  checkStr(IdOrLiteralOrIriVec{lit("one"), lit("two"), lit("three")},
           IdOrLiteralOrIriVec{lit("one"), lit("two"), lit("three")});

  auto T = Id::makeFromBool(true);
  auto F = Id::makeFromBool(false);
  auto dateDate =
      Id::makeFromDate(DateYearOrDuration::parseXsdDate("2025-01-01"));
  auto dateLYear = Id::makeFromDate(
      DateYearOrDuration(11853, DateYearOrDuration::Type::Year));
  // Test `iriOrUriExpression`.
  // test invalid
  checkIriOrUri(IdOrLiteralOrIriVec{U, U, U, U, U, U, U},
                std::tuple{IdOrLiteralOrIriVec{U, IntId(2), DoubleId(12.99),
                                               dateDate, dateLYear, T, F},
                           IdOrLiteralOrIri{LocalVocabEntry{
                               ad_utility::triple_component::Iri{}}}});
  // test valid
  checkIriOrUri(
      IdOrLiteralOrIriVec{
          iriref("<bimbim>"), iriref("<bambim>"),
          iriref("<https://www.bimbimbam/2001/bamString>"),
          iriref("<http://www.w3.\torg/2001/\nXMLSchema#\runsignedShort>"),
          iriref("<http://www.w3.org/2001/XMLSchema#string>"),
          iriref("<http://www.w3.org/2001/XMLSchema#string>"),
          iriref("<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>"),
          iriref("<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>"),
          iriref("<http://example/>"), iriref("<http://\t\t\nexample/>"),
          iriref("<\t\n\r>")},
      std::tuple{
          IdOrLiteralOrIriVec{
              lit("bimbim"), iriref("<bambim>"),
              lit("https://www.bimbimbam/2001/bamString"),
              lit("http://www.w3.\torg/2001/\nXMLSchema#\runsignedShort"),
              lit("http://www.w3.org/2001/XMLSchema#string"),
              iriref("<http://www.w3.org/2001/XMLSchema#string>"),
              testContext().notInVocabIri, testContext().notInVocabIriLit,
              lit("http://example/"), iriref("<http://\t\t\nexample/>"),
              lit("\t\n\r")},
          IdOrLiteralOrIri{
              LocalVocabEntry{ad_utility::triple_component::Iri{}}}});

  // test with base iri
  checkIriOrUri(
      IdOrLiteralOrIriVec{
          U,
          iriref("<http://example.com/hi/bimbim>"),
          iriref("<http://example.com/hi/bambim>"),
          iriref("<https://www.bimbimbam/2001/bamString>"),
          iriref("<http://example.com/hello>"),
          iriref("<http://example.com/hello>"),
      },
      std::tuple{
          IdOrLiteralOrIriVec{U, lit("bimbim"), iriref("<bambim>"),
                              lit("https://www.bimbimbam/2001/bamString"),
                              lit("/hello"), iriref("</hello>")},
          IdOrLiteralOrIri{iriref("<http://example.com/hi>")}});

  // A simple test for uniqueness of the cache key.
  auto c1a = makeStrlenExpression(std::make_unique<IriExpression>(iri("<bim>")))
                 ->getCacheKey({});
  auto c1b = makeStrlenExpression(std::make_unique<IriExpression>(iri("<bim>")))
                 ->getCacheKey({});
  auto c2a = makeStrExpression(std::make_unique<IriExpression>(iri("<bim>")))
                 ->getCacheKey({});
  auto c2b = makeStrExpression(std::make_unique<IriExpression>(iri("<bim>")))
                 ->getCacheKey({});
  auto c3a =
      makeStrlenExpression(
          makeStrExpression(std::make_unique<IriExpression>(iri("<bim>"))))
          ->getCacheKey({});
  auto c3b =
      makeStrlenExpression(
          makeStrExpression(std::make_unique<IriExpression>(iri("<bim>"))))
          ->getCacheKey({});

  EXPECT_EQ(c1a, c1b);
  EXPECT_EQ(c2a, c2b);
  EXPECT_EQ(c3a, c3b);

  EXPECT_NE(c1a, c2a);
  EXPECT_NE(c2a, c3a);
  EXPECT_NE(c1a, c3a);
}

// _____________________________________________________________________________________
auto checkUcase = testUnaryExpression<&makeUppercaseExpression>;
auto checkLcase = testUnaryExpression<&makeLowercaseExpression>;
TEST(SparqlExpression, uppercaseAndLowercase) {
  checkLcase(IdOrLiteralOrIriVec{lit("One"), lit("tWÖ"), U, I(12)},
             IdOrLiteralOrIriVec{lit("one"), lit("twö"), U, U});
  checkUcase(IdOrLiteralOrIriVec{lit("One"), lit("tWÖ"), U, I(12)},
             IdOrLiteralOrIriVec{lit("ONE"), lit("TWÖ"), U, U});
}

// _____________________________________________________________________________________
auto checkStrStarts =
    std::bind_front(testNaryExpression, &makeStrStartsExpression);
auto checkStrEnds = std::bind_front(testNaryExpression, &makeStrEndsExpression);
auto checkContains =
    std::bind_front(testNaryExpression, &makeContainsExpression);
auto checkStrAfter =
    std::bind_front(testNaryExpression, &makeStrAfterExpression);
auto checkStrBefore =
    std::bind_front(testNaryExpression, &makeStrBeforeExpression);
TEST(SparqlExpression, binaryStringOperations) {
  // Test STRSTARTS, STRENDS, CONTAINS, STRBEFORE, and STRAFTER.
  auto F = Id::makeFromBool(false);
  auto T = Id::makeFromBool(true);
  auto S = [](const std::vector<std::variant<Id, std::string>>& input) {
    IdOrLiteralOrIriVec result;
    for (const auto& el : input) {
      if (std::holds_alternative<Id>(el)) {
        result.push_back(std::get<Id>(el));
      } else {
        result.push_back(lit(std::get<std::string>(el)));
      }
    }
    return result;
  };
  checkStrStarts(
      Ids{T, F, T, F, T, T, F, F, F},
      S({"", "", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo",
         "Hällo"}),
      S({"", "x", "", "Hallo", "Häl", "Hällo", "Hällox", "ll", "lo"}));
  checkStrEnds(Ids{T, F, T, F, T, T, F, F, F},
               S({"", "", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo",
                  "Hällo"}),
               S({"", "x", "", "Hallo", "o", "Hällo", "Hällox", "ll", "H"}));
  checkContains(Ids{T, F, T, F, T, T, F},
                S({"", "", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo"}),
                S({"", "x", "", "ullo", "ll", "Hällo", "Hällox"}));
  checkStrAfter(
      S({"", "", "Hällo", "", "o", "", "", "lo"}),
      S({"", "", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo"}),
      S({"", "x", "", "ullo", "ll", "Hällo", "Hällox", "l"}));
  checkStrBefore(
      S({"", "", "Hällo", "", "Hä", "", "", "Hä"}),
      S({"", "", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo"}),
      S({"", "x", "", "ullo", "ll", "Hällo", "Hällox", "l"}));
}

// ______________________________________________________________________________
static auto checkSubstr =
    std::bind_front(testNaryExpression, makeSubstrExpression);
TEST(SparqlExpression, substr) {
  auto strs = [](const std::vector<std::variant<Id, std::string>>& input) {
    VectorWithMemoryLimit<IdOrLiteralOrIri> result(alloc);
    for (const auto& el : input) {
      if (std::holds_alternative<Id>(el)) {
        result.push_back(std::get<Id>(el));
      } else {
        result.push_back(lit(std::get<std::string>(el)));
      }
    }
    return result;
  };

  // Remember: The start position (the second argument to the SUBSTR expression)
  // is 1-based.
  checkSubstr(strs({"one", "two", "three"}), strs({"one", "two", "three"}),
              I(1), I(12));
  checkSubstr(strs({"one", "two", "three"}), strs({"one", "two", "three"}),
              I(0), I(12));
  checkSubstr(strs({"one", "two", "three"}), strs({"one", "two", "three"}),
              I(-2), I(12));

  checkSubstr(strs({"ne", "wo", "hree"}), strs({"one", "two", "three"}), I(2),
              I(12));
  checkSubstr(strs({"ne", "wo", "hree"}), strs({"one", "two", "three"}), D(1.8),
              D(11.7));
  checkSubstr(strs({"ne", "wo", "hree"}), strs({"one", "two", "three"}),
              D(2.449), D(12.449));

  // An actual substring from the middle
  checkSubstr(strs({"es", "os", "re"}), strs({"ones", "twos", "threes"}), I(3),
              I(2));

  // Subtle corner case if the starting position is negative
  // Only the letters at positions  `p < -3 + 6 = 3` are exported (the first two
  // letters, remember that the positions are 1-based).
  checkSubstr(strs({"on", "tw", "th"}), strs({"ones", "twos", "threes"}), I(-3),
              I(6));

  // Correct handling of UTF-8 multibyte characters.
  checkSubstr(strs({"pfel", "pfel", "pfel"}),
              strs({"uApfel", "uÄpfel", "uöpfel"}), I(3), I(18));

  // corner cases: 0 or negative length, or invalid numeric parameter
  checkSubstr(strs({"", "", ""}), strs({"ones", "twos", "threes"}), D(naN),
              I(2));
  checkSubstr(strs({"", "", ""}), strs({"ones", "twos", "threes"}), I(2),
              D(naN));
  checkSubstr(strs({"", "", ""}), strs({"ones", "twos", "threes"}), I(2), I(0));
  checkSubstr(strs({"", "", ""}), strs({"ones", "twos", "threes"}), I(2),
              D(-3.8));

  // Invalid datatypes
  // First must be string.
  auto Ux = IdOrLiteralOrIri{U};
  checkSubstr(Ux, I(3), I(4), I(7));
  checkSubstr(Ux, U, I(4), I(7));
  checkSubstr(Ux, Ux, I(4), I(7));
  // Second and third must be numeric;
  checkSubstr(Ux, IdOrLiteralOrIri{lit("hello")}, U, I(4));
  checkSubstr(Ux, IdOrLiteralOrIri{lit("hello")}, IdOrLiteralOrIri{lit("bye")},
              I(17));
  checkSubstr(Ux, IdOrLiteralOrIri{lit("hello")}, I(4), U);
  checkSubstr(Ux, IdOrLiteralOrIri{lit("hello")}, I(4),
              IdOrLiteralOrIri{lit("bye")});
}

// _____________________________________________________________________________________
TEST(SparqlExpression, strIriDtTagged) {
  auto U = Id::makeUndefined();
  auto checkStrIriTag =
      std::bind_front(testNaryExpression, &makeStrIriDtExpression);
  checkStrIriTag(IdOrLiteralOrIriVec{lit(
                     "123", "^^<http://www.w3.org/2001/XMLSchema#integer>")},
                 IdOrLiteralOrIriVec{lit("123")},
                 IdOrLiteralOrIriVec{
                     iriref("<http://www.w3.org/2001/XMLSchema#integer>")});
  checkStrIriTag(
      IdOrLiteralOrIriVec{lit("iiii", "^^<http://example/romanNumeral>")},
      IdOrLiteralOrIriVec{lit("iiii")},
      IdOrLiteralOrIriVec{iriref("<http://example/romanNumeral>")});
  checkStrIriTag(IdOrLiteralOrIriVec{U},
                 IdOrLiteralOrIriVec{iriref("<http://example/romanNumeral>")},
                 IdOrLiteralOrIriVec{U});
  checkStrIriTag(IdOrLiteralOrIriVec{U}, IdOrLiteralOrIriVec{lit("iiii")},
                 IdOrLiteralOrIriVec{U});
  checkStrIriTag(IdOrLiteralOrIriVec{U}, IdOrLiteralOrIriVec{lit("XVII")},
                 IdOrLiteralOrIriVec{lit("<not/a/iriref>")});
}

// _____________________________________________________________________________________
TEST(SparqlExpression, strLangTagged) {
  auto U = Id::makeUndefined();
  auto checkStrTag =
      std::bind_front(testNaryExpression, &makeStrLangTagExpression);
  checkStrTag(IdOrLiteralOrIriVec{lit("chat", "@en")},
              IdOrLiteralOrIriVec{lit("chat")}, IdOrLiteralOrIriVec{lit("en")});
  checkStrTag(IdOrLiteralOrIriVec{lit("chat", "@en-US")},
              IdOrLiteralOrIriVec{lit("chat")},
              IdOrLiteralOrIriVec{lit("en-US")});
  checkStrTag(IdOrLiteralOrIriVec{lit("Sprachnachricht", "@de-Latn-de")},
              IdOrLiteralOrIriVec{lit("Sprachnachricht")},
              IdOrLiteralOrIriVec{lit("de-Latn-de")});
  checkStrTag(IdOrLiteralOrIriVec{U}, IdOrLiteralOrIriVec{lit("chat")},
              IdOrLiteralOrIriVec{lit("d1235")});
  checkStrTag(IdOrLiteralOrIriVec{U}, IdOrLiteralOrIriVec{lit("reporter")},
              IdOrLiteralOrIriVec{lit("@")});
  checkStrTag(IdOrLiteralOrIriVec{U}, IdOrLiteralOrIriVec{lit("chat")},
              IdOrLiteralOrIriVec{lit("")});
  checkStrTag(IdOrLiteralOrIriVec{U}, IdOrLiteralOrIriVec{U},
              IdOrLiteralOrIriVec{lit("d")});
  checkStrTag(IdOrLiteralOrIriVec{U}, IdOrLiteralOrIriVec{U},
              IdOrLiteralOrIriVec{U});
}

// _____________________________________________________________________________________
TEST(SparqlExpression, unaryNegate) {
  auto checkNegate = testUnaryExpression<&makeUnaryNegateExpression>;
  // Zero and NaN are considered to be false, so their negation is true

  checkNegate(
      Ids{B(true), B(false), I(0), I(3), D(0), D(12), D(naN), U},
      Ids{B(false), B(true), B(true), B(false), B(true), B(false), B(true), U});
  // Empty strings are considered to be true.
  checkNegate(idOrLitOrStringVec({"true", "false", "", "blibb"}),
              Ids{B(false), B(false), B(true), B(false)});
}

// _____________________________________________________________________________________
TEST(SparqlExpression, unaryMinus) {
  auto checkMinus = testUnaryExpression<&makeUnaryMinusExpression>;
  // Zero and NaN are considered to be false, so their negation is true
  checkMinus(
      Ids{B(true), B(false), I(0), I(3), D(0), D(12.8), D(naN), U, Voc(6)},
      Ids{I(-1), I(0), I(0), I(-3), D(-0.0), D(-12.8), D(-naN), U, U});
  checkMinus(idOrLitOrStringVec({"true", "false", "", "<blibb>"}),
             Ids{U, U, U, U});
}

// _____________________________________________________________________________________
TEST(SparqlExpression, builtInNumericFunctions) {
  // Test the built-in numeric functions (floor, abs, round, ceil).
  auto checkFloor = testUnaryExpression<&makeFloorExpression>;
  auto checkAbs = testUnaryExpression<&makeAbsExpression>;
  ;
  auto checkRound = testUnaryExpression<&makeRoundExpression>;
  auto checkCeil = testUnaryExpression<&makeCeilExpression>;

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

// ___________________________________________________________________________
TEST(SparqlExpression, customNumericFunctions) {
  // Test the correctness of the math functions.
  testUnaryExpression<makeLogExpression>(
      std::vector<Id>{I(1), D(2), D(exp(1)), D(0)},
      std::vector<Id>{D(0), D(log(2)), D(1), D(-inf)});
  testUnaryExpression<makeExpExpression>(
      std::vector<Id>{I(0), D(1), D(2), D(-inf)},
      std::vector<Id>{D(1), D(exp(1)), D(exp(2)), D(0)});
  testUnaryExpression<makeSqrtExpression>(
      std::vector<Id>{I(0), D(1), D(2), D(-1)},
      std::vector<Id>{D(0), D(1), D(sqrt(2)), D(naN)});
  testUnaryExpression<makeSinExpression>(
      std::vector<Id>{I(0), D(1), D(2), D(-1)},
      std::vector<Id>{D(0), D(sin(1)), D(sin(2)), D(sin(-1))});
  testUnaryExpression<makeCosExpression>(
      std::vector<Id>{I(0), D(1), D(2), D(-1)},
      std::vector<Id>{D(1), D(cos(1)), D(cos(2)), D(cos(-1))});
  testUnaryExpression<makeTanExpression>(
      std::vector<Id>{I(0), D(1), D(2), D(-1)},
      std::vector<Id>{D(0), D(tan(1)), D(tan(2)), D(tan(-1))});
  auto checkPow = std::bind_front(testNaryExpression, &makePowExpression);
  checkPow(Ids{D(1), D(32), U, U}, Ids{I(5), D(2), U, D(0)},
           IdOrLiteralOrIriVec{I(0), D(5), I(0), lit("abc")});
}

// ____________________________________________________________________________
TEST(SparqlExpression, isSomethingFunctions) {
  Id T = Id::makeFromBool(true);
  Id F = Id::makeFromBool(false);
  Id iri = testContext().x;
  Id literal = testContext().zz;
  Id blank = testContext().blank;
  Id blank2 = Id::makeFromBlankNodeIndex(BlankNodeIndex::make(12));
  Id localLiteral = testContext().notInVocabA;

  IdOrLiteralOrIriVec testIdOrStrings =
      IdOrLiteralOrIriVec{iriref("<i>"), lit("\"l\""), blank2, iri,  literal,
                          blank,         localLiteral, I(42),  D(1), U};
  testUnaryExpression<makeIsIriExpression>(testIdOrStrings,
                                           Ids{T, F, F, T, F, F, F, F, F, F});
  testUnaryExpression<makeIsBlankExpression>(testIdOrStrings,
                                             Ids{F, F, T, F, F, T, F, F, F, F});
  testUnaryExpression<makeIsLiteralExpression>(
      testIdOrStrings, Ids{F, T, F, F, T, F, T, F, F, F});
  testUnaryExpression<makeIsNumericExpression>(
      testIdOrStrings, Ids{F, F, F, F, F, F, F, T, T, F});
  testUnaryExpression<makeBoundExpression>(testIdOrStrings,
                                           Ids{T, T, T, T, T, T, T, T, T, F});
}

// ____________________________________________________________________________
TEST(SparqlExpression, DatatypeExpression) {
  U = Id::makeUndefined();
  auto d1 = DateYearOrDuration::parseXsdDatetime("1900-12-13T03:12:00.33Z");
  auto d2 = DateYearOrDuration::parseGYear("-10000");
  auto d3 = DateYearOrDuration::parseGYear("1900");
  auto d4 = DateYearOrDuration::parseXsdDate("2024-06-13");
  auto d5 = DateYearOrDuration::parseGYearMonth("2024-06");
  auto g1 = GeoPoint(50.0, 50.0);
  Id DateId1 = Id::makeFromDate(d1);
  Id DateId2 = Id::makeFromDate(d2);
  Id DateId3 = Id::makeFromDate(d3);
  Id DateId4 = Id::makeFromDate(d4);
  Id DateId5 = Id::makeFromDate(d5);
  Id GeoId1 = Id::makeFromGeoPoint(g1);
  auto checkGetDatatype = testUnaryExpression<&makeDatatypeExpression>;
  checkGetDatatype(IdOrLiteralOrIriVec{testContext().x},
                   IdOrLiteralOrIriVec{U});
  checkGetDatatype(
      IdOrLiteralOrIriVec{testContext().alpha},
      IdOrLiteralOrIriVec{iriref("<http://www.w3.org/2001/XMLSchema#string>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{testContext().zz},
      IdOrLiteralOrIriVec{
          iriref("<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{testContext().notInVocabB},
      IdOrLiteralOrIriVec{iriref("<http://www.w3.org/2001/XMLSchema#string>")});
  checkGetDatatype(IdOrLiteralOrIriVec{testContext().notInVocabD},
                   IdOrLiteralOrIriVec{U});
  checkGetDatatype(IdOrLiteralOrIriVec{lit(
                       "123", "^^<http://www.w3.org/2001/XMLSchema#integer>")},
                   IdOrLiteralOrIriVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#integer>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{lit("Simple StrStr")},
      IdOrLiteralOrIriVec{iriref("<http://www.w3.org/2001/XMLSchema#string>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{lit("english", "@en")},
      IdOrLiteralOrIriVec{
          iriref("<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>")});
  checkGetDatatype(IdOrLiteralOrIriVec{U}, IdOrLiteralOrIriVec{U});
  checkGetDatatype(IdOrLiteralOrIriVec{DateId1},
                   IdOrLiteralOrIriVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#dateTime>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{DateId2},
      IdOrLiteralOrIriVec{iriref("<http://www.w3.org/2001/XMLSchema#gYear>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{DateId3},
      IdOrLiteralOrIriVec{iriref("<http://www.w3.org/2001/XMLSchema#gYear>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{DateId4},
      IdOrLiteralOrIriVec{iriref("<http://www.w3.org/2001/XMLSchema#date>")});
  checkGetDatatype(IdOrLiteralOrIriVec{DateId5},
                   IdOrLiteralOrIriVec{iriref(
                       "<http://www.w3.org/2001/XMLSchema#gYearMonth>")});
  checkGetDatatype(IdOrLiteralOrIriVec{GeoId1},
                   IdOrLiteralOrIriVec{iriref(
                       "<http://www.opengis.net/ont/geosparql#wktLiteral>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{Id::makeFromInt(212378233)},
      IdOrLiteralOrIriVec{iriref("<http://www.w3.org/2001/XMLSchema#int>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{Id::makeFromDouble(2.3475)},
      IdOrLiteralOrIriVec{iriref("<http://www.w3.org/2001/XMLSchema#double>")});
  checkGetDatatype(IdOrLiteralOrIriVec{Id::makeFromBool(false)},
                   IdOrLiteralOrIriVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#boolean>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{Id::makeFromInt(true)},
      IdOrLiteralOrIriVec{iriref("<http://www.w3.org/2001/XMLSchema#int>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{lit("")},
      IdOrLiteralOrIriVec{iriref("<http://www.w3.org/2001/XMLSchema#string>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{lit(" ", "@de-LATN-de")},
      IdOrLiteralOrIriVec{
          iriref("<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>")});
  checkGetDatatype(
      IdOrLiteralOrIriVec{lit("testval", "^^<http://example/iri/test#test>")},
      IdOrLiteralOrIriVec{iriref("<http://example/iri/test#test>")});
  checkGetDatatype(IdOrLiteralOrIriVec{iriref("<http://example/iri/test>")},
                   IdOrLiteralOrIriVec{U});

  // test corner case DatatypeValueGetter
  TestContext ctx;
  AD_EXPECT_THROW_WITH_MESSAGE(
      sparqlExpression::detail::DatatypeValueGetter{}(Id::max(), &ctx.context),
      ::testing::ContainsRegex("should be unreachable"));
}

// ____________________________________________________________________________
TEST(SparqlExpression, testStrToHashExpressions) {
  auto checkGetMD5Expression = testUnaryExpression<&makeMD5Expression>;
  auto checkGetSHA1Expression = testUnaryExpression<&makeSHA1Expression>;
  auto checkGetSHA256Expression = testUnaryExpression<&makeSHA256Expression>;
  auto checkGetSHA384Expression = testUnaryExpression<&makeSHA384Expression>;
  auto checkGetSHA512Expression = testUnaryExpression<&makeSHA512Expression>;
  std::string testStr1 = "";
  std::string testStr2 = "Friburg23o";
  std::string testStr3 = "abc";
  checkGetMD5Expression(
      idOrLitOrStringVec({U, testStr1, testStr2, testStr3}),
      idOrLitOrStringVec({U, "d41d8cd98f00b204e9800998ecf8427e",
                          "9d9a73f67e20835e516029541595c381",
                          "900150983cd24fb0d6963f7d28e17f72"}));
  checkGetSHA1Expression(
      idOrLitOrStringVec({U, testStr1, testStr2, testStr3}),
      idOrLitOrStringVec({U, "da39a3ee5e6b4b0d3255bfef95601890afd80709",
                          "c3a77a6104fa091f590f594b3e2dba2668196d3c",
                          "a9993e364706816aba3e25717850c26c9cd0d89d"}));
  checkGetSHA256Expression(
      idOrLitOrStringVec({U, testStr1, testStr2, testStr3}),
      idOrLitOrStringVec(
          {U,
           "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
           "af8d98f09845a700aea36b35e8cc3a35632e38d0f7be9c0ca508e53c578da900",
           "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015a"
           "d"}));
  checkGetSHA384Expression(
      idOrLitOrStringVec({U, testStr1, testStr2, testStr3}),
      idOrLitOrStringVec({U,
                          "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0c"
                          "c7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b",
                          "72810006e3b418ebd179812522cafa486cd6c2a988378fac148a"
                          "f1a9a098a01ce3373734c23978f7df68bf7e98955c02",
                          "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b"
                          "605a43ff5bed8086072ba1e7cc2358baeca134c825a7"}));
  checkGetSHA512Expression(
      idOrLitOrStringVec({U, testStr1, testStr2, testStr3}),
      idOrLitOrStringVec(
          {U,
           "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d"
           "0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
           "be4422bfad59ee51e98dc51c540dc9d85333cb786333b152d13b2bebde1bdaa499e"
           "9d4e1370a5bb2e831f4443b1358f2301fd5214ba80554ea0ff1d185c3b027",
           "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a219"
           "2992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"}));
}

// ____________________________________________________________________________
TEST(SparqlExpression, testToNumericExpression) {
  Id T = Id::makeFromBool(true);
  Id F = Id::makeFromBool(false);
  Id G = Id::makeFromGeoPoint(GeoPoint(50.0, 50.0));
  auto checkGetInt = testUnaryExpression<&makeConvertToIntExpression>;
  auto checkGetDouble = testUnaryExpression<&makeConvertToDoubleExpression>;
  auto chekGetDecimal = testUnaryExpression<&makeConvertToDecimalExpression>;

  checkGetInt(
      idOrLitOrStringVec({U, "  -1275", "5.97", "-78.97", "-5BoB6", "FreBurg1",
                          "", " .", " 42\n", " 0.01 ", "", "@", "@?+1", "1", G,
                          "+42"}),
      Ids{U, I(-1275), U, U, U, U, U, U, I(42), U, U, U, U, I(1), U, I(42)});
  checkGetDouble(
      idOrLitOrStringVec({U, "-122.2", "19,96", " 128789334.345 ", "-0.f",
                          "  0.007 ", " -14.75 ", "Q", "@!+?", "1", G, "+42.0",
                          " +1E-2", "1e3 ", "1.3E1"}),
      Ids{U, D(-122.2), U, D(128789334.345), U, D(0.007), D(-14.75), U, U,
          D(1.00), U, D(42), D(0.01), D(1000), D(13)});
  chekGetDecimal(
      idOrLitOrStringVec({U, "-122.2", "19,96", " 128789334.345 ", "-0.f",
                          "  0.007 ", " -14.75 ", "Q", "@!+?", "1", G, "+42.0",
                          " +1E-2", "1e3 ", "1.3E1"}),
      Ids{U, D(-122.2), U, D(128789334.345), U, D(0.007), D(-14.75), U, U,
          D(1.00), U, D(42), U, U, U});
  checkGetInt(idOrLitOrStringVec(
                  {U, I(-12475), I(42), I(0), D(-14.57), D(33.0), D(0.00001)}),
              Ids{U, I(-12475), I(42), I(0), I(-14), I(33), I(0)});
  checkGetDouble(
      idOrLitOrStringVec(
          {U, I(-12475), I(42), I(0), D(-14.57), D(33.0), D(0.00001)}),
      Ids{U, D(-12475.00), D(42.00), D(0.00), D(-14.57), D(33.00), D(0.00001)});
  chekGetDecimal(
      idOrLitOrStringVec(
          {U, I(-12475), I(42), I(0), D(-14.57), D(33.0), D(0.00001)}),
      Ids{U, D(-12475.00), D(42.00), D(0.00), D(-14.57), D(33.00), D(0.00001)});
  checkGetDouble(IdOrLiteralOrIriVec{lit("."), lit("-12.745"), T, F,
                                     lit("0.003"), lit("1")},
                 Ids{U, D(-12.745), D(1.00), D(0.00), D(0.003), D(1.00)});
  chekGetDecimal(IdOrLiteralOrIriVec{lit("."), lit("-12.745"), T, F,
                                     lit("0.003"), lit("1")},
                 Ids{U, D(-12.745), D(1.00), D(0.00), D(0.003), D(1.00)});
  checkGetInt(IdOrLiteralOrIriVec{lit("."), lit("-12.745"), T, F, lit(".03"),
                                  lit("1"), lit("-33")},
              Ids{U, U, I(1), I(0), U, I(1), I(-33)});
}

// ____________________________________________________________________________
TEST(SparqlExpression, testToDateOrDateTimeExpression) {
  using namespace ad_utility::testing;
  Id T = Id::makeFromBool(true);
  Id F = Id::makeFromBool(false);
  Id G = Id::makeFromGeoPoint(GeoPoint(50.0, 50.0));
  auto parserDate = DateYearOrDuration::parseXsdDate;
  auto parserDateTime = DateYearOrDuration::parseXsdDatetime;
  auto parserDuration = DateYearOrDuration::parseXsdDayTimeDuration;
  auto checkGetDate = testUnaryExpression<&makeConvertToDateExpression>;
  auto checkGetDateTime = testUnaryExpression<&makeConvertToDateTimeExpression>;

  checkGetDate(
      idOrLitOrStringVec(
          {"---", T, F, G, "2025-02", I(10), D(0.01), "-2025-02-20",
           "2025-02-20", "2025-1-1", DateId(parserDate, "0000-01-01"),
           DateId(parserDuration, "-PT5H"),
           DateId(parserDateTime, "1900-12-13T03:12:00.33Z"),
           DateId(parserDateTime, "2025-02-20T17:12:00.01-05:00")}),
      Ids{U, U, U, U, U, U, U, DateId(parserDate, "-2025-02-20"),
          DateId(parserDate, "2025-02-20"), U, DateId(parserDate, "0000-01-01"),
          U, DateId(parserDate, "1900-12-13"),
          DateId(parserDate, "2025-02-20")});
  checkGetDateTime(
      idOrLitOrStringVec(
          {"---", T, F, G, "2025-02", I(10), D(0.01), "-2025-02-20",
           "2025-02-20", "2025-1-1", "1900-12-13T03:12:00.33Z",
           "-1900-12-13T03:12:00.33Z", "2025-02-20T17:12:00.01-05:00",
           DateId(parserDateTime, "2025-02-20T17:12:00.01Z"),
           DateId(parserDuration, "PT1H4M"), DateId(parserDate, "0000-01-01")}),
      Ids{U, U, U, U, U, U, U, U, U, U,
          DateId(parserDateTime, "1900-12-13T03:12:00.33Z"),
          DateId(parserDateTime, "-1900-12-13T03:12:00.33Z"),
          DateId(parserDateTime, "2025-02-20T17:12:00.01-05:00"),
          DateId(parserDateTime, "2025-02-20T17:12:00.01Z"), U,
          DateId(parserDateTime, "0000-01-01T00:00:00.00")});
}

// ____________________________________________________________________________
TEST(SparqlExpression, testToBooleanExpression) {
  Id T = Id::makeFromBool(true);
  Id F = Id::makeFromBool(false);
  auto checkGetBoolean = testUnaryExpression<&makeConvertToBooleanExpression>;

  checkGetBoolean(
      IdOrLiteralOrIriVec(
          {sparqlExpression::detail::LiteralOrIri{
               iri("<http://example.org/z>")},
           lit("string"), lit("-10.2E3"), lit("+33.3300"), lit("0.0"), lit("0"),
           lit("0E1"), lit("1.5"), lit("1"), lit("1E0"), lit("13"),
           lit("2002-10-10T17:00:00Z"), lit("false"), lit("true"), T, F,
           lit("0", absl::StrCat("^^<", XSD_PREFIX.second, "boolean>")), I(0),
           I(1), I(-1), D(0.0), D(1.0), D(-1.0),
           // The SPARQL compliance tests for the boolean conversion functions
           // mandate that xds:boolean("0E1") is undefined, but
           // xsd:boolean("0E1"^^xsd:float) is false. The code currently returns
           // undefined in both cases, which is fine, because the SPARQL parser
           // converts the latter into an actual float literal.
           lit("0E1", absl::StrCat("^^<", XSD_PREFIX.second, "float>")),
           lit("1E0", absl::StrCat("^^<", XSD_PREFIX.second, "float>")),
           lit("1.25", absl::StrCat("^^<", XSD_PREFIX.second, "float>")),
           lit("-7.875", absl::StrCat("^^<", XSD_PREFIX.second, "float>"))}),
      Ids{U, U, U, U, U, F, U, U, T, U, U, U, F, T,
          T, F, F, F, T, T, F, T, T, U, U, U, U});

  checkGetBoolean(IdOrLiteralOrIriVec({Id::makeUndefined()}), Ids{U});
}

// ____________________________________________________________________________
TEST(SparqlExpression, geoSparqlExpressions) {
  auto checkLat = testUnaryExpression<&makeLatitudeExpression>;
  auto checkLong = testUnaryExpression<&makeLongitudeExpression>;
  auto checkIsGeoPoint = testUnaryExpression<&makeIsGeoPointExpression>;
  auto checkDist = std::bind_front(testNaryExpression, &makeDistExpression);

  auto p = GeoPoint(26.8, 24.3);
  auto v = ValueId::makeFromGeoPoint(p);

  // Should provide the same values, however with some precision loss, as the
  // bit representation only uses 30 bits per coordinate, not a full double
  auto actualValues = GeoPoint::fromBitRepresentation(
      GeoPoint(26.8, 24.3).toBitRepresentation());
  auto actualLat = actualValues.getLat();
  auto actualLng = actualValues.getLng();
  constexpr auto precision = 0.0001;
  EXPECT_NEAR(actualLat, 26.8, precision);
  EXPECT_NEAR(actualLng, 24.3, precision);
  auto vLat = ValueId::makeFromDouble(actualLat);
  auto vLng = ValueId::makeFromDouble(actualLng);

  checkLat(v, vLat);
  checkLong(v, vLng);
  checkIsGeoPoint(v, B(true));
  checkDist(D(0.0), v, v);
  checkLat(idOrLitOrStringVec({"NotAPoint", I(12)}), Ids{U, U});
  checkLong(idOrLitOrStringVec({D(4.2), "NotAPoint"}), Ids{U, U});
  checkIsGeoPoint(IdOrLiteralOrIri{lit("NotAPoint")}, B(false));
  checkDist(U, v, IdOrLiteralOrIri{I(12)});
  checkDist(U, IdOrLiteralOrIri{I(12)}, v);
  checkDist(U, v, IdOrLiteralOrIri{lit("NotAPoint")});
  checkDist(U, IdOrLiteralOrIri{lit("NotAPoint")}, v);
}

// ________________________________________________________________________________________
TEST(SparqlExpression, ifAndCoalesce) {
  auto checkIf = testNaryExpressionVec<&makeIfExpression>;
  auto checkCoalesce = testNaryExpressionVec<makeCoalesceExpressionVariadic>;

  const auto T = Id::makeFromBool(true);
  const auto F = Id::makeFromBool(false);

  checkIf(idOrLitOrStringVec({I(0), "eins", I(2), I(3), "vier", "fünf"}),
          // UNDEF and the empty string are considered to be `false`.
          std::tuple{idOrLitOrStringVec({T, F, T, "true", U, ""}),
                     Ids{I(0), I(1), I(2), I(3), I(4), I(5)},
                     idOrLitOrStringVec(
                         {"null", "eins", "zwei", "drei", "vier", "fünf"})});
  checkCoalesce(
      idOrLitOrStringVec({I(0), "eins", I(2), I(3), U, D(5.0)}),
      // UNDEF and the empty string are considered to be `false`.
      std::tuple{Ids{I(0), U, I(2), I(3), U, D(5.0)},
                 idOrLitOrStringVec({"null", "eins", "zwei", "drei", U, U}),
                 Ids{U, U, U, U, U, D(5.0)}});
  // Example for COALESCE where we have a constant input and evaluating the last
  // input is not necessary.
  checkCoalesce(
      idOrLitOrStringVec({I(0), "eins", I(2), I(3), "eins", D(5.0)}),
      // UNDEF and the empty string are considered to be `false`.
      std::tuple{Ids{I(0), U, I(2), I(3), U, D(5.0)}, U,
                 IdOrLiteralOrIri{lit("eins")}, Ids{U, U, U, U, U, D(5.0)}});

  // Check COALESCE with no arguments or empty arguments.
  checkCoalesce(IdOrLiteralOrIriVec{}, std::tuple{});
  checkCoalesce(IdOrLiteralOrIriVec{}, std::tuple{Ids{}});
  checkCoalesce(IdOrLiteralOrIriVec{}, std::tuple{Ids{}, Ids{}});
  checkCoalesce(IdOrLiteralOrIriVec{}, std::tuple{Ids{}, Ids{}, Ids{}});

  auto coalesceExpr = makeCoalesceExpressionVariadic(
      std::make_unique<IriExpression>(iri("<bim>")),
      std::make_unique<IriExpression>(iri("<bam>")));
  ASSERT_THAT(coalesceExpr->getCacheKey({}),
              testing::AllOf(::testing::ContainsRegex("CoalesceExpression"),
                             ::testing::HasSubstr("<bim>"),
                             ::testing::HasSubstr("<bam>")));
}

// ________________________________________________________________________________________
TEST(SparqlExpression, concatExpression) {
  auto checkConcat = testNaryExpressionVec<makeConcatExpressionVariadic>;

  const auto T = Id::makeFromBool(true);
  checkConcat(
      idOrLitOrStringVec({"0null", "eins", "2zwei", "3drei", "", "5.35.2"}),
      // UNDEF evaluates to an empty string..
      std::tuple{Ids{I(0), U, I(2), I(3), U, D(5.3)},
                 idOrLitOrStringVec({"null", "eins", "zwei", "drei", U, U}),
                 Ids{U, U, U, U, U, D(5.2)}});
  // Example with some constants in the middle.
  checkConcat(
      idOrLitOrStringVec({"0trueeins", "trueeins", "2trueeins", "3trueeins",
                          "trueeins", "12.3trueeins-2.1"}),
      // UNDEF and the empty string are considered to be `false`.
      std::tuple{Ids{I(0), U, I(2), I(3), U, D(12.3)}, T,
                 IdOrLiteralOrIri{lit("eins")}, Ids{U, U, U, U, U, D(-2.1)}});

  // Only constants
  checkConcat(IdOrLiteralOrIri{lit("trueMe1")},
              std::tuple{T, IdOrLiteralOrIri{lit("Me")}, I(1)});
  // Constants at the beginning.
  checkConcat(IdOrLiteralOrIriVec{lit("trueMe1"), lit("trueMe2")},
              std::tuple{T, IdOrLiteralOrIri{lit("Me")}, Ids{I(1), I(2)}});

  checkConcat(IdOrLiteralOrIri{lit("")}, std::tuple{});
  auto coalesceExpr = makeConcatExpressionVariadic(
      std::make_unique<IriExpression>(iri("<bim>")),
      std::make_unique<IriExpression>(iri("<bam>")));
  ASSERT_THAT(coalesceExpr->getCacheKey({}),
              testing::AllOf(::testing::ContainsRegex("ConcatExpression"),
                             ::testing::HasSubstr("<bim>"),
                             ::testing::HasSubstr("<bam>")));
}

// ______________________________________________________________________________
TEST(SparqlExpression, ReplaceExpression) {
  auto makeReplaceExpressionThreeArgs = [](auto&& arg0, auto&& arg1,
                                           auto&& arg2) {
    return makeReplaceExpression(AD_FWD(arg0), AD_FWD(arg1), AD_FWD(arg2),
                                 nullptr);
  };
  auto checkReplace = testNaryExpressionVec<makeReplaceExpressionThreeArgs>;
  auto checkReplaceWithFlags = testNaryExpressionVec<&makeReplaceExpression>;
  // A simple replace( no regexes involved).
  checkReplace(
      idOrLitOrStringVec({"null", "Eins", "zwEi", "drEi", U, U}),
      std::tuple{idOrLitOrStringVec({"null", "eins", "zwei", "drei", U, U}),
                 IdOrLiteralOrIri{lit("e")}, IdOrLiteralOrIri{lit("E")}});
  // A somewhat more involved regex
  checkReplace(
      idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}),
      std::tuple{idOrLitOrStringVec({"null", "eins", "zwei", "drei", U, U}),
                 IdOrLiteralOrIri{lit("e.[a-z]")}, IdOrLiteralOrIri{lit("X")}});
  // A regex with replacement with substitutions
  checkReplace(idOrLitOrStringVec({R"("$1 \\2 A \\bc")", R"("$1 \\2 DE \\f")"}),
               std::tuple{idOrLitOrStringVec({"Abc", "DEf"}),
                          IdOrLiteralOrIri{lit("([A-Z]+)")},
                          IdOrLiteralOrIri{lit(R"("\\$1 \\2 $1 \\")")}});

  checkReplace(idOrLitOrStringVec({"truebc", "truef"}),
               std::tuple{idOrLitOrStringVec({"Abc", "DEf"}),
                          IdOrLiteralOrIri{lit("([A-Z]+)")},
                          IdOrLiteralOrIri{Id::makeFromBool(true)}});

  // Case-insensitive matching using the hack for google regex:
  checkReplace(idOrLitOrStringVec({"null", "xxns", "zwxx", "drxx"}),
               std::tuple{idOrLitOrStringVec({"null", "eIns", "zwEi", "drei"}),
                          IdOrLiteralOrIri{lit("(?i)[ei]")},
                          IdOrLiteralOrIri{lit("x")}});

  // Case-insensitive matching using the flag
  checkReplaceWithFlags(
      idOrLitOrStringVec({"null", "xxns", "zwxx", "drxx"}),
      std::tuple{idOrLitOrStringVec({"null", "eIns", "zwEi", "drei"}),
                 IdOrLiteralOrIri{lit("[ei]")}, IdOrLiteralOrIri{lit("x")},
                 IdOrLiteralOrIri{lit("i")}});
  // Empty flag
  checkReplaceWithFlags(
      idOrLitOrStringVec({"null", "xIns", "zwEx", "drxx"}),
      std::tuple{idOrLitOrStringVec({"null", "eIns", "zwEi", "drei"}),
                 IdOrLiteralOrIri{lit("[ei]")}, IdOrLiteralOrIri{lit("x")},
                 IdOrLiteralOrIri{lit("")}});

  // Multiple matches within the same string
  checkReplace(
      IdOrLiteralOrIri{lit("wEeDEflE")},
      std::tuple{IdOrLiteralOrIri{lit("weeeDeeflee")},
                 IdOrLiteralOrIri{lit("ee")}, IdOrLiteralOrIri{lit("E")}});

  // Illegal regex.
  checkReplace(
      IdOrLiteralOrIriVec{U, U, U, U, U, U},
      std::tuple{idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}),
                 IdOrLiteralOrIri{lit("e.[a-z")}, IdOrLiteralOrIri{lit("X")}});
  // Undefined regex
  checkReplace(
      IdOrLiteralOrIriVec{U, U, U, U, U, U},
      std::tuple{idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}), U,
                 IdOrLiteralOrIri{lit("X")}});

  checkReplaceWithFlags(
      IdOrLiteralOrIriVec{U, U, U, U, U, U},
      std::tuple{idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}), U,
                 IdOrLiteralOrIri{lit("X")}, IdOrLiteralOrIri{lit("i")}});

  // Undefined flags
  checkReplaceWithFlags(
      IdOrLiteralOrIriVec{U, U, U, U, U, U},
      std::tuple{idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}),
                 IdOrLiteralOrIri{lit("[ei]")}, IdOrLiteralOrIri{lit("X")}, U});

  using ::testing::HasSubstr;
  // Invalid flags
  checkReplaceWithFlags(
      IdOrLiteralOrIriVec{U},
      std::tuple{idOrLitOrStringVec({"null"}), IdOrLiteralOrIri{lit("[n]")},
                 IdOrLiteralOrIri{lit("X")}, IdOrLiteralOrIri{lit("???")}});

  // Illegal replacement.
  checkReplace(
      IdOrLiteralOrIriVec{U, U, U, U, U, U},
      std::tuple{idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}),
                 IdOrLiteralOrIri{lit("e")}, Id::makeUndefined()});
}

// ______________________________________________________________________________
TEST(SparqlExpression, literalExpression) {
  TestContext ctx;
  StringLiteralExpression expr{TripleComponent::Literal::fromEscapedRdfLiteral(
      "\"notInTheVocabulary\"")};
  // Evaluate multiple times to test the caching behavior.
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(
        (ExpressionResult{IdOrLiteralOrIri{lit("\"notInTheVocabulary\"")}}),
        expr.evaluate(&ctx.context));
  }
  // A similar test with a constant entry that is part of the vocabulary and can
  // therefore be converted to an ID.
  IriExpression iriExpr{TripleComponent::Iri::fromIriref("<x>")};
  Id idOfX = ctx.x;
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ((ExpressionResult{IdOrLiteralOrIri{idOfX}}),
              iriExpr.evaluate(&ctx.context));
  }
}

// Test a `VariableExpression` for a variable that is not present in the input
// (and therefore always undefined).
TEST(SparqlExpression, unboundVariableExpression) {
  TestContext ctx;
  VariableExpression var{Variable{"?notFoundAnywhere"}};
  EXPECT_THAT(var.getCacheKey({}), ::testing::HasSubstr("Unbound Variable"));
  EXPECT_THAT(var.evaluate(&ctx.context),
              ::testing::VariantWith<Id>(Id::makeUndefined()));
}

// ______________________________________________________________________________
TEST(SparqlExpression, encodeForUri) {
  auto checkEncodeForUri = testUnaryExpression<&makeEncodeForUriExpression>;
  using IoS = IdOrLiteralOrIri;
  auto l = [](std::string_view s, std::string_view langOrDatatype = "") {
    return IoS{lit(s, langOrDatatype)};
  };
  checkEncodeForUri("Los Angeles", "Los%20Angeles");
  checkEncodeForUri(l("Los Angeles", "@en"), "Los%20Angeles");
  checkEncodeForUri(l("Los Angeles", "^^<someDatatype>"), "Los%20Angeles");
  checkEncodeForUri(l("Los Ängeles", "^^<someDatatype>"), "Los%20%C3%84ngeles");
  checkEncodeForUri(l("\"Los Angeles"), "%22Los%20Angeles");
  checkEncodeForUri(l("L\"os \"Angeles"), "L%22os%20%22Angeles");
  // Literals from the global and local vocab.
  checkEncodeForUri(testContext().aelpha, "%C3%A4lpha");
  checkEncodeForUri(testContext().notInVocabA, "notInVocabA");
  checkEncodeForUri(testContext().notInVocabAelpha, "notInVocab%C3%84lpha");
  // Entities from the local and global vocab (become undefined without STR()
  // around the expression).
  checkEncodeForUri(testContext().label, IoS{U});
  checkEncodeForUri(testContext().notInVocabC, IoS{U});
  checkEncodeForUri(U, IoS{U});
}

// ______________________________________________________________________________
TEST(SparqlExpression, replaceChildThrowsIfOutOfRange) {
  sparqlExpression::IdExpression expr(ValueId::makeFromInt(42));
  std::unique_ptr<SparqlExpression> exprToSubstitute =
      std::make_unique<IdExpression>(ValueId::makeFromInt(42));

  ASSERT_THROW(expr.replaceChild(1, std::move(exprToSubstitute)),
               ad_utility::Exception);
}

// ______________________________________________________________________________
TEST(SparqlExpression, isAggregateAndIsDistinct) {
  using namespace sparqlExpression;
  IdExpression idExpr(ValueId::makeFromInt(42));

  using enum SparqlExpression::AggregateStatus;

  ASSERT_EQ(idExpr.isAggregate(), NoAggregate);
  ASSERT_FALSE(idExpr.isInsideAggregate());

  auto varX = []() {
    return std::make_unique<VariableExpression>(Variable{"?x"});
  };

  // Return a matcher that checks whether a given `SparqlExpression` is an
  // aggregate with the given distinctness. In particular, the expression itself
  // as well as its child must return `true` for the `isInsideAggregate()`
  // function, and the distinctness of the aggregate itself must match.
  // If `hasChild` is `true`, then the aggregate is expected to have at least
  // one child. This is the case for all aggregates except the
  // `CountStarExpression`.
  auto match = [](bool distinct, bool hasChild = true) {
    auto aggStatus = distinct ? DistinctAggregate : NonDistinctAggregate;
    auto distinctMatcher =
        AD_PROPERTY(SparqlExpression, isAggregate, aggStatus);
    auto getChild = [](const SparqlExpression& expression) -> decltype(auto) {
      return *expression.children()[0];
    };
    auto insideAggregate =
        AD_PROPERTY(SparqlExpression, isInsideAggregate, true);
    using namespace ::testing;
    auto childMatcher = [&]() -> Matcher<const SparqlExpression&> {
      if (hasChild) {
        return ResultOf(getChild, insideAggregate);
      } else {
        return ::testing::_;
      }
    }();
    return AllOf(distinctMatcher, insideAggregate, childMatcher);
  };

  EXPECT_THAT(AvgExpression(false, varX()), match(false));
  EXPECT_THAT(AvgExpression(true, varX()), match(true));

  EXPECT_THAT(GroupConcatExpression(false, varX(), " "), match(false));
  EXPECT_THAT(GroupConcatExpression(true, varX(), " "), match(true));

  EXPECT_THAT(StdevExpression(false, varX()), match(false));
  EXPECT_THAT(StdevExpression(true, varX()), match(true));

  EXPECT_THAT(SampleExpression(false, varX()), match(false));
  // For `SAMPLE` the distinctness makes no difference, so we always return `not
  // distinct`.
  EXPECT_THAT(SampleExpression(true, varX()), match(false));

  EXPECT_THAT(*makeCountStarExpression(true), match(true, false));
  EXPECT_THAT(*makeCountStarExpression(false), match(false, false));
}

// ___________________________________________________________________________
TEST(SingleUseExpression, simpleMembersForTestCoverage) {
  SingleUseExpression expression(Id::makeUndefined());
  using namespace ::testing;
  EXPECT_THAT(expression.evaluate(nullptr),
              VariantWith<Id>(Eq(Id::makeUndefined())));
  EXPECT_ANY_THROW(expression.evaluate(nullptr));
  EXPECT_THAT(expression.childrenForTesting(), IsEmpty());
  EXPECT_ANY_THROW(expression.getUnaggregatedVariables());
  EXPECT_ANY_THROW(expression.getCacheKey({}));
}

// This just tests basic functionality of the `ExistsExpression` class. Since
// the actual implementation of the `EXISTS` operator is done in the
// `ExistsJoin` class, most of the testing happens in
// `test/engine/ExistsJoinTest.cpp`.
TEST(ExistsExpression, basicFunctionality) {
  ExistsExpression exists{ParsedQuery{}};
  auto var = exists.variable();
  TestContext context;
  EXPECT_ANY_THROW(exists.evaluate(&context.context));
  using namespace testing;
  EXPECT_THAT(exists.getCacheKey(context.varToColMap),
              HasSubstr("Uninitialized Exists"));
  context.varToColMap[var] = makeAlwaysDefinedColumn(437);
  EXPECT_THAT(exists.evaluate(&context.context), VariantWith<Variable>(var));
  EXPECT_THAT(exists.getCacheKey(context.varToColMap),
              HasSubstr("ExistsExpression col# 437"));
}

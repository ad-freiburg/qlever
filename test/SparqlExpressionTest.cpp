// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gtest/gtest.h>

#include <string>

#include "./SparqlExpressionTestHelpers.h"
#include "./printers/LocalVocabEntryPrinters.h"
#include "./util/GTestHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/CountStarExpression.h"
#include "engine/sparqlExpressions/ExistsExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "engine/sparqlExpressions/StdevExpression.h"
#include "index/Index.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/AllocatorTestHelpers.h"
#include "util/Conversions.h"
#include "util/GeoSparqlHelpers.h"
#include "util/IdTestHelpers.h"

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
auto Dat = ad_utility::testing::DateId;
auto GP = ad_utility::testing::GeoPointId;
auto U = Id::makeUndefined();

using Ids = std::vector<Id>;
using IdOrLocalVocabEntryVec = std::vector<IdOrLocalVocabEntry>;

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

auto lit = [](std::string_view s, std::string_view langtagOrDatatype = "") {
  return LocalVocabEntry{
      ad_utility::testing::tripleComponentLiteral(s, langtagOrDatatype),
      testContext().qec->getLocalVocabContext()};
};

auto iriref = [](std::string_view s) {
  return LocalVocabEntry{iri(s), testContext().qec->getLocalVocabContext()};
};

auto idOrLitOrStringVec =
    [](const std::vector<std::variant<Id, std::string>>& input) {
      IdOrLocalVocabEntryVec result;
      for (const auto& el : input) {
        if (std::holds_alternative<Id>(el)) {
          result.push_back(std::get<Id>(el));
        } else {
          result.push_back(lit(std::get<std::string>(el)));
        }
      }
      return result;
    };

auto geoLit = [](std::string_view content) {
  return IdOrLocalVocabEntry{
      lit(content, "^^<http://www.opengis.net/ont/geosparql#wktLiteral>")};
};

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
    return IdOrLocalVocabEntry{lit(vec)};
  } else {
    return VectorWithMemoryLimit<typename T::value_type>{
        ql::make_move_iterator(vec.begin()), ql::make_move_iterator(vec.end()),
        alloc};
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
// If it is an ID (possibly contained in the `IdOrLocalVocabEntry` variant),
// then the `matchId` matcher from above is used, else we test for equality.
template <typename T>
requires(!isVectorResult<T>) auto nonVectorResultMatcher(const T& expected) {
  if constexpr (std::is_same_v<T, Id>) {
    return matchId(expected);
  } else if constexpr (std::is_same_v<T, IdOrLocalVocabEntry>) {
    return std::visit(
        [](const auto& el) {
          using U = std::decay_t<decltype(el)>;
          return ::testing::MatcherCast<const IdOrLocalVocabEntry&>(
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
auto sparqlExpressionResultMatcher = [](const auto& expected) {
  using Expected = std::decay_t<decltype(expected)>;
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

  // Get the size of `operand`: size for a vector, 1 otherwise. For a
  // `SetOfIntervals`, this actually returns the *minimal* size of an
  // `EvaluationContext` that can expand the `SetOfIntervals`. This is used to
  // implement test cases where all the inputs are `SetOfIntervals` or constant,
  // and the result is NOT a `SetOfIntervals`.
  auto getResultSize = [](const auto& operand) -> size_t {
    using T = std::decay_t<decltype(operand)>;
    if constexpr (isVectorResult<T>) {
      return operand.size();
    } else if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
      return operand._intervals.empty() ? 0ul
                                        : operand._intervals.back().second;
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
       source_location l = AD_CURRENT_SOURCE_LOC()) {
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
struct TestNaryExpressionVec {
  template <VectorOrExpressionResult Exp, VectorOrExpressionResult... Ops>
  void operator()(Exp expected, std::tuple<Ops...> ops,
                  source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto t = generateLocationTrace(l, "testBinaryExpressionVec");

    std::apply(
        [&](auto&... args) {
          testNaryExpression(makeFunction, expected, args...);
        },
        ops);
  }
};

auto testOr = testBinaryExpressionCommutative<&makeOrExpression>;
auto testAnd = testBinaryExpressionCommutative<&makeAndExpression>;
auto testPlus = testBinaryExpressionCommutative<&makeAddExpression>;
auto testMultiply = testBinaryExpressionCommutative<&makeMultiplyExpression>;
auto testMinus = std::bind_front(testNaryExpression, &makeSubtractExpression);
auto testDivide = std::bind_front(testNaryExpression, &makeDivideExpression);

// _____________________________________________________________________________________
TEST(SparqlExpression, logicalOperators) {
  // Test `AndExpression` and `OrExpression`.
  constexpr auto t = B(true);
  constexpr auto f = B(false);
  V<Id> b{{f, t, t, f}, alloc};
  V<Id> d{{D(1.0), D(2.0), D(std::numeric_limits<double>::quiet_NaN()), D(0.0)},
          alloc};
  V<Id> dAsBool{{t, t, f, f}, alloc};

  V<IdOrLocalVocabEntry> s{{lit("true"), lit(""), lit("false"), lit("")},
                           alloc};
  V<Id> sAsBool{{t, f, t, f}, alloc};

  V<Id> i{{I(32), I(-42), I(0), I(5)}, alloc};
  V<Id> iAsId{{t, t, f, t}, alloc};

  V<Id> bOrD{{t, t, t, f}, alloc};
  V<Id> bAndD{{f, t, f, f}, alloc};

  V<Id> bOrS{{t, t, t, f}, alloc};
  V<Id> bAndS{{f, f, t, f}, alloc};

  V<Id> bOrI{{t, t, t, t}, alloc};
  V<Id> bAndI{{f, t, f, f}, alloc};

  V<Id> dOrS{{t, t, t, f}, alloc};
  V<Id> dAndS{{t, f, f, f}, alloc};

  V<Id> dOrI{{t, t, f, t}, alloc};
  V<Id> dAndI{{t, t, f, f}, alloc};

  V<Id> sOrI{{t, t, t, t}, alloc};
  V<Id> sAndI{{t, f, f, f}, alloc};

  V<Id> allTrue{{t, t, t, t}, alloc};
  V<Id> allFalse{{f, f, f, f}, alloc};

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
  {
    auto s1 = S{{{0, 4}}};
    auto s2 = S{{{3, 6}}};
    // The type erased expressions don't use the optimizations between
    // set-of-interval, but always return a fully materialized vector.
#ifdef _QLEVER_TYPE_ERASED_EXPRESSIONS
    V<Id> resultAsVec{{t, t, t, t, t, t}, alloc};
    testOr(resultAsVec, s1, s2);
#else
    S resultAsSet = S{{{0, 6}}};
    testOr(resultAsSet, s1, s2);
#endif
  }

  testAnd(b, b, allTrue);
  testAnd(dAsBool, d, allTrue);
  testAnd(allFalse, b, allFalse);
  testAnd(bAndD, d, b);
  testAnd(bAndI, b, i);
  testAnd(bAndS, b, s);
  testAnd(dAndI, d, i);
  testAnd(dAndS, d, s);
  testAnd(sAndI, s, i);

  using S = ad_utility::SetOfIntervals;
  {
    auto s1 = S{{{0, 4}}};
    auto s2 = S{{{3, 6}}};
    // The type erased expressions don't use the optimizations between
    // set-of-interval, but always return a fully materialized vector.
#ifdef _QLEVER_TYPE_ERASED_EXPRESSIONS
    V<Id> resultAsVec{{f, f, f, t, f, f}, alloc};
    testAnd(resultAsVec, s1, s2);
#else
    S resultAsSet = S{{{3, 4}}};
    testAnd(resultAsSet, s1, s2);
#endif
  }

  testOr(allTrue, b, t);
  testOr(b, b, f);
  testAnd(allFalse, b, f);
  testAnd(b, b, t);

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

  testOr(allTrue, b, IdOrLocalVocabEntry{lit("halo")});
  testOr(b, b, IdOrLocalVocabEntry(lit("")));
  testAnd(allFalse, b, IdOrLocalVocabEntry(lit("")));
  testAnd(b, b, IdOrLocalVocabEntry(lit("yellow")));

  // Test the behavior in the presence of UNDEF values.
  {
    V<Id> allValues1{{t, t, t, f, f, f, U, U, U}, alloc};
    V<Id> allValues2{{t, f, U, t, f, U, t, f, U}, alloc};
    V<Id> resultOr{{t, t, t, t, f, U, t, U, U}, alloc};
    V<Id> resultAnd{{t, f, U, f, f, f, U, f, U}, alloc};
    testOr(resultOr, allValues1, allValues2);
    testAnd(resultAnd, allValues1, allValues2);
  }

  {
    auto expression = makeOrExpression(
        std::make_unique<IdExpression>(ValueId::makeUndefined()),
        std::make_unique<IdExpression>(ValueId::makeUndefined()));
    EXPECT_FALSE(expression->isResultAlwaysDefined(testContext().varToColMap));
  }
  {
    auto expression = makeOrExpression(
        std::make_unique<IdExpression>(ValueId::makeFromInt(42)),
        std::make_unique<IdExpression>(ValueId::makeUndefined()));
    EXPECT_TRUE(expression->isResultAlwaysDefined(testContext().varToColMap));
  }
  {
    auto expression = makeOrExpression(
        std::make_unique<IdExpression>(ValueId::makeUndefined()),
        std::make_unique<IdExpression>(ValueId::makeFromInt(42)));
    EXPECT_TRUE(expression->isResultAlwaysDefined(testContext().varToColMap));
  }
  {
    auto expression = makeAndExpression(
        std::make_unique<IdExpression>(ValueId::makeFromInt(42)),
        std::make_unique<IdExpression>(ValueId::makeUndefined()));
    EXPECT_FALSE(expression->isResultAlwaysDefined(testContext().varToColMap));
  }
  {
    auto expression = makeAndExpression(
        std::make_unique<IdExpression>(ValueId::makeUndefined()),
        std::make_unique<IdExpression>(ValueId::makeFromInt(42)));
    EXPECT_FALSE(expression->isResultAlwaysDefined(testContext().varToColMap));
  }
  {
    auto expression = makeAndExpression(
        std::make_unique<IdExpression>(ValueId::makeFromInt(42)),
        std::make_unique<IdExpression>(ValueId::makeFromInt(42)));
    EXPECT_TRUE(expression->isResultAlwaysDefined(testContext().varToColMap));
  }
}

// _____________________________________________________________________________________
TEST(SparqlExpression, arithmeticOperators) {
  // Test `AddExpression`, `SubtractExpression`, `MultiplyExpression`, and
  // `DivideExpression`.
  //
  // TODO: Also test `UnaryMinusExpression`.
  auto createDat = [](std::string timeString, bool fromDateTime = true) {
    return Dat((fromDateTime ? DateYearOrDuration::parseXsdDatetime
                             : DateYearOrDuration::parseXsdDayTimeDuration),
               timeString);
  };

  V<Id> b{{B(true), B(false), B(false), B(true)}, alloc};
  V<Id> bAsInt{{I(1), I(0), I(0), I(1)}, alloc};

  V<Id> d{{D(1.0), D(-2.0), D(naN), D(0.0)}, alloc};

  V<std::string> s{{"true", "", "false", ""}, alloc};

  V<Id> dat{
      {createDat("1909-10-10T10:11:23Z"), createDat("2009-09-23T01:01:59Z"),
       createDat("1959-03-13T13:13:13Z"), createDat("1889-10-29T00:12:30Z")},
      alloc};

  V<Id> allNan{{D(naN), D(naN), D(naN), D(naN)}, alloc};

  V<Id> i{{I(32), I(-42), I(0), I(5)}, alloc};
  V<Id> iAsDouble{{D(32.0), D(-42.0), D(0.0), D(5.0)}, alloc};

  V<Id> bPlusD{{D(2.0), D(-2.0), D(naN), D(1.0)}, alloc};
  V<Id> bMinusD{{D(0), D(2.0), D(naN), D(1)}, alloc};
  V<Id> dMinusB{{D(0), D(-2.0), D(naN), D(-1)}, alloc};
  V<Id> dMinusDat{{U, U, U, U}, alloc};
  V<Id> datMinusD{{U, U, U, U}, alloc};
  V<Id> bTimesD{{D(1.0), D(-0.0), D(naN), D(0.0)}, alloc};
  // Division by zero is `UNDEF`, to change this behavior a runtime parameter
  // can be set. This is tested explicitly below.
  V<Id> bByD{{D(1.0), D(-0.0), U, U}, alloc};
  V<Id> dByB{{D(1.0), U, U, D(0)}, alloc};

  testPlus(bPlusD, b, d);
  testMinus(bMinusD, b, d);
  testMinus(dMinusB, d, b);
  testPlus(dMinusDat, d, dat);
  testMinus(dMinusDat, d, dat);
  testPlus(datMinusD, dat, d);
  testMinus(datMinusD, dat, d);
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

#ifndef REDUCED_FEATURE_SET_FOR_CPP17
  // Test for `DateTime` - `DateTime`.
  V<Id> minus2000{{createDat("-P32954DT13H48M37S", false),
                   createDat("P3553DT1H1M59S", false),
                   createDat("-P14903DT10H46M47S", false),
                   createDat("-P40239DT23H47M30S", false)},
                  alloc};
  testMinus(minus2000, dat, createDat("2000-01-01T00:00:00Z"));
  V<Id> undefined{{U, U, U, U}, alloc};
  testMinus(undefined, dat, createDat("2013-02-30T00:00:00Z"));
  // Test for `DayTimeDuration` + `DayTimeDuration`.
  V<Id> dat2{
      {createDat("P340DT3H15M20S", false), createDat("P20DT5H1M9S", false),
       createDat("P10DT3H50M9S", false), createDat("P256DT9H11M40S", false)},
      alloc};
  V<Id> plus20Days{
      {createDat("P360DT8H16M29S", false), createDat("P40DT10H2M18S", false),
       createDat("P30DT8H51M18S", false), createDat("P276DT14H12M49S", false)},
      alloc};
  testPlus(plus20Days, dat2, createDat("P20DT5H1M9S", false));
  testPlus(undefined, dat, createDat("2013-02-30T00:00:00Z"));
#else
  V<Id> undefined{{U, U, U, U}, alloc};
  testMinus(undefined, dat, createDat("2000-01-01T00:00:00Z"));
  testPlus(undefined, dat, createDat("2000-01-01T00:00:00Z"));
#endif

  V<Id> mixed2{{B(true), I(250), D(-113.2), Voc(4)}, alloc};
  V<Id> mixed2MinusDat{{U, U, U, U}, alloc};
  V<Id> mixed2PlusDat{{U, U, U, U}, alloc};
  testMinus(mixed2MinusDat, dat, mixed2);
  testMinus(mixed2MinusDat, mixed2, dat);
  testPlus(mixed2PlusDat, dat, mixed2);
  testPlus(mixed2PlusDat, mixed2, dat);

  // For division, all results are doubles, so there is no difference between
  // int and double inputs.
  testDivide(by2, mixed, I(2));
  testDivide(by2, mixed, D(2));
  testMultiply(by2, mixed, D(0.5));
  testDivide(times13, mixed, D(1.0 / 1.3));

  // Division by zero is either `UNDEF` or `NaN/infinity`, depending on a
  // runtime parameter.
  V<Id> undef{{U, U, U, U}, alloc};
  V<Id> nanAndInf{{D(naN), D(naN), D(inf), D(negInf)}, alloc};
  V<Id> divByZeroInputsDouble{{D(-0.0), D(0), D(1.3), D(-1.2)}, alloc};
  V<Id> divByZeroInputsInt{{I(0), I(0), I(1), I(-1)}, alloc};

  testDivide(undef, divByZeroInputsDouble, I(0));
  testDivide(undef, divByZeroInputsInt, I(0));
  testDivide(undef, divByZeroInputsDouble, D(0));
  testDivide(undef, divByZeroInputsInt, D(0));

  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::divisionByZeroIsUndef_>(
          false);
  testDivide(nanAndInf, divByZeroInputsDouble, I(0));
  testDivide(nanAndInf, divByZeroInputsInt, I(0));
  testDivide(nanAndInf, divByZeroInputsDouble, D(0));
  testDivide(nanAndInf, divByZeroInputsInt, D(0));
}

// Test that the unary expression that is specified by the `makeFunction` yields
// the `expected` result when being given the `operand`.
template <auto makeFunction>
auto testUnaryExpression = [](VectorOrExpressionResult auto const& operand,
                              VectorOrExpressionResult auto const& expected,
                              source_location l = AD_CURRENT_SOURCE_LOC()) {
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
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
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
  testYear(IdOrLocalVocabEntryVec{lit("noDate")}, Ids{U});

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
                   IdOrLocalVocabEntryVec{lit("-05:00")});
  checkTimezone(Ids{Id::makeFromDate(d1)}, Ids{Id::makeFromDate(duration1)});
  tz = 23;
  auto d2 = DateYearOrDuration(Date(2011, 1, 10, 14, 45, 13.815, tz));
  auto duration2 = DateYearOrDuration(DayTimeDuration{positive, 0, 23, 0, 0.0});
  checkTimezone(Ids{Id::makeFromDate(d2)}, Ids{Id::makeFromDate(duration2)});
  checkStrTimezone(Ids{Id::makeFromDate(d2)},
                   IdOrLocalVocabEntryVec{lit("+23:00")});
  tz = Date::TimeZoneZ{};
  auto d3 = DateYearOrDuration(Date(2011, 1, 10, 14, 45, 13.815, tz));
  auto duration3 = DateYearOrDuration(DayTimeDuration{});
  checkTimezone(Ids{Id::makeFromDate(d3)}, Ids{Id::makeFromDate(duration3)});
  checkStrTimezone(Ids{Id::makeFromDate(d3)}, IdOrLocalVocabEntryVec{lit("Z")});
  tz = Date::NoTimeZone{};
  DateYearOrDuration d4 =
      DateYearOrDuration(Date(2011, 1, 10, 14, 45, 13.815, tz));
  checkStrTimezone(Ids{Id::makeFromDate(d4)}, IdOrLocalVocabEntryVec{lit("")});
  checkTimezone(Ids{Id::makeFromDate(d4)}, Ids{U});
  DateYearOrDuration d5 = DateYearOrDuration(Date(2012, 1, 4, 14, 45));
  checkStrTimezone(Ids{Id::makeFromDate(d5)}, IdOrLocalVocabEntryVec{lit("")});
  checkTimezone(Ids{Id::makeFromDate(d5)}, Ids{U});
  checkStrTimezone(IdOrLocalVocabEntryVec{lit("2011-01-10T14:")},
                   IdOrLocalVocabEntryVec{U});
  checkStrTimezone(Ids{Id::makeFromDouble(120.0123)},
                   IdOrLocalVocabEntryVec{U});
  checkTimezone(Ids{Id::makeFromDouble(2.34)}, Ids{U});
  checkStrTimezone(Ids{Id::makeUndefined()}, IdOrLocalVocabEntryVec{U});
  DateYearOrDuration d6 =
      DateYearOrDuration(-1394785, DateYearOrDuration::Type::Year);
  checkTimezone(Ids{Id::makeFromDate(d6)}, Ids{U});
  checkStrTimezone(Ids{Id::makeFromDate(d6)}, IdOrLocalVocabEntryVec{lit("")});
  DateYearOrDuration d7 =
      DateYearOrDuration(10000, DateYearOrDuration::Type::Year);
  checkStrTimezone(Ids{Id::makeFromDate(d7)}, IdOrLocalVocabEntryVec{lit("")});
  checkTimezone(Ids{Id::makeFromDate(d7)}, Ids{U});
}

// _____________________________________________________________________________________
namespace {
auto checkStrlen = testUnaryExpression<&makeStrlenExpression>;
auto checkStr = testUnaryExpression<&makeStrExpression>;
auto checkIriOrUri = TestNaryExpressionVec<&makeIriOrUriExpression>{};
auto makeStrlenWithStr = [](auto arg) {
  return makeStrlenExpression(makeStrExpression(std::move(arg)));
};
auto checkStrlenWithStrChild = testUnaryExpression<makeStrlenWithStr>;
}  // namespace
TEST(SparqlExpression, stringOperators) {
  // Test `StrlenExpression` and `StrExpression`.
  checkStrlen(
      IdOrLocalVocabEntryVec{lit("one"), lit("two"), lit("three"), lit("")},
      Ids{I(3), I(3), I(5), I(0)});
  checkStrlenWithStrChild(
      IdOrLocalVocabEntryVec{lit("one"), lit("two"), lit("three"), lit("")},
      Ids{I(3), I(3), I(5), I(0)});

  // Test the different (optimized) behavior depending on whether the STR()
  // function was applied to the argument.
  checkStrlen(
      IdOrLocalVocabEntryVec{lit("one"), lit("tschüss"), I(1), D(3.6), lit("")},
      Ids{I(3), I(7), U, U, I(0)});
  checkStrlenWithStrChild(
      IdOrLocalVocabEntryVec{lit("one"), I(1), D(3.6), lit("")},
      Ids{I(3), I(1), I(3), I(0)});
  checkStr(Ids{I(1), I(2), I(3)},
           IdOrLocalVocabEntryVec{lit("1"), lit("2"), lit("3")});
  checkStr(Ids{D(-1.0), D(1.0), D(2.34), D(NAN), D(INFINITY), D(-INFINITY)},
           IdOrLocalVocabEntryVec{lit("-1"), lit("1"), lit("2.34"), lit("NaN"),
                                  lit("INF"), lit("-INF")});
  checkStr(Ids{B(true), B(false), Id::makeBoolFromZeroOrOne(true),
               Id::makeBoolFromZeroOrOne(false)},
           IdOrLocalVocabEntryVec{lit("true"), lit("false"), lit("true"),
                                  lit("false")});
  checkStr(IdOrLocalVocabEntryVec{lit("one"), lit("two"), lit("three")},
           IdOrLocalVocabEntryVec{lit("one"), lit("two"), lit("three")});
  checkStr(IdOrLocalVocabEntryVec{iriref("<http://example.org/str>"),
                                  iriref("<http://example.org/int>"),
                                  iriref("<http://example.org/bool>")},
           IdOrLocalVocabEntryVec{lit("http://example.org/str"),
                                  lit("http://example.org/int"),
                                  lit("http://example.org/bool")});

  auto T = Id::makeFromBool(true);
  auto F = Id::makeFromBool(false);
  auto dateDate =
      Id::makeFromDate(DateYearOrDuration::parseXsdDate("2025-01-01"));
  auto dateLYear = Id::makeFromDate(
      DateYearOrDuration(11853, DateYearOrDuration::Type::Year));
  // Test `iriOrUriExpression`.
  // test invalid
  checkIriOrUri(IdOrLocalVocabEntryVec{U, U, U, U, U, U, U},
                std::tuple{IdOrLocalVocabEntryVec{U, IntId(2), DoubleId(12.99),
                                                  dateDate, dateLYear, T, F},
                           IdOrLocalVocabEntry{LocalVocabEntry{
                               ad_utility::triple_component::Iri{},
                               testContext().qec->getLocalVocabContext()}}});
  // test valid
  checkIriOrUri(
      IdOrLocalVocabEntryVec{
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
          IdOrLocalVocabEntryVec{
              lit("bimbim"), iriref("<bambim>"),
              lit("https://www.bimbimbam/2001/bamString"),
              lit("http://www.w3.\torg/2001/\nXMLSchema#\runsignedShort"),
              lit("http://www.w3.org/2001/XMLSchema#string"),
              iriref("<http://www.w3.org/2001/XMLSchema#string>"),
              testContext().notInVocabIri, testContext().notInVocabIriLit,
              lit("http://example/"), iriref("<http://\t\t\nexample/>"),
              lit("\t\n\r")},
          IdOrLocalVocabEntry{
              LocalVocabEntry{ad_utility::triple_component::Iri{},
                              testContext().qec->getLocalVocabContext()}}});

  // test with base iri
  checkIriOrUri(
      IdOrLocalVocabEntryVec{
          U,
          iriref("<http://example.com/hi/bimbim>"),
          iriref("<http://example.com/hi/bambim>"),
          iriref("<https://www.bimbimbam/2001/bamString>"),
          iriref("<http://example.com/hello>"),
          iriref("<http://example.com/hello>"),
      },
      std::tuple{
          IdOrLocalVocabEntryVec{U, lit("bimbim"), iriref("<bambim>"),
                                 lit("https://www.bimbimbam/2001/bamString"),
                                 lit("/hello"), iriref("</hello>")},
          IdOrLocalVocabEntry{iriref("<http://example.com/hi>")}});

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
  checkLcase(IdOrLocalVocabEntryVec{lit("One"), lit("tWÖ"), U, I(12)},
             IdOrLocalVocabEntryVec{lit("one"), lit("twö"), U, U});
  checkLcase(
      IdOrLocalVocabEntryVec{
          lit("One", "^^<http://www.w3.org/2001/XMLSchema#string>"),
          lit("One", "@en"), U, I(12)},
      IdOrLocalVocabEntryVec{lit("one"), lit("one", "@en"), U, U});
  checkUcase(IdOrLocalVocabEntryVec{lit("One"), lit("tWÖ"), U, I(12)},
             IdOrLocalVocabEntryVec{lit("ONE"), lit("TWÖ"), U, U});
  checkUcase(
      IdOrLocalVocabEntryVec{
          lit("One", "^^<http://www.w3.org/2001/XMLSchema#string>"),
          lit("One", "@en"), U, I(12)},
      IdOrLocalVocabEntryVec{lit("ONE"), lit("ONE", "@en"), U, U});
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
    IdOrLocalVocabEntryVec result;
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
      S({U, U, "", "", "Hällo", "", "o", "", "", "lo"}),
      S({U, "", "", "", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo"}),
      S({"", U, "", "x", "", "ullo", "ll", "Hällo", "Hällox", "l"}));
  checkStrAfter(
      IdOrLocalVocabEntryVec{lit("bc"), lit("abc"), lit("c", "@en"), lit(""),
                             lit("abc", "@en"), lit("abc", "@en"), lit(""),
                             lit("abc", "@en"), U, U},
      IdOrLocalVocabEntryVec{
          lit("abc", "^^<http://www.w3.org/2001/XMLSchema#string>"),
          lit("abc", "^^<http://www.w3.org/2001/XMLSchema#string>"),
          lit("abc", "@en"), lit("abc", "@en"), lit("abc", "@en"),
          lit("abc", "@en"), lit("abc", "@en"), lit("abc", "@en"), lit("abc"),
          lit("abc", "@en")},
      IdOrLocalVocabEntryVec{
          lit("a"), lit(""), lit("ab"), lit("z"), lit(""), lit("", "@en"),
          lit("z", "@en"),
          lit("", "^^<http://www.w3.org/2001/XMLSchema#string>"),
          lit("a", "@en"), lit("a", "@de")});

  checkStrBefore(
      S({U, U, "", "", "", "", "Hä", "", "", "Hä"}),
      S({U, "", "", "", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo", "Hällo"}),
      S({"", U, "", "x", "", "ullo", "ll", "Hällo", "Hällox", "l"}));
  checkStrBefore(IdOrLocalVocabEntryVec{lit("a"), lit(""), lit("a", "@en"),
                                        lit(""), lit("", "@en"), lit("", "@en"),
                                        lit(""), lit("", "@en"), U, U},
                 IdOrLocalVocabEntryVec{
                     lit("abc", "^^<http://www.w3.org/2001/XMLSchema#string>"),
                     lit("abc", "^^<http://www.w3.org/2001/XMLSchema#string>"),
                     lit("abc", "@en"), lit("abc", "@en"), lit("abc", "@en"),
                     lit("abc", "@en"), lit("abc", "@en"), lit("abc", "@en"),
                     lit("abc"), lit("abc", "@en")},
                 IdOrLocalVocabEntryVec{
                     lit("bc"), lit(""), lit("bc"), lit("z"), lit(""),
                     lit("", "@en"), lit("z", "@en"),
                     lit("", "^^<http://www.w3.org/2001/XMLSchema#string>"),
                     lit("bc", "@en"), lit("bc", "@de")});
}

// ______________________________________________________________________________
static auto checkSubstr =
    std::bind_front(testNaryExpression, makeSubstrExpression);
TEST(SparqlExpression, substr) {
  auto strs = [](const std::vector<std::variant<Id, std::string>>& input) {
    VectorWithMemoryLimit<IdOrLocalVocabEntry> result(alloc);
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
  // First must be LiteralOrIri
  auto Ux = IdOrLocalVocabEntry{U};
  checkSubstr(Ux, U, I(4), I(7));
  checkSubstr(Ux, Ux, I(4), I(7));
  // Second and third must be numeric;
  checkSubstr(Ux, IdOrLocalVocabEntry{lit("hello")}, U, I(4));
  checkSubstr(Ux, IdOrLocalVocabEntry{lit("hello")},
              IdOrLocalVocabEntry{lit("bye")}, I(17));
  checkSubstr(Ux, IdOrLocalVocabEntry{lit("hello")}, I(4), U);
  checkSubstr(Ux, IdOrLocalVocabEntry{lit("hello")}, I(4),
              IdOrLocalVocabEntry{lit("bye")});
  // WithDataType xsd:string
  checkSubstr(
      IdOrLocalVocabEntryVec{lit("Hello")},
      IdOrLocalVocabEntryVec{
          lit("Hello World", "^^<http://www.w3.org/2001/XMLSchema#string>")},
      I(1), I(5));

  // WithLanguageTag
  checkSubstr(IdOrLocalVocabEntryVec{lit("cha", "@en")},
              IdOrLocalVocabEntryVec{lit("chat", "@en")}, I(1), I(3));
}

// _____________________________________________________________________________________
TEST(SparqlExpression, strIriDtTagged) {
  auto U = Id::makeUndefined();
  auto checkStrIriTag =
      std::bind_front(testNaryExpression, &makeStrIriDtExpression);
  Id geoPoint = Id::makeFromGeoPoint(GeoPoint{0, 0});
  Id date = Id::makeFromDate(DateYearOrDuration::parseXsdDate("2026-02-16"));

  // Test the correct encoding of literals to `ValueId`s.
  checkStrIriTag(
      IdOrLocalVocabEntryVec{I(123), I(-42), I(-42), B(true), B(false), D(3.14),
                             D(2.5), D(2.5), date, geoPoint},
      IdOrLocalVocabEntryVec{lit("123"), lit("-42"), lit("-42"), lit("true"),
                             lit("false"), lit("3.14"), lit("2.5"), lit("2.5"),
                             lit("2026-02-16"), lit("POINT(0.0 0.0)")},
      IdOrLocalVocabEntryVec{
          iriref("<http://www.w3.org/2001/XMLSchema#integer>"),
          iriref("<http://www.w3.org/2001/XMLSchema#integer>"),
          iriref("<http://www.w3.org/2001/XMLSchema#int>"),
          iriref("<http://www.w3.org/2001/XMLSchema#boolean>"),
          iriref("<http://www.w3.org/2001/XMLSchema#boolean>"),
          iriref("<http://www.w3.org/2001/XMLSchema#double>"),
          iriref("<http://www.w3.org/2001/XMLSchema#decimal>"),
          iriref("<http://www.w3.org/2001/XMLSchema#float>"),
          iriref("<http://www.w3.org/2001/XMLSchema#date>"),
          iriref("<http://www.opengis.net/ont/geosparql#wktLiteral>")});

  // Unknown datatype: returns a `Literal` with datatype.
  checkStrIriTag(
      IdOrLocalVocabEntryVec{lit("iiii", "^^<http://example/romanNumeral>")},
      IdOrLocalVocabEntryVec{lit("iiii")},
      IdOrLocalVocabEntryVec{iriref("<http://example/romanNumeral>")});

  // Invalid content for the given datatype: falls back to `Literal`.
  checkStrIriTag(
      IdOrLocalVocabEntryVec{
          lit("abc", "^^<http://www.w3.org/2001/XMLSchema#integer>"),
          lit("abc", "^^<http://www.w3.org/2001/XMLSchema#boolean>")},
      IdOrLocalVocabEntryVec{lit("abc"), lit("abc")},
      IdOrLocalVocabEntryVec{
          iriref("<http://www.w3.org/2001/XMLSchema#integer>"),
          iriref("<http://www.w3.org/2001/XMLSchema#boolean>")});

  // Undefined cases.
  checkStrIriTag(
      IdOrLocalVocabEntryVec{U, U, U, U, U, U},
      IdOrLocalVocabEntryVec{
          iriref("<http://example/romanNumeral>"), lit("iiii"), U, lit("XVII"),
          lit("123", "^^<http://www.w3.org/2001/XMLSchema#integer>"),
          lit("chat", "@en")},
      IdOrLocalVocabEntryVec{
          U, U, U, lit("<not/a/iriref>"),
          iriref("<http://www.w3.org/2001/XMLSchema#integer>"),
          iriref("<http://example/romanNumeral>")});
}

// _____________________________________________________________________________________
TEST(SparqlExpression, strLangTagged) {
  auto U = Id::makeUndefined();
  auto checkStrTag =
      std::bind_front(testNaryExpression, &makeStrLangTagExpression);
  checkStrTag(IdOrLocalVocabEntryVec{lit("chat", "@en")},
              IdOrLocalVocabEntryVec{lit("chat")},
              IdOrLocalVocabEntryVec{lit("en")});
  checkStrTag(IdOrLocalVocabEntryVec{lit("chat", "@en-US")},
              IdOrLocalVocabEntryVec{lit("chat")},
              IdOrLocalVocabEntryVec{lit("en-US")});
  checkStrTag(IdOrLocalVocabEntryVec{lit("Sprachnachricht", "@de-Latn-de")},
              IdOrLocalVocabEntryVec{lit("Sprachnachricht")},
              IdOrLocalVocabEntryVec{lit("de-Latn-de")});
  checkStrTag(IdOrLocalVocabEntryVec{U}, IdOrLocalVocabEntryVec{lit("chat")},
              IdOrLocalVocabEntryVec{lit("d1235")});
  checkStrTag(IdOrLocalVocabEntryVec{U},
              IdOrLocalVocabEntryVec{lit("reporter")},
              IdOrLocalVocabEntryVec{lit("@")});
  checkStrTag(IdOrLocalVocabEntryVec{U}, IdOrLocalVocabEntryVec{lit("chat")},
              IdOrLocalVocabEntryVec{lit("")});
  checkStrTag(IdOrLocalVocabEntryVec{U}, IdOrLocalVocabEntryVec{lit("chat")},
              IdOrLocalVocabEntryVec{U});
  checkStrTag(IdOrLocalVocabEntryVec{U}, IdOrLocalVocabEntryVec{U},
              IdOrLocalVocabEntryVec{lit("d")});
  checkStrTag(IdOrLocalVocabEntryVec{U}, IdOrLocalVocabEntryVec{U},
              IdOrLocalVocabEntryVec{U});
  checkStrTag(IdOrLocalVocabEntryVec{U},
              IdOrLocalVocabEntryVec{lit("chat", "@en")},
              IdOrLocalVocabEntryVec{lit("en")});
  checkStrTag(IdOrLocalVocabEntryVec{U},
              IdOrLocalVocabEntryVec{
                  lit("123", "^^<http://www.w3.org/2001/XMLSchema#integer>")},
              IdOrLocalVocabEntryVec{lit("en")});
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
           IdOrLocalVocabEntryVec{I(0), D(5), I(0), lit("abc")});
}

// ____________________________________________________________________________
TEST(SparqlExpression, isSomethingFunctions) {
  Id T = Id::makeFromBool(true);
  Id F = Id::makeFromBool(false);
  Id geo = Id::makeFromGeoPoint(GeoPoint{0, 0});
  Id date = Id::makeFromDate(DateYearOrDuration::parseXsdDate("2025-06-13"));
  Id iri = testContext().x;
  Id literal = testContext().zz;
  Id blank = testContext().blank;
  Id blank2 = Id::makeFromBlankNodeIndex(BlankNodeIndex::make(12));
  Id localLiteral = testContext().notInVocabA;

  IdOrLocalVocabEntryVec testIdOrStrings = IdOrLocalVocabEntryVec{
      iriref("<i>"), lit("\"l\""), blank2, iri, literal, blank, localLiteral,
      I(42),         D(1),         F,      geo, date,    U};
  testUnaryExpression<makeIsIriExpression>(
      testIdOrStrings, Ids{T, F, F, T, F, F, F, F, F, F, F, F, F});
  testUnaryExpression<makeIsBlankExpression>(
      testIdOrStrings, Ids{F, F, T, F, F, T, F, F, F, F, F, F, F});
  testUnaryExpression<makeIsLiteralExpression>(
      testIdOrStrings, Ids{F, T, F, F, T, F, T, T, T, T, T, T, F});
  testUnaryExpression<makeIsNumericExpression>(
      testIdOrStrings, Ids{F, F, F, F, F, F, F, T, T, F, F, F, F});
  testUnaryExpression<makeBoundExpression>(
      testIdOrStrings, Ids{T, T, T, T, T, T, T, T, T, T, T, T, F});

  auto expression = makeBoundExpression(
      std::make_unique<IdExpression>(ValueId::makeUndefined()));
  EXPECT_TRUE(expression->isResultAlwaysDefined(testContext().varToColMap));
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
  checkGetDatatype(IdOrLocalVocabEntryVec{testContext().x},
                   IdOrLocalVocabEntryVec{U});
  checkGetDatatype(IdOrLocalVocabEntryVec{testContext().alpha},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#string>")});
  checkGetDatatype(
      IdOrLocalVocabEntryVec{testContext().zz},
      IdOrLocalVocabEntryVec{
          iriref("<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{testContext().notInVocabB},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#string>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{testContext().notInVocabD},
                   IdOrLocalVocabEntryVec{U});
  checkGetDatatype(IdOrLocalVocabEntryVec{lit(
                       "123", "^^<http://www.w3.org/2001/XMLSchema#integer>")},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#integer>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{lit("Simple StrStr")},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#string>")});
  checkGetDatatype(
      IdOrLocalVocabEntryVec{lit("english", "@en")},
      IdOrLocalVocabEntryVec{
          iriref("<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{U}, IdOrLocalVocabEntryVec{U});
  checkGetDatatype(IdOrLocalVocabEntryVec{DateId1},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#dateTime>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{DateId2},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#gYear>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{DateId3},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#gYear>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{DateId4},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#date>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{DateId5},
                   IdOrLocalVocabEntryVec{iriref(
                       "<http://www.w3.org/2001/XMLSchema#gYearMonth>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{GeoId1},
                   IdOrLocalVocabEntryVec{iriref(
                       "<http://www.opengis.net/ont/geosparql#wktLiteral>")});
  checkGetDatatype(
      IdOrLocalVocabEntryVec{Id::makeFromInt(212378233)},
      IdOrLocalVocabEntryVec{iriref("<http://www.w3.org/2001/XMLSchema#int>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{Id::makeFromDouble(2.3475)},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#double>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{Id::makeFromBool(false)},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#boolean>")});
  checkGetDatatype(
      IdOrLocalVocabEntryVec{Id::makeFromInt(true)},
      IdOrLocalVocabEntryVec{iriref("<http://www.w3.org/2001/XMLSchema#int>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{lit("")},
                   IdOrLocalVocabEntryVec{
                       iriref("<http://www.w3.org/2001/XMLSchema#string>")});
  checkGetDatatype(
      IdOrLocalVocabEntryVec{lit(" ", "@de-LATN-de")},
      IdOrLocalVocabEntryVec{
          iriref("<http://www.w3.org/1999/02/22-rdf-syntax-ns#langString>")});
  checkGetDatatype(
      IdOrLocalVocabEntryVec{
          lit("testval", "^^<http://example/iri/test#test>")},
      IdOrLocalVocabEntryVec{iriref("<http://example/iri/test#test>")});
  checkGetDatatype(IdOrLocalVocabEntryVec{iriref("<http://example/iri/test>")},
                   IdOrLocalVocabEntryVec{U});

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
  checkGetDouble(IdOrLocalVocabEntryVec{lit("."), lit("-12.745"), T, F,
                                        lit("0.003"), lit("1")},
                 Ids{U, D(-12.745), D(1.00), D(0.00), D(0.003), D(1.00)});
  chekGetDecimal(IdOrLocalVocabEntryVec{lit("."), lit("-12.745"), T, F,
                                        lit("0.003"), lit("1")},
                 Ids{U, D(-12.745), D(1.00), D(0.00), D(0.003), D(1.00)});
  checkGetInt(IdOrLocalVocabEntryVec{lit("."), lit("-12.745"), T, F, lit(".03"),
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
      IdOrLocalVocabEntryVec(
          {iriref("<http://example.org/z>"), lit("string"), lit("-10.2E3"),
           lit("+33.3300"), lit("0.0"), lit("0"), lit("0E1"), lit("1.5"),
           lit("1"), lit("1E0"), lit("13"), lit("2002-10-10T17:00:00Z"),
           lit("false"), lit("true"), T, F,
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

  checkGetBoolean(IdOrLocalVocabEntryVec({Id::makeUndefined()}), Ids{U});
}

// ____________________________________________________________________________
TEST(SparqlExpression, geoSparqlExpressions) {
  auto checkLat = testUnaryExpression<&makeLatitudeExpression>;
  auto checkLong = testUnaryExpression<&makeLongitudeExpression>;
  auto checkIsGeoPoint = testUnaryExpression<&makeIsGeoPointExpression>;
  auto checkCentroid = testUnaryExpression<&makeCentroidExpression>;
  auto checkDist = std::bind_front(testNaryExpression, &makeDistExpression);
  auto checkEnvelope = testUnaryExpression<&makeEnvelopeExpression>;
  auto checkEnvelopeLL = testUnaryExpression<&makeEnvelopeLowerLeftExpression>;
  auto checkEnvelopeUR = testUnaryExpression<&makeEnvelopeUpperRightExpression>;
  auto checkGeometryType = testUnaryExpression<&makeGeometryTypeExpression>;

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
  checkCentroid(v, v);
  checkIsGeoPoint(v, B(true));
  checkDist(D(0.0), v, v);
  checkLat(idOrLitOrStringVec({"NotAPoint", I(12)}), Ids{U, U});
  checkLong(idOrLitOrStringVec({D(4.2), "NotAPoint"}), Ids{U, U});
  checkIsGeoPoint(IdOrLocalVocabEntry{lit("NotAPoint")}, B(false));
  checkCentroid(IdOrLocalVocabEntry{lit("NotAPoint")}, U);
  checkDist(U, v, IdOrLocalVocabEntry{I(12)});
  checkDist(U, IdOrLocalVocabEntry{I(12)}, v);
  checkDist(U, v, IdOrLocalVocabEntry{lit("NotAPoint")});
  checkDist(U, IdOrLocalVocabEntry{lit("NotAPoint")}, v);

  auto polygonCentroid = GP({3, 3});
  checkCentroid(IdOrLocalVocabEntry{lit(
                    "\"POLYGON((2 4, 4 4, 4 2, 2 2))\"",
                    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>")},
                polygonCentroid);

  checkEnvelope(
      IdOrLocalVocabEntryVec{U, D(1.0), GP({4, 2}),
                             geoLit("LINESTRING(2 4, 8 8)")},
      IdOrLocalVocabEntryVec{U, U, geoLit("POLYGON((2 4,2 4,2 4,2 4,2 4))"),
                             geoLit("POLYGON((2 4,8 4,8 8,2 8,2 4))")});
  checkEnvelopeLL(IdOrLocalVocabEntryVec{U, D(1.0), GP({4, 2}),
                                         geoLit("LINESTRING(2 4, 8 8)")},
                  Ids{U, U, GP({4, 2}), GP({4, 2})});
  checkEnvelopeUR(IdOrLocalVocabEntryVec{U, D(1.0), GP({4, 2}),
                                         geoLit("LINESTRING(2 4, 8 8)")},
                  Ids{U, U, GP({4, 2}), GP({8, 8})});

  auto sfGeoType = [](std::string_view type) {
    return lit(absl::StrCat("http://www.opengis.net/ont/sf#", type),
               "^^<http://www.w3.org/2001/XMLSchema#anyURI>");
  };
  checkGeometryType(
      IdOrLocalVocabEntryVec{U, D(0.0), v, geoLit("LINESTRING(2 2, 4 4)"),
                             geoLit("POLYGON((2 4, 4 4, 4 2, 2 2))"),
                             geoLit("BLABLIBLU(1 1, 2 2)")},
      IdOrLocalVocabEntryVec{U, U, sfGeoType("Point"), sfGeoType("LineString"),
                             sfGeoType("Polygon"), U});

  // Bounding coordinate expressions
  using enum ad_utility::BoundingCoordinate;
  auto checkMinX =
      testUnaryExpression<&makeBoundingCoordinateExpression<MIN_X>>;
  auto checkMinY =
      testUnaryExpression<&makeBoundingCoordinateExpression<MIN_Y>>;
  auto checkMaxX =
      testUnaryExpression<&makeBoundingCoordinateExpression<MAX_X>>;
  auto checkMaxY =
      testUnaryExpression<&makeBoundingCoordinateExpression<MAX_Y>>;

  const IdOrLocalVocabEntryVec exampleGeoms{
      U,
      D(0.0),
      v,  // POINT(24.3, 26.8)
      geoLit("LINESTRING(2 8, 4 6)"),
      geoLit("POLYGON((2 4, 4 4, 4 2, 2 2, 2 4))"),
      lit("BLABLIBLU(1 1, 2 2)"),
      geoLit("BLABLIBLU(1 1, 2 2)"),
      geoLit("LINESTRING(-5000 0, 1 2)"),
  };
  checkMinX(exampleGeoms, Ids{U, U, D(24.3), D(2), D(2), U, U, U});
  checkMinY(exampleGeoms, Ids{U, U, D(26.8), D(6), D(2), U, U, U});
  checkMaxX(exampleGeoms, Ids{U, U, D(24.3), D(4), D(4), U, U, U});
  checkMaxY(exampleGeoms, Ids{U, U, D(26.8), D(8), D(4), U, U, U});

  auto checkNumGeometries = testUnaryExpression<&makeNumGeometriesExpression>;
  checkNumGeometries(exampleGeoms, Ids{U, U, I(1), I(1), I(1), U, U, I(1)});
  const IdOrLocalVocabEntryVec exampleMultiGeoms{
      geoLit("MULTIPOINT(1 2, 3 4, 5 6, 7 8)"), geoLit("MULTIPOINT(1 2)"),
      geoLit("MULTILINESTRING((1 2, 3 4),(5 6, 7 8, 9 0))"),
      geoLit("MULTIPOLYGON(((1 2, 3 4, 1 2)),((1 2, 3 4, 1 2.5)),((5 6, 7 8, 9 "
             "0, 5 6), (7 6, 5 4, 7 6)))"),
      geoLit("GEOMETRYCOLLECTION(POINT(1 2), LINESTRING(2 8, 4 6))")};
  checkNumGeometries(exampleMultiGeoms, Ids{I(4), I(1), I(2), I(3), I(2)});

  // Since our helpers test doubles for (near) equality and precise lengths
  // depend on the method of calculation, which is not what is tested here, we
  // derive the expected values using the helper.
  auto expectedLength = [](std::string_view literal) -> double {
    auto len = ad_utility::GeometryInfo::getMetricLength(
        absl::StrCat("\"", literal,
                     "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"));
    if (!len.has_value()) {
      return -1;
    }
    return len.value().length();
  };

  auto checkLength = std::bind_front(testNaryExpression, &makeLengthExpression);
  auto checkMetricLength = testUnaryExpression<&makeMetricLengthExpression>;
  const auto kilometer = lit("http://qudt.org/vocab/unit/KiloM",
                             "^^<http://www.w3.org/2001/XMLSchema#anyURI>");

  static constexpr std::string_view line =
      "LINESTRING(7.8412948 47.9977308, 7.8450491 47.9946)";
  const auto expLine = expectedLength(line);
  static constexpr std::string_view polygon =
      "POLYGON((7.8412948 47.9977308, 7.8450491 47.9946, 7.852918 "
      "47.995562, 7.8412948 47.9977308))";
  const auto expPolygon = expectedLength(polygon);
  static constexpr std::string_view collection =
      "GEOMETRYCOLLECTION(LINESTRING(7.8412948 47.9977308, 7.8450491 "
      "47.9946), LINESTRING(7.8412948 47.9977308, 7.852918 "
      "47.995562))";
  const auto expCollection = expectedLength(collection);

  const IdOrLocalVocabEntryVec lengthInputs{U,
                                            D(5),
                                            v,
                                            geoLit(line),
                                            geoLit(polygon),
                                            geoLit(collection),
                                            lit("BLABLIBLU()")};
  checkLength(Ids{U, U, D(0.0), D(expLine / 1000), D(expPolygon / 1000),
                  D(expCollection / 1000), U},
              lengthInputs,
              IdOrLocalVocabEntryVec{kilometer, kilometer, kilometer, kilometer,
                                     kilometer, kilometer, kilometer});
  checkMetricLength(lengthInputs, Ids{U, U, D(0.0), D(expLine), D(expPolygon),
                                      D(expCollection), U});

  auto expectedArea = [](std::string_view literal) -> double {
    auto area = ad_utility::GeometryInfo::getMetricArea(
        absl::StrCat("\"", literal,
                     "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"));
    if (!area.has_value() || std::isnan(area.value().area())) {
      return -1;
    }
    return area.value().area();
  };

  // Geometry area functions
  auto checkArea = std::bind_front(testNaryExpression, &makeAreaExpression);
  auto checkMetricArea = testUnaryExpression<&makeMetricAreaExpression>;
  const auto squareKilometer =
      lit("http://qudt.org/vocab/unit/KiloM2",
          "^^<http://www.w3.org/2001/XMLSchema#anyURI>");

  checkArea(
      Ids{U, U, D(0.0), D(0.0), D(expectedArea(polygon) / 1'000'000), D(0.0),
          U},
      lengthInputs,
      IdOrLocalVocabEntryVec{squareKilometer, squareKilometer, squareKilometer,
                             squareKilometer, squareKilometer, squareKilometer,
                             squareKilometer});
  checkMetricArea(lengthInputs, Ids{U, U, D(0.0), D(0.0),
                                    D(expectedArea(polygon)), D(0.0), U});

  auto checkGeometryN =
      std::bind_front(testNaryExpression, &makeGeometryNExpression);
  // Non-geometry types
  checkGeometryN(IdOrLocalVocabEntryVec{U, U, U, U},
                 Ids{D(5), I(3), B(true), U}, Ids{I(1), U, D(5), I(2)});
  // Extract n-th geometry: Single and invalid geometries.
  checkGeometryN(
      IdOrLocalVocabEntryVec{
          U,
          U,
          geoLit("POINT(24.3 26.8)"),
          geoLit("LINESTRING(2 8,4 6)"),
          geoLit("POLYGON((2 2,4 2,4 4,2 4,2 2))"),
          U,
          U,
          geoLit("LINESTRING(-5000 0,1 2)"),
      },
      exampleGeoms, Ids{I(1), I(1), I(1), I(1), I(1), I(1), I(1), I(1)});
  checkGeometryN(IdOrLocalVocabEntryVec{U, U, U, U, U, U, U, U}, exampleGeoms,
                 Ids{I(0), I(0), I(0), I(0), I(0), I(0), I(0), I(0)});
  // Extract n-th geometry: Collection types.
  checkGeometryN(
      IdOrLocalVocabEntryVec{
          geoLit("POINT(1 2)"),
          geoLit("POINT(1 2)"),
          geoLit("LINESTRING(1 2,3 4)"),
          geoLit("POLYGON((1 2,3 4,1 2))"),
          geoLit("POINT(1 2)"),
      },
      exampleMultiGeoms, Ids{I(1), I(1), I(1), I(1), I(1)});
  checkGeometryN(
      IdOrLocalVocabEntryVec{
          geoLit("POINT(3 4)"),
          U,
          geoLit("LINESTRING(5 6,7 8,9 0)"),
          geoLit("POLYGON((1 2,3 4,1 2.5,1 2))"),
          geoLit("LINESTRING(2 8,4 6)"),
      },
      exampleMultiGeoms, Ids{I(2), I(2), I(2), I(2), I(2)});
}

// ________________________________________________________________________________________
TEST(SparqlExpression, ifAndCoalesce) {
  auto checkIf = TestNaryExpressionVec<&makeIfExpression>{};
  auto checkCoalesce = TestNaryExpressionVec<makeCoalesceExpressionVariadic>{};

  const auto T = Id::makeFromBool(true);
  const auto F = Id::makeFromBool(false);

  checkIf(idOrLitOrStringVec({I(0), "eins", I(2), I(3), U, "fünf"}),
          // The empty string is considered to be `false`.
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
                 IdOrLocalVocabEntry{lit("eins")}, Ids{U, U, U, U, U, D(5.0)}});

  // Check COALESCE with no arguments or empty arguments.
  checkCoalesce(IdOrLocalVocabEntryVec{}, std::tuple{});
  checkCoalesce(IdOrLocalVocabEntryVec{}, std::tuple{Ids{}});
  checkCoalesce(IdOrLocalVocabEntryVec{}, std::tuple{Ids{}, Ids{}});
  checkCoalesce(IdOrLocalVocabEntryVec{}, std::tuple{Ids{}, Ids{}, Ids{}});

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
  auto checkConcat = TestNaryExpressionVec<makeConcatExpressionVariadic>{};

  const auto T = Id::makeFromBool(true);

  checkConcat(idOrLitOrStringVec({U, U, U}),
              std::tuple{Ids{U, I(0), U}, Ids{U, U, I(0)}});

  //  CONCAT function explicitly takes string literals according to the sparql
  //  standard.
  checkConcat(
      idOrLitOrStringVec({"0null0", U, U, U, U}),
      std::tuple{idOrLitOrStringVec({"0", I(1), "zwei", T, D(5.3)}),
                 idOrLitOrStringVec({"null", "eins", I(2), "true", ""}),
                 idOrLitOrStringVec({"0", "EINS", "ZWEI", "TRUE", D(5.2)})});
  // Example with some constants in the middle.
  checkConcat(idOrLitOrStringVec({"0trueeins", U, "2trueeins", "3trueeins", U,
                                  "12.3trueeins-2.1"}),
              std::tuple{idOrLitOrStringVec({"0", U, "2", "3", "", "12.3"}),
                         IdOrLocalVocabEntry{lit("true")},
                         IdOrLocalVocabEntry{lit("eins")},
                         idOrLitOrStringVec({"", "", "", "", I(4), "-2.1"})});

  // Only constants
  checkConcat(IdOrLocalVocabEntry{lit("trueMe1")},
              std::tuple{IdOrLocalVocabEntry{lit("true")},
                         IdOrLocalVocabEntry{lit("Me")},
                         IdOrLocalVocabEntry{lit("1")}});
  // Constants at the beginning.
  checkConcat(IdOrLocalVocabEntryVec{lit("trueMe1"), lit("trueMe2")},
              std::tuple{IdOrLocalVocabEntry{lit("true")},
                         IdOrLocalVocabEntry{lit("Me")},
                         idOrLitOrStringVec({"1", "2"})});

  checkConcat(IdOrLocalVocabEntry{lit("")}, std::tuple{});
  auto coalesceExpr = makeConcatExpressionVariadic(
      std::make_unique<IriExpression>(iri("<bim>")),
      std::make_unique<IriExpression>(iri("<bam>")));
  ASSERT_THAT(coalesceExpr->getCacheKey({}),
              testing::AllOf(::testing::ContainsRegex("ConcatExpression"),
                             ::testing::HasSubstr("<bim>"),
                             ::testing::HasSubstr("<bam>")));
  // Only constants with datatypes or language tags.
  checkConcat(
      IdOrLocalVocabEntry{lit("HelloWorld")},
      std::tuple{IdOrLocalVocabEntry{lit(
                     "Hello", "^^<http://www.w3.org/2001/XMLSchema#string>")},
                 IdOrLocalVocabEntry{lit(
                     "World", "^^<http://www.w3.org/2001/XMLSchema#string>")}});
  checkConcat(IdOrLocalVocabEntry{lit("HelloWorld", "@en")},
              std::tuple{IdOrLocalVocabEntry{lit("Hello", "@en")},
                         IdOrLocalVocabEntry{lit("World", "@en")}});
  checkConcat(
      IdOrLocalVocabEntry{lit("HelloWorld")},
      std::tuple{IdOrLocalVocabEntry{lit(
                     "Hello", "^^<http://www.w3.org/2001/XMLSchema#string>")},
                 IdOrLocalVocabEntry{lit("World")}});
  checkConcat(IdOrLocalVocabEntry{lit("HelloWorld")},
              std::tuple{IdOrLocalVocabEntry{lit("Hello", "@de")},
                         IdOrLocalVocabEntry{lit("World")}});
  checkConcat(IdOrLocalVocabEntry{lit("HelloWorld")},
              std::tuple{IdOrLocalVocabEntry{lit("Hello", "@de")},
                         IdOrLocalVocabEntry{lit("World", "@en")}});
  checkConcat(
      IdOrLocalVocabEntry{lit("HelloWorld")},
      std::tuple{IdOrLocalVocabEntry{lit(
                     "Hello", "^^<http://www.w3.org/2001/XMLSchema#string>")},
                 IdOrLocalVocabEntry{lit("World", "@en")}});

  // Constants at beginning with datatypes or language tags.
  checkConcat(
      IdOrLocalVocabEntryVec{lit("MeHello", "@en"), lit("MeWorld"),
                             lit("MeHallo")},
      std::tuple{IdOrLocalVocabEntry{lit("Me", "@en")},
                 IdOrLocalVocabEntryVec{lit("Hello", "@en"), lit("World"),
                                        lit("Hallo", "@de")}});
  checkConcat(
      IdOrLocalVocabEntryVec{lit("MeHello"), lit("MeWorld"), lit("MeHallo")},
      std::tuple{
          IdOrLocalVocabEntry{
              lit("Me", "^^<http://www.w3.org/2001/XMLSchema#string>")},
          IdOrLocalVocabEntryVec{
              lit("Hello", "^^<http://www.w3.org/2001/XMLSchema#string>"),
              lit("World"), lit("Hallo", "@de")}});

  // Vector at the beginning with datatypes or language tags.
  checkConcat(
      IdOrLocalVocabEntryVec{lit("HelloWorld"), lit("HiWorld"),
                             lit("HalloWorld")},
      std::tuple{
          IdOrLocalVocabEntryVec{
              lit("Hello", "^^<http://www.w3.org/2001/XMLSchema#string>"),
              lit("Hi"), lit("Hallo", "@de")},
          IdOrLocalVocabEntry{
              lit("World", "^^<http://www.w3.org/2001/XMLSchema#string>")}});
  checkConcat(IdOrLocalVocabEntryVec{lit("HelloWorld", "@en"), lit("HiWorld"),
                                     lit("HalloWorld")},
              std::tuple{IdOrLocalVocabEntryVec{lit("Hello", "@en"), lit("Hi"),
                                                lit("Hallo", "@de")},
                         IdOrLocalVocabEntry{lit("World", "@en")}});
  checkConcat(IdOrLocalVocabEntryVec{lit("HelloWorld!"), lit("HalloWorld!")},
              std::tuple{IdOrLocalVocabEntryVec{lit("Hello", "@en"),
                                                lit("Hallo", "@de")},
                         IdOrLocalVocabEntry{lit("World", "@en")},
                         IdOrLocalVocabEntry{lit("!", "@fr")}});
}

// ______________________________________________________________________________
TEST(SparqlExpression, ReplaceExpression) {
  auto makeReplaceExpressionThreeArgs = [](auto&& arg0, auto&& arg1,
                                           auto&& arg2) {
    return makeReplaceExpression(AD_FWD(arg0), AD_FWD(arg1), AD_FWD(arg2),
                                 nullptr);
  };
  auto checkReplace = TestNaryExpressionVec<makeReplaceExpressionThreeArgs>{};
  auto checkReplaceWithFlags = TestNaryExpressionVec<&makeReplaceExpression>{};
  // A simple replace( no regexes involved).
  checkReplace(
      idOrLitOrStringVec({"null", "Eins", "zwEi", "drEi", U, U}),
      std::tuple{idOrLitOrStringVec({"null", "eins", "zwei", "drei", U, U}),
                 IdOrLocalVocabEntry{lit("e")}, IdOrLocalVocabEntry{lit("E")}});
  // A somewhat more involved regex
  checkReplace(
      idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}),
      std::tuple{idOrLitOrStringVec({"null", "eins", "zwei", "drei", U, U}),
                 IdOrLocalVocabEntry{lit("e.[a-z]")},
                 IdOrLocalVocabEntry{lit("X")}});
  // A regex with replacement with substitutions
  checkReplace(idOrLitOrStringVec({R"("$1 \\2 A \\bc")", R"("$1 \\2 DE \\f")"}),
               std::tuple{idOrLitOrStringVec({"Abc", "DEf"}),
                          IdOrLocalVocabEntry{lit("([A-Z]+)")},
                          IdOrLocalVocabEntry{lit(R"("\\$1 \\2 $1 \\")")}});

  // Only simple literals should be replaced.
  checkReplace(idOrLitOrStringVec({U, U}),
               std::tuple{idOrLitOrStringVec({"Abc", "DEf"}),
                          IdOrLocalVocabEntry{lit("([A-Z]+)")},
                          IdOrLocalVocabEntry{Id::makeFromBool(true)}});

  // Case-insensitive matching using the hack for google regex:
  checkReplace(idOrLitOrStringVec({"null", "xxns", "zwxx", "drxx"}),
               std::tuple{idOrLitOrStringVec({"null", "eIns", "zwEi", "drei"}),
                          IdOrLocalVocabEntry{lit("(?i)[ei]")},
                          IdOrLocalVocabEntry{lit("x")}});

  // Case-insensitive matching using the flag
  checkReplaceWithFlags(
      idOrLitOrStringVec({"null", "xxns", "zwxx", "drxx"}),
      std::tuple{idOrLitOrStringVec({"null", "eIns", "zwEi", "drei"}),
                 IdOrLocalVocabEntry{lit("[ei]")},
                 IdOrLocalVocabEntry{lit("x")}, IdOrLocalVocabEntry{lit("i")}});

  // Matching using the all the flags
  checkReplaceWithFlags(
      idOrLitOrStringVec({"null", "xxns", "zwxx", "drxx"}),
      std::tuple{idOrLitOrStringVec({"null", "eIns", "zwEi", "drei"}),
                 IdOrLocalVocabEntry{lit("[ei]")},
                 IdOrLocalVocabEntry{lit("x")},
                 IdOrLocalVocabEntry{lit("imsU")}});
  // Empty flag
  checkReplaceWithFlags(
      idOrLitOrStringVec({"null", "xIns", "zwEx", "drxx"}),
      std::tuple{idOrLitOrStringVec({"null", "eIns", "zwEi", "drei"}),
                 IdOrLocalVocabEntry{lit("[ei]")},
                 IdOrLocalVocabEntry{lit("x")}, IdOrLocalVocabEntry{lit("")}});

  // Multiple matches within the same string
  checkReplace(IdOrLocalVocabEntry{lit("wEeDEflE")},
               std::tuple{IdOrLocalVocabEntry{lit("weeeDeeflee")},
                          IdOrLocalVocabEntry{lit("ee")},
                          IdOrLocalVocabEntry{lit("E")}});

  // Illegal regex.
  checkReplace(
      IdOrLocalVocabEntryVec{U, U, U, U, U, U},
      std::tuple{idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}),
                 IdOrLocalVocabEntry{lit("e.[a-z")},
                 IdOrLocalVocabEntry{lit("X")}});
  // Undefined regex
  checkReplace(
      IdOrLocalVocabEntryVec{U, U, U, U, U, U},
      std::tuple{idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}), U,
                 IdOrLocalVocabEntry{lit("X")}});

  checkReplaceWithFlags(
      IdOrLocalVocabEntryVec{U, U, U, U, U, U},
      std::tuple{idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}), U,
                 IdOrLocalVocabEntry{lit("X")}, IdOrLocalVocabEntry{lit("i")}});

  // Undefined flags
  checkReplaceWithFlags(
      IdOrLocalVocabEntryVec{U, U, U, U, U, U},
      std::tuple{idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}),
                 IdOrLocalVocabEntry{lit("[ei]")},
                 IdOrLocalVocabEntry{lit("X")}, U});

  using ::testing::HasSubstr;
  // Invalid flags
  checkReplaceWithFlags(
      IdOrLocalVocabEntryVec{U},
      std::tuple{idOrLitOrStringVec({"null"}), IdOrLocalVocabEntry{lit("[n]")},
                 IdOrLocalVocabEntry{lit("X")},
                 IdOrLocalVocabEntry{lit("???")}});

  // Illegal replacement.
  checkReplace(
      IdOrLocalVocabEntryVec{U, U, U, U, U, U},
      std::tuple{idOrLitOrStringVec({"null", "Xs", "zwei", "drei", U, U}),
                 IdOrLocalVocabEntry{lit("e")}, Id::makeUndefined()});

  // Datatype or language tag
  checkReplace(
      IdOrLocalVocabEntryVec{lit("Eins"), lit("zwEi", "@en")},
      std::tuple{IdOrLocalVocabEntryVec{
                     lit("eins", "^^<http://www.w3.org/2001/XMLSchema#string>"),
                     lit("zwei", "@en")},
                 IdOrLocalVocabEntry{lit("e")}, IdOrLocalVocabEntry{lit("E")}});
}

// ______________________________________________________________________________
TEST(SparqlExpression, literalExpression) {
  TestContext ctx;
  StringLiteralExpression expr{TripleComponent::Literal::fromEscapedRdfLiteral(
      "\"notInTheVocabulary\"")};
  // Evaluate multiple times to test the caching behavior.
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(
        (ExpressionResult{IdOrLocalVocabEntry{lit("\"notInTheVocabulary\"")}}),
        expr.evaluate(&ctx.context));
  }
  // A similar test with a constant entry that is part of the vocabulary and can
  // therefore be converted to an ID.
  IriExpression iriExpr{TripleComponent::Iri::fromIriref("<x>")};
  Id idOfX = ctx.x;
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ((ExpressionResult{IdOrLocalVocabEntry{idOfX}}),
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
  using IoS = IdOrLocalVocabEntry;
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
  using namespace ::testing;
  ParsedQuery pq;
  pq.selectClause().addVisibleVariable(Variable{"?testVar42"});
  pq.selectClause().setAsterisk();
  ExistsExpression exists{std::move(pq)};
  auto var = exists.variable();
  TestContext context;
  EXPECT_ANY_THROW(exists.evaluate(&context.context));
  using namespace testing;
  EXPECT_THAT(exists.getCacheKey(context.varToColMap),
              HasSubstr("Uninitialized Exists"));
  context.varToColMap[var] = makeAlwaysDefinedColumn(437);
  EXPECT_TRUE(exists.isResultAlwaysDefined(context.varToColMap));
  EXPECT_THAT(exists.evaluate(&context.context), VariantWith<Variable>(var));
  EXPECT_THAT(exists.getCacheKey(context.varToColMap),
              HasSubstr("ExistsExpression col# 437"));
  EXPECT_THAT(exists.containedVariables(),
              ElementsAre(Pointee(Eq(Variable{"?testVar42"}))));
}

// _____________________________________________________________________________
TEST(OrExpression, getLanguageFilterExpression) {
  using LFD = SparqlExpression::LangFilterData;
  using namespace ::testing;
  auto lit = ad_utility::testing::tripleComponentLiteral;

  auto makeSimpleLangFilter = [&](std::string_view language,
                                  Variable variable) {
    auto sle = std::make_unique<StringLiteralExpression>(lit(language));
    auto le = makeLangExpression(
        std::make_unique<VariableExpression>(std::move(variable)));
    return std::make_unique<EqualExpression>(
        std::array<SparqlExpression::Ptr, 2>{std::move(sle), std::move(le)});
  };
  // Simple case
  {
    auto oe = makeOrExpression(makeSimpleLangFilter("\"en\"", Variable{"?x"}),
                               makeSimpleLangFilter("\"de\"", Variable{"?x"}));
    EXPECT_THAT(
        oe->getLanguageFilterExpression(),
        Optional(AllOf(AD_FIELD(LFD, variable_, Eq(Variable{"?x"})),
                       AD_FIELD(LFD, languages_,
                                UnorderedElementsAre(Eq("en"), Eq("de"))))));
  }
  // Simple case with deduplication
  {
    auto sle1 = std::make_unique<StringLiteralExpression>(lit("\"en\""));
    auto le1 = makeLangExpression(
        std::make_unique<VariableExpression>(Variable{"?x"}));
    auto sle2 = std::make_unique<StringLiteralExpression>(lit("\"en\""));
    auto le2 = makeLangExpression(
        std::make_unique<VariableExpression>(Variable{"?x"}));
    auto oe = makeOrExpression(makeSimpleLangFilter("\"en\"", Variable{"?x"}),
                               makeSimpleLangFilter("\"en\"", Variable{"?x"}));
    EXPECT_THAT(oe->getLanguageFilterExpression(),
                Optional(AllOf(AD_FIELD(LFD, variable_, Eq(Variable{"?x"})),
                               AD_FIELD(LFD, languages_,
                                        UnorderedElementsAre(Eq("en"))))));
  }
  // Complicated case with deduplication and IN
  {
    auto makeMultiLangFilter =
        [&](const std::vector<std::string_view>& languages, Variable variable) {
          std::vector<SparqlExpression::Ptr> children;
          for (std::string_view language : languages) {
            children.push_back(
                std::make_unique<StringLiteralExpression>(lit(language)));
          }
          auto le = makeLangExpression(
              std::make_unique<VariableExpression>(std::move(variable)));
          return std::make_unique<InExpression>(std::move(le),
                                                std::move(children));
        };
    auto oe = makeOrExpression(
        makeMultiLangFilter({"\"\"", "\"mul\"", "\"en\""}, Variable{"?x"}),
        makeOrExpression(
            makeSimpleLangFilter("\"en\"", Variable{"?x"}),
            makeOrExpression(
                makeSimpleLangFilter("\"\"", Variable{"?x"}),
                makeMultiLangFilter({"\"de\"", "\"en\""}, Variable{"?x"}))));
    EXPECT_THAT(
        oe->getLanguageFilterExpression(),
        Optional(AllOf(AD_FIELD(LFD, variable_, Eq(Variable{"?x"})),
                       AD_FIELD(LFD, languages_,
                                UnorderedElementsAre(Eq(""), Eq("mul"),
                                                     Eq("en"), Eq("de"))))));
  }

  // Non-matching variables
  {
    auto oe = makeOrExpression(makeSimpleLangFilter("\"en\"", Variable{"?x"}),
                               makeSimpleLangFilter("\"de\"", Variable{"?y"}));
    EXPECT_EQ(oe->getLanguageFilterExpression(), std::nullopt);
  }

  // Left side no language filter
  {
    auto ie = std::make_unique<IdExpression>(Id::makeFromBool(true));
    auto oe = makeOrExpression(std::move(ie),
                               makeSimpleLangFilter("\"de\"", Variable{"?x"}));
    EXPECT_EQ(oe->getLanguageFilterExpression(), std::nullopt);
  }

  // Right side no language filter
  {
    auto ie = std::make_unique<IdExpression>(Id::makeFromBool(true));
    auto oe = makeOrExpression(makeSimpleLangFilter("\"de\"", Variable{"?x"}),
                               std::move(ie));
    EXPECT_EQ(oe->getLanguageFilterExpression(), std::nullopt);
  }
  // Should not apply to AND
  {
    auto ae = makeAndExpression(makeSimpleLangFilter("\"en\"", Variable{"?x"}),
                                makeSimpleLangFilter("\"de\"", Variable{"?x"}));
    EXPECT_EQ(ae->getLanguageFilterExpression(), std::nullopt);
  }
}

// We set up a simple type-erased expression to also test the type erased
// expressions when the remainder of the code uses more strongly typed
// expressions.
struct BinaryOrForTypeErasure {
  auto operator()(bool a, bool b) const { return Id::makeFromBool(a || b); }
};

struct BinaryAndForTypeErasure {
  auto operator()(bool a, bool b) const { return Id::makeFromBool(a && b); }
};

template <typename Func,
          typename ValueGetter = sparqlExpression::detail::IsValidValueGetter>
constexpr auto makeTypeErasedExpression =
    [](SparqlExpression::Ptr child1, SparqlExpression::Ptr child2) {
      using namespace sparqlExpression::detail;
      using Expr = NaryExpressionTypeErased<
          sparqlExpression::detail::Operation<2, FV<Func, ValueGetter>>>;
      return std::make_unique<Expr>(std::move(child1), std::move(child2));
    };

auto makeTypeErasedAndExpression =
    makeTypeErasedExpression<BinaryAndForTypeErasure>;
auto makeTypeErasedOrExpression =
    makeTypeErasedExpression<BinaryOrForTypeErasure>;
// Same function, but different value getter. Used for testing the cache keys.
auto makeTypeErasedOrAlwaysTrue =
    makeTypeErasedExpression<BinaryOrForTypeErasure,
                             sparqlExpression::detail::AlwaysTrueValueGetter>;

// Test the functionality + interface of simple type erased expressions.
TEST(NaryExpressionTypeErased, basicTests) {
  auto makeOr = makeTypeErasedOrExpression;
  auto makeAnd = makeTypeErasedAndExpression;
  auto testOrTe = testBinaryExpressionCommutative<makeTypeErasedOrExpression>;
  // Note: as we use the `IsValidValueGetter` (for simplicity, as it returns a
  // plain `bool`), the only inputs that are counted as `false` are `UNDEF` and
  // `NaN`.
  testOrTe(B(true), B(true), B(true));
  testOrTe(B(true), U, B(true));
  testOrTe(B(false), U, U);
  // test with a non-constant result.
  testOrTe(V<Id>({B(false), B(true)}, alloc), U, V<Id>({U, B(true)}, alloc));

  auto c1 = []() {
    return std::make_unique<IdExpression>(Id::makeFromBool(true));
  };
  auto c2 = []() {
    return std::make_unique<VariableExpression>(Variable{"?x"});
  };

  auto expr = makeTypeErasedOrExpression(c1(), c2());
  using namespace ::testing;

  using namespace ad_utility::use_type_identity;
  auto typeMatcher = [](auto tp) {
    using Tp = typename decltype(tp)::type;
    return Pointer(WhenDynamicCastTo<const Tp*>(NotNull()));
  };

  EXPECT_THAT(expr->children(),
              ElementsAre(typeMatcher(ti<IdExpression>),
                          typeMatcher(ti<VariableExpression>)));

  VariableToColumnMap varColMap;
  varColMap[Variable{"?x"}] = makeAlwaysDefinedColumn(42);

  auto getKey = [&varColMap](const SparqlExpression::Ptr& ptr) {
    return ptr->getCacheKey(varColMap);
  };

  // Changing the function and changing the children should all change the
  // signature
  std::vector<SparqlExpression::Ptr> exprs;
  exprs.push_back(makeOr(c1(), c2()));
  exprs.push_back(makeOr(c1(), c1()));
  exprs.push_back(makeOr(c2(), c1()));
  exprs.push_back(makeAnd(c2(), c1()));
  // Same function, same children, but different `ValueGetter`, should also
  // change the cache key.
  exprs.push_back(makeTypeErasedOrAlwaysTrue(c2(), c1()));
  EXPECT_THAT(exprs, AllUniqueBy(getKey));
}

}  // anonymous namespace

//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <limits>
#include <string>

#include "./SparqlExpressionTestHelpers.h"
#include "./util/AllocatorTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "index/Index.h"

using namespace sparqlExpression;
using namespace ad_utility::testing;
using ad_utility::source_location;
using namespace std::literals;
using enum valueIdComparators::Comparison;
using valueIdComparators::Comparison;

// First some internal helper functions and constants.
namespace {

auto lit = [](std::string_view s) {
  std::string input =
      s.starts_with('"') ? std::string{s} : absl::StrCat("\"", s, "\"");
  return ad_utility::triple_component::LiteralOrIri(
      tripleComponentLiteral(input));
};

auto iriref = [](std::string_view s) {
  return ad_utility::triple_component::LiteralOrIri(iri(s));
};

// Convenient access to constants for "infinity" and "not a number". The
// spelling `NaN` was chosen because `nan` conflicts with the standard library.
const auto inf = std::numeric_limits<double>::infinity();
const auto NaN = std::numeric_limits<double>::quiet_NaN();

// Create and return a `RelationalExpression` with the given Comparison and the
// given operands `leftValue` and `rightValue`.
template <Comparison comp>
auto makeExpression(SingleExpressionResult auto leftValue,
                    SingleExpressionResult auto rightValue) {
  auto leftChild = std::make_unique<DummyExpression>(std::move(leftValue));
  auto rightChild = std::make_unique<DummyExpression>(std::move(rightValue));

  return relational::RelationalExpression<comp>(
      {std::move(leftChild), std::move(rightChild)});
}

// Evaluate the given `expression` on a `TestContext` (see above).
auto evaluateOnTestContext = [](const SparqlExpression& expression) {
  TestContext context{};
  return expression.evaluate(&context.context);
};

// If `input` has a `.clone` member function, return the result of that
// function. Otherwise, return a copy of `input` (via the copy constructor).
// Used to generically create copies for `VectorWithMemoryLimit` (clone()
// method, but no copy constructor) and the other `SingleExpressionResult`s
// (which have a copy constructor, but no `.clone()` method. The name `makeCopy`
// was chosen because `clone` conflicts with the standard library.
auto makeCopy = [](const auto& input) {
  if constexpr (requires { input.clone(); }) {
    return input.clone();
  } else {
    return input;
  }
};

template <typename T>
VectorWithMemoryLimit<ValueId> makeValueIdVector(
    const VectorWithMemoryLimit<T>& vec);

// Convert `t` into a `ValueId`. `T` must be `double`, `int64_t`, or a
// `VectorWithMemoryLimit` of any of those types.
auto liftToValueId = []<typename T>(const T& t) {
  if constexpr (SingleExpressionResult<T>) {
    if constexpr (ad_utility::isInstantiation<T, VectorWithMemoryLimit>) {
      return t.clone();
    } else {
      return t;
    }
  } else if constexpr (std::is_same_v<T, double>) {
    return DoubleId(t);
  } else if constexpr (std::is_integral_v<T>) {
    return IntId(t);
  } else if constexpr (std::is_same_v<T, Id>) {
    return t;
  } else if constexpr (std::is_same_v<T, VectorWithMemoryLimit<Id>>) {
    return t.clone();
  } else if constexpr (ad_utility::isInstantiation<T, VectorWithMemoryLimit>) {
    return makeValueIdVector(t);
  } else {
    static_assert(ad_utility::alwaysFalse<T>);
  }
};

// Convert a vector of doubles into a vector of `ValueId`s that stores the
// values of the original vector.
template <typename T>
VectorWithMemoryLimit<ValueId> makeValueIdVector(
    const VectorWithMemoryLimit<T>& vec) {
  VectorWithMemoryLimit<ValueId> result{makeAllocator()};
  for (const auto& d : vec) {
    result.push_back(liftToValueId(d));
  }
  return result;
}

auto expectTrue = [](const ExpressionResult& result,
                     source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto id = std::get<Id>(result);
  EXPECT_EQ(id.getDatatype(), Datatype::Bool);
  EXPECT_TRUE(id.getBool());
};

auto expectFalse = [](const ExpressionResult& result,
                      source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  auto id = std::get<Id>(result);
  EXPECT_EQ(id.getDatatype(), Datatype::Bool);
  EXPECT_FALSE(id.getBool());
};

// Assert that the given `expression`, when evaluated on the `TestContext` (see
// above), has a single boolean result that is true.
auto expectTrueBoolean = [](const SparqlExpression& expression,
                            source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectTrueBoolean was called here");
  auto result = evaluateOnTestContext(expression);
  auto id = std::get<Id>(result);
  EXPECT_EQ(id.getDatatype(), Datatype::Bool);
  EXPECT_TRUE(id.getBool());
};

// Similar to `expectTrueBoolean`, but assert that the boolean is `false`.
auto expectFalseBoolean = [](const SparqlExpression& expression,
                             source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectFalseBoolean was called here");
  auto result = evaluateOnTestContext(expression);
  auto id = std::get<Id>(result);
  EXPECT_EQ(id.getDatatype(), Datatype::Bool);
  EXPECT_FALSE(id.getBool());
};

auto expectUndefined = [](const SparqlExpression& expression,
                          source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectUndefined was called here");
  auto result = evaluateOnTestContext(expression);
  if (std::holds_alternative<Id>(result)) {
    auto id = std::get<Id>(result);
    EXPECT_EQ(id, Id::makeUndefined());
  } else {
    AD_CORRECTNESS_CHECK(
        (std::holds_alternative<VectorWithMemoryLimit<Id>>(result)));
    const auto& vec = std::get<VectorWithMemoryLimit<Id>>(result);
    EXPECT_TRUE(std::ranges::all_of(
        vec, [](Id id) { return id == Id::makeUndefined(); }));
  }
};

// Run tests for all the different comparisons on constants of type `T` and `U`
// (e.g. numeric or string constants). In the first pair `lessThanPair` the
// first entry must be less than the second. In `greaterThanPair` the first
// entry has to be greater than the second. In `equalPair`, the two entries have
// to be equalPair.
template <typename T, typename U>
auto testLessThanGreaterThanEqualHelper(
    std::pair<T, U> lessThanPair, std::pair<T, U> greaterThanPair,
    std::pair<T, U> equalPair, source_location l = source_location::current()) {
  auto trace = generateLocationTrace(
      l, "testLessThanGreaterThanEqualHelper was called here");
  auto True = expectTrueBoolean;
  auto False = expectFalseBoolean;

  True(makeExpression<LT>(lessThanPair.first, lessThanPair.second));
  True(makeExpression<LE>(lessThanPair.first, lessThanPair.second));
  False(makeExpression<EQ>(lessThanPair.first, lessThanPair.second));
  True(makeExpression<NE>(lessThanPair.first, lessThanPair.second));
  False(makeExpression<GE>(lessThanPair.first, lessThanPair.second));
  False(makeExpression<GT>(lessThanPair.first, lessThanPair.second));

  False(makeExpression<LT>(greaterThanPair.first, greaterThanPair.second));
  False(makeExpression<LE>(greaterThanPair.first, greaterThanPair.second));
  False(makeExpression<EQ>(greaterThanPair.first, greaterThanPair.second));
  True(makeExpression<NE>(greaterThanPair.first, greaterThanPair.second));
  True(makeExpression<GE>(greaterThanPair.first, greaterThanPair.second));
  True(makeExpression<GT>(greaterThanPair.first, greaterThanPair.second));

  False(makeExpression<LT>(equalPair.first, equalPair.second));
  True(makeExpression<LE>(equalPair.first, equalPair.second));
  True(makeExpression<EQ>(equalPair.first, equalPair.second));
  False(makeExpression<NE>(equalPair.first, equalPair.second));
  True(makeExpression<GE>(equalPair.first, equalPair.second));
  False(makeExpression<GT>(equalPair.first, equalPair.second));
}

// Call `testLessThanGreaterThanEqualThan` for the given values and also for the
// following variants: The first element from each pair is converted to a
// `ValueId` before the call; the second element  is ...; both elements are ...
// Requires that both `leftValue` and `rightValue` are numeric constants.
template <typename L, typename R>
void testLessThanGreaterThanEqual(
    std::pair<L, R> lessThanPair, std::pair<L, R> greaterThanPair,
    std::pair<L, R> equalPair, source_location l = source_location::current()) {
  auto trace =
      generateLocationTrace(l, "testLessThanGreaterThanEqual was called here");

  // Helper functions to lift both of the
  // elements of a pair to a valueId.
  auto liftBoth = [](const auto& p) {
    return std::pair{liftToValueId(p.first), liftToValueId(p.second)};
  };

  testLessThanGreaterThanEqualHelper(
      liftBoth(lessThanPair), liftBoth(greaterThanPair), liftBoth(equalPair));
}

// Test that all comparisons between `leftValue` and `rightValue` result in a
// single boolean that is false. The only exception is the `not equal`
// comparison, for which true is expected.
void testNotEqualHelper(auto leftValueIn, auto rightValueIn,
                        source_location l = source_location::current()) {
  auto leftValue = liftToValueId(leftValueIn);
  auto rightValue = liftToValueId(rightValueIn);
  auto trace = generateLocationTrace(l, "testNotEqualHelper was called here");
  auto True = expectTrueBoolean;
  auto False = expectFalseBoolean;

  False(makeExpression<LT>(makeCopy(leftValue), makeCopy(rightValue)));
  False(makeExpression<LE>(makeCopy(leftValue), makeCopy(rightValue)));
  False(makeExpression<EQ>(makeCopy(leftValue), makeCopy(rightValue)));
  True(makeExpression<NE>(makeCopy(leftValue), makeCopy(rightValue)));
  False(makeExpression<GT>(makeCopy(leftValue), makeCopy(rightValue)));
  False(makeExpression<GE>(makeCopy(leftValue), makeCopy(rightValue)));

  False(makeExpression<LT>(makeCopy(rightValue), makeCopy(leftValue)));
  False(makeExpression<LE>(makeCopy(rightValue), makeCopy(leftValue)));
  False(makeExpression<EQ>(makeCopy(rightValue), makeCopy(leftValue)));
  True(makeExpression<NE>(makeCopy(rightValue), makeCopy(leftValue)));
  False(makeExpression<GT>(makeCopy(rightValue), makeCopy(leftValue)));
  False(makeExpression<GE>(makeCopy(rightValue), makeCopy(leftValue)));
}

void testUndefHelper(auto leftValueIn, auto rightValueIn,
                     source_location l = source_location::current()) {
  auto leftValue = liftToValueId(leftValueIn);
  auto rightValue = liftToValueId(rightValueIn);
  auto trace = generateLocationTrace(l, "testUndefHelper was called here");
  auto undef = expectUndefined;

  undef(makeExpression<LT>(makeCopy(leftValue), makeCopy(rightValue)));
  undef(makeExpression<LE>(makeCopy(leftValue), makeCopy(rightValue)));
  undef(makeExpression<EQ>(makeCopy(leftValue), makeCopy(rightValue)));
  undef(makeExpression<NE>(makeCopy(leftValue), makeCopy(rightValue)));
  undef(makeExpression<GT>(makeCopy(leftValue), makeCopy(rightValue)));
  undef(makeExpression<GE>(makeCopy(leftValue), makeCopy(rightValue)));

  undef(makeExpression<LT>(makeCopy(rightValue), makeCopy(leftValue)));
  undef(makeExpression<LE>(makeCopy(rightValue), makeCopy(leftValue)));
  undef(makeExpression<EQ>(makeCopy(rightValue), makeCopy(leftValue)));
  undef(makeExpression<NE>(makeCopy(rightValue), makeCopy(leftValue)));
  undef(makeExpression<GT>(makeCopy(rightValue), makeCopy(leftValue)));
  undef(makeExpression<GE>(makeCopy(rightValue), makeCopy(leftValue)));
}

// Call `testNotEqualHelper` for `leftValue` and `rightValue` and for the
// following combinations: `leftValue` is converted to a ValueID before the
// call. `rightValue` "" both values "" Requires that both `leftValue` and
// `rightValue` are numeric constants.
void testNotEqual(auto leftValue, auto rightValue,
                  source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "testNotEqual was called here");
  testNotEqualHelper(liftToValueId(leftValue), liftToValueId(rightValue));
}

}  // namespace

TEST(RelationalExpression, IntAndDouble) {
  testLessThanGreaterThanEqual<int64_t, double>({3, 3.3}, {4, -inf},
                                                {-12, -12.0});
  testNotEqual(int64_t{3}, NaN);
}

TEST(RelationalExpression, DoubleAndInt) {
  testLessThanGreaterThanEqual<double, int64_t>({3.1, 4}, {4.2, -3},
                                                {-12.0, -12});
  testNotEqual(NaN, int64_t{42});
}

TEST(RelationalExpression, IntAndInt) {
  testLessThanGreaterThanEqual<int64_t, int64_t>({-3, 3}, {4, 3}, {-12, -12});
}

TEST(RelationalExpression, DoubleAndDouble) {
  testLessThanGreaterThanEqual<double, double>({-3.1, -3.0}, {4.2, 4.1},
                                               {-12.83, -12.83});
  testNotEqual(NaN, NaN);
  testNotEqual(12.0, NaN);
  testNotEqual(NaN, -1.23e12);
}

TEST(RelationalExpression, StringAndString) {
  testLessThanGreaterThanEqualHelper<IdOrString, IdOrString>(
      {lit("alpha"), lit("beta")}, {lit("sigma"), lit("delta")},
      {lit("epsilon"), lit("epsilon")});
  // TODO<joka921> These tests only work, when we actually use unicode
  // comparisons for the string based expressions.
  // TODO<joka921> Add an example for strings that are bytewise different but
  // equal on the unicode level (e.g.`ä` vs `a + dots`.
  // testLessThanGreaterThanEqualHelper<IdOrString, IdOrString>({"Alpha",
  // "beta"},
  // {"beta", "äpfel"}, {"xxx", "xxx"});
}

TEST(RelationalExpression, NumericAndStringAreNeverEqual) {
  auto stringVec = VectorWithMemoryLimit<IdOrString>(
      {lit("hallo"), lit("by"), lit("")}, makeAllocator());
  auto intVec =
      VectorWithMemoryLimit<int64_t>({-12365, 0, 12}, makeAllocator());
  auto doubleVec =
      VectorWithMemoryLimit<double>({-12.365, 0, 12.1e5}, makeAllocator());
  testUndefHelper(int64_t{3}, IdOrString{lit("hallo")});
  testUndefHelper(int64_t{3}, IdOrString{lit("3")});
  testUndefHelper(-12.0, IdOrString{lit("hallo")});
  testUndefHelper(-12.0, IdOrString{lit("-12.0")});
  testUndefHelper(intVec.clone(), IdOrString{lit("someString")});
  testUndefHelper(doubleVec.clone(), IdOrString{lit("someString")});
  testUndefHelper(int64_t{3}, stringVec.clone());
  testUndefHelper(intVec.clone(), stringVec.clone());
  testUndefHelper(doubleVec.clone(), stringVec.clone());
}

// At least one of `leftValue`, `rightValue` must be a vector, the other one may
// be a constant or also a vector. The vectors must have 9 elements each. When
// comparing the `leftValue` and `rightValue` elementwise, the following has to
// hold: For i in [0, 2] : rightValue[i] < leftValue[i]; For i in [3, 5] :
// rightValue[i] > leftValue[i]; For i in [6, 8] : rightValue[i] = leftValue[i];
template <typename T, typename U>
void testLessThanGreaterThanEqualMultipleValuesHelper(
    T leftValue, U rightValue, source_location l = source_location::current()) {
  auto trace = generateLocationTrace(
      l, "testLessThanGreaterThanEqualMultipleValuesHelper was called here");

  TestContext testContext;
  sparqlExpression::EvaluationContext* context = &testContext.context;
  AD_CONTRACT_CHECK(rightValue.size() == 9);
  context->_beginIndex = 0;
  context->_endIndex = 9;

  auto m = [&]<auto comp>() {
    auto expression =
        makeExpression<comp>(makeCopy(leftValue), makeCopy(rightValue));
    auto resultAsVariant = expression.evaluate(context);
    auto expressionInverted =
        makeExpression<comp>(makeCopy(rightValue), makeCopy(leftValue));
    auto resultAsVariantInverted = expressionInverted.evaluate(context);
    auto& result = std::get<VectorWithMemoryLimit<Id>>(resultAsVariant);
    auto& resultInverted =
        std::get<VectorWithMemoryLimit<Id>>(resultAsVariantInverted);
    return std::pair{std::move(result), std::move(resultInverted)};
  };
  auto [resultLT, invertedLT] = m.template operator()<LT>();
  auto [resultLE, invertedLE] = m.template operator()<LE>();
  auto [resultEQ, invertedEQ] = m.template operator()<EQ>();
  auto [resultNE, invertedNE] = m.template operator()<NE>();
  auto [resultGE, invertedGE] = m.template operator()<GE>();
  auto [resultGT, invertedGT] = m.template operator()<GT>();

  EXPECT_EQ(resultLT.size(), 9);
  EXPECT_EQ(resultLE.size(), 9);
  EXPECT_EQ(resultEQ.size(), 9);
  EXPECT_EQ(resultNE.size(), 9);
  EXPECT_EQ(resultGE.size(), 9);
  EXPECT_EQ(resultGT.size(), 9);
  for (size_t i = 0; i < 3; ++i) {
    expectFalse(resultLT[i]);
    expectFalse(resultLE[i]);
    expectFalse(resultEQ[i]);
    expectTrue(resultNE[i]);
    expectTrue(resultGE[i]);
    expectTrue(resultGT[i]);

    expectTrue(invertedLT[i]);
    expectTrue(invertedLE[i]);
    expectFalse(invertedEQ[i]);
    expectTrue(invertedNE[i]);
    expectFalse(invertedGE[i]);
    expectFalse(invertedGT[i]);
  }
  for (size_t i = 3; i < 6; ++i) {
    expectTrue(resultLT[i]);
    expectTrue(resultLE[i]);
    expectFalse(resultEQ[i]);
    expectTrue(resultNE[i]);
    expectFalse(resultGE[i]);
    expectFalse(resultGT[i]);

    expectFalse(invertedLT[i]);
    expectFalse(invertedLE[i]);
    expectFalse(invertedEQ[i]);
    expectTrue(invertedNE[i]);
    expectTrue(invertedGE[i]);
    expectTrue(invertedGT[i]);
  }
  for (size_t i = 6; i < 9; ++i) {
    expectFalse(resultLT[i]);
    expectTrue(resultLE[i]);
    expectTrue(resultEQ[i]);
    expectFalse(resultNE[i]);
    expectTrue(resultGE[i]);
    expectFalse(resultGT[i]);

    expectFalse(invertedLT[i]);
    expectTrue(invertedLE[i]);
    expectTrue(invertedEQ[i]);
    expectFalse(invertedNE[i]);
    expectTrue(invertedGE[i]);
    expectFalse(invertedGT[i]);
  }
}

// Call `testLessThanGreaterThanEqualMultipleValuesHelper` with the given
// arguments as well as for the for all of the following cases: `leftValue` is
// converted to a ValueID before the call. `rightValue` "" both values ""
// Requires that both `leftValue` and `rightValue` are either numeric constants
// or numeric vectors, and that at least one of them is a vector.
void testLessThanGreaterThanEqualMultipleValues(
    auto leftValue, auto rightValue,
    source_location l = source_location::current()) {
  auto trace = generateLocationTrace(
      l, "testLessThanGreaterThanEqualMultipleValues was called here");

  /*
  testLessThanGreaterThanEqualMultipleValuesHelper(makeCopy(leftValue),
                                                   makeCopy(rightValue));
  testLessThanGreaterThanEqualMultipleValuesHelper(liftToValueId(leftValue),
                                                   makeCopy(rightValue));
  testLessThanGreaterThanEqualMultipleValuesHelper(makeCopy(leftValue),
                                                   liftToValueId(rightValue));
                                                   */
  testLessThanGreaterThanEqualMultipleValuesHelper(liftToValueId(leftValue),
                                                   liftToValueId(rightValue));
}

// At least one of `T` `U` must be a vector type. The vectors must all have 5
// elements. The corresponding pairs of values must all fulfill the "not equal"
// relation, but none of the other relations (less, less equal, equal  , greater
// equal, greater) must be true.
template <typename T, typename U>
auto testNotComparableHelper(T leftValue, U rightValue,
                             source_location l = source_location::current()) {
  auto trace = generateLocationTrace(
      l, "testLessThanGreaterThanEqualMultipleValuesHelper was called here");
  ad_utility::AllocatorWithLimit<Id> alloc{makeAllocator()};
  VariableToColumnMap map;
  LocalVocab localVocab;
  IdTable table{alloc};
  sparqlExpression::EvaluationContext context{
      *getQec(),  map,
      table,      alloc,
      localVocab, std::make_shared<ad_utility::CancellationHandle<>>()};
  AD_CONTRACT_CHECK(rightValue.size() == 5);
  context._beginIndex = 0;
  context._endIndex = 5;

  auto m = [&]<auto comp>() {
    auto expression =
        makeExpression<comp>(makeCopy(leftValue), makeCopy(rightValue));
    auto resultAsVariant = expression.evaluate(&context);
    auto expressionInverted =
        makeExpression<comp>(makeCopy(rightValue), makeCopy(leftValue));
    auto resultAsVariantInverted = expressionInverted.evaluate(&context);
    auto& result = std::get<VectorWithMemoryLimit<Id>>(resultAsVariant);
    auto& resultInverted =
        std::get<VectorWithMemoryLimit<Id>>(resultAsVariantInverted);
    return std::pair{std::move(result), std::move(resultInverted)};
  };
  auto [resultLT, invertedLT] = m.template operator()<LT>();
  auto [resultLE, invertedLE] = m.template operator()<LE>();
  auto [resultEQ, invertedEQ] = m.template operator()<EQ>();
  auto [resultNE, invertedNE] = m.template operator()<NE>();
  auto [resultGE, invertedGE] = m.template operator()<GE>();
  auto [resultGT, invertedGT] = m.template operator()<GT>();

  EXPECT_EQ(resultLT.size(), 5);
  EXPECT_EQ(resultLE.size(), 5);
  EXPECT_EQ(resultEQ.size(), 5);
  EXPECT_EQ(resultNE.size(), 5);
  EXPECT_EQ(resultGE.size(), 5);
  EXPECT_EQ(resultGT.size(), 5);
  for (size_t i = 0; i < 5; ++i) {
    expectFalse(resultLT[i]);
    expectFalse(resultLE[i]);
    expectFalse(resultEQ[i]);
    expectTrue(resultNE[i]);
    expectFalse(resultGE[i]);
    expectFalse(resultGT[i]);
    expectFalse(invertedLT[i]);
    expectFalse(invertedLE[i]);
    expectFalse(invertedEQ[i]);
    expectTrue(invertedNE[i]);
    expectFalse(invertedGE[i]);
    expectFalse(invertedGT[i]);
  }
}

// Similar to the other `...WithAndWithoutValueIds` functions (see above), but
// for the `testNotComparableHelper` function.
template <typename T, typename U>
auto testNotComparable(T leftValue, U rightValue,
                       source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "testNotComparable was called here");
  /*
  testNotComparableHelper(makeCopy(leftValue), makeCopy(rightValue));
  testNotComparableHelper(liftToValueId(leftValue), makeCopy(rightValue));
   */
  testNotComparableHelper(liftToValueId(leftValue), liftToValueId(rightValue));
}

TEST(RelationalExpression, NumericConstantAndNumericVector) {
  VectorWithMemoryLimit<double> doubles{
      {-24.3, 0.0, 3.0, 12.8, 1235e12, 523.13, 3.8, 3.8, 3.8}, makeAllocator()};
  testLessThanGreaterThanEqualMultipleValues(3.8, doubles.clone());

  VectorWithMemoryLimit<int64_t> ints{{-523, -15, -3, -1, 0, 12305, -2, -2, -2},
                                      makeAllocator()};
  testLessThanGreaterThanEqualMultipleValues(-2.0, ints.clone());
  testLessThanGreaterThanEqualMultipleValues(int64_t{-2}, ints.clone());

  // Test cases for comparison with NaN
  VectorWithMemoryLimit<double> nonNans{{1.0, 2.0, -inf, inf, 12e6},
                                        makeAllocator()};
  VectorWithMemoryLimit<double> Nans{{NaN, NaN, NaN, NaN, NaN},
                                     makeAllocator()};
  VectorWithMemoryLimit<int64_t> fiveInts{{-1, 0, 1, 2, 3}, makeAllocator()};

  testNotComparable(NaN, nonNans.clone());
  testNotComparable(NaN, fiveInts.clone());
  testNotComparable(3.2, Nans.clone());
  testNotComparable(NaN, Nans.clone());
}

TEST(RelationalExpression, StringConstantsAndStringVector) {
  VectorWithMemoryLimit<IdOrString> vec(
      {lit("alpha"), lit("alpaka"), lit("bertram"), lit("sigma"), lit("zeta"),
       lit("kaulquappe"), lit("caesar"), lit("caesar"), lit("caesar")},
      makeAllocator());
  testLessThanGreaterThanEqualMultipleValuesHelper(IdOrString{lit("caesar")},
                                                   vec.clone());

  // TODO<joka921> These tests only work, when we actually use unicode
  // comparisons for the string based expressions. TODDO<joka921> Add an example
  // for strings that are bytewise different but equal on the unicode level
  // (e.g.`ä` vs `a + dots`.
  // VectorWithMemoryLimit<IdOrString> vec2({"AlpHa", "älpaka", "Æ", "sigma",
  // "Eta", "kaulQuappe", "Caesar", "Caesar", "Caesare"}, alloc);
  // testLessThanGreaterThanEqualHelper<IdOrString, IdOrString>({"Alpha",
  // "beta"},
  // {"beta", "äpfel"}, {"xxx", "xxx"});
}

TEST(RelationalExpression, IntVectorAndIntVector) {
  VectorWithMemoryLimit<int64_t> vecA{
      {1065918, 17, 3, 1, 0, -102934, 2, -3120, 0}, makeAllocator()};
  VectorWithMemoryLimit<int64_t> vecB{
      {1065000, 16, -12340, 42, 1, -102930, 2, -3120, 0}, makeAllocator()};
  testLessThanGreaterThanEqualMultipleValues(vecA.clone(), vecB.clone());
}

TEST(RelationalExpression, DoubleVectorAndDoubleVector) {
  VectorWithMemoryLimit<double> vecA{
      {1.1e12, 0.0, 3.120, -1230, -1.3e12, 1.2e-13, -0.0, 13, -1.2e6},
      makeAllocator()};
  VectorWithMemoryLimit<double> vecB{
      {1.0e12, -0.1, -3.120e5, 1230, -1.3e10, 1.1e-12, 0.0, 13, -1.2e6},
      makeAllocator()};
  testLessThanGreaterThanEqualMultipleValues(vecA.clone(), vecB.clone());

  VectorWithMemoryLimit<double> nanA{{NaN, 1.0, NaN, 3.2, NaN},
                                     makeAllocator()};
  VectorWithMemoryLimit<double> nanB{{-12.3, NaN, 0.0, NaN, inf},
                                     makeAllocator()};
  testNotComparable(nanA.clone(), nanB.clone());
}

TEST(RelationalExpression, DoubleVectorAndIntVector) {
  VectorWithMemoryLimit<int64_t> vecA{
      {1065918, 17, 3, 1, 0, -102934, 2, -3120, 0}, makeAllocator()};
  VectorWithMemoryLimit<double> vecB{{1065917.9, 1.5e1, -3.120e5, 1.0e1, 1e-12,
                                      -102934e-1, 20.0e-1, -3.120e3, -0.0},
                                     makeAllocator()};
  testLessThanGreaterThanEqualMultipleValues(vecA.clone(), vecB.clone());

  VectorWithMemoryLimit<int64_t> fiveInts{{0, 1, 2, 3, -4}, makeAllocator()};
  VectorWithMemoryLimit<double> fiveNans{{NaN, NaN, NaN, NaN, NaN},
                                         makeAllocator()};
  testNotComparable(fiveInts.clone(), fiveNans.clone());
}

TEST(RelationalExpression, StringVectorAndStringVector) {
  VectorWithMemoryLimit<IdOrString> vecA{
      {lit("alpha"), lit("beta"), lit("g"), lit("epsilon"), lit("fraud"),
       lit("capitalism"), lit(""), lit("bo'sä30"), lit("Me")},
      makeAllocator()};
  VectorWithMemoryLimit<IdOrString> vecB{
      {lit("alph"), lit("alpha"), lit("f"), lit("epsiloo"), lit("freud"),
       lit("communism"), lit(""), lit("bo'sä30"), lit("Me")},
      makeAllocator()};
  testLessThanGreaterThanEqualMultipleValuesHelper(vecA.clone(), vecB.clone());
  // TODO<joka921> Add a test case for correct unicode collation as soon as that
  // is actually supported.
}

// Assert that the expression `leftValue Comparator rightValue`, when evaluated
// on the `TestContext` (see above), yields the `expected` result.

template <Comparison Comp>
void testWithExplicitIdResult(auto leftValue, auto rightValue,
                              std::vector<Id> expected,
                              source_location l = source_location::current()) {
  static TestContext ctx;
  auto expression = makeExpression<Comp>(liftToValueId(std::move(leftValue)),
                                         liftToValueId(std::move(rightValue)));
  auto trace = generateLocationTrace(l, "test lambda was called here");
  auto resultAsVariant = expression.evaluate(&ctx.context);
  const auto& result = std::get<VectorWithMemoryLimit<Id>>(resultAsVariant);
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}

template <Comparison Comp>
void testWithExplicitResult(auto leftValue, auto rightValue,
                            std::vector<bool> expectedAsBool,
                            source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  std::vector<Id> expected;
  std::ranges::transform(expectedAsBool, std::back_inserter(expected),
                         Id::makeFromBool);

  testWithExplicitIdResult<Comp>(std::move(leftValue), std::move(rightValue),
                                 expected);
}

TEST(RelationalExpression, VariableAndConstant) {
  // ?ints column is `1, 0, -1`
  testWithExplicitResult<LT>(int64_t{0}, Variable{"?ints"},
                             {true, false, false});
  testWithExplicitResult<GE>(-0.0, Variable{"?ints"}, {false, true, true});
  testWithExplicitResult<GE>(Variable{"?ints"}, IntId(0), {true, true, false});

  // ?doubles column is `0.1, -0.1, 2.8`
  testWithExplicitResult<LT>(-3.5, Variable{"?doubles"}, {true, true, true});
  testWithExplicitResult<EQ>(DoubleId(-0.1), Variable{"?doubles"},
                             {false, true, false});
  testWithExplicitResult<GT>(Variable{"?doubles"}, int64_t{0},
                             {true, false, true});

  // ?numeric column is 1, -0.1, 3.4
  testWithExplicitResult<NE>(-0.1, Variable{"?numeric"}, {true, false, true});
  testWithExplicitResult<GE>(Variable{"?numeric"}, int64_t{1},
                             {true, false, true});
  testWithExplicitResult<GT>(Variable{"?numeric"}, DoubleId(1.2),
                             {false, false, true});

  // ?vocab column is `"Beta", "alpha", "älpha"
  testWithExplicitResult<LE>(Variable{"?vocab"}, IdOrString{lit("\"älpha\"")},
                             {false, true, true});
  testWithExplicitResult<GT>(Variable{"?vocab"}, IdOrString{lit("\"alpha\"")},
                             {true, false, true});
  testWithExplicitResult<LT>(IdOrString{lit("\"atm\"")}, Variable{"?vocab"},
                             {true, false, false});

  // ?mixed column is `1, -0.1, <x>`
  auto U = Id::makeUndefined();
  auto B = ad_utility::testing::BoolId;
  testWithExplicitIdResult<GT>(IdOrString{iriref("<xa>")}, Variable{"?mixed"},
                               {U, U, B(true)});
  testWithExplicitIdResult<LT>(IdOrString{iriref("<u>")}, Variable{"?mixed"},
                               {U, U, B(true)});

  // Note: `1` and `<x>` are "not compatible", so even the "not equal"
  // comparison returns false.
  testWithExplicitIdResult<NE>(int64_t{1}, Variable{"?mixed"},
                               {B(false), B(true), U});
  testWithExplicitIdResult<GE>(Variable{"?mixed"}, DoubleId(-0.1),
                               {B(true), B(true), U});
}

TEST(RelationalExpression, VariableAndVariable) {
  auto ints = Variable{"?ints"};
  auto doubles = Variable{"?doubles"};
  auto numeric = Variable{"?numeric"};
  auto vocab = Variable{"?vocab"};
  auto mixed = Variable{"?mixed"};

  // Variables have to be equal to themselves.
  testWithExplicitResult<LT>(ints, ints, {false, false, false});
  testWithExplicitResult<EQ>(ints, ints, {true, true, true});

  testWithExplicitResult<LT>(mixed, mixed, {false, false, false});
  testWithExplicitResult<EQ>(mixed, mixed, {true, true, true});

  auto U = Id::makeUndefined();
  auto B = ad_utility::testing::BoolId;

  // ?ints column is `1, 0, -1`
  // ?doubles column is `0.1, -0.1, 2.8`
  // ?numeric column is 1, -0.1, 3.4
  // ?vocab column is `"Beta", "alpha", "älpha"
  // ?mixed column is `1, -0.1, <x>`

  testWithExplicitResult<GT>(doubles, ints, {false, false, true});
  testWithExplicitResult<LE>(numeric, doubles, {false, true, false});
  // Note: `1` and `<x>` are "not compatible" so even the "not equal" comparison
  // returns false (this is the third entry in the expected result).
  testWithExplicitIdResult<NE>(ints, mixed, {B(false), B(true), U});

  // The same note applies here, double values and vocab entries are always
  // incompatible.
  testWithExplicitIdResult<NE>(doubles, vocab, {U, U, U});
  testWithExplicitIdResult<LE>(vocab, doubles, {U, U, U});
  testWithExplicitIdResult<GE>(vocab, doubles, {U, U, U});

  testWithExplicitIdResult<LT>(vocab, mixed, {U, U, B(true)});
}

// `rightValue` must be a constant. Sort the `IdTable` of the `TestContext`
// `ctx` by the variable `leftValue` and then check that the expression
// `leftValue Comparator rightValue`, when evaluated on this sortest `IdTable`
// yields the `expected` result. The type of `expected`, `SetOfIntervals`
// indicates that the expression was evaluated using binary search on the sorted
// table.
template <Comparison Comp>
void testSortedVariableAndConstant(
    Variable leftValue, auto rightValue, ad_utility::SetOfIntervals expected,
    source_location l = source_location::current()) {
  auto trace = generateLocationTrace(
      l, "test between sorted variable and constant was called here");
  TestContext ctx = TestContext::sortedBy(leftValue);
  auto expression =
      makeExpression<Comp>(leftValue, liftToValueId(std::move(rightValue)));
  auto resultAsVariant = expression.evaluate(&ctx.context);
  const auto& result = std::get<ad_utility::SetOfIntervals>(resultAsVariant);
  ASSERT_EQ(result, expected);
}

TEST(RelationalExpression, VariableAndConstantBinarySearch) {
  // Sorted order (by bits of the valueIds):
  // ?ints column is `0, 1,  -1`
  // ?doubles column is `0.1 , 2.8`,-0.1
  // ?numeric column is 1, 3.4, -0.1
  // ?vocab column is  "alpha", "älpha", "Beta"
  // ?mixed column is `1, -0.1, A`

  auto ints = Variable{"?ints"};
  auto doubles = Variable{"?doubles"};
  auto numeric = Variable{"?numeric"};
  auto vocab = Variable{"?vocab"};
  auto mixed = Variable{"?mixed"};
  testSortedVariableAndConstant<LT>(ints, int64_t{-1}, {});
  testSortedVariableAndConstant<GE>(ints, int64_t{-1}, {{{0, 3}}});
  testSortedVariableAndConstant<LE>(ints, 0.3, {{{0, 1}, {2, 3}}});
  // ints and strings are always incompatible.
  testSortedVariableAndConstant<NE>(ints, IdOrString{lit("a string")}, {});

  testSortedVariableAndConstant<GT>(doubles, int64_t{0}, {{{0, 2}}});
  testSortedVariableAndConstant<EQ>(doubles, 2.8, {{{1, 2}}});
  testSortedVariableAndConstant<LE>(doubles, 0.1, {{{0, 1}, {2, 3}}});

  testSortedVariableAndConstant<GT>(numeric, -0.1, {{{0, 2}}});
  testSortedVariableAndConstant<EQ>(numeric, 1.0, {{{0, 1}}});
  testSortedVariableAndConstant<NE>(numeric, 3.4, {{{0, 1}, {2, 3}}});

  testSortedVariableAndConstant<GT>(vocab, IdOrString{lit("\"alpha\"")},
                                    {{{1, 3}}});
  testSortedVariableAndConstant<GE>(vocab, IdOrString{lit("\"alpha\"")},
                                    {{{0, 3}}});
  testSortedVariableAndConstant<LE>(vocab, IdOrString{lit("\"ball\"")},
                                    {{{0, 2}}});
  testSortedVariableAndConstant<NE>(vocab, IdOrString{lit("\"älpha\"")},
                                    {{{0, 1}, {2, 3}}});
  testSortedVariableAndConstant<LE>(vocab, inf, {});

  // Note: vocab entries and numeric values are not compatible, so every
  // comparison returns false.
  testSortedVariableAndConstant<NE>(vocab, 3.2, {});

  // Note: only *numeric* values that are not equal to 1.0 are considered here.
  testSortedVariableAndConstant<NE>(mixed, 1.0, {{{1, 2}}});
  testSortedVariableAndConstant<GT>(mixed, -inf, {{{0, 2}}});
  testSortedVariableAndConstant<LE>(mixed, IdOrString{iriref("<z>")},
                                    {{{2, 3}}});
}

// TODO<joka921> We currently do not have tests for the `LocalVocab` case,
// because the relational expressions do not work properly with the current
// limited implementation of the local vocabularies. Add those tests, as soon as
// the local vocabularies are implemented properly.

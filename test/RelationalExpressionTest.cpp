//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <limits>
#include <string>

#include "./SparqlParserTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;
using ad_utility::source_location;
using namespace std::literals;

using enum valueIdComparators::Comparison;

namespace {
ad_utility::AllocatorWithLimit<Id> allocator{
    ad_utility::makeAllocationMemoryLeftThreadsafeObject(100'000)};

const auto inf = std::numeric_limits<double>::infinity();
const auto NaN = std::numeric_limits<double>::quiet_NaN();

ValueId Int(int64_t i) { return ValueId::makeFromInt(i); }

ValueId Double(double d) { return ValueId::makeFromDouble(d); }

VectorWithMemoryLimit<ValueId> makeValueIdVector(
    const VectorWithMemoryLimit<double>& vec) {
  VectorWithMemoryLimit<ValueId> result{allocator};
  for (double d : vec) {
    result.push_back(Double(d));
  }
  return result;
}

VectorWithMemoryLimit<ValueId> makeValueIdVector(
    const VectorWithMemoryLimit<int64_t>& vec) {
  VectorWithMemoryLimit<ValueId> result{allocator};
  for (int64_t i : vec) {
    result.push_back(Int(i));
  }
  return result;
}

}  // namespace

template <valueIdComparators::Comparison comp>
auto makeExpression(auto l, auto r) {
  auto leftChild = std::make_unique<DummyExpression>(std::move(l));
  auto rightChild = std::make_unique<DummyExpression>(std::move(r));

  return relational::RelationalExpression<comp>(
      {std::move(leftChild), std::move(rightChild)});
}

auto evaluateWithEmpyContext = [](const SparqlExpression& expression) {
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ResultTable::LocalVocab localVocab;
  IdTable table{allocator};
  QueryExecutionContext* qec = nullptr;
  sparqlExpression::EvaluationContext context{*qec, map, table, allocator,
                                              localVocab};

  return expression.evaluate(&context);
};

auto expectTrueBoolean = [](const SparqlExpression& expression,
                            source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectTrueBoolean was called here");
  auto result = evaluateWithEmpyContext(expression);
  EXPECT_TRUE(std::get<Bool>(result));
};

auto expectFalseBoolean = [](const SparqlExpression& expression,
                             source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectFalseBoolean was called here");
  auto result = evaluateWithEmpyContext(expression);
  EXPECT_FALSE(std::get<Bool>(result));
};

template <typename T, typename U>
auto testNumericConstants(std::pair<T, U> lessThan, std::pair<T, U> greaterThan,
                          std::pair<T, U> equal,
                          source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "testNumericConstants was called here");
  auto True = expectTrueBoolean;
  auto False = expectFalseBoolean;

  True(makeExpression<LT>(lessThan.first, lessThan.second));
  True(makeExpression<LE>(lessThan.first, lessThan.second));
  False(makeExpression<EQ>(lessThan.first, lessThan.second));
  True(makeExpression<NE>(lessThan.first, lessThan.second));
  False(makeExpression<GE>(lessThan.first, lessThan.second));
  False(makeExpression<GT>(lessThan.first, lessThan.second));

  False(makeExpression<LT>(greaterThan.first, greaterThan.second));
  False(makeExpression<LE>(greaterThan.first, greaterThan.second));
  False(makeExpression<EQ>(greaterThan.first, greaterThan.second));
  True(makeExpression<NE>(greaterThan.first, greaterThan.second));
  True(makeExpression<GE>(greaterThan.first, greaterThan.second));
  True(makeExpression<GT>(greaterThan.first, greaterThan.second));

  False(makeExpression<LT>(equal.first, equal.second));
  True(makeExpression<LE>(equal.first, equal.second));
  True(makeExpression<EQ>(equal.first, equal.second));
  False(makeExpression<NE>(equal.first, equal.second));
  True(makeExpression<GE>(equal.first, equal.second));
  False(makeExpression<GT>(equal.first, equal.second));
}

// TODO<joka921> Misnomer, it is "notComparable, constant result", rename and
// comment.
auto testNumericAndString = [](auto numeric, auto s,
                               source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "testNumericAndString was called here");
  auto True = expectTrueBoolean;
  auto False = expectFalseBoolean;

  auto clone = [](const auto& input) {
    if constexpr (requires { input.clone(); }) {
      return input.clone();
    } else {
      return input;
    }
  };

  False(makeExpression<LT>(clone(numeric), clone(s)));
  False(makeExpression<LE>(clone(numeric), clone(s)));
  False(makeExpression<EQ>(clone(numeric), clone(s)));
  True(makeExpression<NE>(clone(numeric), clone(s)));
  False(makeExpression<GT>(clone(numeric), clone(s)));
  False(makeExpression<GE>(clone(numeric), clone(s)));

  False(makeExpression<LT>(clone(s), clone(numeric)));
  False(makeExpression<LE>(clone(s), clone(numeric)));
  False(makeExpression<EQ>(clone(s), clone(numeric)));
  True(makeExpression<NE>(clone(s), clone(numeric)));
  False(makeExpression<GT>(clone(s), clone(numeric)));
  False(makeExpression<GE>(clone(s), clone(numeric)));
};
TEST(RelationalExpression, IntAndDouble) {
  testNumericConstants<int, double>({3, 3.3}, {4, -inf}, {-12, -12.0});
  // Similar tests, but on the level of `ValueIds`.
  testNumericConstants<ValueId, double>({Int(3), inf}, {Int(4), -3.1},
                                        {Int(-12), -12.0});
  testNumericConstants<int, ValueId>({3, Double(inf)}, {4, Double(-3.1)},
                                     {-12, Double(-12.0)});
  testNumericConstants<ValueId, ValueId>(
      {Int(3), Double(inf)}, {Int(4), Double(-3.1)}, {Int(-12), Double(-12.0)});

  testNumericAndString(3, NaN);
  testNumericAndString(3, Double(NaN));
}

TEST(RelationalExpression, DoubleAndInt) {
  testNumericConstants<double, int>({3.1, 4}, {4.2, -3}, {-12.0, -12});
  // Similar tests, but on the level of `ValueIds`.
  testNumericConstants<double, ValueId>({3.1, Int(4)}, {4.2, Int(-3)},
                                        {-12.0, Int(-12)});
  testNumericConstants<ValueId, int>({Double(3.1), 4}, {Double(4.2), -3},
                                     {Double(-12.0), -12});
  testNumericConstants<ValueId, ValueId>(
      {Double(3.1), Int(4)}, {Double(4.2), Int(-3)}, {Double(-12.0), Int(-12)});

  testNumericAndString(NaN, 42);
  testNumericAndString(Double(NaN), 42);
}

TEST(RelationalExpression, IntAndInt) {
  testNumericConstants<int, int>({-3, 3}, {4, 3}, {-12, -12});
  // Similar tests, but on the level of `ValueIds`.
  testNumericConstants<int, ValueId>({-3, Int(3)}, {4, Int(3)},
                                     {-12, Int(-12)});
  testNumericConstants<ValueId, int>({Int(-3), 3}, {Int(4), 3},
                                     {Int(-12), -12});
  testNumericConstants<ValueId, ValueId>({Int(-3), Int(3)}, {Int(4), Int(3)},
                                         {Int(-12), Int(-12)});
}

TEST(RelationalExpression, DoubleAndDouble) {
  testNumericConstants<double, double>({-3.1, -3.0}, {4.2, 4.1},
                                       {-12.83, -12.83});
  // Similar tests, but on the level of `ValueIds`.
  testNumericConstants<ValueId, double>(
      {Double(-3.1), -3.0}, {Double(4.2), 4.1}, {Double(-12.83), -12.83});
  testNumericConstants<double, ValueId>(
      {-3.1, Double(-3.0)}, {4.2, Double(4.1)}, {-12.83, Double(-12.83)});
  testNumericConstants<ValueId, ValueId>({Double(-3.1), Double(-3.0)},
                                         {Double(4.2), Double(4.1)},
                                         {Double(-12.83), Double(-12.83)});

  testNumericAndString(NaN, NaN);
  testNumericAndString(12.0, NaN);
  testNumericAndString(NaN, -1.23e12);
  testNumericAndString(Double(NaN), -1.23e12);
  testNumericAndString(NaN, Double(-1.23e12));
}

TEST(RelationalExpression, StringAndString) {
  testNumericConstants<std::string, std::string>(
      {"alpha", "beta"}, {"sigma", "delta"}, {"epsilon", "epsilon"});
  // TODO<joka921> These tests only work, when we actually use unicode
  // comparisons for the string based expressions. TODDO<joka921> Add an example
  // for strings that are bytewise different but equal on the unicode level
  // (e.g.`ä` vs `a + dots`.
  // testNumericConstants<std::string, std::string>({"Alpha", "beta"}, {"beta",
  // "äpfel"}, {"xxx", "xxx"});

  // TODO<joka921> To compare strings with `ValueId`s we actually need a context
  // with vocabulary etc. which has yet to be mocked.
}

TEST(RelationalExpression, NumericAndStringAreNeverEqual) {
  auto stringVec = VectorWithMemoryLimit<std::string>({"hallo",
                                                       "by"
                                                       ""},
                                                      allocator);
  auto intVec = VectorWithMemoryLimit<int64_t>({-12365, 0, 12}, allocator);
  auto doubleVec =
      VectorWithMemoryLimit<double>({-12.365, 0, 12.1e5}, allocator);
  testNumericAndString(3, "hallo"s);
  testNumericAndString(3, "3"s);
  testNumericAndString(-12.0, "hallo"s);
  testNumericAndString(-12.0, "-12.0"s);
  testNumericAndString(intVec.clone(), "someString"s);
  testNumericAndString(doubleVec.clone(), "someString"s);
  testNumericAndString(3, stringVec.clone());
  testNumericAndString(intVec.clone(), stringVec.clone());
  testNumericAndString(doubleVec.clone(), stringVec.clone());
}

// The vector must consist of 9 elements: The first three must be less than
// constant, the next three greater than constant, and the last three equal to
// constant
// TODO<joka921> The name of the function and the arguments are out of sync.
// The function also works for two vectors.
template <typename T, typename U>
auto testNumericConstantAndVector(
    T constant, U vector, source_location l = source_location::current()) {
  auto trace =
      generateLocationTrace(l, "testNumericConstantAndVector was called here");
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ResultTable::LocalVocab localVocab;
  IdTable table{alloc};
  QueryExecutionContext* qec = nullptr;
  sparqlExpression::EvaluationContext context{*qec, map, table, alloc,
                                              localVocab};
  AD_CHECK(vector.size() == 9);
  context._beginIndex = 0;
  context._endIndex = 9;

  auto clone = [](const auto& input) {
    if constexpr (requires { input.clone(); }) {
      return input.clone();
    } else {
      return input;
    }
  };

  auto m = [&]<auto comp>() {
    auto expression = makeExpression<comp>(clone(constant), clone(vector));
    auto resultAsVariant = expression.evaluate(&context);
    auto expressionInverted =
        makeExpression<comp>(clone(vector), clone(constant));
    auto resultAsVariantInverted = expressionInverted.evaluate(&context);
    auto& result = std::get<VectorWithMemoryLimit<Bool>>(resultAsVariant);
    auto& resultInverted =
        std::get<VectorWithMemoryLimit<Bool>>(resultAsVariantInverted);
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
    EXPECT_FALSE(resultLT[i]);
    EXPECT_FALSE(resultLE[i]);
    EXPECT_FALSE(resultEQ[i]);
    EXPECT_TRUE(resultNE[i]);
    EXPECT_TRUE(resultGE[i]);
    EXPECT_TRUE(resultGT[i]);

    EXPECT_TRUE(invertedLT[i]);
    EXPECT_TRUE(invertedLE[i]);
    EXPECT_FALSE(invertedEQ[i]);
    EXPECT_TRUE(invertedNE[i]);
    EXPECT_FALSE(invertedGE[i]);
    EXPECT_FALSE(invertedGT[i]);
  }
  for (size_t i = 3; i < 6; ++i) {
    EXPECT_TRUE(resultLT[i]);
    EXPECT_TRUE(resultLE[i]);
    EXPECT_FALSE(resultEQ[i]);
    EXPECT_TRUE(resultNE[i]);
    EXPECT_FALSE(resultGE[i]);
    EXPECT_FALSE(resultGT[i]);

    EXPECT_FALSE(invertedLT[i]);
    EXPECT_FALSE(invertedLE[i]);
    EXPECT_FALSE(invertedEQ[i]);
    EXPECT_TRUE(invertedNE[i]);
    EXPECT_TRUE(invertedGE[i]);
    EXPECT_TRUE(invertedGT[i]);
  }
  for (size_t i = 6; i < 9; ++i) {
    EXPECT_FALSE(resultLT[i]);
    EXPECT_TRUE(resultLE[i]);
    EXPECT_TRUE(resultEQ[i]);
    EXPECT_FALSE(resultNE[i]);
    EXPECT_TRUE(resultGE[i]);
    EXPECT_FALSE(resultGT[i]);

    EXPECT_FALSE(invertedLT[i]);
    EXPECT_TRUE(invertedLE[i]);
    EXPECT_TRUE(invertedEQ[i]);
    EXPECT_FALSE(invertedNE[i]);
    EXPECT_TRUE(invertedGE[i]);
    EXPECT_FALSE(invertedGT[i]);
  }
}

template <typename A, typename B>
void testIncludingValueIds(A a, B b,
                           source_location l = source_location::current()) {
  auto trace =
      generateLocationTrace(l, "testIncludingValueIds was called here");

  auto lift = []<typename T>(const T& t) {
    if constexpr (std::is_same_v<T, double>) {
      return Double(t);
    } else if constexpr (std::is_integral_v<T>) {
      return Int(t);
    } else if constexpr (isVectorResult<T>) {
      return makeValueIdVector(t);
    } else {
      static_assert(ad_utility::alwaysFalse<T>);
    }
  };

  auto clone = [](const auto& input) {
    if constexpr (requires { input.clone(); }) {
      return input.clone();
    } else {
      return input;
    }
  };

  testNumericConstantAndVector(clone(a), clone(b));
  testNumericConstantAndVector(lift(a), clone(b));
  testNumericConstantAndVector(clone(a), lift(b));
  testNumericConstantAndVector(lift(a), lift(b));
}

// At least one of `T` `U` must be a vector type. The vectors must all have 5
// elements. The corresponding pairs of values must all fulfill the "not equal"
// relation, but none of the other relations must be true.
template <typename T, typename U>
auto testNotComparable(T constant, U vector,
                       source_location l = source_location::current()) {
  auto trace =
      generateLocationTrace(l, "testNumericConstantAndVector was called here");
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ResultTable::LocalVocab localVocab;
  IdTable table{alloc};
  QueryExecutionContext* qec = nullptr;
  sparqlExpression::EvaluationContext context{*qec, map, table, alloc,
                                              localVocab};
  AD_CHECK(vector.size() == 5);
  context._beginIndex = 0;
  context._endIndex = 5;

  auto clone = [](const auto& input) {
    if constexpr (requires { input.clone(); }) {
      return input.clone();
    } else {
      return input;
    }
  };

  auto m = [&]<auto comp>() {
    auto expression = makeExpression<comp>(clone(constant), clone(vector));
    auto resultAsVariant = expression.evaluate(&context);
    auto expressionInverted =
        makeExpression<comp>(clone(vector), clone(constant));
    auto resultAsVariantInverted = expressionInverted.evaluate(&context);
    auto& result = std::get<VectorWithMemoryLimit<Bool>>(resultAsVariant);
    auto& resultInverted =
        std::get<VectorWithMemoryLimit<Bool>>(resultAsVariantInverted);
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
    EXPECT_FALSE(resultLT[i]);
    EXPECT_FALSE(resultLE[i]);
    EXPECT_FALSE(resultEQ[i]);
    EXPECT_TRUE(resultNE[i]);
    EXPECT_FALSE(resultGE[i]);
    EXPECT_FALSE(resultGT[i]);
    EXPECT_FALSE(invertedLT[i]);
    EXPECT_FALSE(invertedLE[i]);
    EXPECT_FALSE(invertedEQ[i]);
    EXPECT_TRUE(invertedNE[i]);
    EXPECT_FALSE(invertedGE[i]);
    EXPECT_FALSE(invertedGT[i]);
  }
}

TEST(RelationalExpression, NumericConstantAndNumericVector) {
  VectorWithMemoryLimit<double> doubles{
      {-24.3, 0.0, 3.0, 12.8, 1235e12, 523.13, 3.8, 3.8, 3.8}, allocator};
  testIncludingValueIds(3.8, doubles.clone());

  VectorWithMemoryLimit<int64_t> ints{{-523, -15, -3, -1, 0, 12305, -2, -2, -2},
                                      allocator};
  testIncludingValueIds(-2.0, ints.clone());
  testIncludingValueIds(int64_t{-2}, ints.clone());

  // Test cases for comparison with NaN
  VectorWithMemoryLimit<double> nonNans{{1.0, 2.0, -inf, inf, 12e6}, allocator};
  VectorWithMemoryLimit<double> Nans{{NaN, NaN, NaN, NaN, NaN}, allocator};
  VectorWithMemoryLimit<int64_t> fiveInts{{-1, 0, 1, 2, 3}, allocator};

  testNotComparable(NaN, nonNans.clone());
  testNotComparable(NaN, fiveInts.clone());
  testNotComparable(Double(NaN), fiveInts.clone());
  testNotComparable(NaN, makeValueIdVector(fiveInts));
  testNotComparable(3.2, Nans.clone());
  testNotComparable(int64_t{-123}, Nans.clone());
  testNotComparable(NaN, Nans.clone());
}

TEST(RelationalExpression, StringConstantsAndStringVector) {
  VectorWithMemoryLimit<std::string> vec(
      {"alpha", "alpaka", "bertram", "sigma", "zeta", "kaulquappe", "caesar",
       "caesar", "caesar"},
      allocator);
  testNumericConstantAndVector("caesar"s, vec.clone());

  // TODO<joka921> These tests only work, when we actually use unicode
  // comparisons for the string based expressions. TODDO<joka921> Add an example
  // for strings that are bytewise different but equal on the unicode level
  // (e.g.`ä` vs `a + dots`.
  // VectorWithMemoryLimit<std::string> vec2({"AlpHa", "älpaka", "Æ", "sigma",
  // "Eta", "kaulQuappe", "Caesar", "Caesar", "Caesare"}, alloc);
  // testNumericConstants<std::string, std::string>({"Alpha", "beta"}, {"beta",
  // "äpfel"}, {"xxx", "xxx"});

  // TODO<joka921> Add tests for Strings and ValueIds as soon as we have a
  // mocked Vocabulary.
}

TEST(RelationalExpression, IntVectorAndIntVector) {
  VectorWithMemoryLimit<int64_t> vecA{
      {1065918, 17, 3, 1, 0, -102934, 2, -3120, 0}, allocator};
  VectorWithMemoryLimit<int64_t> vecB{
      {1065000, 16, -12340, 42, 1, -102930, 2, -3120, 0}, allocator};
  testIncludingValueIds(vecA.clone(), vecB.clone());
}

TEST(RelationalExpression, DoubleVectorAndDoubleVector) {
  VectorWithMemoryLimit<double> vecA{
      {1.1e12, 0.0, 3.120, -1230, -1.3e12, 1.2e-13, -0.0, 13, -1.2e6},
      allocator};
  VectorWithMemoryLimit<double> vecB{
      {1.0e12, -0.1, -3.120e5, 1230, -1.3e10, 1.1e-12, 0.0, 13, -1.2e6},
      allocator};
  testIncludingValueIds(vecA.clone(), vecB.clone());

  VectorWithMemoryLimit<double> nanA{{NaN, 1.0, NaN, 3.2, NaN}, allocator};
  VectorWithMemoryLimit<double> nanB{{-12.3, NaN, 0.0, NaN, inf}, allocator};
  testNotComparable(nanA.clone(), nanB.clone());
}

TEST(RelationalExpression, DoubleVectorAndIntVector) {
  VectorWithMemoryLimit<int64_t> vecA{
      {1065918, 17, 3, 1, 0, -102934, 2, -3120, 0}, allocator};
  VectorWithMemoryLimit<double> vecB{{1065917.9, 1.5e1, -3.120e5, 1.0e1, 1e-12,
                                      -102934e-1, 20.0e-1, -3.120e3, -0.0},
                                     allocator};
  testIncludingValueIds(vecA.clone(), vecB.clone());

  VectorWithMemoryLimit<int64_t> fiveInts{{0, 1, 2, 3, -4}, allocator};
  VectorWithMemoryLimit<double> fiveNans{{NaN, NaN, NaN, NaN, NaN}, allocator};
  testNotComparable(fiveInts.clone(), fiveNans.clone());

  // TODO<joka921> Consistently also test `includingValueIds` for the `NaN`
  // cases.
}

TEST(RelationalExpression, StringVectorAndStringVector) {
  VectorWithMemoryLimit<std::string> vecA{
      {"alpha", "beta", "g", "epsilon", "fraud", "capitalism", "", "bo'sä30",
       "Me"},
      allocator};
  VectorWithMemoryLimit<std::string> vecB{
      {"alph", "alpha", "f", "epsiloo", "freud", "communism", "", "bo'sä30",
       "Me"},
      allocator};
  testNumericConstantAndVector(vecA.clone(), vecB.clone());
  // TODO<joka921> Add a test case for correct unicode collation as soon as that
  // is actually supported.

  // TODO<joka921> Add comparison between string and ValueId (needs mocking of
  // the vocabulary).
}

// TODO<joka921> vector<ValueId> with mixed types.

// TODO<joka921> tests for the `Variable` type (needs mocking) including the
// binary search case.

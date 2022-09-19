//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <string>

#include "./SparqlParserTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;
using ad_utility::source_location;
using namespace std::literals;

using enum valueIdComparators::Comparison;

template <valueIdComparators::Comparison comp>
auto makeExpression(auto l, auto r) {
  auto leftChild = std::make_unique<DummyExpression>(std::move(l));
  auto rightChild = std::make_unique<DummyExpression>(std::move(r));

  return relational::RelationalExpression<comp>(
      {std::move(leftChild), std::move(rightChild)});
}

auto evaluateWithEmpyContext = [](const SparqlExpression& expression) {
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ResultTable::LocalVocab localVocab;
  IdTable table{alloc};
  QueryExecutionContext* qec = nullptr;
  sparqlExpression::EvaluationContext context{*qec, map, table, alloc,
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
TEST(RelationalExpression, IntAndDouble) {
  testNumericConstants<int, double>({3, 3.3}, {4, -3.1}, {-12, -12.0});
}

TEST(RelationalExpression, DoubleAndInt) {
  testNumericConstants<double, int>({3.1, 4}, {4.2, -3}, {-12.0, -12});
}

TEST(RelationalExpression, IntAndInt) {
  testNumericConstants<int, int>({-3, 3}, {4, 3}, {-12, -12});
}

TEST(RelationalExpression, DoubleAndDouble) {
  testNumericConstants<double, double>({-3.1, -3.0}, {4.2, 4.1},
                                       {-12.83, -12.83});
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
}

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

TEST(RelationalExpression, NumericAndStringAreNeverEqual) {
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  auto stringVec = VectorWithMemoryLimit<std::string>({"hallo",
                                                       "by"
                                                       ""},
                                                      alloc);
  auto intVec = VectorWithMemoryLimit<int64_t>({-12365, 0, 12}, alloc);
  auto doubleVec = VectorWithMemoryLimit<double>({-12.365, 0, 12.1e5}, alloc);
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
template <typename T, typename U>
auto testNumericConstantAndVector(
    T constant, VectorWithMemoryLimit<U> vector,
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
  AD_CHECK(vector.size() == 9);
  context._beginIndex = 0;
  context._endIndex = 9;
  auto m = [&]<auto comp>() {
    auto expression = makeExpression<comp>(constant, vector.clone());
    auto resultAsVariant = expression.evaluate(&context);
    auto expressionInverted = makeExpression<comp>(vector.clone(), constant);
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
  }
  for (size_t i = 6; i < 9; ++i) {
    EXPECT_FALSE(resultLT[i]);
    EXPECT_TRUE(resultLE[i]);
    EXPECT_TRUE(resultEQ[i]);
    EXPECT_FALSE(resultNE[i]);
    EXPECT_TRUE(resultGE[i]);
    EXPECT_FALSE(resultGT[i]);
  }
}

TEST(RelationalExpression, NumericConstantAndNumericVector) {
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  VectorWithMemoryLimit<double> doubles{
      {-24.3, 0.0, 3.0, 12.8, 1235e12, 523.13, 3.8, 3.8, 3.8}, alloc};
  testNumericConstantAndVector(3.8, doubles.clone());
  VectorWithMemoryLimit<int64_t> ints{{-523, -15, -3, -1, 0, 12305, -2, -2, -2},
                                      alloc};
  testNumericConstantAndVector(-2.0, ints.clone());
  testNumericConstantAndVector(int64_t{-2}, ints.clone());
}

TEST(RelationalExpression, StringConstantsAndStringVector) {
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  VectorWithMemoryLimit<std::string> vec(
      {"alpha", "alpaka", "bertram", "sigma", "zeta", "kaulquappe", "caesar",
       "caesar", "caesar"},
      alloc);
  testNumericConstantAndVector("caesar"s, vec.clone());

  // TODO<joka921> These tests only work, when we actually use unicode
  // comparisons for the string based expressions. TODDO<joka921> Add an example
  // for strings that are bytewise different but equal on the unicode level
  // (e.g.`ä` vs `a + dots`.
  // VectorWithMemoryLimit<std::string> vec2({"AlpHa", "älpaka", "Æ", "sigma",
  // "Eta", "kaulQuappe", "Caesar", "Caesar", "Caesare"}, alloc);
  // testNumericConstants<std::string, std::string>({"Alpha", "beta"}, {"beta",
  // "äpfel"}, {"xxx", "xxx"});
}

TEST(SparqlExpression, LessThan) {
  auto three = std::make_unique<DummyExpression>(3);
  auto four = std::make_unique<DummyExpression>(4.2);

  QueryExecutionContext* qec = nullptr;
  auto expr = LessThanExpression{{std::move(three), std::move(four)}};

  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ResultTable::LocalVocab localVocab;
  IdTable table{alloc};

  sparqlExpression::EvaluationContext context{*qec, map, table, alloc,
                                              localVocab};

  auto res = expr.evaluate(&context);
  ASSERT_TRUE(std::get<Bool>(res));
}

//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./SparqlParserTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;
using ad_utility::source_location;

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

auto testNumericAndString = [](auto numeric, std::string s,
                               source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "testNumericAndString was called here");
  auto True = expectTrueBoolean;
  auto False = expectFalseBoolean;

  False(makeExpression<LT>(numeric, s));
  False(makeExpression<LE>(numeric, s));
  False(makeExpression<EQ>(numeric, s));
  True(makeExpression<NE>(numeric, s));
  False(makeExpression<GT>(numeric, s));
  False(makeExpression<GE>(numeric, s));

  False(makeExpression<LT>(s, numeric));
  False(makeExpression<LE>(s, numeric));
  False(makeExpression<EQ>(s, numeric));
  True(makeExpression<NE>(s, numeric));
  False(makeExpression<GT>(s, numeric));
  False(makeExpression<GE>(s, numeric));
};

TEST(RelationalExpression, NumericAndStringAreNeverEqual) {
  testNumericAndString(3, "hallo");
  testNumericAndString(3, "3");
  testNumericAndString(-12.0, "hallo");
  testNumericAndString(-12.0, "-12.0");
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
  auto expression = makeExpression<LT>(constant, vector.clone());
  auto resultAsVariant = expression.evaluate(&context);
  const auto& result = std::get<VectorWithMemoryLimit<Bool>>(resultAsVariant);

  EXPECT_EQ(result.size(), 9);
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_FALSE(result[i])
        << "Constant: " << constant << "vector element" << vector[i];
  }
  for (size_t i = 3; i < 6; ++i) {
    EXPECT_TRUE(result[i]);
  }
  for (size_t i = 6; i < 9; ++i) {
    EXPECT_FALSE(result[i]);
  }

  // TODO<joka921> Continue here with the other expressions (not only less
  // than).
  // TODO<joka921> Also check the inverse (`vector` as lhs of expression).
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

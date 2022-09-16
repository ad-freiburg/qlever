//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./SparqlParserTestHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "gtest/gtest.h"

using namespace sparqlExpression;

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

auto expectTrueBoolean = [](const SparqlExpression& expression) {
  auto result = evaluateWithEmpyContext(expression);
  EXPECT_TRUE(std::get<Bool>(result));
};

auto expectFalseBoolean = [](const SparqlExpression& expression) {
  auto result = evaluateWithEmpyContext(expression);
  EXPECT_FALSE(std::get<Bool>(result));
};

template <typename T, typename U>
auto testNumericConstants(std::pair<T, U> lessThan, std::pair<T, U> greaterThan,
                          std::pair<T, U> equal) {
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

auto testNumericAndString = [](auto numeric, std::string s) {
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

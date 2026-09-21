// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <tuple>
#include <variant>
#include <vector>

#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "backports/concepts.h"
#include "engine/sparqlExpressions/HomogeneousNumericExpressionHelpers.h"

namespace {

using namespace sparqlExpression;
using namespace sparqlExpression::detail;
using namespace sparqlExpression::detail::homogeneousNumeric;

auto I = ad_utility::testing::IntId;
auto D = ad_utility::testing::DoubleId;

// Simple function used to test the homogeneous evaluation helper independently
// of the concrete SPARQL arithmetic expressions.
struct TestAdd {
  Id operator()(int64_t lhs, int64_t rhs) const {
    return Id::makeFromInt(lhs + rhs);
  }
};

struct TestNumericAdd {
  Id operator()(int64_t lhs, int64_t rhs) const {
    return Id::makeFromInt(lhs + rhs);
  }

  Id operator()(int64_t lhs, double rhs) const {
    return Id::makeFromDouble(lhs + rhs);
  }

  Id operator()(double lhs, int64_t rhs) const {
    return Id::makeFromDouble(lhs + rhs);
  }

  Id operator()(double lhs, double rhs) const {
    return Id::makeFromDouble(lhs + rhs);
  }
};

// Only used to make the generic fallback of the test function well-formed.
// The tests below contain only integer and double values, so the fallback is
// never reached.
struct TestNumericValueGetter {
  int64_t operator()(ValueId value, EvaluationContext*) const {
    return value.getInt();
  }
};

struct TestAddThree {
  Id operator()(int64_t a, double b, int64_t c) const {
    return Id::makeFromDouble(a + b + c);
  }
};

class HomogeneousNumericExpressionHelpersTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
  VariableToColumnMap variableToColumnMap_;
  LocalVocab localVocab_;
  IdTable table_{qec_->getAllocator()};

  EvaluationContext context_{
      *qec_,
      variableToColumnMap_,
      table_.asStaticView<0>(),
      qec_->getAllocator(),
      localVocab_,
      std::make_shared<ad_utility::CancellationHandle<>>(),
      EvaluationContext::TimePoint::max()};

  HomogeneousNumericExpressionHelpersTest() {
    table_.setNumColumns(1);
    table_.push_back({I(0)});
    table_.push_back({I(0)});
    table_.push_back({I(0)});

    context_._inputTable = table_.asStaticView<0>();
    context_._beginIndex = 0;
    context_._endIndex = table_.size();
  }
};

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, SupportedValueGetters) {
  struct UnsupportedValueGetter {};
  static_assert(supportsHomogeneousNumericFastPath<NumericValueGetter>);
  static_assert(supportsHomogeneousNumericFastPath<NumericOrDateValueGetter>);
  static_assert(!supportsHomogeneousNumericFastPath<UnsupportedValueGetter>);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, SupportedOperandTypes) {
  static_assert(supportsHomogeneousNumericOperand<ValueId>());
  static_assert(supportsHomogeneousNumericOperand<ql::span<const ValueId>>());
  static_assert(!supportsHomogeneousNumericOperand<int>());

  EXPECT_TRUE(supportsHomogeneousNumericOperand<ValueId>());
  EXPECT_TRUE(supportsHomogeneousNumericOperand<ql::span<const ValueId>>());
  EXPECT_FALSE(supportsHomogeneousNumericOperand<int>());
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, HomogeneousNumericTypeToIndex) {
  EXPECT_EQ(homogeneousNumericTypeToIndex(HomogeneousNumericType::Int), 0);
  EXPECT_EQ(homogeneousNumericTypeToIndex(HomogeneousNumericType::Double), 1);
  EXPECT_ANY_THROW(
      homogeneousNumericTypeToIndex(HomogeneousNumericType::Other));
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, ClassifySingleValueId) {
  EXPECT_EQ(classifyNumericOperand(I(42)), HomogeneousNumericType::Int);
  EXPECT_EQ(classifyNumericOperand(D(3.5)), HomogeneousNumericType::Double);
  EXPECT_EQ(classifyNumericOperand(Id::makeFromBool(true)),
            HomogeneousNumericType::Other);
  EXPECT_EQ(classifyNumericOperand(Id::makeUndefined()),
            HomogeneousNumericType::Other);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, ClassifySpan) {
  std::array<ValueId, 3> ints{I(1), I(-2), I(3)};
  std::array<ValueId, 3> doubles{D(1.0), D(-2.5), D(3.5)};
  std::array<ValueId, 3> mixed{I(1), D(2.0), I(3)};
  std::array<ValueId, 0> empty{};

  EXPECT_EQ(classifyNumericOperand(ql::span<const ValueId>{ints}, &context_),
            HomogeneousNumericType::Int);

  EXPECT_EQ(classifyNumericOperand(ql::span<const ValueId>{doubles}, &context_),
            HomogeneousNumericType::Double);

  EXPECT_EQ(classifyNumericOperand(ql::span<const ValueId>{mixed}, &context_),
            HomogeneousNumericType::Other);

  EXPECT_EQ(classifyNumericOperand(ql::span<const ValueId>{empty}, &context_),
            HomogeneousNumericType::Other);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, ClassifyOperands) {
  std::array<ValueId, 3> ints{I(1), I(2), I(3)};
  std::array<ValueId, 3> doubles{D(1.0), D(2.0), D(3.0)};

  auto intsSpan = ql::span<const ValueId>{ints};
  auto doublesSpan = ql::span<const ValueId>{doubles};

  const auto types = classifyNumericOperands(&context_, intsSpan, doublesSpan);

  EXPECT_EQ(types[0], HomogeneousNumericType::Int);
  EXPECT_EQ(types[1], HomogeneousNumericType::Double);

  const auto ternaryTypes =
      classifyNumericOperands(&context_, intsSpan, doublesSpan, I(1));

  EXPECT_EQ(ternaryTypes[0], HomogeneousNumericType::Int);
  EXPECT_EQ(ternaryTypes[1], HomogeneousNumericType::Double);
  EXPECT_EQ(ternaryTypes[2], HomogeneousNumericType::Int);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       ClassifySingleValueIdWithPreferredType) {
  const auto intClassification = classifyNumericOperandWithPreferredType(I(42));
  EXPECT_EQ(intClassification.homogeneousType_, HomogeneousNumericType::Int);
  EXPECT_EQ(intClassification.preferredType_, HomogeneousNumericType::Int);

  const auto doubleClassification =
      classifyNumericOperandWithPreferredType(D(3.5));
  EXPECT_EQ(doubleClassification.homogeneousType_,
            HomogeneousNumericType::Double);
  EXPECT_EQ(doubleClassification.preferredType_,
            HomogeneousNumericType::Double);

  const auto otherClassification =
      classifyNumericOperandWithPreferredType(Id::makeFromBool(true));
  EXPECT_EQ(otherClassification.homogeneousType_,
            HomogeneousNumericType::Other);
  EXPECT_EQ(otherClassification.preferredType_, HomogeneousNumericType::Other);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, ClassifySpanWithPreferredType) {
  std::array<ValueId, 4> ints{I(1), I(2), I(3), I(4)};
  std::array<ValueId, 4> doubles{D(1.0), D(2.0), D(3.0), D(4.0)};
  std::array<ValueId, 4> mostlyInts{I(1), I(2), I(3), D(4.0)};
  std::array<ValueId, 4> mostlyDoubles{I(1), D(2.0), D(3.0), D(4.0)};
  std::array<ValueId, 4> tied{I(1), I(2), D(3.0), D(4.0)};
  std::array<ValueId, 0> empty{};
  std::array<ValueId, 4> withNonNumeric{I(1), I(2), Id::makeFromBool(true),
                                        D(4.0)};
  std::array<ValueId, 4> withUndefined{I(1), I(2), Id::makeUndefined(), I(4)};
  std::vector<ValueId> mostlyNonNumeric(200, Id::makeFromBool(true));
  mostlyNonNumeric.push_back(D(1.0));
  mostlyNonNumeric.push_back(D(2.0));
  mostlyNonNumeric.push_back(D(3.0));
  mostlyNonNumeric.push_back(I(1));
  mostlyNonNumeric.push_back(I(2));
  std::array<ValueId, 5> tiedWithNonNumeric{
      I(1), I(2), D(3.0), Id::makeFromBool(true), Id::makeFromBool(false)};

  auto classification = classifyNumericOperandWithPreferredType(
      ql::span<const ValueId>{ints}, &context_);
  EXPECT_EQ(classification.homogeneousType_, HomogeneousNumericType::Int);
  EXPECT_EQ(classification.preferredType_, HomogeneousNumericType::Int);

  classification = classifyNumericOperandWithPreferredType(
      ql::span<const ValueId>{doubles}, &context_);
  EXPECT_EQ(classification.homogeneousType_, HomogeneousNumericType::Double);
  EXPECT_EQ(classification.preferredType_, HomogeneousNumericType::Double);

  classification = classifyNumericOperandWithPreferredType(
      ql::span<const ValueId>{mostlyInts}, &context_);
  EXPECT_EQ(classification.homogeneousType_, HomogeneousNumericType::Other);
  EXPECT_EQ(classification.preferredType_, HomogeneousNumericType::Int);

  classification = classifyNumericOperandWithPreferredType(
      ql::span<const ValueId>{mostlyDoubles}, &context_);
  EXPECT_EQ(classification.homogeneousType_, HomogeneousNumericType::Other);
  EXPECT_EQ(classification.preferredType_, HomogeneousNumericType::Double);

  classification = classifyNumericOperandWithPreferredType(
      ql::span<const ValueId>{tied}, &context_);
  EXPECT_EQ(classification.homogeneousType_, HomogeneousNumericType::Other);
  EXPECT_EQ(classification.preferredType_, HomogeneousNumericType::Other);

  classification = classifyNumericOperandWithPreferredType(
      ql::span<const ValueId>{empty}, &context_);
  EXPECT_EQ(classification.homogeneousType_, HomogeneousNumericType::Other);
  EXPECT_EQ(classification.preferredType_, HomogeneousNumericType::Other);

  classification = classifyNumericOperandWithPreferredType(
      ql::span<const ValueId>{withNonNumeric}, &context_);
  EXPECT_EQ(classification.homogeneousType_, HomogeneousNumericType::Other);
  EXPECT_EQ(classification.preferredType_, HomogeneousNumericType::Int);

  classification = classifyNumericOperandWithPreferredType(
      ql::span<const ValueId>{withUndefined}, &context_);
  EXPECT_EQ(classification.homogeneousType_, HomogeneousNumericType::Other);
  EXPECT_EQ(classification.preferredType_, HomogeneousNumericType::Int);

  classification = classifyNumericOperandWithPreferredType(
      ql::span<const ValueId>{mostlyNonNumeric}, &context_);
  EXPECT_EQ(classification.homogeneousType_, HomogeneousNumericType::Other);
  EXPECT_EQ(classification.preferredType_, HomogeneousNumericType::Other);

  classification = classifyNumericOperandWithPreferredType(
      ql::span<const ValueId>{tiedWithNonNumeric}, &context_);
  EXPECT_EQ(classification.homogeneousType_, HomogeneousNumericType::Other);
  EXPECT_EQ(classification.preferredType_, HomogeneousNumericType::Other);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       ClassifyOperandsWithPreferredType) {
  std::array<ValueId, 3> mostlyInts{I(1), I(2), D(3.0)};
  std::array<ValueId, 3> mostlyDoubles{I(1), D(2.0), D(3.0)};

  auto intsSpan = ql::span<const ValueId>{mostlyInts};
  auto doublesSpan = ql::span<const ValueId>{mostlyDoubles};

  const auto classifications = classifyNumericOperandsWithPreferredType(
      &context_, intsSpan, doublesSpan, I(5));

  EXPECT_EQ(classifications[0].homogeneousType_, HomogeneousNumericType::Other);
  EXPECT_EQ(classifications[0].preferredType_, HomogeneousNumericType::Int);

  EXPECT_EQ(classifications[1].homogeneousType_, HomogeneousNumericType::Other);
  EXPECT_EQ(classifications[1].preferredType_, HomogeneousNumericType::Double);

  EXPECT_EQ(classifications[2].homogeneousType_, HomogeneousNumericType::Int);
  EXPECT_EQ(classifications[2].preferredType_, HomogeneousNumericType::Int);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, GetHomogeneousNumericValue) {
  EXPECT_EQ(getHomogeneousNumericValue<int64_t>(I(-42)), -42);
  EXPECT_DOUBLE_EQ(getHomogeneousNumericValue<double>(D(3.5)), 3.5);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, MakeHomogeneousNumericGetter) {
  std::array<ValueId, 3> ints{I(4), I(5), I(6)};
  ql::span<const ValueId> span{ints};

  auto vectorGetter = makeHomogeneousNumericGetter<int64_t>(span);

  EXPECT_EQ(vectorGetter(0), 4);
  EXPECT_EQ(vectorGetter(1), 5);
  EXPECT_EQ(vectorGetter(2), 6);

  auto constantGetter = makeHomogeneousNumericGetter<int64_t>(I(7));

  EXPECT_EQ(constantGetter(0), 7);
  EXPECT_EQ(constantGetter(100), 7);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       EvaluateHomogeneousNumericOperation) {
  std::array<ValueId, 3> left{I(1), I(2), I(3)};
  std::array<ValueId, 3> right{I(4), I(5), I(6)};

  auto leftSpan = ql::span<const ValueId>{left};
  auto rightSpan = ql::span<const ValueId>{right};

  auto result = evaluateHomogeneousNumericOperation<TestAdd, int64_t, int64_t>(
      std::tie(leftSpan, rightSpan), &context_);

  const auto* resultVector = std::get_if<VectorWithMemoryLimit<Id>>(&result);

  ASSERT_NE(resultVector, nullptr);
  ASSERT_EQ(resultVector->size(), 3);

  EXPECT_EQ((*resultVector)[0], I(5));
  EXPECT_EQ((*resultVector)[1], I(7));
  EXPECT_EQ((*resultVector)[2], I(9));
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       EvaluateSpeculativeNumericOperationVectorVector) {
  std::array<ValueId, 3> left{I(1), D(2.5), I(3)};
  std::array<ValueId, 3> right{I(4), I(5), D(6.5)};

  auto leftSpan = ql::span<const ValueId>{left};
  auto rightSpan = ql::span<const ValueId>{right};

  auto result = evaluateSpeculativeNumericOperation<
      TestNumericAdd, TestNumericValueGetter, TestNumericValueGetter, int64_t,
      int64_t>(leftSpan, rightSpan, &context_);

  const auto* resultVector = std::get_if<VectorWithMemoryLimit<Id>>(&result);

  ASSERT_NE(resultVector, nullptr);
  ASSERT_EQ(resultVector->size(), 3);

  EXPECT_EQ((*resultVector)[0], I(5));
  EXPECT_EQ((*resultVector)[1], D(7.5));
  EXPECT_EQ((*resultVector)[2], D(9.5));
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       EvaluateSpeculativeNumericOperationVectorConstant) {
  std::array<ValueId, 3> left{I(1), D(2.5), I(3)};
  auto leftSpan = ql::span<const ValueId>{left};

  auto result = evaluateSpeculativeNumericOperation<
      TestNumericAdd, TestNumericValueGetter, TestNumericValueGetter, int64_t,
      int64_t>(leftSpan, I(4), &context_);

  const auto* resultVector = std::get_if<VectorWithMemoryLimit<Id>>(&result);

  ASSERT_NE(resultVector, nullptr);
  ASSERT_EQ(resultVector->size(), 3);

  EXPECT_EQ((*resultVector)[0], I(5));
  EXPECT_EQ((*resultVector)[1], D(6.5));
  EXPECT_EQ((*resultVector)[2], I(7));
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       EvaluateSpeculativeNumericOperationConstantVector) {
  std::array<ValueId, 3> right{I(1), D(2.5), I(3)};
  auto rightSpan = ql::span<const ValueId>{right};

  auto result = evaluateSpeculativeNumericOperation<
      TestNumericAdd, TestNumericValueGetter, TestNumericValueGetter, int64_t,
      int64_t>(I(4), rightSpan, &context_);

  const auto* resultVector = std::get_if<VectorWithMemoryLimit<Id>>(&result);

  ASSERT_NE(resultVector, nullptr);
  ASSERT_EQ(resultVector->size(), 3);

  EXPECT_EQ((*resultVector)[0], I(5));
  EXPECT_EQ((*resultVector)[1], D(6.5));
  EXPECT_EQ((*resultVector)[2], I(7));
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       EvaluateSpeculativeNumericOperationWithPreferredDouble) {
  std::array<ValueId, 3> left{D(1.5), D(2.5), I(3)};
  std::array<ValueId, 3> right{D(4.0), D(5.0), D(6.5)};

  auto leftSpan = ql::span<const ValueId>{left};
  auto rightSpan = ql::span<const ValueId>{right};

  auto result = evaluateSpeculativeNumericOperation<
      TestNumericAdd, TestNumericValueGetter, TestNumericValueGetter, double,
      double>(leftSpan, rightSpan, &context_);

  const auto* resultVector = std::get_if<VectorWithMemoryLimit<Id>>(&result);

  ASSERT_NE(resultVector, nullptr);
  ASSERT_EQ(resultVector->size(), 3);

  EXPECT_EQ((*resultVector)[0], D(5.5));
  EXPECT_EQ((*resultVector)[1], D(7.5));
  EXPECT_EQ((*resultVector)[2], D(9.5));
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       EvaluateSpeculativeNumericOperationSlowPathNumericCombinations) {
  std::array<ValueId, 3> left{I(1), D(2.5), D(3.5)};
  std::array<ValueId, 3> right{I(4), I(5), D(6.5)};

  auto leftSpan = ql::span<const ValueId>{left};
  auto rightSpan = ql::span<const ValueId>{right};

  auto result = evaluateSpeculativeNumericOperation<
      TestNumericAdd, TestNumericValueGetter, TestNumericValueGetter, double,
      int64_t>(leftSpan, rightSpan, &context_);

  const auto* resultVector = std::get_if<VectorWithMemoryLimit<Id>>(&result);

  ASSERT_NE(resultVector, nullptr);
  ASSERT_EQ(resultVector->size(), 3);

  EXPECT_EQ((*resultVector)[0], I(5));
  EXPECT_EQ((*resultVector)[1], D(7.5));
  EXPECT_EQ((*resultVector)[2], D(10.0));
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       DispatchHomogeneousNumericTypes) {
  auto result = dispatchHomogeneousNumericTypes(
      std::array{HomogeneousNumericType::Int, HomogeneousNumericType::Double},
      [](auto leftType, auto rightType) {
        using Left = typename decltype(leftType)::type;
        using Right = typename decltype(rightType)::type;

        return ql::concepts::same_as<Left, int64_t> &&
               ql::concepts::same_as<Right, double>;
      });

  EXPECT_TRUE(result);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       EvaluateHomogeneousNumericOperationWithThreeOperands) {
  std::array<ValueId, 3> first{I(1), I(2), I(3)};
  std::array<ValueId, 3> second{D(0.5), D(1.5), D(2.5)};

  auto firstSpan = ql::span<const ValueId>{first};
  auto secondSpan = ql::span<const ValueId>{second};
  auto constant = I(10);

  auto result = evaluateHomogeneousNumericOperation<TestAddThree, int64_t,
                                                    double, int64_t>(
      std::tie(firstSpan, secondSpan, constant), &context_);

  const auto* resultVector = std::get_if<VectorWithMemoryLimit<Id>>(&result);

  ASSERT_NE(resultVector, nullptr);
  ASSERT_EQ(resultVector->size(), 3);

  EXPECT_EQ((*resultVector)[0], D(11.5));
  EXPECT_EQ((*resultVector)[1], D(13.5));
  EXPECT_EQ((*resultVector)[2], D(15.5));
}

}  // namespace

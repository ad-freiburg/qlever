// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <map>
#include <memory>
#include <tuple>
#include <variant>
#include <vector>

#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "backports/concepts.h"
#include "engine/sparqlExpressions/HomogeneousNumericExpressionHelpers.h"

namespace sparqlExpression::detail {
// The counters distinguish typed rows, direct `UNDEF` and generic
// getter/variant evaluation without replacing the exact production getter type.
struct DateAwareTestFunction {
  static inline size_t genericCalls = 0;
  Id operator()(DateYearOrDuration, DateYearOrDuration) const {
    return Id::makeFromInt(7);
  }
  template <typename Left, typename Right>
  Id operator()(Left left, Right right) const {
    if constexpr (std::is_arithmetic_v<Left> && std::is_arithmetic_v<Right>) {
      return Id::makeFromDouble(left + right);
    } else {
      return Id::makeUndefined();
    }
  }
  Id operator()(NumericOrDateValue left, NumericOrDateValue right) const {
    ++genericCalls;
    return std::visit(*this, left, right);
  }
};
template <>
inline constexpr bool undefForNonNumericOrDate<DateAwareTestFunction> = true;
}  // namespace sparqlExpression::detail

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

// Test that `hasNonNumericOperand` rejects exactly the non-numeric `Id`s, and
// only for functions with `undefForNonNumeric` and the `NumericValueGetter`.
TEST_F(HomogeneousNumericExpressionHelpersTest, NonNumericFallbackEligibility) {
  // `MakeNumericExpression` opts in, an arbitrary function does not.
  using Function = MakeNumericExpression<std::multiplies<>>;
  using Getters = std::tuple<NumericValueGetter>;
  static_assert(undefForNonNumeric<Function>);
  static_assert(!undefForNonNumeric<TestNumericAdd>);

  // Integers, doubles and booleans are numeric.
  for (auto value :
       {I(1), D(2.5), Id::makeFromBool(true), Id::makeFromBool(false)}) {
    EXPECT_FALSE((hasNonNumericOperand<Function, Getters>(value)));
  }

  // All other datatypes are rejected, but not for a function without the
  // property or for a getter other than `NumericValueGetter`.
  for (auto value :
       {Id::makeUndefined(), Id::makeFromVocabIndex(VocabIndex::make(4)),
        Id::makeFromGeoPoint(GeoPoint{1.0, 2.0})}) {
    EXPECT_TRUE((hasNonNumericOperand<Function, Getters>(value)));
    EXPECT_FALSE((hasNonNumericOperand<TestNumericAdd, Getters>(value)));
    EXPECT_FALSE(
        (hasNonNumericOperand<Function, std::tuple<NumericOrDateValueGetter>>(
            value)));
  }

  // One non-numeric operand among several is enough.
  EXPECT_TRUE(
      (hasNonNumericOperand<Function,
                            std::tuple<NumericValueGetter, NumericValueGetter>>(
          I(1), Id::makeUndefined())));
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, SupportedValueGetters) {
  struct UnsupportedValueGetter {};
  static_assert(supportsNumericFastPath<NumericValueGetter>);
  static_assert(supportsNumericFastPath<NumericOrDateValueGetter>);
  static_assert(!supportsNumericFastPath<UnsupportedValueGetter>);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, SupportedOperandTypes) {
  static_assert(supportsNumericFastPathOperand<ValueId>());
  static_assert(supportsNumericFastPathOperand<ConstIdColumnRef>());
  static_assert(!supportsNumericFastPathOperand<int>());

  EXPECT_TRUE(supportsNumericFastPathOperand<ValueId>());
  EXPECT_TRUE(supportsNumericFastPathOperand<ConstIdColumnRef>());
  EXPECT_FALSE(supportsNumericFastPathOperand<int>());
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, NumericTypeToIndex) {
  EXPECT_EQ(numericTypeToIndex(NumericType::Int), 0);
  EXPECT_EQ(numericTypeToIndex(NumericType::Double), 1);
  EXPECT_ANY_THROW(numericTypeToIndex(NumericType::Other));
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, ClassifySingleValueId) {
  const auto intClassification = classifyNumericOperand(I(42));
  EXPECT_EQ(intClassification.homogeneousType_, NumericType::Int);
  EXPECT_EQ(intClassification.majorityType_, NumericType::Int);

  const auto doubleClassification = classifyNumericOperand(D(3.5));
  EXPECT_EQ(doubleClassification.homogeneousType_, NumericType::Double);
  EXPECT_EQ(doubleClassification.majorityType_, NumericType::Double);

  const auto boolClassification =
      classifyNumericOperand(Id::makeFromBool(true));
  EXPECT_EQ(boolClassification.homogeneousType_, NumericType::Other);
  EXPECT_EQ(boolClassification.majorityType_, NumericType::Other);

  const auto undefinedClassification =
      classifyNumericOperand(Id::makeUndefined());
  EXPECT_EQ(undefinedClassification.homogeneousType_, NumericType::Other);
  EXPECT_EQ(undefinedClassification.majorityType_, NumericType::Other);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, ClassifySpan) {
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

  auto classification =
      classifyNumericOperand(ConstIdColumnRef{ints}, &context_);
  EXPECT_EQ(classification.homogeneousType_, NumericType::Int);
  EXPECT_EQ(classification.majorityType_, NumericType::Int);

  classification = classifyNumericOperand(ConstIdColumnRef{doubles}, &context_);
  EXPECT_EQ(classification.homogeneousType_, NumericType::Double);
  EXPECT_EQ(classification.majorityType_, NumericType::Double);

  classification =
      classifyNumericOperand(ConstIdColumnRef{mostlyInts}, &context_);
  EXPECT_EQ(classification.homogeneousType_, NumericType::Other);
  EXPECT_EQ(classification.majorityType_, NumericType::Int);

  classification =
      classifyNumericOperand(ConstIdColumnRef{mostlyDoubles}, &context_);
  EXPECT_EQ(classification.homogeneousType_, NumericType::Other);
  EXPECT_EQ(classification.majorityType_, NumericType::Double);

  classification = classifyNumericOperand(ConstIdColumnRef{tied}, &context_);
  EXPECT_EQ(classification.homogeneousType_, NumericType::Other);
  EXPECT_EQ(classification.majorityType_, NumericType::Other);

  classification = classifyNumericOperand(ConstIdColumnRef{empty}, &context_);
  EXPECT_EQ(classification.homogeneousType_, NumericType::Other);
  EXPECT_EQ(classification.majorityType_, NumericType::Other);

  classification =
      classifyNumericOperand(ConstIdColumnRef{withNonNumeric}, &context_);
  EXPECT_EQ(classification.homogeneousType_, NumericType::Other);
  EXPECT_EQ(classification.majorityType_, NumericType::Int);

  classification =
      classifyNumericOperand(ConstIdColumnRef{withUndefined}, &context_);
  EXPECT_EQ(classification.homogeneousType_, NumericType::Other);
  EXPECT_EQ(classification.majorityType_, NumericType::Int);

  classification =
      classifyNumericOperand(ConstIdColumnRef{mostlyNonNumeric}, &context_);
  EXPECT_EQ(classification.homogeneousType_, NumericType::Other);
  EXPECT_EQ(classification.majorityType_, NumericType::Other);

  classification =
      classifyNumericOperand(ConstIdColumnRef{tiedWithNonNumeric}, &context_);
  EXPECT_EQ(classification.homogeneousType_, NumericType::Other);
  EXPECT_EQ(classification.majorityType_, NumericType::Other);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, ClassifyOperands) {
  std::array<ValueId, 3> mostlyInts{I(1), I(2), D(3.0)};
  std::array<ValueId, 3> mostlyDoubles{I(1), D(2.0), D(3.0)};

  auto intsSpan = ConstIdColumnRef{mostlyInts};
  auto doublesSpan = ConstIdColumnRef{mostlyDoubles};

  const auto classifications =
      classifyNumericOperands(&context_, intsSpan, doublesSpan, I(5));

  EXPECT_EQ(classifications[0].homogeneousType_, NumericType::Other);
  EXPECT_EQ(classifications[0].majorityType_, NumericType::Int);

  EXPECT_EQ(classifications[1].homogeneousType_, NumericType::Other);
  EXPECT_EQ(classifications[1].majorityType_, NumericType::Double);

  EXPECT_EQ(classifications[2].homogeneousType_, NumericType::Int);
  EXPECT_EQ(classifications[2].majorityType_, NumericType::Int);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, GetHomogeneousNumericValue) {
  EXPECT_EQ(getPrimitiveNumericValue<int64_t>(I(-42)), -42);
  EXPECT_DOUBLE_EQ(getPrimitiveNumericValue<double>(D(3.5)), 3.5);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, MakeHomogeneousNumericGetter) {
  std::array<ValueId, 3> ints{I(4), I(5), I(6)};
  ConstIdColumnRef span{ints};

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

  auto leftSpan = ConstIdColumnRef{left};
  auto rightSpan = ConstIdColumnRef{right};

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
TEST_F(HomogeneousNumericExpressionHelpersTest, DatatypeForNumericType) {
  EXPECT_EQ(datatypeForNumericType<int64_t>(), Datatype::Int);
  EXPECT_EQ(datatypeForNumericType<double>(), Datatype::Double);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       EvaluateSpeculativeNumericOperationVectorVector) {
  std::array<ValueId, 3> left{I(1), D(2.5), I(3)};
  std::array<ValueId, 3> right{I(4), I(5), D(6.5)};

  auto leftSpan = ConstIdColumnRef{left};
  auto rightSpan = ConstIdColumnRef{right};

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
  auto leftSpan = ConstIdColumnRef{left};

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
  auto rightSpan = ConstIdColumnRef{right};

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

  auto leftSpan = ConstIdColumnRef{left};
  auto rightSpan = ConstIdColumnRef{right};

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

  auto leftSpan = ConstIdColumnRef{left};
  auto rightSpan = ConstIdColumnRef{right};

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
TEST_F(HomogeneousNumericExpressionHelpersTest, DispatchNumericTypes) {
  auto result =
      dispatchNumericTypes(std::array{NumericType::Int, NumericType::Double},
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

  auto firstSpan = ConstIdColumnRef{first};
  auto secondSpan = ConstIdColumnRef{second};
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

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, GetHomogeneousNumericTypes) {
  std::array classifications{
      NumericOperandClassification{NumericType::Int, NumericType::Double},
      NumericOperandClassification{NumericType::Double, NumericType::Int}};

  const auto result = getHomogeneousNumericTypes(classifications);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value()[0], NumericType::Int);
  EXPECT_EQ(result.value()[1], NumericType::Double);

  classifications[1].homogeneousType_ = NumericType::Other;
  EXPECT_FALSE(getHomogeneousNumericTypes(classifications).has_value());
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, GetMajorityNumericTypes) {
  std::array classifications{
      NumericOperandClassification{NumericType::Other, NumericType::Int},
      NumericOperandClassification{NumericType::Other, NumericType::Double}};

  const auto result = getMajorityNumericTypes(classifications);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value()[0], NumericType::Int);
  EXPECT_EQ(result.value()[1], NumericType::Double);

  classifications[1].majorityType_ = NumericType::Other;
  EXPECT_FALSE(getMajorityNumericTypes(classifications).has_value());
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, DateClassification) {
  const auto date =
      Id::makeFromDate(DateYearOrDuration::parseXsdDate("2020-01-01"));
  const auto datetime = Id::makeFromDate(
      DateYearOrDuration::parseXsdDatetime("2020-01-01T12:00:00Z"));
  const auto duration =
      Id::makeFromDate(DateYearOrDuration::parseXsdDayTimeDuration("P1D"));
  const auto undef = Id::makeUndefined();
  const auto vocab = Id::makeFromVocabIndex(VocabIndex::make(4));
  EXPECT_EQ(classifyNumericOperand(date).homogeneousType_, NumericType::Other);
  EXPECT_EQ(classifyNumericOperand<true>(date).homogeneousType_,
            NumericType::Date);
  auto check = [&](std::vector<Id> values, NumericType homogeneous,
                   NumericType majority) {
    auto result =
        classifyNumericOperand<true>(ConstIdColumnRef{values}, &context_);
    EXPECT_EQ(result.homogeneousType_, homogeneous);
    EXPECT_EQ(result.majorityType_, majority);
  };
  check({date, datetime, duration}, NumericType::Date, NumericType::Date);
  check({date, date, undef}, NumericType::Other, NumericType::Date);
  check({date, date, I(1), undef, vocab}, NumericType::Other,
        NumericType::Date);
  check({date, I(1)}, NumericType::Other, NumericType::Other);
  check({date, date, undef, undef}, NumericType::Other, NumericType::Other);
  check({date, date, undef, undef, undef}, NumericType::Other,
        NumericType::Other);
  check({I(1), I(2), date}, NumericType::Other, NumericType::Int);
  check({D(1), D(2), date}, NumericType::Other, NumericType::Double);
  check({}, NumericType::Other, NumericType::Other);
  // Numeric-only classification still counts dates as competing values.
  std::vector<Id> values{I(1), I(2), date, date};
  EXPECT_EQ(
      classifyNumericOperand(ConstIdColumnRef{values}, &context_).majorityType_,
      NumericType::Other);
  context_.cancellationHandle_->cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(
      classifyNumericOperand<true>(ConstIdColumnRef{values}, &context_),
      ad_utility::CancellationException);
}

// Compare the classifier with independent per-value frequency counts across
// homogeneous/mixed blocks, incomplete blocks, and a cancellation-chunk
// boundary.
TEST_F(HomogeneousNumericExpressionHelpersTest, ClassificationBlockBoundaries) {
  const auto date =
      Id::makeFromDate(DateYearOrDuration::parseXsdDate("2020-01-01"));
  const std::array types{I(1),
                         D(2),
                         date,
                         Id::makeUndefined(),
                         Id::makeFromBool(true),
                         Id::makeFromVocabIndex(VocabIndex::make(4))};
  auto check = [&]<bool IncludeDates>(const std::vector<Id>& values) {
    std::map<Datatype, size_t> frequencies;
    for (auto value : values) {
      ++frequencies[value.getDatatype()];
    }
    NumericOperandClassification expected;
    if (!values.empty()) {
      expected.containsValueRejectedByGetter_ =
          std::any_of(values.begin(), values.end(), [](Id value) {
            const auto type = value.getDatatype();
            return type != Datatype::Int && type != Datatype::Double &&
                   type != Datatype::Bool &&
                   (!IncludeDates || type != Datatype::Date);
          });
    }
    const std::array candidates{
        std::pair{Datatype::Int, NumericType::Int},
        std::pair{Datatype::Double, NumericType::Double},
        std::pair{Datatype::Date, NumericType::Date}};
    for (auto [datatype, type] : candidates) {
      if (!IncludeDates && datatype == Datatype::Date) {
        continue;
      }
      const size_t count = frequencies[datatype];
      if (!values.empty() && count == values.size()) {
        expected.homogeneousType_ = type;
      }
      if (count != 0 &&
          std::all_of(frequencies.begin(), frequencies.end(), [&](auto item) {
            return item.first == datatype || item.second < count;
          })) {
        expected.majorityType_ = type;
      }
    }
    auto actual = classifyNumericOperand<IncludeDates>(ConstIdColumnRef{values},
                                                       &context_);
    EXPECT_EQ(actual.homogeneousType_, expected.homogeneousType_);
    EXPECT_EQ(actual.majorityType_, expected.majorityType_);
    EXPECT_EQ(actual.containsValueRejectedByGetter_,
              expected.containsValueRejectedByGetter_);
  };
  for (size_t size :
       {0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100001}) {
    SCOPED_TRACE(size);
    std::vector<Id> values(size, I(1));
    check.template operator()<false>(values);
    check.template operator()<true>(values);
    // Exercise the aggregated two-type branch as well as arbitrary mixtures.
    for (size_t i = 0; i < size; ++i) {
      values[i] = i % 3 == 0 ? date : Id::makeUndefined();
    }
    check.template operator()<false>(values);
    check.template operator()<true>(values);
    for (size_t i = 0; i < size; ++i) {
      values[i] = i / 8 % 3 == 0 ? I(1) : types[(i * 7 + i / 8) % types.size()];
    }
    check.template operator()<false>(values);
    check.template operator()<true>(values);
  }
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, SupportedMixedOperands) {
  const auto date =
      Id::makeFromDate(DateYearOrDuration::parseXsdDate("2020-01-01"));
  std::vector<Id> values{I(1), D(2), Id::makeFromBool(true), date};
  EXPECT_FALSE(classifyNumericOperand<true>(ConstIdColumnRef{values}, &context_)
                   .containsValueRejectedByGetter_);
  EXPECT_TRUE(classifyNumericOperand(ConstIdColumnRef{values}, &context_)
                  .containsValueRejectedByGetter_);
  EXPECT_FALSE(classifyNumericOperand(Id::makeFromBool(true))
                   .containsValueRejectedByGetter_);
  EXPECT_FALSE(
      classifyNumericOperand<true>(date).containsValueRejectedByGetter_);
  values.push_back(Id::makeUndefined());
  EXPECT_TRUE(classifyNumericOperand<true>(ConstIdColumnRef{values}, &context_)
                  .containsValueRejectedByGetter_);
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest, DateAwareUndefinedEligibility) {
  using Function = DateAwareTestFunction;
  using Getters = std::tuple<NumericOrDateValueGetter>;
  const auto date =
      Id::makeFromDate(DateYearOrDuration::parseXsdDate("2020-01-01"));
  static_assert(supportsDateOperands<Getters>);
  static_assert(!supportsDateOperands<std::tuple<NumericValueGetter>>);
  static_assert(!supportsDateOperands<
                std::tuple<NumericValueGetter, NumericOrDateValueGetter>>);
  for (auto value : {date, I(1), D(2), Id::makeFromBool(true)}) {
    EXPECT_FALSE((hasNonNumericOperand<Function, Getters>(value)));
  }
  for (auto value :
       {Id::makeUndefined(), Id::makeFromVocabIndex(VocabIndex::make(4)),
        Id::makeFromGeoPoint(GeoPoint{1.0, 2.0})}) {
    EXPECT_TRUE((hasNonNumericOperand<Function, Getters>(value)));
    EXPECT_FALSE((hasNonNumericOperand<TestNumericAdd, Getters>(value)));
  }
}

// _____________________________________________________________________________
TEST_F(HomogeneousNumericExpressionHelpersTest,
       SpeculativeDateBypassesGenericGetter) {
  using Function = DateAwareTestFunction;
  using Getter = NumericOrDateValueGetter;
  using Getters = std::tuple<Getter, Getter>;
  const auto date =
      Id::makeFromDate(DateYearOrDuration::parseXsdDate("2020-01-01"));
  const auto undef = Id::makeUndefined();
  VectorWithMemoryLimit<Id> left{{date, undef, date}, context_._allocator};
  VectorWithMemoryLimit<Id> right{{date, date, undef}, context_._allocator};
  Function::genericCalls = 0;
  auto binary = evaluateSpeculativeNumericOperation<
      Function, Getter, Getter, DateYearOrDuration, DateYearOrDuration>(
      left, right, &context_);
  EXPECT_EQ(
      (std::get<VectorWithMemoryLimit<Id>>(binary)),
      (VectorWithMemoryLimit<Id>{{I(7), undef, undef}, context_._allocator}));
  EXPECT_EQ(Function::genericCalls, 0);
  auto nary = evaluateSpeculativeNaryNumericOperation<
      Function, Getters, DateYearOrDuration, DateYearOrDuration>(
      std::tie(left, right), &context_);
  EXPECT_EQ(
      (std::get<VectorWithMemoryLimit<Id>>(nary)),
      (VectorWithMemoryLimit<Id>{{I(7), undef, undef}, context_._allocator}));
  EXPECT_EQ(Function::genericCalls, 0);
  // Defined numeric and boolean outliers keep the getter's existing conversion.
  left[1] = Id::makeFromBool(true);
  right[1] = I(2);
  auto boolean = evaluateSpeculativeNumericOperation<
      Function, Getter, Getter, DateYearOrDuration, DateYearOrDuration>(
      left, right, &context_);
  EXPECT_EQ(std::get<VectorWithMemoryLimit<Id>>(boolean)[1], D(3));
  EXPECT_EQ(Function::genericCalls, 1);
}

}  // namespace

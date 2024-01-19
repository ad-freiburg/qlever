// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Fabian Krause (fabian.krause@students.uni-freiburg.de)

#pragma once

#include "engine/sparqlExpressions/RelationalExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

struct AvgAggregationData {
  using ValueGetter = sparqlExpression::detail::NumericValueGetter;
  bool error_ = false;
  double sum_ = 0;
  int64_t count_ = 0;
  void addValue(auto&& value, const sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    auto intValueAdder = [this](int64_t intValue) {
      sum_ += static_cast<double>(intValue);
    };
    auto doubleValueAdder = [this](double doubleValue) { sum_ += doubleValue; };
    auto notNumericAdder = [this](sparqlExpression::detail::NotNumeric) {
      error_ = true;
    };
    std::visit(ad_utility::OverloadCallOperator(intValueAdder, doubleValueAdder,
                                                notNumericAdder),
               val);
    count_++;
  };
  [[nodiscard]] ValueId calculateResult(
      [[maybe_unused]] const LocalVocab* localVocab) const {
    if (error_)
      return ValueId::makeUndefined();
    else
      return ValueId::makeFromDouble(sum_ / static_cast<double>(count_));
  }
};

// Data to perform the COUNT aggregation using the HashMap optimization.
struct CountAggregationData {
  using ValueGetter = sparqlExpression::detail::IsValidValueGetter;
  int64_t count_ = 0;
  void addValue(auto&& value, const sparqlExpression::EvaluationContext* ctx) {
    if (ValueGetter{}(AD_FWD(value), ctx)) count_++;
  }
  [[nodiscard]] ValueId calculateResult(
      [[maybe_unused]] const LocalVocab* localVocab) const {
    return ValueId::makeFromInt(count_);
  }
};

// Data to perform MIN/MAX aggregation using the HashMap optimization.
template <valueIdComparators::Comparison Comp>
struct ExtremumAggregationData {
  sparqlExpression::IdOrString currentValue_ = ValueId::makeUndefined();
  void addValue(const sparqlExpression::IdOrString& value,
                const sparqlExpression::EvaluationContext* ctx) {
    if (auto val = std::get_if<ValueId>(&currentValue_);
        val != nullptr && val->isUndefined()) {
      currentValue_ = value;
      return;
    }
    auto impl = [&ctx]<typename X, typename Y>(const X& x, const Y& y) {
      return toBoolNotUndef(
          sparqlExpression::compareIdsOrStrings<
              Comp, valueIdComparators::ComparisonForIncompatibleTypes::
                        CompareByType>(x, y, ctx));
    };
    if (std::visit(impl, value, currentValue_)) currentValue_ = value;
  }
  [[nodiscard]] ValueId calculateResult(LocalVocab* localVocab) const {
    auto valueIdResultGetter = [](ValueId id) { return id; };
    auto stringResultGetter = [localVocab](const std::string& str) {
      auto localVocabIndex = localVocab->getIndexAndAddIfNotContained(str);
      return ValueId::makeFromLocalVocabIndex(localVocabIndex);
    };
    return std::visit(ad_utility::OverloadCallOperator(valueIdResultGetter,
                                                       stringResultGetter),
                      currentValue_);
  }
};

using MinAggregationData =
    ExtremumAggregationData<valueIdComparators::Comparison::LT>;
using MaxAggregationData =
    ExtremumAggregationData<valueIdComparators::Comparison::GT>;

// Data to perform the SUM aggregation using the HashMap optimization.
struct SumAggregationData {
  using ValueGetter = sparqlExpression::detail::NumericValueGetter;
  bool error_ = false;
  double sum_ = 0;
  void addValue(auto&& value, const sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    auto intValueAdder = [this](int64_t intValue) {
      sum_ += static_cast<double>(intValue);
    };
    auto doubleValueAdder = [this](double doubleValue) { sum_ += doubleValue; };
    auto notNumericAdder = [this](sparqlExpression::detail::NotNumeric) {
      error_ = true;
    };
    std::visit(ad_utility::OverloadCallOperator(intValueAdder, doubleValueAdder,
                                                notNumericAdder),
               val);
  };
  [[nodiscard]] ValueId calculateResult(
      [[maybe_unused]] const LocalVocab* localVocab) const {
    if (error_)
      return ValueId::makeUndefined();
    else
      return ValueId::makeFromDouble(sum_);
  }
};

// Data to perform GROUP_CONCAT aggregation using the HashMap optimization.
struct GroupConcatAggregationData {
  using ValueGetter = sparqlExpression::detail::StringValueGetter;
  std::string currentValue_;
  std::string separator_;
  void addValue(auto&& value, const sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    if (val.has_value()) {
      if (!currentValue_.empty()) currentValue_.append(separator_);
      currentValue_.append(val.value());
    }
  }
  [[nodiscard]] ValueId calculateResult(LocalVocab* localVocab) const {
    auto localVocabIndex =
        localVocab->getIndexAndAddIfNotContained(currentValue_);
    return ValueId::makeFromLocalVocabIndex(localVocabIndex);
  }

  explicit GroupConcatAggregationData(std::string separator)
      : separator_{std::move(separator)} {
    currentValue_.reserve(20000);
  }
};

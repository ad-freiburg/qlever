// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Fabian Krause (fabian.krause@students.uni-freiburg.de)

#pragma once

#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

// _____________________________________________________________________________
// For `AVG`, add value to sum if it is numeric, otherwise
// set error flag.
static constexpr auto valueAdder = []() {
  auto numericValueAdder =
      []<typename T>(T value, double& sum, [[maybe_unused]] const bool& error)
          requires std::is_arithmetic_v<T> {
    sum += static_cast<double>(value);
  };
  auto nonNumericValueAdder = [](sparqlExpression::detail::NotNumeric,
                                 [[maybe_unused]] const double& sum,
                                 bool& error) { error = true; };
  return ad_utility::OverloadCallOperator(numericValueAdder,
                                          nonNumericValueAdder);
}();

// Data to perform the AVG aggregation using the HashMap optimization.
struct AvgAggregationData {
  using ValueGetter = sparqlExpression::detail::NumericValueGetter;
  bool error_ = false;
  double sum_ = 0;
  int64_t count_ = 0;

  // _____________________________________________________________________________
  void addValue(auto&& value, const sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    std::visit([this](auto val) { valueAdder(val, sum_, error_); }, val);
    count_++;
  };

  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(
      [[maybe_unused]] const LocalVocab* localVocab) const {
    if (error_) {
      return ValueId::makeUndefined();
    }
    // AVG(empty group) = 0, this is mandated by the SPARQL 1.1 standard.
    if (count_ == 0) {
      return ValueId::makeFromInt(0);
    }

    return ValueId::makeFromDouble(sum_ / static_cast<double>(count_));
  }
};

// Data to perform the COUNT aggregation using the HashMap optimization.
struct CountAggregationData {
  using ValueGetter = sparqlExpression::detail::IsValidValueGetter;
  int64_t count_ = 0;

  // _____________________________________________________________________________
  void addValue(auto&& value, const sparqlExpression::EvaluationContext* ctx) {
    if (ValueGetter{}(AD_FWD(value), ctx)) count_++;
  }

  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(
      [[maybe_unused]] const LocalVocab* localVocab) const {
    return ValueId::makeFromInt(count_);
  }
};

// Data to perform MIN/MAX aggregation using the HashMap optimization.
template <valueIdComparators::Comparison Comp>
struct ExtremumAggregationData {
  sparqlExpression::IdOrLiteralOrIri currentValue_;
  bool firstValueSet_ = false;

  // _____________________________________________________________________________
  void addValue(const sparqlExpression::IdOrLiteralOrIri& value,
                const sparqlExpression::EvaluationContext* ctx) {
    if (!firstValueSet_) {
      currentValue_ = value;
      firstValueSet_ = true;
      return;
    }

    currentValue_ = sparqlExpression::detail::minMaxLambdaForAllTypes<Comp>(
        value, currentValue_, ctx);
  }

  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(LocalVocab* localVocab) const {
    auto valueIdResultGetter = [](ValueId id) { return id; };
    auto stringResultGetter =
        [localVocab](const ad_utility::triple_component::LiteralOrIri& str) {
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
  bool intSumValid_ = true;
  double sum_ = 0;
  int64_t intSum_ = 0;

  // _____________________________________________________________________________
  void addValue(auto&& value, const sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);

    auto doubleValueAdder = [this](double value) {
      sum_ += value;
      intSumValid_ = false;
    };
    auto intValueAdder = [this](int64_t value) {
      sum_ += static_cast<double>(value);
      intSum_ += value;
    };
    auto nonNumericValueAdder = [this](sparqlExpression::detail::NotNumeric) {
      error_ = true;
    };
    auto sumValueAdder = ad_utility::OverloadCallOperator(
        doubleValueAdder, intValueAdder, nonNumericValueAdder);

    std::visit(sumValueAdder, val);
  };

  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(
      [[maybe_unused]] const LocalVocab* localVocab) const {
    if (error_) {
      return ValueId::makeUndefined();
    }
    if (intSumValid_) {
      return ValueId::makeFromInt(intSum_);
    }
    return ValueId::makeFromDouble(sum_);
  }
};

// Data to perform GROUP_CONCAT aggregation using the HashMap optimization.
struct GroupConcatAggregationData {
  using ValueGetter = sparqlExpression::detail::StringValueGetter;
  std::string currentValue_;
  std::string_view separator_;

  // _____________________________________________________________________________
  void addValue(auto&& value, const sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    if (val.has_value()) {
      if (!currentValue_.empty()) currentValue_.append(separator_);
      currentValue_.append(val.value());
    }
  }

  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(LocalVocab* localVocab) const {
    using namespace ad_utility::triple_component;
    using Lit = ad_utility::triple_component::Literal;
    auto localVocabIndex = localVocab->getIndexAndAddIfNotContained(
        LiteralOrIri{Lit::literalWithNormalizedContent(
            asNormalizedStringViewUnsafe(currentValue_))});
    return ValueId::makeFromLocalVocabIndex(localVocabIndex);
  }

  // _____________________________________________________________________________
  explicit GroupConcatAggregationData(std::string_view separator)
      : separator_{separator} {
    currentValue_.reserve(20000);
  }
};

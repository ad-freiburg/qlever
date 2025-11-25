// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Fabian Krause (fabian.krause@students.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_GROUPBYHASHMAPOPTIMIZATION_H
#define QLEVER_SRC_ENGINE_GROUPBYHASHMAPOPTIMIZATION_H

#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/GroupConcatHelper.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

// _____________________________________________________________________________
// For `AVG`, add value to sum if it is numeric, otherwise
// set error flag.
static constexpr auto valueAdder = []() {
  auto numericValueAdder = [](auto value, double& sum,
                              [[maybe_unused]] const bool& error)
      -> CPP_ret(void)(requires std::is_arithmetic_v<decltype(value)>) {
    sum += static_cast<double>(value);
  };
  auto nonNumericValueAdder = [](sparqlExpression::detail::NotNumeric,
                                 [[maybe_unused]] const double& sum,
                                 bool& error) { error = true; };
  return ad_utility::OverloadCallOperator{numericValueAdder,
                                          nonNumericValueAdder};
}();

// Data to perform the AVG aggregation using the HashMap optimization.
struct AvgAggregationData {
  using ValueGetter = sparqlExpression::detail::NumericValueGetter;
  bool error_ = false;
  double sum_ = 0;
  int64_t count_ = 0;

  // _____________________________________________________________________________
  template <typename T>
  void addValue(T&& value, const sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    std::visit([this](auto val) { valueAdder(val, sum_, error_); }, val);
    count_++;
  };

  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(
      [[maybe_unused]] const LocalVocab* localVocab) const;

  void reset() { *this = AvgAggregationData{}; }
};

// Data to perform the COUNT aggregation using the HashMap optimization.
struct CountAggregationData {
  using ValueGetter = sparqlExpression::detail::IsValidValueGetter;
  int64_t count_ = 0;

  // _____________________________________________________________________________
  template <typename T>
  void addValue(T&& value, const sparqlExpression::EvaluationContext* ctx) {
    if (ValueGetter{}(AD_FWD(value), ctx)) count_++;
  }

  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(
      [[maybe_unused]] const LocalVocab* localVocab) const;

  void reset() { *this = CountAggregationData{}; }
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

    currentValue_ = sparqlExpression::detail::MinMaxLambdaForAllTypes<Comp>{}(
        value, currentValue_, ctx);
  }

  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(LocalVocab* localVocab) const;

  void reset() { *this = ExtremumAggregationData{}; }
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
  template <typename T>
  void addValue(T&& value, const sparqlExpression::EvaluationContext* ctx) {
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
    auto sumValueAdder = ad_utility::OverloadCallOperator{
        doubleValueAdder, intValueAdder, nonNumericValueAdder};

    std::visit(sumValueAdder, val);
  };

  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(
      [[maybe_unused]] const LocalVocab* localVocab) const;

  void reset() { *this = SumAggregationData{}; }
};

// Data to perform GROUP_CONCAT aggregation using the HashMap optimization.
struct GroupConcatAggregationData {
  using ValueGetter =
      sparqlExpression::detail::LiteralValueGetterWithoutStrFunction;
  bool undefined_ = false;
  bool first_ = true;
  std::string currentValue_;
  std::string_view separator_;
  std::optional<std::string> langTag_;

  // _____________________________________________________________________________
  template <typename T>
  void addValue(T&& value, const sparqlExpression::EvaluationContext* ctx) {
    // No need to compute anything in this case.
    if (undefined_) {
      return;
    }
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    addValueImpl(val);
  }

  // Actual implementation of `addValue`, but without template parameters.
  void addValueImpl(
      const std::optional<ad_utility::triple_component::Literal>& value);

  [[nodiscard]] ValueId calculateResult(LocalVocab* localVocab) const;

  explicit GroupConcatAggregationData(std::string_view separator);

  void reset();
};

// Data to perform SAMPLE aggregation using the HashMap optimization.
struct SampleAggregationData {
  std::optional<sparqlExpression::IdOrLiteralOrIri> value_ = std::nullopt;

  // _____________________________________________________________________________
  void addValue(const sparqlExpression::IdOrLiteralOrIri& value,
                [[maybe_unused]] const sparqlExpression::EvaluationContext*) {
    if (!value_.has_value()) {
      value_.emplace(value);
    }
  }

  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(LocalVocab* localVocab) const;

  void reset() { *this = SampleAggregationData{}; }
};

#endif  // QLEVER_SRC_ENGINE_GROUPBYHASHMAPOPTIMIZATION_H

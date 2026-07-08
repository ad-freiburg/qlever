// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Fabian Krause (fabian.krause@students.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_GROUPBYHASHMAPOPTIMIZATION_H
#define QLEVER_SRC_ENGINE_GROUPBYHASHMAPOPTIMIZATION_H

#include "engine/sparqlExpressions/AggregateExpression.h"
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
  auto nonNumericValueAdder = [](qlever::sparqlExpression::detail::NotNumeric,
                                 [[maybe_unused]] const double& sum,
                                 bool& error) { error = true; };
  return ad_utility::OverloadCallOperator{numericValueAdder,
                                          nonNumericValueAdder};
}();

// Data to perform the AVG aggregation using the HashMap optimization.
struct AvgAggregationData {
  using ValueGetter = qlever::sparqlExpression::detail::NumericValueGetter;
  bool error_ = false;
  double sum_ = 0;
  int64_t count_ = 0;

  // _____________________________________________________________________________
  template <typename T>
  void addValue(T&& value,
                const qlever::sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    std::visit([this](auto val) { valueAdder(val, sum_, error_); }, val);
    count_++;
  };

  // _____________________________________________________________________________
  [[nodiscard]] qlever::ValueId calculateResult(
      [[maybe_unused]] const qlever::LocalVocabContext& context,
      [[maybe_unused]] const qlever::LocalVocab* localVocab) const;

  void reset() { *this = AvgAggregationData{}; }
};

// Data to perform the COUNT aggregation using the HashMap optimization.
struct CountAggregationData {
  using ValueGetter = qlever::sparqlExpression::detail::IsValidValueGetter;
  int64_t count_ = 0;

  // _____________________________________________________________________________
  template <typename T>
  void addValue(T&& value,
                const qlever::sparqlExpression::EvaluationContext* ctx) {
    if (ValueGetter{}(AD_FWD(value), ctx)) count_++;
  }

  // _____________________________________________________________________________
  [[nodiscard]] qlever::ValueId calculateResult(
      [[maybe_unused]] const qlever::LocalVocabContext& context,
      [[maybe_unused]] const qlever::LocalVocab* localVocab) const;

  void reset() { *this = CountAggregationData{}; }
};

// Data to perform MIN/MAX aggregation using the HashMap optimization.
template <qlever::valueIdComparators::Comparison Comp>
struct ExtremumAggregationData {
  qlever::sparqlExpression::IdOrLocalVocabEntry currentValue_;
  bool firstValueSet_ = false;

  // _____________________________________________________________________________
  void addValue(const qlever::sparqlExpression::IdOrLocalVocabEntry& value,
                const qlever::sparqlExpression::EvaluationContext* ctx) {
    if (!firstValueSet_) {
      currentValue_ = value;
      firstValueSet_ = true;
      return;
    }

    currentValue_ =
        qlever::sparqlExpression::detail::MinMaxLambdaForAllTypes<Comp>{}(
            value, currentValue_, ctx);
  }

  // _____________________________________________________________________________
  [[nodiscard]] qlever::ValueId calculateResult(
      [[maybe_unused]] const qlever::LocalVocabContext& context,
      qlever::LocalVocab* localVocab) const;

  void reset() { *this = ExtremumAggregationData{}; }
};

using MinAggregationData =
    ExtremumAggregationData<qlever::valueIdComparators::Comparison::LT>;
using MaxAggregationData =
    ExtremumAggregationData<qlever::valueIdComparators::Comparison::GT>;

// Data to perform the SUM aggregation using the HashMap optimization.
struct SumAggregationData {
  using ValueGetter = qlever::sparqlExpression::detail::NumericValueGetter;
  bool error_ = false;
  bool intSumValid_ = true;
  double sum_ = 0;
  int64_t intSum_ = 0;

  // _____________________________________________________________________________
  template <typename T>
  void addValue(T&& value,
                const qlever::sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);

    auto doubleValueAdder = [this](double value) {
      sum_ += value;
      intSumValid_ = false;
    };
    auto intValueAdder = [this](int64_t value) {
      sum_ += static_cast<double>(value);
      intSum_ += value;
    };
    auto nonNumericValueAdder =
        [this](qlever::sparqlExpression::detail::NotNumeric) { error_ = true; };
    auto sumValueAdder = ad_utility::OverloadCallOperator{
        doubleValueAdder, intValueAdder, nonNumericValueAdder};

    std::visit(sumValueAdder, val);
  };

  // _____________________________________________________________________________
  [[nodiscard]] qlever::ValueId calculateResult(
      [[maybe_unused]] const qlever::LocalVocabContext& context,
      [[maybe_unused]] const qlever::LocalVocab* localVocab) const;

  void reset() { *this = SumAggregationData{}; }
};

// Data to perform GROUP_CONCAT aggregation using the HashMap optimization.
struct GroupConcatAggregationData {
  using ValueGetter =
      qlever::sparqlExpression::detail::LiteralValueGetterWithoutStrFunction;
  bool undefined_ = false;
  bool first_ = true;
  std::string currentValue_;
  std::string_view separator_;

  // _____________________________________________________________________________
  template <typename T>
  void addValue(T&& value,
                const qlever::sparqlExpression::EvaluationContext* ctx) {
    // No need to compute anything in this case.
    if (undefined_) {
      return;
    }
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    addValueImpl(val);
  }

  // Actual implementation of `addValue`, but without template parameters.
  void addValueImpl(
      const std::optional<qlever::triple_component::Literal>& value);

  [[nodiscard]] qlever::ValueId calculateResult(
      const qlever::LocalVocabContext& context,
      qlever::LocalVocab* localVocab) const;

  explicit GroupConcatAggregationData(std::string_view separator);

  void reset();
};

// Data to perform SAMPLE aggregation using the HashMap optimization.
struct SampleAggregationData {
  std::optional<qlever::sparqlExpression::IdOrLocalVocabEntry> value_ =
      std::nullopt;

  // _____________________________________________________________________________
  void addValue(
      const qlever::sparqlExpression::IdOrLocalVocabEntry& value,
      [[maybe_unused]] const qlever::sparqlExpression::EvaluationContext*) {
    if (!value_.has_value()) {
      value_.emplace(value);
    }
  }

  // _____________________________________________________________________________
  [[nodiscard]] qlever::ValueId calculateResult(
      [[maybe_unused]] const qlever::LocalVocabContext& context,
      qlever::LocalVocab* localVocab) const;

  void reset() { *this = SampleAggregationData{}; }
};
#endif  // QLEVER_SRC_ENGINE_GROUPBYHASHMAPOPTIMIZATION_H

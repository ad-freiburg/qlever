//
// Created by Benedikt Kantz on 16.11.26.
//

#ifndef QLEVER_SRC_TENSORAGGREGATE_H
#define QLEVER_SRC_TENSORAGGREGATE_H

#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "rdfTypes/TensorData.h"

// Data to perform the SUM aggregation using the HashMap optimization.
struct TensorAggregationData {
  using ValueGetter = sparqlExpression::detail::TensorValueGetter;
  bool error_ = false;
  std::optional<ad_utility::TensorData> result_;
  // _____________________________________________________________________________
  [[nodiscard]] ValueId calculateResult(
      [[maybe_unused]] const LocalVocab* localVocab) const;
};
struct TensorSumAggregationData : TensorAggregationData {

  template <typename T>
  void addValue(T&& value, const sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    auto adder = [this](ad_utility::TensorData value) {
      try {
        result_ = result_ + value;
      } catch (...) {
        error_ = true;
      }
    };
    std::visit(adder, val);
  };

  void reset() { *this = TensorSumAggregationData{}; }
};
struct TensorMeanAggregationData:TensorAggregationData {

  template <typename T>
  void addValue(T&& value, const sparqlExpression::EvaluationContext* ctx) {
    auto val = ValueGetter{}(AD_FWD(value), ctx);
    auto adder = [this](ad_utility::TensorData value) {
      try {
        sum_ = sum_ + value;
      } catch (...) {
        error_ = true;
      }
    };
    std::visit(adder, val);
  };
  void reset() { *this = TensorMeanAggregationData{}; }
};
#endif  // QLEVER_TENSORAGGREGATE_H

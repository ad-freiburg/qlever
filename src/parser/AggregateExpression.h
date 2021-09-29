//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 28.09.21.
//

#ifndef QLEVER_AGGREGATEEXPRESSION_H
#define QLEVER_AGGREGATEEXPRESSION_H

#include "./SparqlExpression.h"
namespace sparqlExpression {

namespace detail {
template <typename RangeCalculation, typename ValueGetter, typename AggregateOp,
          typename FinalOp, TagString Tag>
class AggregateExpression : public SparqlExpression {
 public:
  AggregateExpression(bool distinct, Ptr&& child,
                      AggregateOp aggregateOp = AggregateOp{})
      : _distinct(distinct),
        _child{std::move(child)},
        _aggregateOp{std::move(aggregateOp)} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override { return {&_child, 1}; }

  // _________________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override {
    // This is an aggregation, so it never leaves any unaggregated variables.
    return {};
  }

  // __________________________________________________________________________
  string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return std::string{Tag} + "(" + _child->getCacheKey(varColMap) + ")";
  }

  // __________________________________________________________________________
  std::optional<string> getVariableForNonDistinctCountOrNullopt()
      const override {
    if (Tag == "COUNT" && !_distinct) {
      return _child->getVariableOrNullopt();
    }
    return std::nullopt;
  }

 private:
  bool _distinct;
  Ptr _child;
  AggregateOp _aggregateOp;
};

// This can be used as the FinalOp parameter to an Aggregate if there is nothing
// to be done on the final result
inline auto noop = []<typename T>(T&& result, size_t) {
  return std::forward<T>(result);
};

}  // namespace detail

// The Aggregate expressions.

// __________________________________________________________________________
inline auto count = [](const auto& a, const auto& b) -> int64_t {
  return a + b;
};

using CountExpression =
    detail::AggregateExpression<detail::NoCalculationWithSetOfIntervals,
                                detail::IsValidValueGetter, decltype(count),
                                decltype(detail::noop), "COUNT">;

inline auto add = [](const auto& a, const auto& b) -> double { return a + b; };
using SumExpression =
    detail::AggregateExpression<detail::NoCalculationWithSetOfIntervals,
                                detail::NumericValueGetter, decltype(add),
                                decltype(detail::noop), "SUM">;

inline auto averageFinalOp = [](const auto& aggregation, size_t numElements) {
  return numElements ? static_cast<double>(aggregation) /
                           static_cast<double>(numElements)
                     : std::numeric_limits<double>::quiet_NaN();
};
using AvgExpression =
    detail::AggregateExpression<detail::NoCalculationWithSetOfIntervals,
                                detail::NumericValueGetter, decltype(add),
                                decltype(averageFinalOp), "AVG">;

inline auto minLambda = [](const auto& a, const auto& b) -> double {
  return a < b ? a : b;
};
inline auto maxLambda = [](const auto& a, const auto& b) -> double {
  return a > b ? a : b;
};

using MinExpression =
    detail::AggregateExpression<detail::NoCalculationWithSetOfIntervals,
                                detail::NumericValueGetter, decltype(minLambda),
                                decltype(detail::noop), "MIN">;
using MaxExpression =
    detail::AggregateExpression<detail::NoCalculationWithSetOfIntervals,
                                detail::NumericValueGetter, decltype(maxLambda),
                                decltype(detail::noop), "MAX">;

/// The GROUP_CONCAT Expression
class GroupConcatExpression : public SparqlExpression {
 public:
  GroupConcatExpression(bool distinct, Ptr&& child, std::string separator);

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override {
    return {&_actualExpression, 1};
  }

  vector<std::string> getUnaggregatedVariables() override {
    // This is an aggregation, so it never leaves any unaggregated variables.
    return {};
  }

  string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return "["s + _separator + "]" + _actualExpression->getCacheKey(varColMap);
  }

 private:
  Ptr _actualExpression;
  std::string _separator;
};

}  // namespace sparqlExpression

#endif  // QLEVER_AGGREGATEEXPRESSION_H

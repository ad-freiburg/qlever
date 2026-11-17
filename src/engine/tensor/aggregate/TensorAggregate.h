// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#ifndef QLEVER_SRC_TENSORAGGREGATE_H
#define QLEVER_SRC_TENSORAGGREGATE_H

#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "rdfTypes/TensorData.h"

namespace sparqlExpression {
namespace detail {
using namespace ad_utility;
using OptTensor = std::optional<TensorData>;
template <typename AggregateOperation,
          typename FinalOperation = sparqlExpression::Identity>
class TensorAggregateExpression
    : public AggregateExpression<AggregateOperation, FinalOperation> {
 public:
  using Ptr = std::unique_ptr<SparqlExpression>;

  TensorAggregateExpression(
      bool distinct, Ptr&& child,
      AggregateOperation aggregateOp = AggregateOperation{})
      : AggregateExpression<AggregateOperation, FinalOperation>(
            distinct, std::move(child), aggregateOp) {}
  ValueId resultForEmptyGroup() const override { return Id::makeUndefined(); }
};
// template <typename AggregateOperation, typename FinalOperation = Identity>
// using TensorAggregateExpression =
//     AggregateExpression<AggregateOperation, FinalOperation>;

template <typename Function, typename ValueGetter>
using T_AGG_EXP = TensorAggregateExpression<
    Operation<2, FunctionAndValueGetters<Function, ValueGetter>>>;

// The intermediate aggregation result is an `OptTensor` (the type returned by
// the `TensorValueGetter`), so the operation has to return one as well.
template <typename Op>
struct TensorExpressionForAggregate {
  template <typename... Args>
  OptTensor operator()(const Args&... args) const {
    return Op{}(args...);
  }
};
struct TensorAddOperation {
  OptTensor operator()(const OptTensor& t1, const OptTensor& t2) const {

    if (t1.has_value() && t2.has_value()) {
      return t1.value() + t2.value();
    }
    if (t1.has_value()) {
      return t1.value();
    }
    if (t2.has_value()) {
      return t2.value();
    }
    throw std::runtime_error{"Not tensor data for aggregation (add)!"};
  }
};
struct TensorScaleOperation {
  TensorData operator()(const OptTensor& t, const float& scale) const {
    if (t.has_value()) {
      return TensorData::scale(t.value(), scale);
    }
    throw std::runtime_error{"Not tensor data for aggregation (scaling)!"};
  }
};

// Aggregate expression for dta:sum.
using TensorAddForSum = TensorExpressionForAggregate<TensorAddOperation>;
using TensorSumExpressionBase = T_AGG_EXP<TensorAddForSum, TensorValueGetter>;
class TensorSumExpression
    : public T_AGG_EXP<TensorAddForSum, TensorValueGetter> {
  using TensorSumExpressionBase::TensorSumExpressionBase;

 public:
  ValueId resultForEmptyGroup() const override { return Id::makeUndefined(); }
};

// Aggregate expression for dta:avg.
struct TensorAvgFinalOperation {
  OptTensor operator()(const OptTensor& aggregation,
                       size_t numElements) const {
    if (!aggregation.has_value()) {
      return std::nullopt;
    }
    return TensorScaleOperation()(aggregation, 1.f / numElements);
  }
};
using TensorAvgOperation =
    Operation<2, FunctionAndValueGetters<TensorAddForSum, TensorValueGetter>>;
using TensorAvgExpressionBase =
    TensorAggregateExpression<TensorAvgOperation, TensorAvgFinalOperation>;
class TensorAvgExpression : public TensorAvgExpressionBase {
  using TensorAvgExpressionBase::TensorAvgExpressionBase;

 public:
  ValueId resultForEmptyGroup() const override { return Id::makeUndefined(); }
};
}  // namespace detail
using detail::TensorAddForSum;
using detail::TensorAvgExpression;
using detail::TensorSumExpression;

}  // namespace sparqlExpression
#endif  // QLEVER_TENSORAGGREGATE_H

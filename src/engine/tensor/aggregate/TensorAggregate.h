//
// Created by Benedikt Kantz on 16.11.26.
//

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
            distinct,
            std::make_unique<TensorAggregateExpression>(std::move(child)),
            aggregateOp) {}
  ValueId resultForEmptyGroup() const override { return Id::makeUndefined(); }
};
// template <typename AggregateOperation, typename FinalOperation = Identity>
// using TensorAggregateExpression =
//     AggregateExpression<AggregateOperation, FinalOperation>;

template <typename Function, typename ValueGetter>
using T_AGG_EXP = TensorAggregateExpression<
    Operation<2, FunctionAndValueGetters<Function, ValueGetter>>>;

template <typename Op>
struct TensorExpressionForAggregate {
  template <typename... Args>
  TensorData operator()(const Args&... args) const {
    auto visitor = []<typename... T0>(const T0&... t) -> TensorData {
      return (Op{}(t...));
    };
    return std::visit(visitor, args...);
  }
};
struct TensorAddOperation {
  TensorData operator()(const OptTensor& t1, const OptTensor& t2) const {
    if (t1.has_value() && t2.has_value()) {
      return t1.value() + t2.value();
    }
    throw std::runtime_error{"Not tensor data for aggregation!"};
  }
};
struct TensorScaleOperation {
  TensorData operator()(const OptTensor& t, const float& scale) const {
    if (t.has_value()) {
      return TensorData::scale(t.value(), scale);
    }
    throw std::runtime_error{"Not tensor data for aggregation!"};
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
  TensorData operator()(const OptTensor aggregation, size_t numElements) const {
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

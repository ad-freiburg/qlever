//
// Created by Benedikt Kantz on 16.11.26.
//

#ifndef QLEVER_TENSORVAR_H
#define QLEVER_TENSORVAR_H
#include "engine/sparqlExpressions/StdevExpression.h"
#include "engine/tensor/aggregate/TensorAggregate.h"

namespace sparqlExpression {

using namespace ad_utility;
using namespace detail;

// Mirrored from StdevExpression.h

class TensorDeviationExpression : public DeviationExpression {
 private:
  Ptr child_;

 public:
  explicit TensorDeviationExpression(Ptr&& child)
      : DeviationExpression(std::move(child)), child_(std::move(child)) {}
  ExpressionResult evaluate(EvaluationContext* context) const override;
};

template <typename AggregateOperation, typename FinalOperation = Identity>
class TensorDeviationAggExpression
    : public AggregateExpression<AggregateOperation, FinalOperation> {
public:
  // __________________________________________________________________________
  TensorDeviationAggExpression(
      bool distinct, SparqlExpression::Ptr&& child,
      AggregateOperation aggregateOp = AggregateOperation{})
      : AggregateExpression<AggregateOperation, FinalOperation>(
            distinct,
            std::make_unique<TensorDeviationExpression>(std::move(child)),
            aggregateOp) {}
};
// Aggregate expression for dta:var.

struct TensorDivAndRootOperation {
  TensorData operator()(const TensorData& t, float scale) const {
    return unaryTensorMap(
        t, [&scale](auto elem) { return std::sqrt(elem / scale); });
  }
};
struct TensorVarFinalOperation {
  TensorData operator()(const TensorData& aggregation,
                        size_t numElements) const {
    return TensorDivAndRootOperation()(aggregation,
                                       static_cast<double>(numElements));
  }
};
using TensorVarExpressionBase =
    TensorDeviationAggExpression<TensorAvgOperation, TensorVarFinalOperation>;
class TensorVarExpression : public TensorVarExpressionBase {
  using TensorVarExpressionBase::TensorVarExpressionBase;
  ValueId resultForEmptyGroup() const override { return Id::makeUndefined(); }
};

}  // namespace sparqlExpression
#endif  // QLEVER_TENSORVAR_H

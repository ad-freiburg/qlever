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
 public:
  // The child is owned by `DeviationExpression`, access it via `children()`.
  explicit TensorDeviationExpression(Ptr&& child)
      : DeviationExpression(std::move(child)) {}
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
            std::move(child),
            aggregateOp) {}
};
// Aggregate expression for dta:var.

// The (elementwise) sample variance: divide the sum of the squared deviations by
// the degrees of freedom `n - 1`. Like `DivAndRoot` for `STDEV`, the result is
// zero if there are no degrees of freedom.
struct TensorDivByDegreesOfFreedomOperation {
  TensorData operator()(const TensorData& t, float degreesOfFreedom) const {
    return unaryTensorMap(t, [&degreesOfFreedom](auto elem) {
      return degreesOfFreedom <= 0 ? 0.f : elem / degreesOfFreedom;
    });
  }
};
struct TensorVarFinalOperation {
  OptTensor operator()(const OptTensor& aggregation,
                       size_t numElements) const {
    if (!aggregation.has_value()) {
      return std::nullopt;
    }
    return TensorDivByDegreesOfFreedomOperation()(
        aggregation.value(), static_cast<float>(numElements) - 1);
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

// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_STDEVEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_STDEVEXPRESSION_H

#include <cmath>
#include <memory>
#include <variant>

#include "backports/functional.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/ValueId.h"

namespace sparqlExpression {

namespace detail {

/// The STDEV Expression

// Helper expression: The individual deviation squares. A DeviationExpression
// over X corresponds to the value (X - AVG(X))^2.
class DeviationExpression : public SparqlExpression {
 private:
  Ptr child_;

 public:
  explicit DeviationExpression(Ptr&& child) : child_{std::move(child)} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // __________________________________________________________________________
  AggregateStatus isAggregate() const override {
    return SparqlExpression::AggregateStatus::NoAggregate;
  }

  // __________________________________________________________________________
  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("[ SQ.DEVIATION ]", child_->getCacheKey(varColMap));
  }

 private:
  // _________________________________________________________________________
  ql::span<SparqlExpression::Ptr> childrenImpl() override {
    return {&child_, 1};
  }
};

// Separate subclass of AggregateOperation, that replaces its child with a
// DeviationExpression of this child. Everything else is left untouched.
template <typename AggregateOperation,
          typename FinalOperation = decltype(identity)>
class DeviationAggExpression
    : public AggregateExpression<AggregateOperation, FinalOperation> {
 public:
  // __________________________________________________________________________
  DeviationAggExpression(bool distinct, SparqlExpression::Ptr&& child,
                         AggregateOperation aggregateOp = AggregateOperation{})
      : AggregateExpression<AggregateOperation, FinalOperation>(
            distinct, std::make_unique<DeviationExpression>(std::move(child)),
            aggregateOp) {}
};

// The final operation for dividing by degrees of freedom and calculation square
// root after summing up the squared deviation
inline auto stdevFinalOperation = [](const NumericValue& aggregation,
                                     size_t numElements) {
  auto divAndRoot = [](double value, double degreesOfFreedom) {
    if (degreesOfFreedom <= 0) {
      return 0.0;
    } else {
      return std::sqrt(value / degreesOfFreedom);
    }
  };
  return makeNumericExpressionForAggregate<decltype(divAndRoot)>()(
      aggregation, NumericValue{static_cast<double>(numElements) - 1});
};

// The actual Standard Deviation Expression
// Mind the explicit instantiation of StdevExpressionBase in
// AggregateExpression.cpp
using StdevExpressionBase =
    DeviationAggExpression<AvgOperation, decltype(stdevFinalOperation)>;
class StdevExpression : public StdevExpressionBase {
  using StdevExpressionBase::StdevExpressionBase;
  ValueId resultForEmptyGroup() const override { return Id::makeFromDouble(0); }
};

}  // namespace detail

using detail::StdevExpression;

}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_STDEVEXPRESSION_H

// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#ifndef QLEVER_STDEVEXPRESSION_H
#define QLEVER_STDEVEXPRESSION_H

#include <cmath>
#include <functional>
#include <memory>
#include <variant>

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

// Helper function to extract a double from a NumericValue variant
auto inline numValToDouble =
    []<typename T>(T&& value) -> std::optional<double> {
  if constexpr (ad_utility::isSimilar<T, double> ||
                ad_utility::isSimilar<T, int64_t>) {
    return static_cast<double>(value);
  } else {
    return std::nullopt;
  }
};

// Helper expression: The individual deviation squares. A DeviationExpression
// over X corresponds to the value (X - AVG(X))^2.
class DeviationExpression : public SparqlExpression {
 private:
  Ptr child_;
  bool distinct_;

 public:
  DeviationExpression(bool distinct, Ptr&& child)
      : child_{std::move(child)}, distinct_{distinct} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    auto impl =
        [this, context](SingleExpressionResult auto&& el) -> ExpressionResult {
      // Prepare space for result
      VectorWithMemoryLimit<IdOrLiteralOrIri> exprResult{context->_allocator};
      std::fill_n(std::back_inserter(exprResult), context->size(),
                  IdOrLiteralOrIri{Id::makeUndefined()});

      auto devImpl = [&exprResult, context](auto generator) {
        double sum = 0.0;
        std::vector<double> childResults = {};

        // Collect values as doubles
        for (auto& inp : generator) {
          const auto& s = detail::NumericValueGetter{}(std::move(inp), context);
          auto v = std::visit(numValToDouble, s);
          if (v.has_value()) {
            childResults.push_back(v.value());
            sum += v.value();
          }
          context->cancellationHandle_->throwIfCancelled();
        }

        // Calculate squared deviation and save for result
        double avg = sum / static_cast<double>(context->size());
        for (size_t i = 0; i < childResults.size(); i++) {
          exprResult.at(i) = IdOrLiteralOrIri{
              ValueId::makeFromDouble(std::pow(childResults.at(i) - avg, 2))};
        }
      };

      auto generator =
          detail::makeGenerator(AD_FWD(el), context->size(), context);
      if (distinct_) {
        context->cancellationHandle_->throwIfCancelled();
        devImpl(detail::getUniqueElements(context, context->size(),
                                          std::move(generator)));
      } else {
        devImpl(std::move(generator));
      }

      return exprResult;
    };
    auto childRes = child_->evaluate(context);
    return std::visit(impl, std::move(childRes));
  };

  // __________________________________________________________________________
  AggregateStatus isAggregate() const override {
    return SparqlExpression::AggregateStatus::NoAggregate;
  }

  // __________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("[ SQ.DEVIATION ", distinct_ ? " DISTINCT " : "", "]",
                        child_->getCacheKey(varColMap));
  }

 private:
  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> childrenImpl() override {
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
            distinct,
            std::make_unique<DeviationExpression>(distinct, std::move(child)),
            aggregateOp){};
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
  ValueId resultForEmptyGroup() const override { return Id::makeFromInt(0); }
};

}  // namespace detail

using detail::StdevExpression;

}  // namespace sparqlExpression

#endif  // QLEVER_STDEVEXPRESSION_H

// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/StdevExpression.h"

namespace sparqlExpression {

namespace detail {

// _____________________________________________________________________________
ExpressionResult DeviationExpression::evaluate(
    EvaluationContext* context) const {
  auto impl = [context](SingleExpressionResult auto&& el) -> ExpressionResult {
    // Prepare space for result
    VectorWithMemoryLimit<IdOrLiteralOrIri> exprResult{context->_allocator};
    std::fill_n(std::back_inserter(exprResult), context->size(),
                IdOrLiteralOrIri{Id::makeUndefined()});
    bool undef = false;

    auto devImpl = [&undef, &exprResult, context](auto generator) {
      double sum = 0.0;
      // Intermediate storage of the results returned from the child
      // expression
      VectorWithMemoryLimit<double> childResults{context->_allocator};

      // Collect values as doubles
      for (auto& inp : generator) {
        const auto& n = detail::NumericValueGetter{}(std::move(inp), context);
        auto v = std::visit(
            []<typename T>(T&& value) -> std::optional<double> {
              if constexpr (ad_utility::isSimilar<T, double> ||
                            ad_utility::isSimilar<T, int64_t>) {
                return static_cast<double>(value);
              } else {
                return std::nullopt;
              }
            },
            n);
        if (v.has_value()) {
          childResults.push_back(v.value());
          sum += v.value();
        } else {
          // There is a non-numeric value in the input. Therefore the entire
          // result will be undef.
          undef = true;
          return;
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
    devImpl(std::move(generator));

    if (undef) {
      return IdOrLiteralOrIri{Id::makeUndefined()};
    }
    return exprResult;
  };
  auto childRes = child_->evaluate(context);
  return std::visit(impl, std::move(childRes));
};

}  // namespace detail
}  // namespace sparqlExpression

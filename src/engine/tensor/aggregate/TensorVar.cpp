//
// Created by Benedikt Kantz on 16.11.26.
//

#include "TensorVar.h"

#include "engine/tensor/aggregate/TensorAggregate.h"
namespace sparqlExpression {

ExpressionResult TensorDeviationExpression::evaluate(
    EvaluationContext* context) const {
  // Helper to replace child expression results with their squared deviation
  auto devImpl = [context](bool& undef,
                           VectorWithMemoryLimit<IdOrLiteralOrIri>& exprResult,
                           auto generator) {
    OptTensor sum = std::nullopt;
    // Intermediate storage of the results returned from the child
    // expression
    VectorWithMemoryLimit<OptTensor> childResults{context->_allocator};

    for (auto& inp : generator) {
      const OptTensor& tensor =
          detail::TensorValueGetter{}(std::move(inp), context);

      if (tensor.has_value()) {
        childResults.push_back(tensor.value());
        sum.has_value() ? sum = sum + tensor.value() : sum = tensor.value();
      } else {
        // There is a non-tensor value in the input. Therefore the entire
        // result will be undef.
        undef = true;
        return;
      }
      context->cancellationHandle_->throwIfCancelled();
    }
    if (!sum.has_value()) {
      return;
    }
    // Calculate squared deviation and save for result
    OptTensor avg = TensorData::scale(
        sum.value(), 1.f / static_cast<float>(childResults.size()));
    for (size_t i = 0; i < childResults.size(); i++) {
      auto diff = childResults[i].value() - avg.value();
      auto pow = diff * diff;
      exprResult.at(i) = IdOrLiteralOrIri{pow.toLiteral()};
    }
  };

  // Visitor for child expression result
  auto impl = [context, devImpl](auto&& el)
      -> CPP_ret(ExpressionResult)(
          requires SingleExpressionResult<decltype(el)>) {
    // Prepare space for result
    VectorWithMemoryLimit<IdOrLiteralOrIri> exprResult{context->_allocator};
    exprResult.resize(context->size());
    bool undef = false;

    auto generator =
        detail::makeGenerator(AD_FWD(el), context->size(), context);
    devImpl(undef, exprResult, std::move(generator));

    if (undef) {
      return IdOrLiteralOrIri{Id::makeUndefined()};
    }
    return exprResult;
  };

  auto childRes = children()[0]->evaluate(context);
  return std::visit(impl, std::move(childRes));
};

}  // namespace sparqlExpression

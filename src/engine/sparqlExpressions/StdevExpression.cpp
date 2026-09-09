// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/sparqlExpressions/StdevExpression.h"

#include "engine/sparqlExpressions/SparqlExpressionTypes.h"

namespace sparqlExpression::detail {

// _____________________________________________________________________________
ExpressionResult DeviationExpression::evaluate(
    EvaluationContext* context) const {
  // Helper: Extracts a double or int (as double) from a variant
  auto numValVisitor = [](double value) -> std::optional<double> {
    return value;
  };

  // Helper to replace child expression results with their squared deviation
  auto devImpl = [context, numValVisitor](
                     bool& undef,
                     VectorWithMemoryLimit<IdOrLocalVocabEntry>& exprResult,
                     auto generator) {
    double sum = 0.0;
    // Intermediate storage of the results returned from the child
    // expression
    VectorWithMemoryLimit<double> childResults{context->_allocator};

    // Collect values as doubles
    for (auto& inp : generator) {
      const auto& n = detail::NumericValueGetter{}(std::move(inp), context);
      auto v = ad_utility::visitIf(
          n, numValVisitor,
          [](const auto&) -> std::optional<double> { return std::nullopt; });
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
      exprResult.at(i) = IdOrLocalVocabEntry{
          ValueId::makeFromDouble(std::pow(childResults.at(i) - avg, 2))};
    }
  };

  // Visitor for child expression result
  auto impl = [context, devImpl](auto&& el)
      -> CPP_ret(ExpressionResult)(
          requires SingleExpressionResult<decltype(el)>) {
    // Prepare space for result
    VectorWithMemoryLimit<IdOrLocalVocabEntry> exprResult{context->_allocator};
    exprResult.resize(context->size());
    bool undef = false;

    auto generator =
        detail::makeGenerator(AD_FWD(el), context->size(), context);
    devImpl(undef, exprResult, std::move(generator));

    if (undef) {
      return IdOrLocalVocabEntry{Id::makeUndefined()};
    }
    return exprResult;
  };

  auto childRes = child_->evaluate(context);
  return std::visit(impl, std::move(childRes));
}

}  // namespace sparqlExpression::detail

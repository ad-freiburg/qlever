//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/GroupConcatExpression.h"

#include <absl/strings/str_cat.h>

#include "engine/sparqlExpressions/GroupConcatHelper.h"

// __________________________________________________________________________
sparqlExpression::GroupConcatExpression::GroupConcatExpression(
    bool distinct, Ptr&& child, std::string separator)
    : child_{std::move(child)},
      separator_{std::move(separator)},
      distinct_{distinct} {
  setIsInsideAggregate();
}

// __________________________________________________________________________
sparqlExpression::ExpressionResult
sparqlExpression::GroupConcatExpression::evaluate(
    EvaluationContext* context) const {
  auto impl = [this, context](auto&& el)
      -> CPP_ret(ExpressionResult)(
          requires SingleExpressionResult<decltype(el)>) {
    bool undefined = false;
    std::string result;
    std::optional<std::string> langTag;
    auto groupConcatImpl = [this, &result, context, &undefined,
                            &langTag](auto generator) {
      // TODO<joka921> Make this a configurable constant.
      result.reserve(20000);
      bool firstIteration = true;
      for (auto& inp : generator) {
        auto literal = detail::LiteralValueGetterWithoutStrFunction{}(
            std::move(inp), context);
        if (firstIteration) {
          firstIteration = false;
          detail::pushLanguageTag(langTag, literal);
        } else {
          result.append(separator_);
        }
        if (literal.has_value()) {
          result.append(asStringViewUnsafe(literal.value().getContent()));
          detail::mergeLanguageTags(langTag, literal.value());
        } else {
          undefined = true;
          return;
        }
        context->cancellationHandle_->throwIfCancelled();
      }
    };
    auto generator =
        detail::makeGenerator(AD_FWD(el), context->size(), context);
    if (distinct_) {
      context->cancellationHandle_->throwIfCancelled();
      groupConcatImpl(detail::getUniqueElements(context, context->size(),
                                                std::move(generator)));
    } else {
      groupConcatImpl(std::move(generator));
    }
    if (undefined) {
      return Id::makeUndefined();
    }
    result.shrink_to_fit();
    return IdOrLiteralOrIri{
        detail::stringWithOptionalLangTagToLiteral(result, std::move(langTag))};
  };

  auto childRes = child_->evaluate(context);
  return std::visit(impl, std::move(childRes));
}

// __________________________________________________________________________
std::string sparqlExpression::GroupConcatExpression::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  return absl::StrCat("[ GROUP_CONCAT", distinct_ ? " DISTINCT " : "",
                      separator_, "]", child_->getCacheKey(varColMap));
}

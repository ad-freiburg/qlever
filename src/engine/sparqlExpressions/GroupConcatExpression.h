// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_GROUPCONCATEXPRESSION_H
#define QLEVER_GROUPCONCATEXPRESSION_H

#include <absl/strings/str_cat.h>

#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/GroupConcatHelper.h"

namespace sparqlExpression {
/// The GROUP_CONCAT Expression
class GroupConcatExpression : public SparqlExpression {
 private:
  Ptr child_;
  std::string separator_;
  bool distinct_;

 public:
  GroupConcatExpression(bool distinct, Ptr&& child, std::string separator)
      : child_{std::move(child)},
        separator_{std::move(separator)},
        distinct_{distinct} {
    setIsInsideAggregate();
  }

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
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
      return IdOrLiteralOrIri{detail::stringWithOptionalLangTagToLiteral(
          result, std::move(langTag))};
    };

    auto childRes = child_->evaluate(context);
    return std::visit(impl, std::move(childRes));
  }

  // Required when using the hash map optimization.
  [[nodiscard]] const std::string& getSeparator() const { return separator_; }

  // A `GroupConcatExpression` is an aggregate.
  AggregateStatus isAggregate() const override {
    return distinct_ ? AggregateStatus::DistinctAggregate
                     : AggregateStatus::NonDistinctAggregate;
  }

  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("[ GROUP_CONCAT", distinct_ ? " DISTINCT " : "",
                        separator_, "]", child_->getCacheKey(varColMap));
  }

 private:
  // ___________________________________________________________________________
  ql::span<Ptr> childrenImpl() override { return {&child_, 1}; }
};
}  // namespace sparqlExpression

#endif  // QLEVER_GROUPCONCATEXPRESSION_H

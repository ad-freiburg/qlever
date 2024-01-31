// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_GROUPCONCATEXPRESSION_H
#define QLEVER_GROUPCONCATEXPRESSION_H

#include "absl/strings/str_cat.h"
#include "engine/sparqlExpressions/AggregateExpression.h"

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
    auto impl =
        [this, context](SingleExpressionResult auto&& el) -> ExpressionResult {
      std::string result;
      auto groupConcatImpl = [this, &result, context](auto generator) {
        // TODO<joka921> Make this a configurable constant.
        result.reserve(20000);
        for (auto& inp : generator) {
          const auto& s = detail::StringValueGetter{}(std::move(inp), context);
          if (s.has_value()) {
            if (!result.empty()) {
              result.append(separator_);
            }
            result.append(s.value());
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
      result.shrink_to_fit();
      return IdOrString(std::move(result));
    };

    auto childRes = child_->evaluate(context);
    return std::visit(impl, std::move(childRes));
  }

  // Required when using the hash map optimization.
  [[nodiscard]] const std::string& getSeparator() const { return separator_; }

  // A `GroupConcatExpression` is an aggregate, so it never leaves any
  // unaggregated variables.
  vector<Variable> getUnaggregatedVariables() override { return {}; }

  // A `GroupConcatExpression` is an aggregate.
  bool isAggregate() const override { return true; }

  bool isDistinct() const override { return distinct_; }

  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("[ GROUP_CONCAT", distinct_ ? " DISTINCT " : "",
                        separator_, "]", child_->getCacheKey(varColMap));
  }

 private:
  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> childrenImpl() override {
    return {&child_, 1};
  }
};
}  // namespace sparqlExpression

#endif  // QLEVER_GROUPCONCATEXPRESSION_H

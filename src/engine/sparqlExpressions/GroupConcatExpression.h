// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_GROUPCONCATEXPRESSION_H
#define QLEVER_GROUPCONCATEXPRESSION_H

#include "AggregateExpression.h"
#include "absl/strings/str_cat.h"

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
    if (distinct) {
      throw std::runtime_error{
          "DISTINCT in combination with GROUP_CONCAT is currently not "
          "supported by QLever"};
    }
    setIsInsideAggregate();
  }

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    auto impl = [context,
                 this](SingleExpressionResult auto&& el) -> ExpressionResult {
      auto generator =
          detail::makeGenerator(AD_FWD(el), context->size(), context);
      std::string result;
      // TODO<joka921> Make this a configurable constant.
      result.reserve(20000);
      for (auto& inp : generator) {
        auto&& s = detail::StringValueGetter{}(std::move(inp), context);
        if (s.has_value()) {
          if (!result.empty()) {
            result.append(separator_);
          }
          result.append(s.value());
        }
      }
      result.shrink_to_fit();
      return IdOrString(std::move(result));
    };

    auto childRes = child_->evaluate(context);
    return std::visit(impl, std::move(childRes));
  }

  // A `GroupConcatExpression` is an aggregate, so it never leaves any
  // unaggregated variables.
  vector<Variable> getUnaggregatedVariables() override { return {}; }

  // A `GroupConcatExpression` is an aggregate.
  bool containsAggregate() const override { return true; }

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

// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_GROUPCONCATEXPRESSION_H
#define QLEVER_GROUPCONCATEXPRESSION_H

#include "AggregateExpression.h"
#include "absl/strings/str_cat.h"

namespace sparqlExpression {
/// The GROUP_CONCAT Expression

namespace detail {

struct PerformConcat {
  std::string separator_;
  std::string operator()(string&& a,
                         const std::optional<std::string>& b) const {
    if (a.empty()) [[unlikely]] {
      return b.value_or("");
    } else [[likely]] {
      if (b.has_value()) {
        a.append(separator_);
        a.append(b.value());
      }
      return std::move(a);
    }
  }
};

}  // namespace detail
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
      auto getString = [context](const SingleExpressionResult auto& s) {
        return detail::StringValueGetter{}(s, context);
      };

      // TODO<joka921> The strings should conform to the memory limit.
      auto filtered =
          generator | std::views::transform(getString) |
          std::views::filter(&std::optional<std::string>::has_value);
      auto toString = std::views::transform(
          filtered, [](auto&& opt) { return AD_FWD(opt).value(); });
      return IdOrString(ad_utility::lazyStrJoin(toString, separator_));
    };

    auto childRes = child_->evaluate(context);
    return std::visit(impl, std::move(childRes));
  }

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override { return {&child_, 1}; }

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
};
}  // namespace sparqlExpression

#endif  // QLEVER_GROUPCONCATEXPRESSION_H

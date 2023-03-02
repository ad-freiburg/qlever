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
  std::string operator()(string&& a, const string& b) const {
    if (a.empty()) [[unlikely]] {
      return b;
    } else [[likely]] {
      a.append(separator_);
      a.append(b);
      return std::move(a);
    }
  }
};

}  // namespace detail
class GroupConcatExpression : public SparqlExpression {
 public:
  GroupConcatExpression(bool distinct, Ptr&& child, std::string separator)
      : _separator{std::move(separator)} {
    using OP = detail::AGG_OP<detail::PerformConcat, detail::StringValueGetter>;
    auto groupConcatOp = OP{detail::PerformConcat{_separator}};
    using AGG_EXP = detail::AggregateExpression<OP>;
    _actualExpression = std::make_unique<AGG_EXP>(distinct, std::move(child),
                                                  std::move(groupConcatOp));
  }

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    // The child is already set up to perform all the work.
    return _actualExpression->evaluate(context);
  }

  // _________________________________________________________________________
  std::span<SparqlExpression::Ptr> children() override {
    return {&_actualExpression, 1};
  }

  // A `GroupConcatExpression` is an aggregate, so it never leaves any
  // unaggregated variables.
  vector<std::string> getUnaggregatedVariables() override { return {}; }

  // A `GroupConcatExpression` is an aggregate.
  bool containsAggregate() const override { return true; }

  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("[", _separator, "]",
                        _actualExpression->getCacheKey(varColMap));
  }

 private:
  Ptr _actualExpression;
  std::string _separator;
};
}  // namespace sparqlExpression

#endif  // QLEVER_GROUPCONCATEXPRESSION_H

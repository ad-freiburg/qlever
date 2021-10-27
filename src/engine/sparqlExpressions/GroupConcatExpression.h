// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_GROUPCONCATEXPRESSION_H
#define QLEVER_GROUPCONCATEXPRESSION_H

#include "AggregateExpression.h"

namespace sparqlExpression {
/// The GROUP_CONCAT Expression
class GroupConcatExpression : public SparqlExpression {
 public:
  GroupConcatExpression(bool distinct, Ptr&& child, std::string separator)
      : _separator{std::move(separator)} {
    auto performConcat = [separator = _separator](string&& a,
                                                  const string& b) -> string {
      if (a.empty()) [[unlikely]] {
        return b;
      } else [[likely]] {
        a.append(separator);
        a.append(b);
        return std::move(a);
      }
    };

    using OP = AGOP<decltype(performConcat), StringValueGetter>;
    auto groupConcatOp = OP{performConcat};
    using AGG_EXP = AggregateExpression<OP>;
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

  vector<std::string> getUnaggregatedVariables() override {
    // This is an aggregation, so it never leaves any unaggregated variables.
    return {};
  }

  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    return "["s + _separator + "]" + _actualExpression->getCacheKey(varColMap);
  }

 private:
  Ptr _actualExpression;
  std::string _separator;
};
}  // namespace sparqlExpression

#endif  // QLEVER_GROUPCONCATEXPRESSION_H

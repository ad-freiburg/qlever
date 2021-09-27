//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// Relational Expressions like = < >= etc.

#ifndef QLEVER_RELATIONALEXPRESSION_H
#define QLEVER_RELATIONALEXPRESSION_H

#include "./SparqlExpression.h"
namespace sparqlExpression {

// Boolean equality
class EqualsExpression : public SparqlExpression {
 public:
  EqualsExpression(Ptr&& childLeft, Ptr&& childRight)
      : _children{std::move(childLeft), std::move(childRight)} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _______________________________________________________________________
  vector<std::string> getUnaggregatedVariables() override {
    auto result = _childLeft->getUnaggregatedVariables();
    auto resultRight = _childRight->getUnaggregatedVariables();
    result.insert(result.end(), std::make_move_iterator(resultRight.begin()),
                  std::make_move_iterator(resultRight.end()));
    return result;
  }

  string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return "(" + _childLeft->getCacheKey(varColMap) + ") = (" +
           _childRight->getCacheKey(varColMap) + ")";
  }

  // Get the direct child expressions.
  std::span<SparqlExpression::Ptr> children() override {
    return {_children.data(), _children.size()};
  }

 private:
  std::array<Ptr, 2> _children;
  Ptr& _childLeft = _children[0];
  Ptr& _childRight = _children[1];
};

}  // namespace sparqlExpression

#endif  // QLEVER_RELATIONALEXPRESSION_H

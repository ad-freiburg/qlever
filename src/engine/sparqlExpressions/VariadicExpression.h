//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_VARIADICEXPRESSION_H
#define QLEVER_VARIADICEXPRESSION_H

#include <vector>

#include "engine/sparqlExpressions/SparqlExpression.h"
namespace sparqlExpression::detail {
// A base class for variadic expressions, i.e. expressions for which the number
// of child expressions is only known at runtime. This class implements every
// function except for `evaluate`. In particular, it manages the child
// expressions.
class VariadicExpression : public SparqlExpression {
 private:
  std::vector<SparqlExpression::Ptr> children_;

 protected:
  // We cannot call it `children` because it would shadown an inherited member
  // function.
  auto& childrenVec() { return children_; }
  const auto& childrenVec() const { return children_; }

 public:
  explicit VariadicExpression(std::vector<SparqlExpression::Ptr> children)
      : children_{std::move(children)} {}

  // _________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* ctx) const override = 0;

  // ___________________________________________________
  std::string getCacheKey(const VariableToColumnMap& varColMap) const override {
    string key = typeid(*this).name();
    auto childKeys = ad_utility::lazyStrJoin(
        children_ | ql::views::transform([&varColMap](const auto& childPtr) {
          return childPtr->getCacheKey(varColMap);
        }),
        ", ");
    return absl::StrCat(key, "(", childKeys, ")");
  }

 private:
  // ___________________________________________________
  std::span<SparqlExpression::Ptr> childrenImpl() override {
    return {children_.data(), children_.size()};
  }
};

}  // namespace sparqlExpression::detail

#endif  // QLEVER_VARIADICEXPRESSION_H

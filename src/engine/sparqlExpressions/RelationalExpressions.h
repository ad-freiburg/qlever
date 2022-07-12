//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_RELATIONALEXPRESSIONS_H
#define QLEVER_RELATIONALEXPRESSIONS_H

#include "./SparqlExpression.h"


#endif  // QLEVER_RELATIONALEXPRESSIONS_H

namespace sparqlExpression::relational {

inline auto operation = [](const auto& a, const auto& b) { return a == b; };

template <typename T>
concept Arithmetic = std::integral<T> || std::floating_point<T>;

Bool evaluateR(Arithmetic auto a, Arithmetic auto b, auto operation, auto) {
  return operation(a, b);
}

template <Arithmetic T>
VectorWithMemoryLimit<Bool> evaluateR(const VectorWithMemoryLimit<T>& a,
                                     Arithmetic auto b, auto operation, auto allocator) {
  VectorWithMemoryLimit<Bool> result(allocator);
  result.reserve(a.size());
  for (const auto& x : a) {
    result.push_back(operation(x, b));
  }
  return result;
}

Bool evaluateR(const auto& a, const auto& b, auto, auto) {
  throw std::runtime_error("Not implemented, TODO");
}



template <typename Operation>
class RelationalExpression : public SparqlExpression {
 public:
  using Children = std::array<SparqlExpression::Ptr, 2>;
 private:
  Children _children;
  Operation _operation;
 public:
  RelationalExpression(Children children) : _children{std::move(children)}{}

  ExpressionResult evaluate(EvaluationContext* context) const override {
    auto resA = _children[0]->evaluate(context);
    auto resB = _children[1]->evaluate(context);

    auto visitor = [this, context](const auto& a, const auto& b) -> ExpressionResult {
      return evaluateR(a, b, _operation, context->_allocator);
    };

    return std::visit(visitor, resA, resB);
  }

  std::span<SparqlExpression::Ptr> children() override {
    return {_children.data(), _children.size()};
  }

  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    string key = typeid(*this).name();
    for (const auto& child : _children) {
      key += child->getCacheKey(varColMap);
    }
    return key;
  }
};

}

namespace sparqlExpression {
 using LessThanExpression = relational::RelationalExpression<std::less<>>;
}


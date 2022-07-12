//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "RelationalExpressions.h"

using namespace sparqlExpression;

namespace {

using valueIdComparators::Comparison;

template <valueIdComparators::Comparison Comp, typename Dummy = int>
bool applyComparison(const auto& a, const auto& b) {
  if constexpr (Comp == valueIdComparators::Comparison::LT) {
    return a < b;
  } else if constexpr (Comp == valueIdComparators::Comparison::LE) {
    return a <= b;
  } else if constexpr (Comp == valueIdComparators::Comparison::EQ) {
    return a == b;
  } else if constexpr (Comp == valueIdComparators::Comparison::NE) {
    return a != b;
  } else if constexpr (Comp == valueIdComparators::Comparison::GE) {
    return a >= b;
  } else if constexpr (Comp == valueIdComparators::Comparison::GT) {
    return a > b;
  } else {
    static_assert(ad_utility::alwaysFalse<Dummy>);
  }
}

template <typename T>
concept Arithmetic = std::integral<T> || std::floating_point<T>;

template <typename T>
concept Boolean =
    std::is_same_v<T, Bool> || std::is_same_v<T, ad_utility::SetOfIntervals> ||
    std::is_same_v<VectorWithMemoryLimit<Bool>, T>;

template <Comparison Comp>
Bool evaluateR(const Arithmetic auto& a, const Arithmetic auto& b, auto) {
  return applyComparison<Comp>(a, b);
}

template <Comparison Comp, Arithmetic T>
VectorWithMemoryLimit<Bool> evaluateR(const VectorWithMemoryLimit<T>& a,
                                      Arithmetic auto b,
                                      EvaluationContext* context) {
  VectorWithMemoryLimit<Bool> result(context->_allocator);
  result.reserve(a.size());
  for (const auto& x : a) {
    result.push_back(applyComparison<Comp>(x, b));
  }
  return result;
}

template <Comparison, typename A, typename B>
    Bool evaluateR(const A&, const B&,
                   EvaluationContext*) requires Boolean<A> ||
    Boolean<B> {
  throw std::runtime_error(
      "Relational expressions like <, >, == are currently not supported");
}

template <Comparison Comp>
VectorWithMemoryLimit<Bool> evaluateR(const Variable& a, const Variable& b,
                                      EvaluationContext* context) {
  auto idxA = getIndexForVariable(a, context);
  auto idxB = getIndexForVariable(b, context);

  VectorWithMemoryLimit<Bool> result{context->_allocator};
  result.reserve(context->_endIndex - context->_beginIndex);

  // TODO<joka921> Use the CALL_FIXED_SIZE optimization here
  for (auto i = context->_beginIndex; i < context->_endIndex; ++i) {
    auto idA = context->_inputTable(i, idxA);
    auto idB = context->_inputTable(i, idxB);
    result.push_back(valueIdComparators::compareIds(idA, idB, Comp));
  }
  return result;
}

template <Comparison>
Bool evaluateR(const auto&, const auto&, EvaluationContext*) {
  throw std::runtime_error("Not implemented, TODO");
}
}  // namespace

namespace sparqlExpression::relational {
template <Comparison Comp>
ExpressionResult RelationalExpression<Comp>::evaluate(
    EvaluationContext* context) const {
  auto resA = _children[0]->evaluate(context);
  auto resB = _children[1]->evaluate(context);

  auto visitor = [this, context](const auto& a,
                                 const auto& b) -> ExpressionResult {
    return evaluateR<Comp>(a, b, context);
  };

  return std::visit(visitor, resA, resB);
}

template <Comparison Comp>
string RelationalExpression<Comp>::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  string key = typeid(*this).name();
  for (const auto& child : _children) {
    key += child->getCacheKey(varColMap);
  }
  return key;
}

template <Comparison Comp>
std::span<SparqlExpression::Ptr> RelationalExpression<Comp>::children() {
  return {_children.data(), _children.size()};
}

// Explicit instantiations
template class RelationalExpression<Comparison::LT>;
template class RelationalExpression<Comparison::LE>;
template class RelationalExpression<Comparison::EQ>;
template class RelationalExpression<Comparison::NE>;
template class RelationalExpression<Comparison::GT>;
template class RelationalExpression<Comparison::GE>;
}  // namespace sparqlExpression::relational

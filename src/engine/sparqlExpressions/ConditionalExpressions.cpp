// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "NaryExpression.h"
#include "NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail::conditional_expressions {
using namespace sparqlExpression::detail;
[[maybe_unused]] auto ifImpl = []<typename T, typename U>(
    EffectiveBooleanValueGetter::Result condition, T&& i, U&& e) -> IdOrString
    requires std::is_rvalue_reference_v<T&&> && std::is_rvalue_reference_v<U&&>
{
  if (condition == EffectiveBooleanValueGetter::Result::True) {
    return AD_FWD(i);
  } else {
    return AD_FWD(e);
  }
};
NARY_EXPRESSION(IfExpression, 3,
                FV<decltype(ifImpl), EffectiveBooleanValueGetter,
                   ActualValueGetter, ActualValueGetter>);

// The implementation of the COALESCE expression. It (at least currently) has to
// be done manually as we have no Generic implementation for variadic
// expressions, as it is the first one.
class CoalesceExpression : public SparqlExpression {
  std::vector<SparqlExpression::Ptr> children_;

 public:
  // Construct from the child expressions.
  explicit CoalesceExpression(std::vector<SparqlExpression::Ptr> children)
      : children_{std::move(children)} {};

  // _____________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    // TODO<joka921> There are several optimizations that we can implement in
    // the future. We deliberately don't implement them just yet, as we probably
    // want to do this in a generic way for all the expressions at once.
    // 1. Only evaluate the children in chunks (can even be done in parallel).
    // 2. Early stopping if all results are already bound (gets more relevant
    // once 1. is implemented, as we can also do
    //    this on a per chunk basis.

    // First evaluate all the children's result.
    std::vector<ExpressionResult> childResults;
    std::ranges::for_each(children_, [&](const auto& child) {
      childResults.push_back(child->evaluate(context));
    });

    // Set up one vector with the indices of the elements that are still unbound
    // so far and one for the indices that remain unbound after applying one of
    // the children.
    std::vector<uint64_t> unboundResults;
    std::vector<uint64_t> nextUnboundResults;
    unboundResults.reserve(context->size());
    nextUnboundResults.reserve(context->size());

    // Initially all results are unbound
    for (size_t i = 0; i < context->size(); ++i) {
      unboundResults.push_back(i);
    }
    VectorWithMemoryLimit<IdOrString> results{context->_allocator};
    std::fill_n(std::back_inserter(results), context->size(),
                IdOrString{Id::makeUndefined()});

    // For a single child result, write the result at the indices where the
    // result so far is unbound, and the child result is bound. While doing so,
    // set up the `nextUnboundResults` vector  for the next step.
    auto visitExpressionResult = [&]<SingleExpressionResult T>(T && res)
        requires std::is_rvalue_reference_v<T&&> {
      nextUnboundResults.clear();
      // TODO<joka921> We can make this more efficient for constant results
      // (which are either always bound or always unbound).
      auto gen = detail::makeGenerator(AD_FWD(res), context->size(), context);
      // Index of the current row.
      size_t i = 0;
      // Iterator to the next index where the result so far is unbound.
      auto nextUnbound = unboundResults.begin();
      if (nextUnbound == unboundResults.end()) {
        return;
      }

      for (auto& el : gen) {
        // Skip all the indices where the result is already bound from a
        // previous child.
        if (i == *nextUnbound) {
          IdOrString val{std::move(el)};
          if (std::holds_alternative<Id>(val) &&
              std::get<Id>(val) == Id::makeUndefined()) {
            nextUnboundResults.push_back(i);
          } else {
            results.at(*nextUnbound) = std::move(val);
          }
          ++nextUnbound;
          if (nextUnbound == unboundResults.end()) {
            return;
          }
        }
        ++i;
      }
    };

    std::ranges::for_each(std::move(childResults),
                          [&](ExpressionResult& child) {
                            std::visit(visitExpressionResult, std::move(child));
                            unboundResults = std::move(nextUnboundResults);
                          });
    return results;
  }

  // ___________________________________________________
  std::string getCacheKey(const VariableToColumnMap& varColMap) const override {
    auto childKeys = ad_utility::lazyStrJoin(
        children_ | std::views::transform([&varColMap](const auto& childPtr) {
          return childPtr->getCacheKey(varColMap);
        }),
        ", ");
    return absl::StrCat("COALESCE(", childKeys, ")");
  }

 private:
  // ___________________________________________________
  std::span<SparqlExpression::Ptr> childrenImpl() override {
    return {children_.data(), children_.size()};
  }
};

}  // namespace detail::conditional_expressions
using namespace detail::conditional_expressions;
SparqlExpression::Ptr makeIfExpression(SparqlExpression::Ptr child1,
                                       SparqlExpression::Ptr child2,
                                       SparqlExpression::Ptr child3) {
  return std::make_unique<IfExpression>(std::move(child1), std::move(child2),
                                        std::move(child3));
}

SparqlExpression::Ptr makeCoalesceExpression(
    std::vector<SparqlExpression::Ptr> children) {
  return std::make_unique<CoalesceExpression>(std::move(children));
}

}  // namespace sparqlExpression

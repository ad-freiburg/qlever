// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "NaryExpression.h"
#include "NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail::conditional_expressions {
using namespace sparqlExpression::detail;
[[maybe_unused]] auto ifImpl =
    []<SingleExpressionResult T, SingleExpressionResult U>(
        EffectiveBooleanValueGetter::Result condition, T&& i,
        U&& e) -> IdOrString requires std::is_rvalue_reference_v<T&&> &&
                                      std::is_rvalue_reference_v<U&&> {
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
  ExpressionResult evaluate(EvaluationContext* ctx) const override {
    // Set up one vector with the indices of the elements that are still unbound
    // so far and one for the indices that remain unbound after applying one of
    // the children.
    std::vector<uint64_t> unboundIndices;
    std::vector<uint64_t> nextUnboundIndices;
    unboundIndices.reserve(ctx->size());
    nextUnboundIndices.reserve(ctx->size());

    // Initially all result are unbound.
    for (size_t i = 0; i < ctx->size(); ++i) {
      // TODO<C++23> use `std::ranges::to<vector>(std::views::iota...)`.
      unboundIndices.push_back(i);
    }
    VectorWithMemoryLimit<IdOrString> result{ctx->_allocator};
    std::fill_n(std::back_inserter(result), ctx->size(),
                IdOrString{Id::makeUndefined()});

    auto isUnbound = [](const IdOrString& x) {
      return (std::holds_alternative<Id>(x) &&
              std::get<Id>(x) == Id::makeUndefined());
    };

    auto visitConstantExpressionResult = [
      &nextUnboundIndices, &unboundIndices, &isUnbound, &result
    ]<SingleExpressionResult T>(T && childResult) requires isConstantResult<T> {
      IdOrString constantResult{AD_FWD(childResult)};
      if (isUnbound(constantResult)) {
        nextUnboundIndices = std::move(unboundIndices);
        return;
      }
      for (const auto& idx : unboundIndices) {
        result[idx] = constantResult;
      }
    };

    // For a single child result, write the result at the indices where the
    // result so far is unbound, and the child result is bound. While doing so,
    // set up the `nextUnboundIndices` vector  for the next step.
    auto visitVectorExpressionResult =
        [&result, &unboundIndices, &nextUnboundIndices, &ctx, &
         isUnbound ]<SingleExpressionResult T>(T && childResult)
            requires std::is_rvalue_reference_v<T&&> {
      static_assert(!isConstantResult<T>);
      auto gen = detail::makeGenerator(AD_FWD(childResult), ctx->size(), ctx);
      // Index of the current row.
      size_t i = 0;
      // Iterator to the next index where the result so far is unbound.
      auto unboundIdxIt = unboundIndices.begin();
      AD_CORRECTNESS_CHECK(unboundIdxIt != unboundIndices.end());
      for (auto& el : gen) {
        // Skip all the indices where the result is already bound from a
        // previous child.
        if (i == *unboundIdxIt) {
          if (IdOrString val{std::move(el)}; isUnbound(val)) {
            nextUnboundIndices.push_back(i);
          } else {
            result.at(*unboundIdxIt) = std::move(val);
          }
          ++unboundIdxIt;
          if (unboundIdxIt == unboundIndices.end()) {
            return;
          }
        }
        ++i;
      }
    };
    auto visitExpressionResult =
        [
          &visitConstantExpressionResult, &visitVectorExpressionResult
        ]<SingleExpressionResult T>(T && childResult)
            requires std::is_rvalue_reference_v<T&&> {
      // If the previous expression result is a constant, we can skip the loop.
      if constexpr (isConstantResult<T>) {
        visitConstantExpressionResult(AD_FWD(childResult));
      } else {
        visitVectorExpressionResult(AD_FWD(childResult));
      }
    };

    // Evaluate the children one by one, stopping as soon as all result are
    // bound.
    for (const auto& child : children_) {
      std::visit(visitExpressionResult, child->evaluate(ctx));
      unboundIndices = std::move(nextUnboundIndices);
      nextUnboundIndices.clear();
      // Early stopping if no more unbound result remain.
      if (unboundIndices.empty()) {
        break;
      }
    }
    return result;
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

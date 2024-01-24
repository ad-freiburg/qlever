// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/VariadicExpression.h"
#include "util/ChunkedForLoop.h"
#include "util/CompilerWarnings.h"

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
class CoalesceExpression : public VariadicExpression {
 public:
  using VariadicExpression::VariadicExpression;

  // _____________________________________________________________
  ExpressionResult evaluate(EvaluationContext* ctx) const override {
    // Arbitrarily chosen interval after which to check for cancellation.
    constexpr size_t CHUNK_SIZE = 1'000'000;
    // Set up one vector with the indices of the elements that are still unbound
    // so far and one for the indices that remain unbound after applying one of
    // the children.
    std::vector<uint64_t> unboundIndices;
    std::vector<uint64_t> nextUnboundIndices;
    unboundIndices.reserve(ctx->size());
    nextUnboundIndices.reserve(ctx->size());

    // Initially all result are unbound.
    ad_utility::chunkedForLoop<CHUNK_SIZE>(
        0, ctx->size(),
        [&unboundIndices](size_t i) {
          // TODO<C++23> use `std::ranges::to<vector>(std::views::iota...)`.
          unboundIndices.push_back(i);
        },
        [ctx]() {
          ctx->cancellationHandle_->throwIfCancelled("CoalesceExpression");
        });
    VectorWithMemoryLimit<IdOrString> result{ctx->_allocator};
    std::fill_n(std::back_inserter(result), ctx->size(),
                IdOrString{Id::makeUndefined()});

    ctx->cancellationHandle_->throwIfCancelled("CoalesceExpression");

    auto isUnbound = [](const IdOrString& x) {
      return (std::holds_alternative<Id>(x) &&
              std::get<Id>(x) == Id::makeUndefined());
    };

    auto visitConstantExpressionResult = [
      ctx, &nextUnboundIndices, &unboundIndices, &isUnbound, &result
    ]<SingleExpressionResult T>(T && childResult) requires isConstantResult<T> {
      // GCC 12 & 13 report this as potentially uninitialized
      // variable, which seems to be a false positive, so suppress the
      // warning here
      SUPPRESS_MAYBE_UNINITIALIZED(
          IdOrString constantResult{AD_FWD(childResult)};)
      if (isUnbound(constantResult)) {
        nextUnboundIndices = std::move(unboundIndices);
        return;
      }
      ad_utility::chunkedForLoop<CHUNK_SIZE>(
          0, unboundIndices.size(),
          [&unboundIndices, &result, &constantResult](size_t idx) {
            result[unboundIndices[idx]] = constantResult;
          },
          [ctx]() {
            ctx->cancellationHandle_->throwIfCancelled(
                "CoalesceExpression constant expression result");
          });
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
      // Iterator to the next index where the result so far is unbound.
      auto unboundIdxIt = unboundIndices.begin();
      AD_CORRECTNESS_CHECK(unboundIdxIt != unboundIndices.end());
      auto generatorIterator = gen.begin();
      ad_utility::chunkedForLoop<CHUNK_SIZE>(
          0, ctx->size(),
          [&unboundIdxIt, &isUnbound, &nextUnboundIndices, &result,
           &unboundIndices,
           &generatorIterator](size_t i, const auto& breakLoop) {
            // Skip all the indices where the result is already bound from a
            // previous child.
            if (i == *unboundIdxIt) {
              if (IdOrString val{std::move(*generatorIterator)};
                  isUnbound(val)) {
                nextUnboundIndices.push_back(i);
              } else {
                result.at(*unboundIdxIt) = std::move(val);
              }
              ++unboundIdxIt;
              if (unboundIdxIt == unboundIndices.end()) {
                breakLoop();
                return;
              }
            }
            ++generatorIterator;
          },
          [ctx]() {
            ctx->cancellationHandle_->throwIfCancelled(
                "CoalesceExpression vector expression result");
          });
    };
    auto visitExpressionResult =
        [
          &visitConstantExpressionResult, &visitVectorExpressionResult
        ]<SingleExpressionResult T>(T && childResult)
            requires std::is_rvalue_reference_v<T&&> {
      // If the previous expression result is a constant, we can skip the
      // loop.
      if constexpr (isConstantResult<T>) {
        visitConstantExpressionResult(AD_FWD(childResult));
      } else {
        visitVectorExpressionResult(AD_FWD(childResult));
      }
    };

    // Evaluate the children one by one, stopping as soon as all result are
    // bound.
    for (const auto& child : childrenVec()) {
      std::visit(visitExpressionResult, child->evaluate(ctx));
      unboundIndices = std::move(nextUnboundIndices);
      nextUnboundIndices.clear();
      ctx->cancellationHandle_->throwIfCancelled("CoalesceExpression");
      // Early stopping if no more unbound result remain.
      if (unboundIndices.empty()) {
        break;
      }
    }
    // TODO<joka921> The result is wrong in the case when all children are
    // constants (see the implementation of `CONCAT`).
    return result;
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

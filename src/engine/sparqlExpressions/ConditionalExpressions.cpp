// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/VariadicExpression.h"
#include "util/ChunkedForLoop.h"
#include "util/CompilerWarnings.h"

namespace sparqlExpression {
namespace detail::conditional_expressions {
using namespace sparqlExpression::detail;
struct IfImpl {
  CPP_template(typename I, typename E)(
      requires SingleExpressionResult<I>&& SingleExpressionResult<E>&&
          std::is_rvalue_reference_v<I&&>&& std::is_rvalue_reference_v<E&&>)
      IdOrLiteralOrIri
      operator()(EffectiveBooleanValueGetter::Result condition, I&& i,
                 E&& e) const {
    if (condition == EffectiveBooleanValueGetter::Result::True) {
      return AD_FWD(i);
    } else if (condition == EffectiveBooleanValueGetter::Result::False) {
      return AD_FWD(e);
    }
    AD_CORRECTNESS_CHECK(condition ==
                         EffectiveBooleanValueGetter::Result::Undef);
    return IdOrLiteralOrIri{Id::makeUndefined()};
  }
};

// This macro implements an expression that evaluates the `IF()` function, but
// will be extended below by additional member functions.
NARY_EXPRESSION(IfExpressionImpl, 3,
                FV<IfImpl, EffectiveBooleanValueGetter, ActualValueGetter,
                   ActualValueGetter>);

// The actual `IfExpression` class that adds an override for
// `isResultAlwaysDefined`.
class IfExpression : public IfExpressionImpl {
 public:
  using IfExpressionImpl::IfExpressionImpl;

  // _____________________________________________________________
  bool isResultAlwaysDefined(
      const VariableToColumnMap& varColMap) const override {
    const auto& childrenSpan = children();
    AD_CORRECTNESS_CHECK(childrenSpan.size() == 3);
    const SparqlExpression* condition = childrenSpan[0].get();
    const SparqlExpression* thenBranch = childrenSpan[1].get();
    const SparqlExpression* elseBranch = childrenSpan[2].get();

    // Special case: IF(BOUND(someExpr), someExpr, someOtherExpr)
    // In this case, the result is always defined iff someOtherExpr is always
    // defined.

    // Check if condition is a `BOUND()` expression using RTTI.
    // Create a dummy expression to get the typeid.
    static const auto& dummyBoundExprRef = []() -> const SparqlExpression& {
      static auto expr = makeBoundExpression(
          std::make_unique<VariableExpression>(Variable{"?dummy"}));
      return *expr;
    }();
    if (typeid(*condition) == typeid(dummyBoundExprRef)) {
      // condition is a BOUND expression, get its argument
      const auto& boundChildren = condition->children();
      AD_CORRECTNESS_CHECK(boundChildren.size() == 1);
      auto boundVar = boundChildren[0]->getVariableOrNullopt();
      auto thenVar = thenBranch->getVariableOrNullopt();
      if (boundVar.has_value() && boundVar == thenVar) {
        // Pattern matches: `IF(BOUND(?someVar), ?someVar, someOtherExpr)`
        // Result is then always defined iff any of the if or else branch are
        // always defined.
        return elseBranch->isResultAlwaysDefined(varColMap) ||
               thenBranch->isResultAlwaysDefined(varColMap);
      }
    }

    // General case: result is always defined iff both branches are always
    // defined
    return thenBranch->isResultAlwaysDefined(varColMap) &&
           elseBranch->isResultAlwaysDefined(varColMap);
  }
};

// The implementation of the COALESCE expression. It (at least currently) has to
// be done manually as we have no Generic implementation for variadic
// expressions, as it is the first one.
class CoalesceExpression : public VariadicExpression {
 public:
  using VariadicExpression::VariadicExpression;

  // _____________________________________________________________
  bool isResultAlwaysDefined(
      const VariableToColumnMap& varColMap) const override {
    // COALESCE is always defined if any of its children is always defined.
    return ql::ranges::any_of(
        childrenVec(), [&varColMap](const auto& childPtr) {
          return childPtr->isResultAlwaysDefined(varColMap);
        });
  }

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
        [&unboundIndices](size_t i) { unboundIndices.push_back(i); },
        [ctx]() { ctx->cancellationHandle_->throwIfCancelled(); });
    VectorWithMemoryLimit<IdOrLiteralOrIri> result{ctx->_allocator};
    std::fill_n(std::back_inserter(result), ctx->size(),
                IdOrLiteralOrIri{Id::makeUndefined()});
    if (result.empty()) {
      return result;
    }

    ctx->cancellationHandle_->throwIfCancelled();

    auto isUnbound = [](const IdOrLiteralOrIri& x) {
      return (std::holds_alternative<Id>(x) &&
              std::get<Id>(x) == Id::makeUndefined());
    };

    auto visitConstantExpressionResult =
        CPP_template_lambda(&nextUnboundIndices, &unboundIndices, &isUnbound,
                            &result, ctx)(typename T)(T && childResult)(
            requires SingleExpressionResult<T> && isConstantResult<T>) {
      IdOrLiteralOrIri constantResult{AD_FWD(childResult)};
      if (isUnbound(constantResult)) {
        nextUnboundIndices = std::move(unboundIndices);
        return;
      }
      ad_utility::chunkedForLoop<CHUNK_SIZE>(
          0, unboundIndices.size(),
          [&unboundIndices, &result, &constantResult](size_t idx) {
            // GCC 12 & 13 report this as potential uninitialized
            // use of a variable when compiling with -O3, which seems to
            // be a false positive, so we suppress the warning here. See
            // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=109561 for
            // more information.
            DISABLE_UNINITIALIZED_WARNINGS
            result[unboundIndices[idx]] = constantResult;
          },
          [ctx]() { ctx->cancellationHandle_->throwIfCancelled(); });
    };
    GCC_REENABLE_WARNINGS

    // For a single child result, write the result at the indices where the
    // result so far is unbound, and the child result is bound. While doing so,
    // set up the `nextUnboundIndices` vector  for the next step.
    auto visitVectorExpressionResult =
        CPP_template_lambda(&result, &unboundIndices, &nextUnboundIndices, &ctx,
                            &isUnbound)(typename T)(T && childResult)(
            requires CPP_NOT(isConstantResult<T> && SingleExpressionResult<T> &&
                             std::is_rvalue_reference_v<T&&>)) {
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
              if (IdOrLiteralOrIri val{std::move(*generatorIterator)};
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
          [ctx]() { ctx->cancellationHandle_->throwIfCancelled(); });
    };
    auto visitExpressionResult = CPP_template_lambda(
        &visitConstantExpressionResult,
        &visitVectorExpressionResult)(typename T)(T && childResult)(
        requires SingleExpressionResult<T> && std::is_rvalue_reference_v<T&&>) {
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
      ctx->cancellationHandle_->throwIfCancelled();
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

// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <optional>

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
      IdOrLocalVocabEntry
      operator()(EffectiveBooleanValueGetter::Result condition, I&& i,
                 E&& e) const {
    if (condition == EffectiveBooleanValueGetter::Result::True) {
      return AD_FWD(i);
    } else if (condition == EffectiveBooleanValueGetter::Result::False) {
      return AD_FWD(e);
    }
    AD_CORRECTNESS_CHECK(condition ==
                         EffectiveBooleanValueGetter::Result::Undef);
    return IdOrLocalVocabEntry{Id::makeUndefined()};
  }
};

// This class implements an expression that evaluates the `IF()` function, but
// will be extended below by additional member functions. It always uses
// `NaryExpressionStronglyTyped` explicitly because `ActualValueGetter` doesn't
// have a uniform result type, and therefore cannot be type-erased.
using IfExpressionImpl = NaryExpressionStronglyTyped<
    detail::Operation<3, FV<IfImpl, EffectiveBooleanValueGetter,
                            ActualValueGetter, ActualValueGetter>>>;

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
    // The IIFE returns a reference to a `static` local, which is valid, but
    // GCC's `-Wdangling-reference` cannot trace through it.
    DISABLE_DANGLING_REFERENCE_WARNINGS
    static const auto& dummyBoundExprRef = []() -> const SparqlExpression& {
      static auto expr = makeBoundExpression(
          std::make_unique<VariableExpression>(Variable{"?dummy"}));
      return *expr;
    }();
    GCC_REENABLE_WARNINGS
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

// Helpers for the implementation of the `COALESCE` expression below.
namespace {
// A single value is "unbound" (and is therefore overwritten by the next child
// of a `COALESCE`) iff it is the UNDEF `Id`.
bool isUnbound(const IdOrLocalVocabEntry& x) {
  return std::holds_alternative<Id>(x) && std::get<Id>(x).isUndefined();
}

// Holds the state while the children of a `COALESCE` are applied one after the
// other: `result_` are the values bound so far (initially all UNDEF), and
// `unboundIndices_` are the indices (in ascending order) at which `result_` is
// still unbound and which the remaining children may therefore still bind.
class CoalesceEvaluation {
  // Arbitrarily chosen interval after which to check for cancellation.
  static constexpr size_t CHUNK_SIZE = 1'000'000;

  EvaluationContext* ctx_;
  VectorWithMemoryLimit<IdOrLocalVocabEntry> result_;
  // The indices that are unbound before the current child is applied, and those
  // that remain unbound after applying it.
  std::vector<uint64_t> unboundIndices_;
  std::vector<uint64_t> nextUnboundIndices_;

 public:
  // Set up the state for an evaluation where nothing is bound yet.
  explicit CoalesceEvaluation(EvaluationContext* ctx)
      : ctx_{ctx}, result_{ctx->_allocator} {
    unboundIndices_.reserve(ctx_->size());
    nextUnboundIndices_.reserve(ctx_->size());
    ad_utility::chunkedForLoop<CHUNK_SIZE>(
        0, ctx_->size(), [this](size_t i) { unboundIndices_.push_back(i); },
        [this]() { checkCancellation(); });
    std::fill_n(std::back_inserter(result_), ctx_->size(),
                IdOrLocalVocabEntry{Id::makeUndefined()});
    checkCancellation();
  }

  // True iff no child has bound any result so far. Note that for an empty
  // evaluation context this is trivially and permanently true, as there is no
  // result that could be bound in the first place.
  bool nothingBoundYet() const {
    return unboundIndices_.size() == ctx_->size();
  }

  // True iff every result is bound, so that the remaining children of the
  // `COALESCE` don't have to be evaluated at all.
  bool allResultsBound() const {
    return unboundIndices_.empty() && !nothingBoundYet();
  }

  // Apply a child with a constant result. Returns that constant iff the result
  // of the whole `COALESCE` is that constant, and `std::nullopt` otherwise.
  template <typename T>
  std::optional<IdOrLocalVocabEntry> applyConstantResult(T&& childResult) {
    using U = decltype(childResult);
    static_assert(SingleExpressionResult<U> && isConstantResult<U>);
    IdOrLocalVocabEntry constantResult{AD_FWD(childResult)};
    if (isUnbound(constantResult)) {
      // This child binds nothing, so all the unbound indices stay unbound.
      nextUnboundIndices_ = std::move(unboundIndices_);
      return std::nullopt;
    }
    // If nothing was bound before, then this constant binds *all* the results,
    // so the whole `COALESCE` evaluates to it.
    if (nothingBoundYet()) {
      return constantResult;
    }
    // GCC 12 & 13 report the assignment below as potential uninitialized use of
    // a variable when compiling with -O3, which seems to be a false positive,
    // so we suppress the warning here. See
    // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=109561 for more information.
    DISABLE_UNINITIALIZED_WARNINGS
    ad_utility::chunkedForLoop<CHUNK_SIZE>(
        0, unboundIndices_.size(),
        [this, &constantResult](size_t idx) {
          result_[unboundIndices_[idx]] = constantResult;
        },
        [this]() { checkCancellation(); });
    GCC_REENABLE_WARNINGS
    return std::nullopt;
  }

  // Apply a child that yields one value per row: Write its result at the
  // indices where the result so far is unbound and the child's result is bound.
  // While doing so, set up `nextUnboundIndices_` for the next child.
  template <typename T>
  void applyVectorResult(T&& childResult) {
    using U = decltype(childResult);
    static_assert(!(isConstantResult<U> && SingleExpressionResult<U> &&
                    std::is_rvalue_reference_v<U>));
    if (unboundIndices_.empty()) {
      // There is nothing left to bind. For a nonempty evaluation context we
      // would have stopped evaluating children already (see `allResultsBound`),
      // so this can only happen for an empty context, for which this child
      // simply contributes no value at all.
      AD_CORRECTNESS_CHECK(nothingBoundYet());
      return;
    }
    auto gen = detail::makeGenerator(AD_FWD(childResult), ctx_->size(), ctx_);
    // Iterator to the next index where the result so far is unbound.
    auto unboundIdxIt = unboundIndices_.begin();
    auto generatorIterator = gen.begin();
    ad_utility::chunkedForLoop<CHUNK_SIZE>(
        0, ctx_->size(),
        [this, &unboundIdxIt, &generatorIterator](size_t i,
                                                  const auto& breakLoop) {
          // Skip all the indices where the result is already bound from a
          // previous child.
          if (i == *unboundIdxIt) {
            if (IdOrLocalVocabEntry val{std::move(*generatorIterator)};
                isUnbound(val)) {
              nextUnboundIndices_.push_back(i);
            } else {
              result_.at(*unboundIdxIt) = std::move(val);
            }
            ++unboundIdxIt;
            if (unboundIdxIt == unboundIndices_.end()) {
              breakLoop();
              return;
            }
          }
          ++generatorIterator;
        },
        [this]() { checkCancellation(); });
  }

  // Move on to the next child.
  void advanceToNextChild() {
    unboundIndices_ = std::move(nextUnboundIndices_);
    nextUnboundIndices_.clear();
    checkCancellation();
  }

  // The result of the `COALESCE` after all children have been applied.
  ExpressionResult moveResultOut() && {
    // Prefer returning a single UNDEF over a vector of UNDEFs if all the
    // results are unbound. Note that for an empty evaluation context nothing
    // can ever be bound by a child with a per-row result, so the result there
    // is UNDEF unless some child had a bound constant value.
    if (nothingBoundYet()) {
      return Id::makeUndefined();
    }
    return std::move(result_);
  }

 private:
  void checkCancellation() const {
    ctx_->cancellationHandle_->throwIfCancelled();
  }
};
}  // namespace

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
    CoalesceEvaluation evaluation{ctx};

    auto applyChildResult =
        [&evaluation](
            auto&& childResult) -> std::optional<IdOrLocalVocabEntry> {
      using T = decltype(childResult);
      static_assert(SingleExpressionResult<T> && std::is_rvalue_reference_v<T>);
      if constexpr (isConstantResult<T>) {
        return evaluation.applyConstantResult(AD_FWD(childResult));
      } else {
        evaluation.applyVectorResult(AD_FWD(childResult));
        return std::nullopt;
      }
    };

    // Evaluate the children one by one, stopping as soon as all results are
    // bound.
    //
    // NOTE: We must not short-circuit on an *empty* evaluation context before
    // the children have been consulted. Like every other expression, `COALESCE`
    // yields a constant whenever its inputs are constant (compare
    // `getResultSize` for the generic expressions, or `ConcatExpression`), and
    // it neither knows nor needs to know whether it is evaluated inside a
    // `GROUP BY`. An empty context arises for an implicit `GROUP BY` (no
    // `GROUP BY` variables) over an empty input, where all the children are
    // aggregates that do have a constant value for the empty group (`SUM`
    // yields `0`, `MIN` yields UNDEF, etc.), so simply evaluating them gives
    // the correct constant, which `GroupBy` then requires.
    for (const auto& child : childrenVec()) {
      std::optional<IdOrLocalVocabEntry> constantValue =
          std::visit(applyChildResult, child->evaluate(ctx));
      if (constantValue.has_value()) {
        return std::move(constantValue).value();
      }
      evaluation.advanceToNextChild();
      if (evaluation.allResultsBound()) {
        break;
      }
    }
    return std::move(evaluation).moveResultOut();
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

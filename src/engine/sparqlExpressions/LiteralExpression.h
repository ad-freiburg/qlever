//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_LITERALEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_LITERALEXPRESSION_H

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/TypeTraits.h"

namespace sparqlExpression {
namespace detail {

/// An expression with a single value, for example a numeric (42.0) or boolean
/// (false) constant or a variable (?x) or a string or iri (<Human>). These are
/// the "leaves" in the expression tree.
template <typename T>
class LiteralExpression : public SparqlExpression {
 private:
  // For string literals, cache the result of the evaluation as it doesn't
  // change when `evaluate` is called multiple times. It is a `std::atomic` to
  // make the `const` evaluate function threadsafe and lock-free.
  // TODO<joka921> Make this unnecessary by completing multiple small groups at
  // once during the GROUP BY.
  mutable std::atomic<IdOrLiteralOrIri*> cachedResult_ = nullptr;

 public:
  // ___________________________________________________________________________
  explicit LiteralExpression(T _value) : _value{std::move(_value)} {}
  ~LiteralExpression() override {
    delete cachedResult_.load(std::memory_order_relaxed);
  }
  // Disallow copying and moving. We currently don't need it, and we would have
  // to take care of the cache for proper implementations.
  LiteralExpression(const LiteralExpression&) = delete;
  LiteralExpression& operator=(const LiteralExpression&) = delete;

  // A simple getter for the stored value.
  const T& value() const { return _value; }

  // Evaluating just returns the constant/literal value.
  ExpressionResult evaluate(EvaluationContext* context) const override {
    // Common code for the `Literal` and `Iri` case.
    auto getIdOrString = [this, &context](const auto& s)
        -> CPP_ret(ExpressionResult)(
            requires ad_utility::SameAsAny<std::decay_t<decltype(s)>,
                                           TripleComponent::Literal,
                                           TripleComponent::Iri>) {
      if (auto ptr = cachedResult_.load(std::memory_order_relaxed)) {
        return *ptr;
      }
      TripleComponent tc{s};
      std::optional<Id> id = tc.toValueId(context->_qec.getIndex().getVocab());
      IdOrLiteralOrIri result =
          id.has_value()
              ? IdOrLiteralOrIri{id.value()}
              : IdOrLiteralOrIri{ad_utility::triple_component::LiteralOrIri{s}};
      auto ptrForCache = std::make_unique<IdOrLiteralOrIri>(result);
      ptrForCache.reset(std::atomic_exchange_explicit(
          &cachedResult_, ptrForCache.release(), std::memory_order_relaxed));
      context->cancellationHandle_->throwIfCancelled();
      return result;
    };
    if constexpr (ad_utility::SameAsAny<T, TripleComponent::Literal,
                                        TripleComponent::Iri>) {
      return getIdOrString(_value);
    } else if constexpr (std::is_same_v<Variable, T>) {
      return evaluateIfVariable(context, _value);
    } else if constexpr (std::is_same_v<VectorWithMemoryLimit<ValueId>, T>) {
      // TODO<kcaliban> Change ExpressionResult such that it might point or
      //                refer to this vector instead of having to clone it.
      return _value.clone();
    } else {
      return _value;
    }
  }

  // Variables and string constants add their values.
  ql::span<const Variable> getContainedVariablesNonRecursive() const override {
    if constexpr (std::is_same_v<T, ::Variable>) {
      return {&_value, 1};
    } else {
      return {};
    }
  }

  // ___________________________________________________________________________
  std::vector<Variable> getUnaggregatedVariables() const override {
    if constexpr (std::is_same_v<T, ::Variable>) {
      return {_value};
    } else {
      return {};
    }
  }

  // ___________________________________________________________________________
  std::string getCacheKey(const VariableToColumnMap& varColMap) const override {
    if constexpr (std::is_same_v<T, ::Variable>) {
      if (!varColMap.contains(_value)) {
        return "Unbound Variable";
      }
      return {"#column_" + std::to_string(varColMap.at(_value).columnIndex_) +
              "#"};
    } else if constexpr (std::is_same_v<T, std::string>) {
      return _value;
    } else if constexpr (std::is_same_v<T, ValueId>) {
      return absl::StrCat("#valueId ", _value.getBits(), "#");
    } else if constexpr (std::is_same_v<T, TripleComponent::Literal>) {
      return absl::StrCat("#literal: ", _value.toStringRepresentation());
    } else if constexpr (std::is_same_v<T, TripleComponent::Iri>) {
      return absl::StrCat("#iri: ", _value.toStringRepresentation());
    } else if constexpr (std::is_same_v<T, VectorWithMemoryLimit<ValueId>>) {
      // We should never cache this, as objects of this type of expression are
      // used exactly *once* in the HashMap optimization of the GROUP BY
      // operation
      AD_THROW("Trying to get cache key for value that should not be cached.");
    } else {
      return {std::to_string(_value)};
    }
  }

  // ___________________________________________________________________________
  bool isConstantExpression() const override {
    return !std::is_same_v<T, ::Variable>;
  }

 protected:
  // ___________________________________________________________________________
  std::optional<::Variable> getVariableOrNullopt() const override {
    if constexpr (std::is_same_v<T, ::Variable>) {
      return _value;
    }
    return std::nullopt;
  }

 private:
  T _value;

  // Evaluate the expression if it is a variable expression with the given
  // `variable`. The variable is passed in explicitly because this function
  // might be called recursively.
  ExpressionResult evaluateIfVariable(EvaluationContext* context,
                                      const Variable& variable) const {
    // If this is a variable that is not visible in the input but was bound by a
    // previous alias in the same SELECT clause, then read the constant value of
    // the variable from the data structures dedicated to this case.
    if (auto resultFromSameRow =
            context->getResultFromPreviousAggregate(variable);
        resultFromSameRow.has_value() &&
        !context->_groupedVariables.contains(variable)) {
      // If the expression is a simple renaming of a variable `(?x AS ?y)` we
      // have to recurse to track a possible chain of such renamings in the
      // SELECT clause.
      if (const Variable* var =
              std::get_if<Variable>(&resultFromSameRow.value());
          var != nullptr) {
        return evaluateIfVariable(context, *var);
      } else {
        return std::move(resultFromSameRow.value());
      }
    }

    // If the variable is not part of the input, then it is always UNDEF.
    auto column = context->getColumnIndexForVariable(variable);
    if (!column.has_value()) {
      return Id::makeUndefined();
    }
    // If a variable is grouped, then we know that it always has the same
    // value and can treat it as a constant. This is not possible however when
    // we are inside an aggregate, because for example `SUM(?variable)` must
    // still compute the sum over the whole group.
    if (context->_groupedVariables.contains(variable) && !isInsideAggregate()) {
      const auto& table = context->_inputTable;
      auto constantValue = table.at(context->_beginIndex, column.value());
      AD_EXPENSIVE_CHECK((
          std::all_of(table.begin() + context->_beginIndex,
                      table.begin() + context->_endIndex, [&](const auto& row) {
                        return row[column.value()] == constantValue;
                      })));
      return constantValue;
    } else {
      return variable;
    }
  }

  // Literal expressions don't have children
  ql::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};

// A simple expression that just returns an explicit result. It can only be used
// once as the result is moved out.
struct SingleUseExpression : public SparqlExpression {
  explicit SingleUseExpression(ExpressionResult result)
      : result_{std::move(result)} {}
  mutable ExpressionResult result_;
  mutable std::atomic<bool> resultWasMoved_ = false;
  ExpressionResult evaluate(EvaluationContext*) const override {
    AD_CONTRACT_CHECK(!resultWasMoved_);
    resultWasMoved_ = true;
    return std::move(result_);
  }

  std::vector<Variable> getUnaggregatedVariables() const override {
    // This class should only be used as an implementation of other expressions,
    // not as a "normal" part of an expression tree.
    AD_FAIL();
  }
  std::string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    // This class should only be used as an implementation of other expressions,
    // not as a "normal" part of an expression tree.
    AD_FAIL();
  }

  ql::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};

}  // namespace detail

///  The actual instantiations and aliases of LiteralExpressions.
using VariableExpression = detail::LiteralExpression<::Variable>;
using IriExpression = detail::LiteralExpression<TripleComponent::Iri>;
using StringLiteralExpression =
    detail::LiteralExpression<TripleComponent::Literal>;
using IdExpression = detail::LiteralExpression<ValueId>;
using VectorIdExpression =
    detail::LiteralExpression<VectorWithMemoryLimit<ValueId>>;
using SingleUseExpression = detail::SingleUseExpression;

namespace detail {

//______________________________________________________________________________
using IdOrLocalVocabEntry = prefilterExpressions::IdOrLocalVocabEntry;
// Given a `SparqlExpression*` pointing to a `LiteralExpression`, this helper
// function retrieves a corresponding `IdOrLocalVocabEntry` variant
// (`std::variant<ValueId, LocalVocabEntry>`) for `LiteralExpression`s that
// contain a suitable type.
// Given the boolean flag `stringAndIriOnly` is set to `true`, only `Literal`s,
// `Iri`s and `ValueId`s of type `VocabIndex`/`LocalVocabIndex` are returned. If
// `stringAndIriOnly` is set to `false` (default), all `ValueId` types retrieved
// from `LiteralExpression<ValueId>` will be returned.
inline std::optional<IdOrLocalVocabEntry>
getIdOrLocalVocabEntryFromLiteralExpression(const SparqlExpression* child,
                                            bool stringAndIriOnly = false) {
  using enum Datatype;
  if (const auto* idExpr = dynamic_cast<const IdExpression*>(child)) {
    auto idType = idExpr->value().getDatatype();
    if (stringAndIriOnly && idType != VocabIndex && idType != LocalVocabIndex) {
      return std::nullopt;
    }
    return idExpr->value();
  } else if (const auto* literalExpr =
                 dynamic_cast<const StringLiteralExpression*>(child)) {
    return LocalVocabEntry{literalExpr->value()};
  } else if (const auto* iriExpr = dynamic_cast<const IriExpression*>(child)) {
    return LocalVocabEntry{iriExpr->value()};
  } else {
    return std::nullopt;
  }
}

//______________________________________________________________________________
// Given a `SparqlExpression*` pointing to a `StringLiteralExpression`, return
// the contained `Literal`. For all other `LiteralExpression` types return
// `std::nullopt`.
inline std::optional<TripleComponent::Literal> getLiteralFromLiteralExpression(
    const SparqlExpression* child) {
  using enum Datatype;
  if (const auto* literalExpr =
          dynamic_cast<const StringLiteralExpression*>(child)) {
    return literalExpr->value();
  }
  return std::nullopt;
}

}  // namespace detail

}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_LITERALEXPRESSION_H

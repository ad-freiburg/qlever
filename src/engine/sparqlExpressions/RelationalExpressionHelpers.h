//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_RELATIONALEXPRESSIONHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_RELATIONALEXPRESSIONHELPERS_H

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "global/ValueIdComparators.h"

namespace sparqlExpression {
using valueIdComparators::Comparison;
// For `T == VectorWithMemoryLimit<U>`, `ValueType<T>` is `U`. For any other
// type `T`, `ValueType<T>` is `T`.
namespace detail {
// TODO<joka921> This helper function may never be called and could in principle
// be formulated directly within the `decltype` statement below. However, this
// doesn't compile with G++11. Find out, why.
template <typename T>
constexpr auto getObjectOfValueTypeHelper(T&& t) {
  if constexpr (ad_utility::similarToInstantiation<T, VectorWithMemoryLimit>) {
    return std::move(t[0]);
  } else {
    return AD_FWD(t);
  }
}
}  // namespace detail
template <typename T>
using ValueType =
    decltype(detail::getObjectOfValueTypeHelper<T>(std::declval<T>()));

// Concept that requires that `T` logically stores numeric values.
template <typename T>
CPP_concept StoresNumeric = concepts::integral<ValueType<T>> ||
                            ql::concepts::floating_point<ValueType<T>>;

// Concept that requires that `T` logically stores `std::string`s.
template <typename T>
CPP_concept StoresStrings = ad_utility::SimilarTo<ValueType<T>, std::string>;

// Concept that requires that `T` logically stores boolean values.
template <typename T>
CPP_concept StoresBoolean =
    ad_utility::SimilarTo<T, ad_utility::SetOfIntervals>;

// Concept that requires that `T` logically stores `ValueId`s.
template <typename T>
CPP_concept StoresValueId =
    ad_utility::SimilarTo<T, Variable> ||
    ad_utility::SimilarTo<T, VectorWithMemoryLimit<ValueId>> ||
    ad_utility::SimilarTo<T, ValueId>;

// When `A` and `B` are `AreIncomparable`, comparisons between them will always
// yield `not equal`, independent of the concrete values.
template <typename A, typename B>
CPP_concept AreIncomparable = (StoresNumeric<A> && StoresStrings<B>) ||
                              (StoresNumeric<B> && StoresStrings<A>);

// The types for which comparisons like `<` are supported and not always false.
template <typename A, typename B>
CPP_concept AreComparable =
    !AreIncomparable<A, B> && (StoresValueId<A> || !StoresValueId<B>);

// Apply the given `Comparison` to `a` and `b`. For example, if the `Comparison`
// is `LT`, returns `a < b`. Note that the second template argument `Dummy` is
// only needed to make the static check for the exhaustiveness of the if-else
// cascade possible.
template <Comparison Comp, typename Dummy = int, typename A, typename B>
bool applyComparison(const A& a, const B& b) {
  using enum Comparison;
  if constexpr (Comp == LT) {
    return a < b;
  } else if constexpr (Comp == LE) {
    return a <= b;
  } else if constexpr (Comp == EQ) {
    return a == b;
  } else if constexpr (Comp == NE) {
    return a != b;
  } else if constexpr (Comp == GE) {
    return a >= b;
  } else if constexpr (Comp == GT) {
    return a > b;
  } else {
    static_assert(ad_utility::alwaysFalse<Dummy>);
  }
}

// Get the comparison that yields the same result when the arguments are
// swapped. For example the swapped comparison of `less than` is `greater than`
// because `a < b` if and only if `b > a`.
constexpr Comparison getComparisonForSwappedArguments(Comparison comp) {
  switch (comp) {
    case Comparison::LE:
      return Comparison::GE;
    case Comparison::LT:
      return Comparison::GT;
    case Comparison::EQ:
    case Comparison::NE:
      return comp;
    case Comparison::GE:
      return Comparison::LE;
    case Comparison::GT:
      return Comparison::LT;
  }
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  AD_FAIL();
#endif
}

// A concept for various types that either represent a string or an ID. For its
// usage see below.
template <typename S>
CPP_concept StoresStringOrId =
    ad_utility::SimilarToAny<S, ValueId, LocalVocabEntry, IdOrLocalVocabEntry>;
// Convert a `LocalVocabEntry` or `IdOrLocalVocabEntry` value into the
// corresponding `ValueId`. This function also takes `ValueId`s which are simply
// returned unchanged. This makes the usage of this function easier.
CPP_template(typename S)(requires StoresStringOrId<S>)
Id makeValueId(const S& value, const EvaluationContext* context) {
  if constexpr (ad_utility::isSimilar<S, ValueId>) {
    return value;
  } else if constexpr (ad_utility::isSimilar<S, IdOrLocalVocabEntry>) {
    auto visitor = [context](const auto& x) {
      auto res = makeValueId(x, context);
      return res;
    };
    return std::visit(visitor, value);

  } else {
    static_assert(ad_utility::isSimilar<S, LocalVocabEntry>);
    return Id::makeFromLocalVocabIndex(&value);
  }
}

// Compare two elements that are either strings or IDs in some way (see the
// `StoresStringOrId` concept) according to the specified comparison (see
// `ValueIdComparators.h` for details). The `EvaluationContext` is required to
// compare strings to Ids.
template <valueIdComparators::Comparison Comp>
inline const auto compareIdsOrStrings =
    [](const auto& a, const auto& b, const EvaluationContext* ctx)
    -> CPP_ret(valueIdComparators::ComparisonResult)(
        requires StoresStringOrId<std::decay_t<decltype(a)>>&&
            StoresStringOrId<std::decay_t<decltype(b)>>) {
  using T = std::decay_t<decltype(a)>;
  using U = std::decay_t<decltype(b)>;
  if constexpr (ad_utility::isSimilar<LocalVocabEntry, T> &&
                ad_utility::isSimilar<LocalVocabEntry, U>) {
    return valueIdComparators::fromBool(applyComparison<Comp>(a, b));
  } else {
    return valueIdComparators::compareIds(makeValueId(a, ctx),
                                          makeValueId(b, ctx), Comp);
  }
};
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_RELATIONALEXPRESSIONHELPERS_H

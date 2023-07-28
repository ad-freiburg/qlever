//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

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
concept StoresNumeric =
    std::integral<ValueType<T>> || std::floating_point<ValueType<T>>;

// Concept that requires that `T` logically stores `std::string`s.
template <typename T>
concept StoresStrings = ad_utility::SimilarTo<ValueType<T>, std::string>;

// Concept that requires that `T` logically stores boolean values.
template <typename T>
concept StoresBoolean = std::is_same_v<T, ad_utility::SetOfIntervals>;

// Concept that requires that `T` logically stores `ValueId`s.
template <typename T>
concept StoresValueId =
    ad_utility::SimilarTo<T, Variable> ||
    ad_utility::SimilarTo<T, VectorWithMemoryLimit<ValueId>> ||
    ad_utility::SimilarTo<T, ValueId>;

// When `A` and `B` are `AreIncomparable`, comparisons between them will always
// yield `not equal`, independent of the concrete values.
template <typename A, typename B>
concept AreIncomparable = (StoresNumeric<A> && StoresStrings<B>) ||
                          (StoresNumeric<B> && StoresStrings<A>);

// True iff any of `A, B` is `StoresBoolean` (see above).
template <typename A, typename B>
concept AtLeastOneIsBoolean = StoresBoolean<A> || StoresBoolean<B>;

// The types for which comparisons like `<` are supported and not always false.
template <typename A, typename B>
concept AreComparable = !AtLeastOneIsBoolean<A, B> && !AreIncomparable<A, B> &&
                        (StoresValueId<A> || !StoresValueId<B>);

// Apply the given `Comparison` to `a` and `b`. For example, if the `Comparison`
// is `LT`, returns `a < b`. Note that the second template argument `Dummy` is
// only needed to make the static check for the exhaustiveness of the if-else
// cascade possible.
template <Comparison Comp, typename Dummy = int>
bool applyComparison(const auto& a, const auto& b) {
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
}

// Return the ID range `[begin, end)` in which the entries of the vocabulary
// compare equal to `s`. This is a range because words that are different on
// the byte level can still logically be equal, depending on the chosen Unicode
// collation level.
// TODO<joka921> Make the collation level configurable.
inline std::pair<ValueId, ValueId> getRangeFromVocab(
    const std::string& s, const EvaluationContext* context) {
  auto level = TripleComponentComparator::Level::QUARTERNARY;
  // TODO<joka921> This should be `Vocab::equal_range`
  const ValueId lower = Id::makeFromVocabIndex(
      context->_qec.getIndex().getVocab().lower_bound(s, level));
  const ValueId upper = Id::makeFromVocabIndex(
      context->_qec.getIndex().getVocab().upper_bound(s, level));
  return {lower, upper};
}

// Convert an int, double, or string value into a `ValueId`. For int and double
// this is a single `ValueId`, for strings it is a `pair<ValueId, ValueId>` that
// denotes a range (see `getRangeFromVocab` above).
// template <SingleExpressionResult S>
// requires isConstantResult<S>
template <typename S>
requires((!SingleExpressionResult<S> || isConstantResult<S>))
auto makeValueId(const S& value, const EvaluationContext* context) {
  if constexpr (std::is_integral_v<S>) {
    return ValueId::makeFromInt(value);
  } else if constexpr (std::is_floating_point_v<S>) {
    return ValueId::makeFromDouble(value);
  } else if constexpr (ad_utility::isSimilar<S, ValueId>) {
    return value;
  } else if constexpr (ad_utility::isSimilar<S, std::pair<Id, Id>>) {
    return value;
  } else if constexpr (ad_utility::isSimilar<S, IdOrString>) {
    auto visitor = [context](const auto& x) {
      auto res = makeValueId(x, context);
      if constexpr (ad_utility::isSimilar<decltype(res), Id>) {
        return std::pair{res, res};
      } else {
        return res;
      }
    };
    return value.visit(visitor);

  } else {
    static_assert(ad_utility::isSimilar<S, std::string>);
    return getRangeFromVocab(value, context);
  }
};

// TODO<joka921> Comment
// TODO<joka921> There is a duplication between this function and the
// RelationalExpression. Get rid of it.
template <valueIdComparators::Comparison Comp,
          valueIdComparators::ComparisonForIncompatibleTypes
              comparisonForIncompatibleTypes>
inline const auto compareIdsOrStrings =
    []<typename T, typename U>(
        const T& a, const U& b,
        const EvaluationContext* ctx) -> valueIdComparators::ComparisonResult {
  if constexpr (ad_utility::isSimilar<std::string, T> &&
                ad_utility::isSimilar<std::string, U>) {
    // TODO<joka921> integrate comparison via ICU and proper handling for
    // IRIs/ Literals/etc.
    return valueIdComparators::fromBool(applyComparison<Comp>(a, b));
  } else {
    auto x = makeValueId(a, ctx);
    auto y = makeValueId(b, ctx);
    if constexpr (requires { valueIdComparators::compareIds(x, y, Comp); }) {
      // Compare two `ValueId`s
      return valueIdComparators::compareIds<comparisonForIncompatibleTypes>(
          x, y, Comp);
    } else if constexpr (requires {
                           valueIdComparators::compareWithEqualIds(
                               x, y.first, y.second, Comp);
                         }) {
      // Compare `ValueId` with range of equal `ValueId`s (used when `value2`
      // is `string` or `vector<string>`.
      return valueIdComparators::compareWithEqualIds<
          comparisonForIncompatibleTypes>(x, y.first, y.second, Comp);
    } else if constexpr (requires {
                           valueIdComparators::compareWithEqualIds(
                               y, x.first, x.second, Comp);
                         }) {
      // Compare `ValueId` with range of equal `ValueId`s (used when `value2`
      // is `string` or `vector<string>`.
      return valueIdComparators::compareWithEqualIds<
          comparisonForIncompatibleTypes>(
          y, x.first, x.second, getComparisonForSwappedArguments(Comp));
    } else {
      // static_assert(ad_utility::alwaysFalse<decltype(x)>);
      static_assert(ad_utility::alwaysFalse<decltype(y)>);
    }
  }
};
}  // namespace sparqlExpression

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICEXPRESSIONWRAPPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICEXPRESSIONWRAPPERS_H

#include <tuple>
#include <type_traits>
#include <variant>

#include "backports/concepts.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/Forward.h"

namespace sparqlExpression::detail {

// Common wrappers for numeric SPARQL expressions.
// They convert primitive numeric functions into the `Id`- and
// `NumericValue`-based interfaces used by the expression framework.

// Takes a `Function` that returns a built-in arithmetic type (integral or
// floating-point type) and converts it to a function with the same arguments
// that returns the result as an `Id`. If `nanToUndef` is true, floating-point
// NaN and infinity results are converted to `UNDEF`.
template <typename Function, bool nanToUndef = false>
struct NumericIdWrapper {
  [[no_unique_address]] Function function_{};

  template <typename... Args>
  Id operator()(Args&&... args) const {
    return makeNumericId<nanToUndef>(function_(AD_FWD(args)...));
  }
};

// Takes a `Function` that operates on built-in arithmetic types (integral or
// floating-point types) and lifts it to accept `NumericValue` arguments.
// Non-numeric arguments yield `UNDEF`, and arithmetic results are converted
// to `Id`. If `NanOrInfToUndef` is true, floating-point NaN and infinity
// results are converted to `UNDEF`.
template <typename Function, bool NanOrInfToUndef = false>
struct MakeNumericExpression {
  [[no_unique_address]] Function function_{};

  template <typename... Args>
  Id operator()(const Args&... args) const {
    CPP_assert((concepts::same_as<std::decay_t<Args>, NumericValue> && ...));

    auto visitor = [](const auto&... t) {
      if constexpr ((... ||
                     std::is_same_v<NotNumeric, std::decay_t<decltype(t)>>)) {
        return Id::makeUndefined();
      } else {
        return makeNumericId<NanOrInfToUndef>(Function{}(t...));
      }
    };

    return std::visit(visitor, args...);
  }
};

// If `undefForNonNumeric<Function>` is true, `Function` must always return an
// undefined ID when any argument is non-numeric. Expressions use this property
// to return UNDEF for such inputs without evaluating the function.
//
// NOTE: This property is opt-in and independent of `supportsNumericFastPath`.
// Date-aware operations also support homogeneous numeric execution, but can
// return defined results for non-numeric dates.
template <typename Function>
inline constexpr bool undefForNonNumeric = false;

template <typename Function, bool NanOrInfToUndef>
inline constexpr bool
    undefForNonNumeric<MakeNumericExpression<Function, NanOrInfToUndef>> = true;

// If true, Function returns UNDEF for any input outside numeric values and
// dates. This separate opt-in permits dates for NumericOrDateValueGetter while
// retaining the numeric-only property above for NumericValueGetter.
template <typename Function>
inline constexpr bool undefForNonNumericOrDate = false;

// Return whether any raw ValueId argument is guaranteed to make Function return
// UNDEF, based on its datatype and corresponding value getter. ValueGetters is
// a tuple with one getter per argument, in the same order as values.
//
// With NumericValueGetter and undefForNonNumeric<Function>, accept Int, Double,
// and Bool; with NumericOrDateValueGetter and
// undefForNonNumericOrDate<Function>, also accept Date. All other datatypes can
// return UNDEF without invoking the getters or Function. This is the same
// acceptance rule recorded by
// NumericOperandClassification::containsValueRejectedByGetter_.
//
// Booleans are accepted because both getters convert them to integers. Date
// payloads, including invalid or incompatible dates, still reach the existing
// typed arithmetic. Other getters and non-ValueId arguments are never rejected
// here, and functions without the corresponding opt-in retain normal
// evaluation.
template <typename Function, typename ValueGetters, typename... Values>
bool hasNonNumericOperand(const Values&... values) {
  return [&]<size_t... I>(std::index_sequence<I...>) {
    auto rejects = []<typename Getter>(const auto& value) {
      if constexpr (undefForNonNumeric<Function> &&
                    std::is_same_v<Getter, NumericValueGetter> &&
                    std::is_same_v<std::decay_t<decltype(value)>, ValueId>) {
        using enum Datatype;
        const auto type = value.getDatatype();
        return type != Int && type != Double && type != Bool;
      } else if constexpr (undefForNonNumericOrDate<Function> &&
                           std::is_same_v<Getter, NumericOrDateValueGetter> &&
                           std::is_same_v<std::decay_t<decltype(value)>,
                                          ValueId>) {
        using enum Datatype;
        const auto type = value.getDatatype();
        return type != Int && type != Double && type != Bool && type != Date;
      } else {
        return false;
      }
    };
    return (rejects.template operator()<std::tuple_element_t<I, ValueGetters>>(
                values) ||
            ...);
  }(std::index_sequence_for<Values...>{});
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICEXPRESSIONWRAPPERS_H

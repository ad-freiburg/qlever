//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SPARQLEXPRESSIONVALUEGETTERS_H
#define QLEVER_SPARQLEXPRESSIONVALUEGETTERS_H

#include "../../global/Id.h"
#include "../ResultTable.h"
#include "./SparqlExpressionTypes.h"

/// Several classes that can be used as the `ValueGetter` template
/// argument in the SparqlExpression templates in `SparqlExpression.h`

namespace sparqlExpression::detail {

// An empty struct to represent a non-numeric value in a context where only
// numeric values make sense.
struct NotNumeric {};
// The input to an expression that expects a numeric value.
using NumericValue = std::variant<NotNumeric, double, int64_t>;

// Convert a numeric value (either a plain number, or the `NumericValue` variant
// from above) into an `ID`. When `NanToUndef` is `true` then floating point NaN
// values will become `Id::makeUndefined()`.
template <bool NanToUndef = false, typename T>
requires std::integral<T> || std::floating_point<T> ||
         ad_utility::isTypeAnyOf<T, Id, NotNumeric, NumericValue>
Id makeNumericId(T t) {
  if constexpr (std::integral<T>) {
    return Id::makeFromInt(t);
  } else if constexpr (std::floating_point<T> && NanToUndef) {
    return std::isnan(t) ? Id::makeUndefined() : Id::makeFromDouble(t);
  } else if constexpr (std::floating_point<T> && !NanToUndef) {
    return Id::makeFromDouble(t);
  } else if constexpr (std::same_as<NotNumeric, T>) {
    return Id::makeUndefined();
  } else if constexpr (std::same_as<NumericValue, T>) {
    return std::visit([](const auto& x) { return makeNumericId(x); }, t);
  } else {
    static_assert(std::same_as<Id, T>);
    return t;
  }
}

// Return `NumericValue` which is then used as the input to numeric expressions.
struct NumericValueGetter {
  NumericValue operator()(const string&, const EvaluationContext*) const {
    return NotNumeric{};
  }

  NumericValue operator()(ValueId id, const EvaluationContext*) const;
};

/// Return the type exactly as it was passed in.
/// This class is needed for the distinct calculation in the aggregates.
struct ActualValueGetter {
  // Simply preserve the input from numeric values
  template <typename T>
  T operator()(T v, const EvaluationContext*) const {
    return v;
  }
};

/// Returns true iff the valid is not a NULL/UNDEF value (from optional) and
/// not a nan (signalling an error in a previous calculation).
struct IsValidValueGetter {
  // check for NULL/UNDEF values.
  bool operator()(ValueId id, const EvaluationContext*) const;

  bool operator()(const string&, const EvaluationContext*) const {
    return true;
  }
};

/// Return a boolean value that is used for AND, OR and NOT expressions.
/// See section 17.2.2 of the Sparql Standard
struct EffectiveBooleanValueGetter {
  enum struct Result { False, True, Undef };
  // _________________________________________________________________________
  Result operator()(ValueId id, const EvaluationContext*) const;

  // Nonempty strings are true.
  Result operator()(std::string_view s, const EvaluationContext*) const {
    return s.empty() ? Result::False : Result::True;
  }
};

/// This class can be used as the `ValueGetter` argument of Expression
/// templates. It produces a string value.
struct StringValueGetter {
  string operator()(ValueId, const EvaluationContext*) const;

  string operator()(string s, const EvaluationContext*) const { return s; }
};

/// This class can be used as the `ValueGetter` argument of Expression
/// templates. It produces a `std::optional<DateOrLargeYear>`.
struct DateValueGetter {
  using Opt = std::optional<DateOrLargeYear>;

  Opt operator()(ValueId id, const EvaluationContext*) const {
    if (id.getDatatype() == Datatype::Date) {
      return id.getDate();
    } else {
      return std::nullopt;
    }
  }

  Opt operator()(const std::string&, const EvaluationContext*) const {
    return std::nullopt;
  }
};

// If the `id` points to a literal, return the contents of that literal (without
// the quotation marks). For all other types (IRIs, numbers, etc.) return
// `std::nullopt`. This is used for expressions that work on strings, but for
// the input of which the `STR()` function was not used in a query.
struct LiteralFromIdGetter {
  std::optional<string> operator()(ValueId id,
                                   const EvaluationContext* context) const;
};

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SPARQLEXPRESSIONVALUEGETTERS_H

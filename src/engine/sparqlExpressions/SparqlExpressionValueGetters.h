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

/// Returns a numeric value.

struct NumericValueGetter {
  // This is the current error-signalling mechanism
  double operator()(const string&, EvaluationContext*) const {
    return std::numeric_limits<double>::quiet_NaN();
  }

  // Convert an id from a result table to a double value.
  // TODO<joka921> Also convert to integer types.
  double operator()(ValueId id, EvaluationContext*) const;
};

struct NotNumeric {};
using NumericValue = std::variant<NotNumeric, double, int64_t>;
struct CorrectNumericValueGetter {
  // This is the current error-signalling mechanism
  NumericValue operator()(const string&, EvaluationContext*) const {
    return NotNumeric{};
  }

  // Convert an id from a result table to a double value.
  // TODO<joka921> Also convert to integer types.
  NumericValue operator()(ValueId id, EvaluationContext*) const;
};

/// Return the type exactly as it was passed in.
/// This class is needed for the distinct calculation in the aggregates.
struct ActualValueGetter {
  // Simply preserve the input from numeric values
  template <typename T>
  T operator()(T v, EvaluationContext*) const {
    return v;
  }
};

/// Returns true iff the valid is not a NULL/UNDEF value (from optional) and
/// not a nan (signalling an error in a previous calculation).
struct IsValidValueGetter {
  // check for NULL/UNDEF values.
  bool operator()(ValueId id, EvaluationContext*) const;

  bool operator()(const string&, EvaluationContext*) const { return true; }
};

/// Return a boolean value that is used for AND, OR and NOT expressions.
/// See section 17.2.2 of the Sparql Standard
struct EffectiveBooleanValueGetter {
  enum struct Result { False, True, Undef };
  // _________________________________________________________________________
  Result operator()(ValueId id, EvaluationContext*) const;

  // Nonempty strings are true.
  Result operator()(const string& s, EvaluationContext*) {
    return s.empty() ? Result::False : Result::True;
  }
};

/// This class can be used as the `ValueGetter` argument of Expression
/// templates. It produces a string value.
struct StringValueGetter {
  string operator()(ValueId, EvaluationContext*) const;

  string operator()(string s, EvaluationContext*) const { return s; }
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

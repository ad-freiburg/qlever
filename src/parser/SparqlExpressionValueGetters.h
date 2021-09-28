//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 23.09.21.
//

#ifndef QLEVER_SPARQLEXPRESSIONVALUEGETTERS_H
#define QLEVER_SPARQLEXPRESSIONVALUEGETTERS_H

#include "../engine/ResultTable.h"
#include "../global/Id.h"
#include "./SparqlExpressionTypes.h"

/// Several classes that can be used as the `ValueGetter` template
/// argument in the SparqlExpression templates in `SparqlExpression.h`

namespace sparqlExpression::detail {

/// Returns a numeric value.
struct NumericValueGetter {
  // Simply preserve the input from numeric values
  double operator()(double v, EvaluationContext*) const { return v; }
  int64_t operator()(int64_t v, EvaluationContext*) const { return v; }
  bool operator()(bool v, EvaluationContext*) const { return v; }

  // This is the current error-signalling mechanism
  bool operator()(const string&, EvaluationContext*) const {
    return std::numeric_limits<double>::quiet_NaN();
  }

  // Convert an id from a result table to a double value.
  // TODO<joka921> Also convert to integer types.
  double operator()(StrongId id, ResultTable::ResultType type,
                    EvaluationContext*) const;
};

/// Return the type exactly as it was passed in.
/// This class is needed for the distinct calculation in the aggregates.
struct ActualValueGetter {
  // Simply preserve the input from numeric values
  template <typename T>
  T operator()(T v, EvaluationContext*) const {
    return v;
  }

  // _________________________________________________________________________
  StrongId operator()(StrongId id, ResultTable::ResultType,
                      EvaluationContext*) const {
    return id;
  }
};

/// Returns true iff the valid is not a NULL/UNDEF value (from optional) and
/// not a nan (signalling an error in a previous calculation).
struct IsValidGetter {
  // Numeric constants are true iff they are non-zero and not nan.
  bool operator()(double v, EvaluationContext*) const { return !std::isnan(v); }
  bool operator()(int64_t, EvaluationContext*) const { return true; }
  bool operator()(bool, EvaluationContext*) const { return true; }

  // check for NULL/UNDEF values.
  bool operator()(StrongId id, ResultTable::ResultType type,
                  EvaluationContext*) const;

  bool operator()(const string&, EvaluationContext*) { return true; }
};

/// Return a boolean value that is used for AND, OR and NOT expressions.
/// See section 17.2.2 of the Sparql Standard
struct EffectiveBooleanValueGetter {
  // Numeric constants are true iff they are non-zero and not nan.
  bool operator()(double v, EvaluationContext*) const { return  v && !std::isnan(v); }
  bool operator()(int64_t v, EvaluationContext*) const { return v != 0; }
  bool operator()(bool v, EvaluationContext*) const { return v; }

  // _________________________________________________________________________
  bool operator()(StrongId id, ResultTable::ResultType type,
                  EvaluationContext*) const;

  // Nonempty strings are true.
  bool operator()(const string& s, EvaluationContext*) { return !s.empty(); }
};

/// This class can be used as the `ValueExtractor` argument of Expression
/// templates. It produces a string value.
struct StringValueGetter {
  template <typename T>
  requires(std::is_arithmetic_v<T>) string operator()(
      T v, EvaluationContext*) const {
    return std::to_string(v);
  }
  string operator()(StrongId id, ResultTable::ResultType type,
                    EvaluationContext*) const;

  string operator()(string s, EvaluationContext*) { return s; }
};
}  // namespace sparqlExpression::detail

#endif  // QLEVER_SPARQLEXPRESSIONVALUEGETTERS_H

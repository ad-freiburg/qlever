//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once
#include <re2/re2.h>

#include "engine/ResultTable.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "global/Id.h"
#include "util/TypeTraits.h"

/// Several classes that can be used as the `ValueGetter` template
/// argument in the SparqlExpression templates in `SparqlExpression.h`

namespace sparqlExpression::detail {

// An empty struct to represent a non-numeric value in a context where only
// numeric values make sense.
struct NotNumeric {};
// The input to an expression that expects a numeric value.
using NumericValue = std::variant<NotNumeric, double, int64_t>;
using IntOrDouble = std::variant<double, int64_t>;

// Convert a numeric value (either a plain number, or the `NumericValue` variant
// from above) into an `ID`. When `NanToUndef` is `true` then floating point NaN
// values will become `Id::makeUndefined()`.
template <bool NanToUndef = false, typename T>
requires std::integral<T> || std::floating_point<T> ||
         ad_utility::SimilarToAny<T, Id, NotNumeric, NumericValue>
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

  NumericValue operator()(IdOrString s, const EvaluationContext* ctx) const {
    return std::visit([this, ctx](auto el) { return operator()(el, ctx); },
                      std::move(s));
  }
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
  bool operator()(IdOrString s, const EvaluationContext* ctx) const {
    return std::visit([this, ctx](auto el) { return operator()(el, ctx); },
                      std::move(s));
  }
};

/// Return a boolean value that is used for AND, OR and NOT expressions.
/// See section 17.2.2 of the Sparql Standard
struct EffectiveBooleanValueGetter {
  enum struct Result { False, True, Undef };
  // _________________________________________________________________________
  Result operator()(ValueId id, const EvaluationContext*) const;

  // Nonempty strings are true.
  Result operator()(const std::string& s, const EvaluationContext*) const {
    return s.empty() ? Result::False : Result::True;
  }

  Result operator()(const IdOrString& s, const EvaluationContext* ctx) const {
    return std::visit(
        [this, ctx](const auto& el) { return operator()(el, ctx); }, s);
  }
};

/// This class can be used as the `ValueGetter` argument of Expression
/// templates. It produces a string value.
struct StringValueGetter {
  std::optional<string> operator()(ValueId, const EvaluationContext*) const;

  std::optional<string> operator()(string s, const EvaluationContext*) const {
    // Strip quotes
    // TODO<joka921> Use stronger types to encode literals/ IRIs/ ETC
    if (s.size() >= 2 && s.starts_with('"') && s.ends_with('"')) {
      return s.substr(1, s.size() - 2);
    }
    return s;
  }

  std::optional<string> operator()(IdOrString s,
                                   const EvaluationContext* ctx) const {
    return std::visit([this, ctx](auto el) { return operator()(el, ctx); },
                      std::move(s));
  }
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
  Opt operator()(IdOrString s, const EvaluationContext* ctx) const {
    return std::visit([this, ctx](auto el) { return operator()(el, ctx); },
                      std::move(s));
  }
};

// If the `id` points to a literal, return the contents of that literal (without
// the quotation marks). For all other types (IRIs, numbers, etc.) return
// `std::nullopt`. This is used for expressions that work on strings, but for
// the input of which the `STR()` function was not used in a query.
struct LiteralFromIdGetter {
  std::optional<string> operator()(ValueId id,
                                   const EvaluationContext* context) const;
  std::optional<string> operator()(std::string s,
                                   const EvaluationContext*) const {
    // Strip quotes
    // TODO<joka921> Use stronger types to encode literals/ IRIs/ ETC
    if (s.size() >= 2 && s.starts_with('"') && s.ends_with('"')) {
      return s.substr(1, s.size() - 2);
    }
    return s;
  }

  std::optional<string> operator()(IdOrString s,
                                   const EvaluationContext* ctx) const {
    return std::visit([this, ctx](auto el) { return operator()(el, ctx); },
                      std::move(s));
  }
};

// Convert the input into a `unique_ptr<RE2>`. Return nullptr if the input is
// not convertible to a string.
struct RegexValueGetter {
  template <SingleExpressionResult S>
  requires std::invocable<StringValueGetter, S&&, const EvaluationContext*>
  std::unique_ptr<re2::RE2> operator()(S&& input,
                                       const EvaluationContext* context) const {
    auto str = StringValueGetter{}(AD_FWD(input), context);
    if (!str.has_value()) {
      return nullptr;
    }
    return std::make_unique<re2::RE2>(str.value(), re2::RE2::Quiet);
  }
};

}  // namespace sparqlExpression::detail

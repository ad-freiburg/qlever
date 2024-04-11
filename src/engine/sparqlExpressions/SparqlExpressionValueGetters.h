// Copyright 2021 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
//
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//           Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once
#include <re2/re2.h>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/ResultTable.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "global/Id.h"
#include "util/ConstexprSmallString.h"
#include "util/TypeTraits.h"

/// Several classes that can be used as the `ValueGetter` template
/// argument in the SparqlExpression templates in `SparqlExpression.h`

namespace sparqlExpression::detail {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

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

// All the numeric value getters have an `operator()` for `ValueId` and one for
// `std::string`. This mixin adds the `operator()` for the `IdOrLiteralOrIri`
// variant via the CRTP pattern.
template <typename Self>
struct Mixin {
  decltype(auto) operator()(IdOrLiteralOrIri s,
                            const EvaluationContext* ctx) const {
    return std::visit(
        [this, ctx](auto el) {
          return static_cast<const Self*>(this)->operator()(el, ctx);
        },
        std::move(s));
  }
};

// Return `NumericValue` which is then used as the input to numeric expressions.
struct NumericValueGetter : Mixin<NumericValueGetter> {
  using Mixin<NumericValueGetter>::operator();
  NumericValue operator()(const LiteralOrIri&, const EvaluationContext*) const {
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
struct IsValidValueGetter : Mixin<IsValidValueGetter> {
  using Mixin<IsValidValueGetter>::operator();
  // check for NULL/UNDEF values.
  bool operator()(ValueId id, const EvaluationContext*) const;

  bool operator()(const LiteralOrIri&, const EvaluationContext*) const {
    return true;
  }
};

// Return a boolean value that is used for AND, OR and NOT expressions.
// See section 17.2.2 of the Sparql Standard
struct EffectiveBooleanValueGetter : Mixin<EffectiveBooleanValueGetter> {
  using Mixin<EffectiveBooleanValueGetter>::operator();
  enum struct Result { False, True, Undef };

  Result operator()(ValueId id, const EvaluationContext*) const;

  // Nonempty strings are true.
  Result operator()(const LiteralOrIri& s, const EvaluationContext*) const {
    return s.getContent().empty() ? Result::False : Result::True;
  }
};

/// This class can be used as the `ValueGetter` argument of Expression
/// templates. It produces a string value.
struct StringValueGetter : Mixin<StringValueGetter> {
  using Mixin<StringValueGetter>::operator();
  std::optional<string> operator()(ValueId, const EvaluationContext*) const;

  // TODO<joka921> probably we should return a reference or a view here.
  // TODO<joka921> use a `NormalizedStringView` inside the expressions.
  std::optional<string> operator()(const LiteralOrIri& s,
                                   const EvaluationContext*) const {
    return std::string(asStringViewUnsafe(s.getContent()));
  }
};

// Value getter for `isBlank`.
struct IsBlankNodeValueGetter : Mixin<IsBlankNodeValueGetter> {
  using Mixin<IsBlankNodeValueGetter>::operator();
  Id operator()(ValueId id, const EvaluationContext*) const {
    return Id::makeFromBool(id.getDatatype() == Datatype::BlankNodeIndex);
  }

  Id operator()(const LiteralOrIri&, const EvaluationContext*) const {
    return Id::makeFromBool(false);
  }
};

// Value getters for `isIRI`, `isBlank`, and `isLiteral`.
template <auto isSomethingFunction, auto isLiteralOrIriSomethingFunction>
struct IsSomethingValueGetter
    : Mixin<IsSomethingValueGetter<isSomethingFunction,
                                   isLiteralOrIriSomethingFunction>> {
  using Mixin<IsSomethingValueGetter>::operator();
  Id operator()(ValueId id, const EvaluationContext* context) const;

  Id operator()(const LiteralOrIri& s, const EvaluationContext*) const {
    // TODO<joka921> Use the `isLiteral` etc. functions directly as soon as the
    // local vocabulary also stores `LiteralOrIri`.
    return Id::makeFromBool(s.toStringRepresentation().starts_with(
        isLiteralOrIriSomethingFunction));
  }
};
static constexpr auto isIriPrefix = ad_utility::ConstexprSmallString<2>{"<"};
static constexpr auto isLiteralPrefix =
    ad_utility::ConstexprSmallString<2>{"\""};
using IsIriValueGetter =
    IsSomethingValueGetter<&Index::Vocab::isIri, isIriPrefix>;
using IsLiteralValueGetter =
    IsSomethingValueGetter<&Index::Vocab::isLiteral, isLiteralPrefix>;

// Value getter for `isNumeric`. Regarding which datatypes count as numeric,
// see https://www.w3.org/TR/sparql11-query/#operandDataTypes .
struct IsNumericValueGetter : Mixin<IsNumericValueGetter> {
  using Mixin<IsNumericValueGetter>::operator();
  Id operator()(ValueId id, const EvaluationContext*) const {
    Datatype datatype = id.getDatatype();
    return Id::makeFromBool(datatype == Datatype::Double ||
                            datatype == Datatype::Int);
  }
  Id operator()(const LiteralOrIri&, const EvaluationContext*) const {
    return Id::makeFromBool(false);
  }
};

/// This class can be used as the `ValueGetter` argument of Expression
/// templates. It produces a `std::optional<DateOrLargeYear>`.
struct DateValueGetter : Mixin<DateValueGetter> {
  using Mixin<DateValueGetter>::operator();
  using Opt = std::optional<DateOrLargeYear>;

  Opt operator()(ValueId id, const EvaluationContext*) const {
    if (id.getDatatype() == Datatype::Date) {
      return id.getDate();
    } else {
      return std::nullopt;
    }
  }

  Opt operator()(const LiteralOrIri&, const EvaluationContext*) const {
    return std::nullopt;
  }
};

// If the `id` points to a literal, return the contents of that literal (without
// the quotation marks). For all other types (IRIs, numbers, etc.) return
// `std::nullopt`. This is used for expressions that work on strings, but for
// the input of which the `STR()` function was not used in a query.
struct LiteralFromIdGetter : Mixin<LiteralFromIdGetter> {
  using Mixin<LiteralFromIdGetter>::operator();
  std::optional<string> operator()(ValueId id,
                                   const EvaluationContext* context) const;
  std::optional<string> operator()(const LiteralOrIri& s,
                                   const EvaluationContext*) const {
    if (s.isIri()) {
      return std::nullopt;
    }
    return std::string{asStringViewUnsafe(s.getContent())};
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

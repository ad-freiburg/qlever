// Copyright 2021 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
//
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//           Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONVALUEGETTERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONVALUEGETTERS_H

#include <re2/re2.h>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/ConstexprSmallString.h"
#include "util/LruCache.h"
#include "util/TypeTraits.h"

/// Several classes that can be used as the `ValueGetter` template
/// argument in the SparqlExpression templates in `SparqlExpression.h`

namespace sparqlExpression::detail {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
using Iri = ad_utility::triple_component::Iri;

// An empty struct to represent a non-numeric value in a context where only
// numeric values make sense.
struct NotNumeric {};
// The input to an expression that expects a numeric value.
using NumericValue = std::variant<NotNumeric, double, int64_t>;
using IntOrDouble = std::variant<double, int64_t>;

// Return type for `DatatypeValueGetter`.
using LiteralOrString =
    std::variant<std::monostate, ad_utility::triple_component::Literal,
                 std::string>;

// Used as return type for `IriValueGetter` and `DatatypeValueGetter`
using OptIri = std::optional<Iri>;

// Used in `ConvertToNumericExpression.cpp` to allow for conversion of more
// general args to a numeric value (-> `int64_t or double`).
using IntDoubleStr = std::variant<std::monostate, int64_t, double, std::string>;

// Ensures that the T value is convertible to a numeric Id.
template <typename T>
CPP_concept ValueAsNumericId =
    concepts::integral<T> || ad_utility::FloatingPoint<T> ||
    ad_utility::SimilarToAny<T, Id, NotNumeric, NumericValue>;

// Convert a numeric value (either a plain number, or the `NumericValue` variant
// from above) into an `ID`. When `NanOrInfToUndef` is `true` then floating
// point `NaN` or `+-infinity` values will become `Id::makeUndefined()`.
CPP_template(bool NanOrInfToUndef = false,
             typename T)(requires ValueAsNumericId<T>) Id makeNumericId(T t) {
  if constexpr (concepts::integral<T>) {
    return Id::makeFromInt(t);
  } else if constexpr (ad_utility::FloatingPoint<T> && NanOrInfToUndef) {
    return std::isfinite(t) ? Id::makeFromDouble(t) : Id::makeUndefined();
  } else if constexpr (ad_utility::FloatingPoint<T> && !NanOrInfToUndef) {
    return Id::makeFromDouble(t);
  } else if constexpr (concepts::same_as<NotNumeric, T>) {
    return Id::makeUndefined();
  } else if constexpr (concepts::same_as<NumericValue, T>) {
    return std::visit([](const auto& x) { return makeNumericId(x); }, t);
  } else {
    static_assert(concepts::same_as<Id, T>);
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

// This class can be used as the `ValueGetter` argument of Expression
// templates. It produces a string value.
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

// This class can be used as the `ValueGetter` argument of Expression
// templates. It implicitly applies the STR() function. In particular,
// all datatypes are removed, language tags are preserved,
// see `ExportQueryExecutionTrees::idToLiteral` for details.
struct LiteralValueGetterWithStrFunction
    : Mixin<LiteralValueGetterWithStrFunction> {
  using Mixin<LiteralValueGetterWithStrFunction>::operator();

  std::optional<ad_utility::triple_component::Literal> operator()(
      ValueId, const EvaluationContext*) const;

  std::optional<ad_utility::triple_component::Literal> operator()(
      const LiteralOrIri& s, const EvaluationContext*) const;
};

// Same as above but only literals no datatype are
// returned. This is used in the string expressions in `StringExpressions.cpp`.
struct LiteralValueGetterWithoutStrFunction
    : Mixin<LiteralValueGetterWithoutStrFunction> {
  using Mixin<LiteralValueGetterWithoutStrFunction>::operator();

  std::optional<ad_utility::triple_component::Literal> operator()(
      ValueId, const EvaluationContext*) const;

  std::optional<ad_utility::triple_component::Literal> operator()(
      const LiteralOrIri& s, const EvaluationContext*) const;
};
// Boolean value getter that checks whether the given `Id` is a `ValueId` of the
// given `datatype`.
template <Datatype datatype>
struct IsValueIdValueGetter : Mixin<IsValueIdValueGetter<datatype>> {
  using Mixin<IsValueIdValueGetter>::operator();
  Id operator()(Id id, const EvaluationContext*) const {
    return Id::makeFromBool(id.getDatatype() == datatype);
  }

  Id operator()(const LiteralOrIri&, const EvaluationContext*) const {
    return Id::makeFromBool(false);
  }
};

// Boolean value getter for `isNumeric`. Regarding which datatypes count as
// numeric, see https://www.w3.org/TR/sparql11-query/#operandDataTypes .
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

// Boolean value getters for `isIRI`, `isBlank`, and `isLiteral`.
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

// This class can be used as the `ValueGetter` argument of Expression
// templates. It produces a `std::optional<DateYearOrDuration>`.
struct DateValueGetter : Mixin<DateValueGetter> {
  using Mixin<DateValueGetter>::operator();
  using Opt = std::optional<DateYearOrDuration>;

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

/// This class can be used as the `ValueGetter` argument of Expression
/// templates. It produces a `std::optional<GeoPoint>`.
struct GeoPointValueGetter : Mixin<GeoPointValueGetter> {
  using Mixin<GeoPointValueGetter>::operator();
  using Opt = std::optional<GeoPoint>;

  Opt operator()(ValueId id, const EvaluationContext*) const {
    if (id.getDatatype() == Datatype::GeoPoint) {
      return id.getGeoPoint();
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

// Similar to `LiteralFromIdGetter`, but correctly preprocesses strings so that
// they can be used by re2 as replacement strings. So '$1 \abc \$' becomes
// '\1 \\abc $', where the former variant is valid in the SPARQL standard and
// the latter represents the format that re2 expects.
struct ReplacementStringGetter : Mixin<ReplacementStringGetter> {
  using Mixin<ReplacementStringGetter>::operator();
  std::optional<std::string> operator()(ValueId,
                                        const EvaluationContext*) const;

  std::optional<std::string> operator()(const LiteralOrIri& s,
                                        const EvaluationContext*) const;

 private:
  static std::string convertToReplacementString(std::string_view view);
};

// Convert the input into a `shared_ptr<RE2>`. Return nullptr if the input is
// not convertible to a string.
struct RegexValueGetter {
  mutable ad_utility::util::LRUCache<std::string, std::shared_ptr<RE2>> cache_{
      100};
  template <typename S>
  auto operator()(S&& input, const EvaluationContext* context) const
      -> CPP_ret(std::shared_ptr<RE2>)(
          requires SingleExpressionResult<S>&& ranges::invocable<
              LiteralFromIdGetter, S&&, const EvaluationContext*>) {
    auto str = LiteralFromIdGetter{}(AD_FWD(input), context);
    if (!str.has_value()) {
      return nullptr;
    }
    return cache_.getOrCompute(str.value(), [](const std::string& value) {
      return std::make_shared<RE2>(value, RE2::Quiet);
    });
  }
};

// `ToNumericValueGetter` returns `IntDoubleStr` a `std::variant` object which
// can contain: `int64_t`, `double`, `std::string` or `std::monostate`(empty).
struct ToNumericValueGetter : Mixin<ToNumericValueGetter> {
  using Mixin<ToNumericValueGetter>::operator();
  IntDoubleStr operator()(ValueId id, const EvaluationContext*) const;
  IntDoubleStr operator()(const LiteralOrIri& s,
                          const EvaluationContext*) const;
};

// ValueGetter for implementation of datatype() in RdfTermExpressions.cpp.
// Returns an object of type std::variant<std::monostate,
// ad_utility::triple_component::Literal, std::string> object.
struct DatatypeValueGetter : Mixin<DatatypeValueGetter> {
  using Mixin<DatatypeValueGetter>::operator();
  OptIri operator()(ValueId id, const EvaluationContext* context) const;
  OptIri operator()(const LiteralOrIri& litOrIri,
                    const EvaluationContext* context) const;
};

// `IriValueGetter` returns an
// `std::optional<ad_utility::triple_component::Iri>` object. If the
// `LiteralOrIri` object contains an `Iri`, the Iri is returned. This
// ValueGetter is currently used in `StringExpressions.cpp` within the
// implementation of `STRDT()`.
struct IriValueGetter : Mixin<IriValueGetter> {
  using Mixin<IriValueGetter>::operator();
  OptIri operator()([[maybe_unused]] ValueId id,
                    const EvaluationContext*) const {
    return std::nullopt;
  }
  OptIri operator()(const LiteralOrIri& s, const EvaluationContext*) const;
};

// `UnitOfMeasurementValueGetter` returns a `UnitOfMeasurement`.
struct UnitOfMeasurementValueGetter : Mixin<UnitOfMeasurementValueGetter> {
  mutable ad_utility::util::LRUCache<ValueId, UnitOfMeasurement> cache_{5};
  using Mixin<UnitOfMeasurementValueGetter>::operator();
  UnitOfMeasurement operator()(ValueId id, const EvaluationContext*) const;
  UnitOfMeasurement operator()(const LiteralOrIri& s,
                               const EvaluationContext*) const;

  // The actual implementation for a given `LiteralOrIri` which is guaranteed
  // not to use the `EvaluationContext`. This method can be used to convert
  // `LiteralOrIri` objects holding a unit of measurement, even when no
  // `EvaluationContext` is available. Currently used for `geof:distance` filter
  // substitution during query planning.
  static UnitOfMeasurement litOrIriToUnit(const LiteralOrIri& s);
};

// `LanguageTagValueGetter` returns an `std::optional<std::string>` object
// which contains the language tag if previously set w.r.t. given
// `Id`/`Literal`. This ValueGetter is currently used within
// `LangExpression.cpp` for the (simple) implementation of the
// `LANG()`-expression.
struct LanguageTagValueGetter : Mixin<LanguageTagValueGetter> {
  using Mixin<LanguageTagValueGetter>::operator();
  std::optional<std::string> operator()(ValueId id,
                                        const EvaluationContext* context) const;
  std::optional<std::string> operator()(
      const LiteralOrIri& litOrIri,
      [[maybe_unused]] const EvaluationContext*) const {
    if (litOrIri.isLiteral()) {
      if (litOrIri.hasLanguageTag()) {
        return std::string(asStringViewUnsafe(litOrIri.getLanguageTag()));
      }
      // If we encounter a literal without a language tag, we return an empty
      // string by the definition of the Sparql-standard.
      return "";
    } else {
      return std::nullopt;
    }
  }
};

// Value getter for implementing the expressions `IRI()`/`URI()`.
struct IriOrUriValueGetter : Mixin<IriOrUriValueGetter> {
  using Mixin<IriOrUriValueGetter>::operator();
  IdOrLiteralOrIri operator()(ValueId id,
                              const EvaluationContext* context) const;
  IdOrLiteralOrIri operator()(const LiteralOrIri& litOrIri,
                              const EvaluationContext* context) const;
};

// Value getter for `GeometryInfo` objects or parts thereof. If a `ValueId`
// holding a `VocabIndex` is given and QLever's index is built using the
// `GeoVocabulary`, the `GeometryInfo` is fetched from the precomputed file
// and the `RequestedInfo` is extracted from it. If no precomputed
// `GeometryInfo` is available, the WKT literal is parsed and only the
// `RequestedInfo` is computed ad hoc (for example the bounding box is not
// calculated, when requesting the centroid).
template <typename RequestedInfo = ad_utility::GeometryInfo>
requires ad_utility::RequestedInfoT<RequestedInfo>
struct GeometryInfoValueGetter : Mixin<GeometryInfoValueGetter<RequestedInfo>> {
  using Mixin<GeometryInfoValueGetter<RequestedInfo>>::operator();
  std::optional<RequestedInfo> operator()(
      ValueId id, const EvaluationContext* context) const;
  std::optional<RequestedInfo> operator()(
      const LiteralOrIri& litOrIri, const EvaluationContext* context) const;

  // Helper: This function returns a `GeometryInfo` object if it can be fetched
  // from a precomputation result. Otherwise `std::nullopt` is returned.
  static std::optional<ad_utility::GeometryInfo> getPrecomputedGeometryInfo(
      ValueId id, const EvaluationContext* context);
};

// Defines the return type for value-getter `StringOrDateGetter`.
using OptStringOrDate =
    std::optional<std::variant<DateYearOrDuration, std::string>>;

// This value-getter retrieves `DateYearOrDuration` or `std::string`
// (from literal) values.
struct StringOrDateGetter : Mixin<StringOrDateGetter> {
  using Mixin<StringOrDateGetter>::operator();
  // Remark: We use only LiteralFromIdGetter because Iri values should never
  // contain date-related string values.
  OptStringOrDate operator()(ValueId id,
                             const EvaluationContext* context) const {
    if (id.getDatatype() == Datatype::Date) {
      return id.getDate();
    }
    return LiteralFromIdGetter{}(id, context);
  }
  OptStringOrDate operator()(const LiteralOrIri& litOrIri,
                             const EvaluationContext* context) const {
    return LiteralFromIdGetter{}(litOrIri, context);
  }
};

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONVALUEGETTERS_H

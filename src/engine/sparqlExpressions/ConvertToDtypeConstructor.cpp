//  Copyright 2024 - 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <absl/strings/ascii.h>
#include <absl/strings/charconv.h>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

/*
  The SparqlExpressions specified in the following namespace sections
  enable datatype casting/mapping for XML-schema-datatype values.

  For more details regarding the casting/mapping definition see:
  https://www.w3.org/TR/sparql11-query/#FunctionMapping

  EXAMPLES
  (1) `xsd:dateTime(?var)` attempts to convert the date-time provided in form of
  a xsd:string value, which is bound to ?var, into an actual xsd:dateTime
  datatype value. If the conversion fails, the result is `undefined`.

  (2) `xsd:integer(?var)` attempts to convert the value bound to ?var into an
  xsd:integer. If the conversion fails, the result is `undefined`.
*/

namespace sparqlExpression {

//______________________________________________________________________________
// CONVERT TO NUMERIC
namespace detail::to_numeric {

// class that converts an input `int64_t`, `double` or `std::string`
// to a numeric value `int64_t` or `double`
CPP_template(typename T, bool AllowExponentialNotation = true)(
    requires(concepts::same_as<int64_t, T> ||
             concepts::same_as<double, T>)) class ToNumericImpl {
 private:
  ValueId getFromString(const std::string& input) const {
    auto str = absl::StripAsciiWhitespace(input);
    // Abseil and the standard library don't match leading + signs, so we skip
    // them.
    if (str.starts_with('+')) {
      str.remove_prefix(1);
    }
    auto strEnd = str.data() + str.size();
    auto strStart = str.data();
    T resT{};
    if constexpr (std::is_same_v<T, int64_t>) {
      auto conv = std::from_chars(strStart, strEnd, resT);
      if (conv.ec == std::error_code{} && conv.ptr == strEnd) {
        return Id::makeFromInt(resT);
      }
    } else {
      auto conv = absl::from_chars(strStart, strEnd, resT,
                                   AllowExponentialNotation
                                       ? absl::chars_format::general
                                       : absl::chars_format::fixed);
      if (conv.ec == std::error_code{} && conv.ptr == strEnd) {
        return Id::makeFromDouble(resT);
      }
    }
    return Id::makeUndefined();
  };

  // ___________________________________________________________________________
  template <typename N>
  auto getFromNumber(N number) const
      -> CPP_ret(Id)(requires(concepts::integral<N> ||
                              ad_utility::FloatingPoint<N>)) {
    auto resNumber = static_cast<T>(number);
    if constexpr (std::is_same_v<T, int64_t>) {
      return Id::makeFromInt(resNumber);
    } else {
      return Id::makeFromDouble(resNumber);
    }
  };

 public:
  ValueId operator()(IntDoubleStr value) const {
    if (std::holds_alternative<std::string>(value)) {
      return getFromString(std::get<std::string>(value));
    } else if (std::holds_alternative<int64_t>(value)) {
      return getFromNumber<int64_t>(std::get<int64_t>(value));
    } else if (std::holds_alternative<double>(value)) {
      return getFromNumber<double>(std::get<double>(value));
    } else {
      AD_CORRECTNESS_CHECK(std::holds_alternative<std::monostate>(value));
      return Id::makeUndefined();
    }
  }
};

using ToInteger = NARY<1, FV<ToNumericImpl<int64_t>, ToNumericValueGetter>>;
using ToDouble = NARY<1, FV<ToNumericImpl<double>, ToNumericValueGetter>>;
using ToDecimal =
    NARY<1, FV<ToNumericImpl<double, false>, ToNumericValueGetter>>;
}  // namespace detail::to_numeric

//______________________________________________________________________________
// CONVERT TO BOOLEAN
namespace detail::to_boolean {
class ToBooleanImpl {
 public:
  Id operator()(IntDoubleStr value) const {
    if (std::holds_alternative<std::string>(value)) {
      const std::string& str = std::get<std::string>(value);
      if (str == "true" || str == "1") {
        return Id::makeFromBool(true);
      }
      if (str == "false" || str == "0") {
        return Id::makeFromBool(false);
      }
      return Id::makeUndefined();
    } else if (std::holds_alternative<int64_t>(value)) {
      return Id::makeFromBool(std::get<int64_t>(value) != 0);
    } else if (std::holds_alternative<double>(value)) {
      return Id::makeFromBool(std::get<double>(value) != 0);
    } else {
      AD_CORRECTNESS_CHECK(std::holds_alternative<std::monostate>(value));
      return Id::makeUndefined();
    }
  }
};
using ToBoolean = NARY<1, FV<ToBooleanImpl, ToNumericValueGetter>>;
}  // namespace detail::to_boolean

//______________________________________________________________________________
// CONVERT TO DATE(TIME)
namespace detail::to_datetime {

// Cast to xsd:dateTime or xsd:date (ValueId)
template <bool ToJustXsdDate>
inline const auto castStringToDateTimeValueId = [](OptStringOrDate input) {
  if (!input.has_value()) return Id::makeUndefined();

  using DYD = DateYearOrDuration;
  std::optional<DYD> optValueId = std::visit(
      [&](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (ad_utility::isSimilar<T, DYD>) {
          return ToJustXsdDate ? DYD::convertToXsdDate(value)
                               : DYD::convertToXsdDatetime(value);
        } else {
          static_assert(ad_utility::isSimilar<T, std::string>);
          return ToJustXsdDate ? DYD::parseXsdDateGetOptDate(value)
                               : DYD::parseXsdDatetimeGetOptDate(value);
        }
      },
      input.value());
  return optValueId.has_value() ? Id::makeFromDate(optValueId.value())
                                : Id::makeUndefined();
};

NARY_EXPRESSION(
    ToXsdDateTime, 1,
    FV<decltype(castStringToDateTimeValueId<false>), StringOrDateGetter>);
NARY_EXPRESSION(
    ToXsdDate, 1,
    FV<decltype(castStringToDateTimeValueId<true>), StringOrDateGetter>);
}  // namespace detail::to_datetime

using namespace detail::to_numeric;
using namespace detail::to_boolean;
using namespace detail::to_datetime;
using Expr = SparqlExpression::Ptr;

Expr makeConvertToIntExpression(Expr child) {
  return std::make_unique<ToInteger>(std::move(child));
}

Expr makeConvertToDoubleExpression(Expr child) {
  return std::make_unique<ToDouble>(std::move(child));
}

Expr makeConvertToDecimalExpression(Expr child) {
  return std::make_unique<ToDecimal>(std::move(child));
}

Expr makeConvertToBooleanExpression(Expr child) {
  return std::make_unique<ToBoolean>(std::move(child));
}

Expr makeConvertToDateTimeExpression(Expr child) {
  return std::make_unique<ToXsdDateTime>(std::move(child));
}

Expr makeConvertToDateExpression(Expr child) {
  return std::make_unique<ToXsdDate>(std::move(child));
}

}  // namespace sparqlExpression

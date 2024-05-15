//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail::to_numeric {

// class that converts an input `int64_t`, `double` or `std::string`
// to a numeric value `int64_t` or `double`
template <typename T>
requires std::same_as<int64_t, T> || std::same_as<double, T>
class ToNumericImpl {
 private:
  Id getFromString(const std::string& input) const {
    auto str = absl::StripAsciiWhitespace(input);
    auto strEnd = str.data() + str.size();
    auto strStart = str.data();
    T resT{};
    if constexpr (std::is_same_v<T, int64_t>) {
      auto conv = std::from_chars(strStart, strEnd, resT);
      if (conv.ec == std::error_code{} && conv.ptr == strEnd) {
        return Id::makeFromInt(resT);
      }
    } else {
      auto conv = absl::from_chars(strStart, strEnd, resT);
      if (conv.ec == std::error_code{} && conv.ptr == strEnd) {
        return Id::makeFromDouble(resT);
      }
    }
    return Id::makeUndefined();
  };

  // ___________________________________________________________________________
  template <typename N>
  requires std::integral<N> || std::floating_point<N>
  Id getFromNumber(N number) const {
    auto resNumber = static_cast<T>(number);
    if constexpr (std::is_same_v<T, int64_t>) {
      return Id::makeFromInt(resNumber);
    } else {
      return Id::makeFromDouble(resNumber);
    }
  };

 public:
  Id operator()(IntDoubleStr value) const {
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
}  // namespace detail::to_numeric

using namespace detail::to_numeric;
using Expr = SparqlExpression::Ptr;

Expr makeConvertToIntExpression(Expr child) {
  return std::make_unique<ToInteger>(std::move(child));
}

Expr makeConvertToDoubleExpression(Expr child) {
  return std::make_unique<ToDouble>(std::move(child));
}
}  // namespace sparqlExpression

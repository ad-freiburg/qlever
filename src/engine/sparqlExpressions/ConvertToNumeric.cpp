// Copyright 2024

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail::to_numeric {

// class that converts given intput to integer or double
template <typename T>
class ToNumericImpl {
 private:
  Id getIdFromString(std::optional<std::string>& input) const {
    auto str = absl::StripAsciiWhitespace(input.value());
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

  Id getIdFromNum(auto value) {
    if (std::is_same_v<decltype(value), int> ||
        std::is_same_v<decltype(value), double>) {
      auto res = static_cast<T>(value);
      if constexpr (std::is_same_v<T, int64_t>) {
        return Id::makeFromInt(res);
      } else {
        return Id::makeFromDouble(res);
      }
    }
    return Id::makeUndefined();
  };

 public:
  Id operator()(std::optional<std::string> input) const {
    if (!input.has_value()) {
      return Id::makeUndefined();
    } else {
      return getIdFromString(input);
    }
  }

  Id operator()(double value) { return getIdFromNum(value); }

  Id operator()(int value) { return getIdFromNum(value); }
};

using ToInteger = NARY<1, FV<ToNumericImpl<int64_t>, StringValueGetter>>;
using ToDouble = NARY<1, FV<ToNumericImpl<double>, StringValueGetter>>;
}  // namespace detail::to_numeric

using namespace detail::to_numeric;
using Expr = SparqlExpression::Ptr;

Expr makeIntExpression(Expr child) {
  return std::make_unique<ToInteger>(std::move(child));
}

Expr makeDoubleExpression(Expr child) {
  return std::make_unique<ToDouble>(std::move(child));
}
}  // namespace sparqlExpression

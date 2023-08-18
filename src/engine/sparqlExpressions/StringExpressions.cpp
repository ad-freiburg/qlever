//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
namespace sparqlExpression {
namespace detail {
// String functions.
[[maybe_unused]] auto strImpl = [](std::optional<std::string> s) {
  return IdOrString{std::move(s)};
};
NARY_EXPRESSION(StrExpressionImpl, 1, FV<decltype(strImpl), StringValueGetter>);

class StrExpression : public StrExpressionImpl {
  using StrExpressionImpl::StrExpressionImpl;
  bool isStrExpression() const override { return true; }
};

// Template for an expression that works on string literals. The arguments are
// the same as those to `NaryExpression` with the difference that the value
// getter is deduced automatically. If the child of the expression is the
// `STR()` expression, then the `StringValueGetter` will be used (which also
// returns string values for IRIs, numeric literals, etc.), otherwise the
// `LiteralFromIdGetter` is used (which returns `std::nullopt` for these cases).
template <size_t N, typename Function,
          typename... AdditionalNonStringValueGetters>
class StringExpressionImpl : public SparqlExpression {
 private:
  using ExpressionWithStr =
      NARY<N,
           FV<Function, StringValueGetter, AdditionalNonStringValueGetters...>>;
  using ExpressionWithoutStr = NARY<
      N, FV<Function, LiteralFromIdGetter, AdditionalNonStringValueGetters...>>;

  SparqlExpression::Ptr impl_;

 public:
  explicit StringExpressionImpl(
      SparqlExpression::Ptr child,
      std::same_as<SparqlExpression::Ptr> auto... children)
      requires(sizeof...(children) + 1 == N) {
    AD_CORRECTNESS_CHECK(child != nullptr);
    if (child->isStrExpression()) {
      auto childrenOfStr = std::move(*child).moveChildrenOut();
      AD_CORRECTNESS_CHECK(childrenOfStr.size() == 1);
      impl_ = std::make_unique<ExpressionWithStr>(
          std::move(childrenOfStr.at(0)), std::move(children)...);
    } else {
      impl_ = std::make_unique<ExpressionWithoutStr>(std::move(child),
                                                     std::move(children)...);
    }
  }

  ExpressionResult evaluate(EvaluationContext* context) const override {
    return impl_->evaluate(context);
  }
  std::string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return impl_->getCacheKey(varColMap);
  }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override {
    return impl_->children();
  }
};

// STRLEN
inline auto strlen = [](std::optional<std::string> s) {
  if (!s.has_value()) {
    return Id::makeUndefined();
  }
  return Id::makeFromInt(static_cast<int64_t>(s.value().size()));
};
using StrlenExpression = StringExpressionImpl<1, decltype(strlen)>;

// SUBSTR
inline auto substr = [](std::optional<std::string> s, NumericValue start,
                        NumericValue length) -> IdOrString {
  if (!s.has_value() || std::holds_alternative<NotNumeric>(start) ||
      std::holds_alternative<NotNumeric>(length)) {
    return Id::makeUndefined();
  }

  auto isNan = [](NumericValue n) {
    auto ptr = std::get_if<double>(&n);
    return ptr != nullptr && std::isnan(*ptr);
  };

  if (isNan(start) || isNan(length)) {
    return std::string{};
  }

  auto roundImpl = []<typename T>(const T& value) -> int64_t {
    if constexpr (std::floating_point<T>) {
      if (value < 0) {
        return static_cast<int64_t>(-std::round(-value));
      } else {
        return static_cast<int64_t>(std::round(value));
      }
    } else if constexpr (std::integral<T>) {
      return static_cast<int64_t>(value);
    } else {
      AD_FAIL();
    }
  };

  int64_t startInt = std::visit(roundImpl, start) - 1;
  int64_t lengthInt = std::visit(roundImpl, length);

  // If the starting position is negative, we have to correct the length.
  if (startInt < 0) {
    lengthInt += startInt;
  }

  const auto& str = s.value();
  auto clamp = [sz = str.size()](int64_t n) -> std::size_t {
    if (n < 0) {
      return 0;
    }
    if (static_cast<size_t>(n) > sz) {
      return sz;
    }
    return static_cast<size_t>(n);
  };

  return std::string{
      ad_utility::getUTF8Substring(str, clamp(startInt), clamp(lengthInt))};
};

using SubstrExpression =
    StringExpressionImpl<3, decltype(substr), NumericValueGetter,
                         NumericValueGetter>;

}  // namespace detail
using namespace detail;
SparqlExpression::Ptr makeStrExpression(SparqlExpression::Ptr child) {
  return std::make_unique<StrExpression>(std::move(child));
}
SparqlExpression::Ptr makeStrlenExpression(SparqlExpression::Ptr child) {
  return std::make_unique<StrlenExpression>(std::move(child));
}

SparqlExpression::Ptr makeSubstrExpression(SparqlExpression::Ptr string,
                                           SparqlExpression::Ptr start,
                                           SparqlExpression::Ptr length) {
  return std::make_unique<SubstrExpression>(std::move(string), std::move(start),
                                            std::move(length));
}
}  // namespace sparqlExpression

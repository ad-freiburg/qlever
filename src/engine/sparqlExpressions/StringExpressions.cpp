//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
namespace sparqlExpression {
namespace detail::string_expressions {
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

// Lift a `Function` that takes one or multiple `std::string`s (possibly via
// references) and returns an `Id` or `std::string` to a function that takes the
// same number of `std::optional<std::string>` and returns `Id` or `IdOrString`.
// If any of the optionals is `std::nullopt`, then UNDEF is returned, else the
// result of the `Function` with the values of the optionals. This is a useful
// helper function for implementing expressions that work on strings.
template <typename Function>
struct LiftStringFunction {
  template <std::same_as<std::optional<std::string>>... Arguments>
  auto operator()(Arguments... arguments) const {
    using ResultOfFunction =
        decltype(std::invoke(Function{}, std::move(arguments.value())...));
    static_assert(std::same_as<ResultOfFunction, Id> ||
                      std::same_as<ResultOfFunction, std::string>,
                  "Template argument of `LiftStringFunction` must return `Id` "
                  "or `std::string`");
    using Result =
        std::conditional_t<ad_utility::isSimilar<ResultOfFunction, Id>, Id,
                           IdOrString>;
    if ((... || !arguments.has_value())) {
      return Result{Id::makeUndefined()};
    }
    return Result{std::invoke(Function{}, std::move(arguments.value())...)};
  }
};

// STRLEN
[[maybe_unused]] auto strlen = [](std::string_view s) {
  return Id::makeFromInt(static_cast<int64_t>(s.size()));
};

using StrlenExpression =
    StringExpressionImpl<1, LiftStringFunction<decltype(strlen)>>;

// LCASE
[[maybe_unused]] auto lowercaseImpl =
    [](std::optional<std::string> input) -> IdOrString {
  if (!input.has_value()) {
    return Id::makeUndefined();
  } else {
    return ad_utility::utf8ToLower(input.value());
  }
};
using LowercaseExpression = StringExpressionImpl<1, decltype(lowercaseImpl)>;

// UCASE
[[maybe_unused]] auto uppercaseImpl =
    [](std::optional<std::string> input) -> IdOrString {
  if (!input.has_value()) {
    return Id::makeUndefined();
  } else {
    return ad_utility::utf8ToUpper(input.value());
  }
};
using UppercaseExpression = StringExpressionImpl<1, decltype(uppercaseImpl)>;

// SUBSTR
class SubstrImpl {
 private:
  static bool isNan(NumericValue n) {
    auto ptr = std::get_if<double>(&n);
    return ptr != nullptr && std::isnan(*ptr);
  };

  // Round an integer or floating point to the nearest integer according to the
  // SPARQL standard. This means that -1.5 is rounded to -1.
  static constexpr auto round = []<typename T>(const T& value) -> int64_t {
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

 public:
  IdOrString operator()(std::optional<std::string> s, NumericValue start,
                        NumericValue length) const {
    if (!s.has_value() || std::holds_alternative<NotNumeric>(start) ||
        std::holds_alternative<NotNumeric>(length)) {
      return Id::makeUndefined();
    }

    if (isNan(start) || isNan(length)) {
      return std::string{};
    }

    // In SPARQL indices are 1-based, but the implemenetation we use is 0 based,
    // hence the `- 1`.
    int64_t startInt = std::visit(round, start) - 1;
    int64_t lengthInt = std::visit(round, length);

    // If the starting position is negative, we have to correct the length.
    if (startInt < 0) {
      lengthInt += startInt;
    }

    const auto& str = s.value();
    // Clamp the number such that it is in `[0, str.size()]`. That way we end up
    // with valid arguments for the `getUTF8Substring` method below for both
    // starting position and length since all the other corner cases have been
    // dealt with above.
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
  }
};

using SubstrExpression =
    StringExpressionImpl<3, SubstrImpl, NumericValueGetter, NumericValueGetter>;

// STRSTARTS
[[maybe_unused]] auto strStartsImpl = [](std::string_view text,
                                         std::string_view pattern) -> Id {
  return Id::makeFromBool(text.starts_with(pattern));
};

using StrStartsExpression =
    StringExpressionImpl<2, LiftStringFunction<decltype(strStartsImpl)>,
                         StringValueGetter>;

// STRENDS
[[maybe_unused]] auto strEndsImpl = [](std::string_view text,
                                       std::string_view pattern) {
  return Id::makeFromBool(text.ends_with(pattern));
};

using StrEndsExpression =
    StringExpressionImpl<2, LiftStringFunction<decltype(strEndsImpl)>,
                         StringValueGetter>;

// STRCONTAINS
[[maybe_unused]] auto containsImpl = [](std::string_view text,
                                        std::string_view pattern) {
  return Id::makeFromBool(text.find(pattern) != std::string::npos);
};

using ContainsExpression =
    StringExpressionImpl<2, LiftStringFunction<decltype(containsImpl)>,
                         StringValueGetter>;

// STRAFTER / STRBEFORE
template <bool isStrAfter>
[[maybe_unused]] auto strAfterOrBeforeImpl =
    [](std::string text, std::string_view pattern) -> std::string {
  // Required by the SPARQL standard.
  if (pattern.empty()) {
    return text;
  }
  auto pos = text.find(pattern);
  if (pos >= text.size()) {
    return "";
  }
  if constexpr (isStrAfter) {
    return text.substr(pos + pattern.size());
  } else {
    // STRBEFORE
    return text.substr(0, pos);
  }
};

auto strAfter = strAfterOrBeforeImpl<true>;

using StrAfterExpression =
    StringExpressionImpl<2, LiftStringFunction<decltype(strAfter)>,
                         StringValueGetter>;

auto strBefore = strAfterOrBeforeImpl<false>;
using StrBeforeExpression =
    StringExpressionImpl<2, LiftStringFunction<decltype(strBefore)>,
                         StringValueGetter>;

}  // namespace detail::string_expressions
using namespace detail::string_expressions;
using std::make_unique;
using std::move;
using Expr = SparqlExpression::Ptr;

template <typename T>
Expr make(std::same_as<Expr> auto&... children) {
  return std::make_unique<T>(std::move(children)...);
}
Expr makeStrExpression(Expr child) { return make<StrExpression>(child); }
Expr makeStrlenExpression(Expr child) { return make<StrlenExpression>(child); }

Expr makeSubstrExpression(Expr string, Expr start, Expr length) {
  return make<SubstrExpression>(string, start, length);
}

Expr makeStrStartsExpression(Expr child1, Expr child2) {
  return make<StrStartsExpression>(child1, child2);
}

Expr makeLowercaseExpression(Expr child) {
  return make<LowercaseExpression>(child);
}

Expr makeUppercaseExpression(Expr child) {
  return make<UppercaseExpression>(child);
}

Expr makeStrEndsExpression(Expr child1, Expr child2) {
  return make<StrEndsExpression>(child1, child2);
}
Expr makeStrAfterExpression(Expr child1, Expr child2) {
  return make<StrAfterExpression>(child1, child2);
}
Expr makeStrBeforeExpression(Expr child1, Expr child2) {
  return make<StrBeforeExpression>(child1, child2);
}
Expr makeContainsExpression(Expr child1, Expr child2) {
  return make<ContainsExpression>(child1, child2);
}
}  // namespace sparqlExpression

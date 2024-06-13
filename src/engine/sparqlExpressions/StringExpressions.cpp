//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <boost/url.hpp>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/VariadicExpression.h"

namespace sparqlExpression {
namespace detail::string_expressions {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

// Convert a `string_view` to a `LiteralOrIri` that stores a `Literal`.
// Note: This currently requires a copy of a string since the `Literal` class
// has to add the quotation marks.
constexpr auto toLiteral = [](std::string_view normalizedContent) {
  return LiteralOrIri{
      ad_utility::triple_component::Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(normalizedContent))};
};

// String functions.
[[maybe_unused]] auto strImpl =
    [](std::optional<std::string> s) -> IdOrLiteralOrIri {
  if (s.has_value()) {
    return IdOrLiteralOrIri{toLiteral(s.value())};
  } else {
    return Id::makeUndefined();
  }
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
// same number of `std::optional<std::string>` and returns `Id` or
// `IdOrLiteralOrIri`. If any of the optionals is `std::nullopt`, then UNDEF is
// returned, else the result of the `Function` with the values of the optionals.
// This is a useful helper function for implementing expressions that work on
// strings.
template <typename Function>
struct LiftStringFunction {
  template <std::same_as<std::optional<std::string>>... Arguments>
  auto operator()(Arguments... arguments) const {
    using ResultOfFunction =
        decltype(std::invoke(Function{}, std::move(arguments.value())...));
    static_assert(std::same_as<ResultOfFunction, Id> ||
                      std::same_as<ResultOfFunction, LiteralOrIri>,
                  "Template argument of `LiftStringFunction` must return `Id` "
                  "or `std::string`");
    using Result =
        std::conditional_t<ad_utility::isSimilar<ResultOfFunction, Id>, Id,
                           IdOrLiteralOrIri>;
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
    [](std::optional<std::string> input) -> IdOrLiteralOrIri {
  if (!input.has_value()) {
    return Id::makeUndefined();
  } else {
    return toLiteral(ad_utility::utf8ToLower(input.value()));
  }
};
using LowercaseExpression = StringExpressionImpl<1, decltype(lowercaseImpl)>;

// UCASE
[[maybe_unused]] auto uppercaseImpl =
    [](std::optional<std::string> input) -> IdOrLiteralOrIri {
  if (!input.has_value()) {
    return Id::makeUndefined();
  } else {
    return toLiteral(ad_utility::utf8ToUpper(input.value()));
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
  IdOrLiteralOrIri operator()(std::optional<std::string> s, NumericValue start,
                              NumericValue length) const {
    if (!s.has_value() || std::holds_alternative<NotNumeric>(start) ||
        std::holds_alternative<NotNumeric>(length)) {
      return Id::makeUndefined();
    }

    if (isNan(start) || isNan(length)) {
      return toLiteral("");
    }

    // In SPARQL indices are 1-based, but the implementation we use is 0 based,
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

    return toLiteral(
        ad_utility::getUTF8Substring(str, clamp(startInt), clamp(lengthInt)));
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
    [](std::string_view text, std::string_view pattern) {
      // Required by the SPARQL standard.
      if (pattern.empty()) {
        return toLiteral(text);
      }
      auto pos = text.find(pattern);
      if (pos >= text.size()) {
        return toLiteral("");
      }
      if constexpr (isStrAfter) {
        return toLiteral(text.substr(pos + pattern.size()));
      } else {
        // STRBEFORE
        return toLiteral(text.substr(0, pos));
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

[[maybe_unused]] auto replaceImpl =
    [](std::optional<std::string> input,
       const std::unique_ptr<re2::RE2>& pattern,
       const std::optional<std::string>& replacement) -> IdOrLiteralOrIri {
  if (!input.has_value() || !pattern || !replacement.has_value()) {
    return Id::makeUndefined();
  }
  auto& in = input.value();
  const auto& pat = *pattern;
  // Check for invalid regexes.
  if (!pat.ok()) {
    return Id::makeUndefined();
  }
  const auto& repl = replacement.value();
  re2::RE2::GlobalReplace(&in, pat, repl);
  return toLiteral(in);
};

using ReplaceExpression =
    StringExpressionImpl<3, decltype(replaceImpl), RegexValueGetter,
                         StringValueGetter>;

// CONCAT
class ConcatExpression : public detail::VariadicExpression {
 public:
  using VariadicExpression::VariadicExpression;

  // _________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* ctx) const override {
    using StringVec = VectorWithMemoryLimit<std::string>;
    // We evaluate one child after the other and append the strings from child i
    // to the strings already constructed for children 0, …, i - 1. The
    // seemingly more natural row-by-row approach has two problems. First, the
    // distinction between children with constant results and vector results
    // would not be cache-efficient. Second, when the evaluation is part of
    // GROUP BY, we don't have the information in advance whether all children
    // have constant results (in which case, we need to evaluate the whole
    // expression only once).

    // We store the (intermediate) result either as single string or a vector.
    // If the result is a string, then all the previously evaluated children
    // were constants (see above).
    std::variant<std::string, StringVec> result{std::string{""}};
    auto visitSingleExpressionResult =
        [&ctx, &result ]<SingleExpressionResult T>(T && s)
            requires std::is_rvalue_reference_v<T&&> {
      if constexpr (isConstantResult<T>) {
        std::string strFromConstant = StringValueGetter{}(s, ctx).value_or("");
        if (std::holds_alternative<std::string>(result)) {
          // All previous children were constants, and the current child also is
          // a constant.
          std::get<std::string>(result).append(strFromConstant);
        } else {
          // One of the previous children was not a constant, so we already
          // store a vector.
          auto& resultAsVector = std::get<StringVec>(result);
          std::ranges::for_each(resultAsVector, [&](std::string& target) {
            target.append(strFromConstant);
          });
        }
      } else {
        auto gen = sparqlExpression::detail::makeGenerator(AD_FWD(s),
                                                           ctx->size(), ctx);

        if (std::holds_alternative<std::string>(result)) {
          // All previous children were constants, but now we have a
          // non-constant child, so we have to expand the `result` from a single
          // string to a vector.
          std::string constantResultSoFar =
              std::move(std::get<std::string>(result));
          result.emplace<StringVec>(ctx->_allocator);
          auto& resultAsVec = std::get<StringVec>(result);
          resultAsVec.reserve(ctx->size());
          std::fill_n(std::back_inserter(resultAsVec), ctx->size(),
                      constantResultSoFar);
        }

        // The `result` already is a vector, and the current child also returns
        // multiple results, so we do the `natural` way.
        auto& resultAsVec = std::get<StringVec>(result);
        // TODO<C++23> Use `std::views::zip` or `enumerate`.
        size_t i = 0;
        for (auto& el : gen) {
          if (auto str = StringValueGetter{}(std::move(el), ctx);
              str.has_value()) {
            resultAsVec[i].append(str.value());
          }
          ctx->cancellationHandle_->throwIfCancelled();
          ++i;
        }
      }
      ctx->cancellationHandle_->throwIfCancelled();
    };
    std::ranges::for_each(
        childrenVec(), [&ctx, &visitSingleExpressionResult](const auto& child) {
          std::visit(visitSingleExpressionResult, child->evaluate(ctx));
        });

    // Lift the result from `string` to `IdOrLiteralOrIri` which is needed for
    // the expression module.
    if (std::holds_alternative<std::string>(result)) {
      return IdOrLiteralOrIri{toLiteral(std::get<std::string>(result))};
    } else {
      auto& stringVec = std::get<StringVec>(result);
      VectorWithMemoryLimit<IdOrLiteralOrIri> resultAsVec(ctx->_allocator);
      resultAsVec.reserve(stringVec.size());
      std::ranges::copy(stringVec | std::views::transform(toLiteral),
                        std::back_inserter(resultAsVec));
      return resultAsVec;
    }
  }
};

// ENCODE_FOR_URI
[[maybe_unused]] auto encodeForUriImpl =
    [](std::optional<std::string> input) -> IdOrLiteralOrIri {
  if (!input.has_value()) {
    return Id::makeUndefined();
  } else {
    std::string_view value{input.value()};

    return toLiteral(boost::urls::encode(value, boost::urls::unreserved_chars));
  }
};
using EncodeForUriExpression =
    StringExpressionImpl<1, decltype(encodeForUriImpl)>;

// STRING WITH LANGUAGE TAG
[[maybe_unused]] inline auto strLangTag =
    [](std::optional<std::string> input,
       std::optional<std::string> langTag) -> IdOrLiteralOrIri {
  if (!input.has_value() || !langTag.has_value()) {
    return Id::makeUndefined();
  } else if (!ad_utility::strIsLangTag(langTag.value())) {
    return Id::makeUndefined();
  } else {
    auto lit =
        ad_utility::triple_component::Literal::literalWithNormalizedContent(
            asNormalizedStringViewUnsafe(input.value()),
            std::move(langTag.value()));
    return LiteralOrIri{lit};
  }
};

using StrLangTagged = StringExpressionImpl<2, decltype(strLangTag)>;

// STRING WITH DATATYPE IRI
[[maybe_unused]] inline auto strIriDtTag =
    [](std::optional<std::string> inputStr,
       OptIri inputIri) -> IdOrLiteralOrIri {
  if (!inputStr.has_value() || !inputIri.has_value()) {
    return Id::makeUndefined();
  } else {
    auto lit =
        ad_utility::triple_component::Literal::literalWithNormalizedContent(
            asNormalizedStringViewUnsafe(inputStr.value()), inputIri.value());
    return LiteralOrIri{lit};
  }
};

using StrIriTagged =
    StringExpressionImpl<2, decltype(strIriDtTag), IriValueGetter>;

// HASH
template <auto HashFunc>
[[maybe_unused]] inline constexpr auto hash =
    [](std::optional<std::string> input) -> IdOrLiteralOrIri {
  if (!input.has_value()) {
    return Id::makeUndefined();
  } else {
    std::vector<unsigned char> hashed = HashFunc(input.value());
    auto hexStr = absl::StrJoin(hashed, "", ad_utility::hexFormatter);
    return toLiteral(std::move(hexStr));
  }
};

using MD5Expression =
    StringExpressionImpl<1, decltype(hash<ad_utility::hashMd5>)>;
using SHA1Expression =
    StringExpressionImpl<1, decltype(hash<ad_utility::hashSha1>)>;
using SHA256Expression =
    StringExpressionImpl<1, decltype(hash<ad_utility::hashSha256>)>;
using SHA384Expression =
    StringExpressionImpl<1, decltype(hash<ad_utility::hashSha384>)>;
using SHA512Expression =
    StringExpressionImpl<1, decltype(hash<ad_utility::hashSha512>)>;

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

Expr makeReplaceExpression(Expr input, Expr pattern, Expr repl) {
  return make<ReplaceExpression>(input, pattern, repl);
}
Expr makeContainsExpression(Expr child1, Expr child2) {
  return make<ContainsExpression>(child1, child2);
}
Expr makeConcatExpression(std::vector<Expr> children) {
  return std::make_unique<ConcatExpression>(std::move(children));
}

Expr makeEncodeForUriExpression(Expr child) {
  return make<EncodeForUriExpression>(child);
}

Expr makeStrLangTagExpression(Expr child1, Expr child2) {
  return make<StrLangTagged>(child1, child2);
}

Expr makeStrIriDtExpression(Expr child1, Expr child2) {
  return make<StrIriTagged>(child1, child2);
}

Expr makeMD5Expression(Expr child) { return make<MD5Expression>(child); }

Expr makeSHA1Expression(Expr child) { return make<SHA1Expression>(child); }

Expr makeSHA256Expression(Expr child) { return make<SHA256Expression>(child); }

Expr makeSHA384Expression(Expr child) { return make<SHA384Expression>(child); }

Expr makeSHA512Expression(Expr child) { return make<SHA512Expression>(child); }

}  // namespace sparqlExpression

//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <boost/url.hpp>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/StringExpressionsHelper.h"
#include "engine/sparqlExpressions/VariadicExpression.h"

namespace sparqlExpression {
namespace detail::string_expressions {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

// Convert a `string_view` to a `LiteralOrIri` that stores a `Literal`.
// Note: This currently requires a copy of a string since the `Literal` class
// has to add the quotation marks.
struct ToLiteral {
  LiteralOrIri operator()(std::string_view normalizedContent,
                          const std::optional<std::variant<Iri, std::string>>&
                              descriptor = std::nullopt) const {
    return LiteralOrIri{
        ad_utility::triple_component::Literal::literalWithNormalizedContent(
            asNormalizedStringViewUnsafe(normalizedContent), descriptor)};
  }
};
static constexpr ToLiteral toLiteral{};

// Return `true` if the byte representation of `c` does not start with `10`,
// meaning that it is not a UTF-8 continuation byte, and therefore the start of
// a codepoint.
static constexpr bool isUtf8CodepointStart(char c) {
  using b = std::byte;
  return (static_cast<b>(c) & b{0xC0}) != b{0x80};
}
// Count UTF-8 characters by skipping continuation bytes (those starting with
// "10").
static std::size_t utf8Length(std::string_view s) {
  return ql::ranges::count_if(s, &isUtf8CodepointStart);
}

// Initialize or append a Literal. If both literals are valid and initialized,
// concatenate nextLiteral into literalSoFar. If not initialized yet, set
// literalSoFar to nextLiteral. If either is UNDEF, set literalSoFar to nullopt
// to indicate an undefined result.
void concatOrSetLiteral(
    std::optional<ad_utility::triple_component::Literal>& literalSoFarOpt,
    const std::optional<ad_utility::triple_component::Literal>& nextLiteral,
    const bool isFirstLiteral) {
  if (!nextLiteral.has_value() || !literalSoFarOpt.has_value()) {
    literalSoFarOpt = std::nullopt;  // UNDEF
  } else if (isFirstLiteral) {
    literalSoFarOpt = nextLiteral.value();
  } else {
    literalSoFarOpt.value().concat(nextLiteral.value());
  }
}

// Convert UTF-8 position to byte offset. If utf8Pos exceeds the
// string length, the byte offset will be set to the size of the string.
static std::size_t utf8ToByteOffset(std::string_view str, int64_t utf8Pos) {
  int64_t charCount = 0;
  for (auto [byteOffset, c] : ranges::views::enumerate(str)) {
    if (isUtf8CodepointStart(c)) {
      if (charCount == utf8Pos) {
        return byteOffset;
      }
      ++charCount;
    }
  }
  return str.size();
}

// String functions.
struct StrImpl {
  IdOrLiteralOrIri operator()(std::optional<std::string> s) const {
    if (s.has_value()) {
      return IdOrLiteralOrIri{toLiteral(s.value())};
    } else {
      return Id::makeUndefined();
    }
  }
};
NARY_EXPRESSION(StrExpressionImpl, 1, FV<StrImpl, StringValueGetter>);

class StrExpression : public StrExpressionImpl {
  using StrExpressionImpl::StrExpressionImpl;
  bool isStrExpression() const override { return true; }
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
  CPP_template(typename... Arguments)(requires(
      concepts::same_as<Arguments, std::optional<std::string>>&&...)) auto
  operator()(Arguments... arguments) const {
    using ResultOfFunction =
        decltype(std::invoke(Function{}, std::move(arguments.value())...));
    static_assert(concepts::same_as<ResultOfFunction, Id> ||
                      concepts::same_as<ResultOfFunction, LiteralOrIri>,
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

// IRI or URI
//
// 1. What's the correct behavior for non-strings, like `1` or `true`?
//
// @1: Only a `LiteralOrIri` or an `Id` from `Vocab`/`LocalVocab` is in
// consideration within the `IriOrUriValueGetter`, hence automatically
// ignores values like `1`, `true`, `Date` etc.

const Iri& extractIri(const IdOrLiteralOrIri& litOrIri) {
  AD_CORRECTNESS_CHECK(std::holds_alternative<LocalVocabEntry>(litOrIri));
  const auto& baseIriOrUri = std::get<LocalVocabEntry>(litOrIri);
  AD_CORRECTNESS_CHECK(baseIriOrUri.isIri());
  return baseIriOrUri.getIri();
}

struct ApplyBaseIfPresent {
  IdOrLiteralOrIri operator()(IdOrLiteralOrIri iri,
                              const IdOrLiteralOrIri& base) const {
    if (std::holds_alternative<Id>(iri)) {
      AD_CORRECTNESS_CHECK(std::get<Id>(iri).isUndefined());
      return iri;
    }
    const auto& baseIri = extractIri(base);
    if (baseIri.empty()) {
      return iri;
    }
    // TODO<RobinTF> Avoid unnecessary string copies because of conversion.
    return LiteralOrIri{Iri::fromIrirefConsiderBase(
        extractIri(iri).toStringRepresentation(), baseIri.getBaseIri(false),
        baseIri.getBaseIri(true))};
  }
};
using IriOrUriExpression = NARY<2, FV<ApplyBaseIfPresent, IriOrUriValueGetter>>;

// STRLEN
struct Strlen {
  Id operator()(std::string_view s) const {
    return Id::makeFromInt(utf8Length(s));
  }
};
using StrlenExpression = StringExpressionImpl<1, LiftStringFunction<Strlen>>;

// UCase and LCase
template <auto toLowerOrToUpper>
auto upperOrLowerCaseImpl =
    [](std::optional<ad_utility::triple_component::Literal> input)
    -> IdOrLiteralOrIri {
  if (!input.has_value()) {
    return Id::makeUndefined();
  }
  auto& literal = input.value();
  auto newContent =
      std::invoke(toLowerOrToUpper, asStringViewUnsafe(literal.getContent()));
  literal.replaceContent(newContent);
  return LiteralOrIri(std::move(literal));
};
auto uppercaseImpl = upperOrLowerCaseImpl<&ad_utility::utf8ToUpper>;
auto lowercaseImpl = upperOrLowerCaseImpl<&ad_utility::utf8ToLower>;

using UppercaseExpression = LiteralExpressionImpl<1, decltype(uppercaseImpl)>;
using LowercaseExpression = LiteralExpressionImpl<1, decltype(lowercaseImpl)>;

// SUBSTR
class SubstrImpl {
 private:
  static bool isNan(NumericValue n) {
    auto ptr = std::get_if<double>(&n);
    return ptr != nullptr && std::isnan(*ptr);
  };

  // Round an integer or floating point to the nearest integer according to the
  // SPARQL standard. This means that -1.5 is rounded to -1.
  static constexpr auto round = [](const auto& value) -> int64_t {
    using T = std::decay_t<decltype(value)>;
    if constexpr (ql::concepts::floating_point<T>) {
      if (value < 0) {
        return static_cast<int64_t>(-std::round(-value));
      } else {
        return static_cast<int64_t>(std::round(value));
      }
    } else if constexpr (concepts::integral<T>) {
      return static_cast<int64_t>(value);
    } else {
      AD_FAIL();
    }
  };

 public:
  IdOrLiteralOrIri operator()(
      std::optional<ad_utility::triple_component::Literal> s,
      NumericValue start, NumericValue length) const {
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
    const auto& str = asStringViewUnsafe(s.value().getContent());
    std::size_t utf8len = utf8Length(str);
    // Clamp the number such that it is in `[0, str.size()]`. That way we end up
    // with valid arguments for the `setSubstr` method below for both
    // starting position and length since all the other corner cases have been
    // dealt with above.
    auto clamp = [utf8len](int64_t n) {
      return static_cast<size_t>(
          std::clamp(n, int64_t{0}, static_cast<int64_t>(utf8len)));
    };

    startInt = clamp(startInt);
    lengthInt = clamp(lengthInt);
    std::size_t startByteOffset = utf8ToByteOffset(str, startInt);
    std::size_t endByteOffset = utf8ToByteOffset(str, startInt + lengthInt);
    std::size_t byteLength = endByteOffset - startByteOffset;

    s.value().setSubstr(startByteOffset, byteLength);
    return LiteralOrIri(std::move(s.value()));
  }
};

using SubstrExpression =
    LiteralExpressionImpl<3, SubstrImpl, NumericValueGetter,
                          NumericValueGetter>;

// STRSTARTS
struct StrStartsImpl {
  Id operator()(std::string_view text, std::string_view pattern) const {
    return Id::makeFromBool(ql::starts_with(text, pattern));
  }
};

namespace {

CPP_template(typename NaryOperation)(
    requires isOperation<NaryOperation>) class StrStartsExpressionImpl
    : public NaryExpression<NaryOperation> {
 public:
  using NaryExpression<NaryOperation>::NaryExpression;
  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      [[maybe_unused]] bool isNegated) const override {
    std::vector<PrefilterExprVariablePair> prefilterVec;
    const auto& children = this->children();
    AD_CORRECTNESS_CHECK(children.size() == 2);

    auto var = children[0].get()->getVariableOrNullopt();
    if (!var.has_value()) {
      return prefilterVec;
    }
    auto prefixStr = getLiteralFromLiteralExpression(children[1].get());
    if (!prefixStr.has_value()) {
      return prefilterVec;
    }

    prefilterVec.emplace_back(
        std::make_unique<prefilterExpressions::PrefixRegexExpression>(
            prefixStr.value()),
        var.value());
    return prefilterVec;
  }
};

}  // namespace

using StrStartsExpression = StrStartsExpressionImpl<
    Operation<2, FV<LiftStringFunction<StrStartsImpl>, StringValueGetter>>>;

// STRENDS
struct StrEndsImpl {
  Id operator()(std::string_view text, std::string_view pattern) const {
    return Id::makeFromBool(ql::ends_with(text, pattern));
  }
};

using StrEndsExpression =
    StringExpressionImpl<2, LiftStringFunction<StrEndsImpl>, StringValueGetter>;

// STRCONTAINS
struct ContainsImpl {
  Id operator()(std::string_view text, std::string_view pattern) const {
    return Id::makeFromBool(text.find(pattern) != std::string::npos);
  }
};

using ContainsExpression =
    StringExpressionImpl<2, LiftStringFunction<ContainsImpl>,
                         StringValueGetter>;

// STRAFTER / STRBEFORE
template <bool isStrAfter>
struct StrAfterOrBeforeImpl {
  IdOrLiteralOrIri operator()(
      std::optional<ad_utility::triple_component::Literal> optLiteral,
      std::optional<ad_utility::triple_component::Literal> optPattern) const {
    if (!optPattern.has_value() || !optLiteral.has_value()) {
      return Id::makeUndefined();
    }
    auto& literal = optLiteral.value();
    const auto& patternLit = optPattern.value();
    // Check if arguments are compatible with their language tags.
    if (patternLit.hasLanguageTag() &&
        (!literal.hasLanguageTag() ||
         literal.getLanguageTag() != patternLit.getLanguageTag())) {
      return Id::makeUndefined();
    }
    const auto& pattern = asStringViewUnsafe(optPattern.value().getContent());
    //  Required by the SPARQL standard.
    if (pattern.empty()) {
      if (isStrAfter) {
        return LiteralOrIri(std::move(literal));
      } else {
        literal.setSubstr(0, 0);
        return LiteralOrIri(std::move(literal));
      }
    }
    auto literalContent = literal.getContent();
    auto pos = asStringViewUnsafe(literalContent).find(pattern);
    if (pos >= literalContent.size()) {
      return toLiteral("");
    }
    if constexpr (isStrAfter) {
      literal.setSubstr(pos + pattern.size(),
                        literalContent.size() - pos - pattern.size());
    } else {
      // STRBEFORE
      literal.setSubstr(0, pos);
    }
    return LiteralOrIri(std::move(literal));
  }
};

using StrAfterExpression =
    LiteralExpressionImpl<2, StrAfterOrBeforeImpl<true>,
                          LiteralValueGetterWithoutStrFunction>;

using StrBeforeExpression =
    LiteralExpressionImpl<2, StrAfterOrBeforeImpl<false>,
                          LiteralValueGetterWithoutStrFunction>;

struct MergeFlagsIntoRegex {
  IdOrLiteralOrIri operator()(std::optional<std::string> regex,
                              const std::optional<std::string>& flags) const {
    if (!flags.has_value() || !regex.has_value()) {
      return Id::makeUndefined();
    }
    auto firstInvalidFlag = flags.value().find_first_not_of("imsU");
    if (firstInvalidFlag != std::string::npos) {
      return Id::makeUndefined();
    }

    // In Google RE2 the flags are directly part of the regex.
    std::string result =
        flags.value().empty()
            ? std::move(regex.value())
            : absl::StrCat("(?", flags.value(), ":", regex.value() + ")");
    return toLiteral(std::move(result));
  }
};

using MergeRegexPatternAndFlagsExpression =
    StringExpressionImpl<2, MergeFlagsIntoRegex, LiteralFromIdGetter>;

struct ReplaceImpl {
  IdOrLiteralOrIri operator()(
      std::optional<ad_utility::triple_component::Literal> s,
      const std::shared_ptr<RE2>& pattern,
      const std::optional<std::string>& replacement) const {
    if (!s.has_value() || !pattern || !replacement.has_value()) {
      return Id::makeUndefined();
    }
    std::string in(asStringViewUnsafe(s.value().getContent()));
    const auto& pat = *pattern;
    // Check for invalid regexes.
    if (!pat.ok()) {
      return Id::makeUndefined();
    }
    const auto& repl = replacement.value();
    RE2::GlobalReplace(&in, pat, repl);
    s.value().replaceContent(in);
    return LiteralOrIri(std::move(s.value()));
  }
};

using ReplaceExpression =
    LiteralExpressionImpl<3, ReplaceImpl, RegexValueGetter,
                          ReplacementStringGetter>;

// CONCAT
class ConcatExpression : public detail::VariadicExpression {
 public:
  using VariadicExpression::VariadicExpression;

  // _________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* ctx) const override {
    using Literal = ad_utility::triple_component::Literal;
    using LiteralVec = VectorWithMemoryLimit<std::optional<Literal>>;
    // We evaluate one child after the other and append the strings from child i
    // to the strings already constructed for children 0, …, i - 1. The
    // seemingly more natural row-by-row approach has two problems. First, the
    // distinction between children with constant results and vector results
    // would not be cache-efficient. Second, when the evaluation is part of
    // GROUP BY, we don't have the information in advance whether all children
    // have constant results (in which case, we need to evaluate the whole
    // expression only once).

    // We store the (intermediate) result either as single literal or a vector.
    // If the result is a Literal, then all the previously evaluated children
    // were constants (see above).

    // `LiteralVec` stores literals whose string contents are the result of
    // concatenation. Each literal also carries a suffix (language tag or
    // datatype) that is determined by the previously processed children. For
    // each row, the suffix reflects either the current matching suffix or a
    // mismatch (in which case the final suffix will be empty).

    auto valueGetter =
        sparqlExpression::detail::LiteralValueGetterWithoutStrFunction{};
    std::variant<std::optional<Literal>, LiteralVec> result =
        Literal::literalWithNormalizedContent(asNormalizedStringViewUnsafe(""));
    bool isFirstLiteral = true;

    auto moveLiteralToResult =
        [](std::optional<Literal>& literal) -> IdOrLiteralOrIri {
      if (!literal.has_value()) {
        return Id::makeUndefined();
      }
      return LiteralOrIri(std::move(literal.value()));
    };

    auto visitSingleExpressionResult = CPP_template_lambda(
        &ctx, &result, &isFirstLiteral, &valueGetter)(typename T)(T && s)(
        requires SingleExpressionResult<T> && std::is_rvalue_reference_v<T&&>) {
      if constexpr (isConstantResult<T>) {
        auto literalFromConstant = valueGetter(std::forward<T>(s), ctx);

        auto concatOrSetLitFromConst =
            [&](std::optional<Literal>& literalSoFar) {
              concatOrSetLiteral(literalSoFar, literalFromConstant,
                                 isFirstLiteral);
            };

        // All previous children were constants, and the current child also is
        // a constant.
        auto visitLiteralConcat =
            [&concatOrSetLitFromConst](std::optional<Literal>&& literalSoFar) {
              concatOrSetLitFromConst(literalSoFar);
            };

        // One of the previous children was not a constant, so we already
        // store a vector.
        auto visitLiteralVecConcat = [&concatOrSetLitFromConst](
                                         LiteralVec&& literalVec) {
          ql::ranges::for_each(std::move(literalVec), [&](auto& literalSoFar) {
            concatOrSetLitFromConst(literalSoFar);
          });
        };

        std::visit(ad_utility::OverloadCallOperator{visitLiteralConcat,
                                                    visitLiteralVecConcat},
                   std::move(result));
      } else {
        auto gen = sparqlExpression::detail::makeGenerator(AD_FWD(s),
                                                           ctx->size(), ctx);

        if (std::holds_alternative<std::optional<Literal>>(result)) {
          // All previous children were constants, but now we have a
          // non-constant child, so we have to expand the `result` from a single
          // string to a vector.
          std::optional<Literal> constantResultSoFar =
              std::move(std::get<std::optional<Literal>>(result));
          result.emplace<LiteralVec>(ctx->_allocator);
          auto& resultAsVec = std::get<LiteralVec>(result);
          resultAsVec.reserve(ctx->size());
          std::fill_n(std::back_inserter(resultAsVec), ctx->size(),
                      constantResultSoFar);
        }

        // The `result` already is a vector, and the current child also returns
        // multiple results, so we do the `natural` way.
        auto& resultAsVec = std::get<LiteralVec>(result);
        // TODO<C++23> Use `ql::views::zip` or `enumerate`.
        size_t i = 0;
        for (auto& el : gen) {
          auto literal = valueGetter(std::move(el), ctx);
          concatOrSetLiteral(resultAsVec[i], literal, isFirstLiteral);
          ctx->cancellationHandle_->throwIfCancelled();
          ++i;
        }
      }
      ctx->cancellationHandle_->throwIfCancelled();
    };
    ql::ranges::for_each(childrenVec(), [&ctx, &visitSingleExpressionResult,
                                         &isFirstLiteral](const auto& child) {
      std::visit(visitSingleExpressionResult, child->evaluate(ctx));
      isFirstLiteral = false;
    });

    // Lift the result from `string` to `IdOrLiteralOrIri` which is needed for
    // the expression module.

    auto visitLiteralResult =
        [&moveLiteralToResult](
            std::optional<Literal>& literalSoFar) -> ExpressionResult {
      return moveLiteralToResult(literalSoFar);
    };

    auto visitLiteralVecResult =
        [&ctx,
         &moveLiteralToResult](LiteralVec& literalVec) -> ExpressionResult {
      VectorWithMemoryLimit<IdOrLiteralOrIri> resultAsVec(ctx->_allocator);
      resultAsVec.reserve(literalVec.size());
      ql::ranges::copy(literalVec | ql::views::transform(moveLiteralToResult),
                       std::back_inserter(resultAsVec));
      return resultAsVec;
    };
    return std::visit(ad_utility::OverloadCallOperator{visitLiteralResult,
                                                       visitLiteralVecResult},
                      result);
  }
};

// ENCODE_FOR_URI
struct EncodeForUriImpl {
  IdOrLiteralOrIri operator()(std::optional<std::string> input) const {
    if (!input.has_value()) {
      return Id::makeUndefined();
    } else {
      std::string_view value{input.value()};

      return toLiteral(
          boost::urls::encode(value, boost::urls::unreserved_chars));
    }
  }
};
using EncodeForUriExpression = StringExpressionImpl<1, EncodeForUriImpl>;

// LANGMATCHES
struct LangMatching {
  Id operator()(std::optional<std::string> languageTag,
                std::optional<std::string> languageRange) const {
    if (!languageTag.has_value() || !languageRange.has_value()) {
      return Id::makeUndefined();
    } else {
      return Id::makeFromBool(ad_utility::isLanguageMatch(
          languageTag.value(), languageRange.value()));
    }
  }
};

using LangMatches = StringExpressionImpl<2, LangMatching, StringValueGetter>;

// STRING WITH LANGUAGE TAG
struct StrLangTag {
  IdOrLiteralOrIri operator()(
      std::optional<ad_utility::triple_component::Literal> literal,
      std::optional<std::string> langTag) const {
    if (!literal.has_value() || !langTag.has_value() ||
        !literal.value().isPlain()) {
      return Id::makeUndefined();
    } else if (!ad_utility::strIsLangTag(langTag.value())) {
      return Id::makeUndefined();
    } else {
      literal.value().addLanguageTag(std::move(langTag.value()));
      return LiteralOrIri{std::move(literal.value())};
    }
  }
};

using StrLangTagged = LiteralExpressionImpl<2, StrLangTag, StringValueGetter>;

// STRING WITH DATATYPE IRI
struct StrIriDtTag {
  IdOrLiteralOrIri operator()(
      std::optional<ad_utility::triple_component::Literal> literal,
      OptIri inputIri) const {
    if (!literal.has_value() || !inputIri.has_value() ||
        !literal.value().isPlain()) {
      return Id::makeUndefined();
    } else {
      literal.value().addDatatype(inputIri.value());
      return LiteralOrIri{std::move(literal.value())};
    }
  }
};

using StrIriTagged = LiteralExpressionImpl<2, StrIriDtTag, IriValueGetter>;

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

CPP_template(typename T,
             typename... C)(requires(concepts::same_as<Expr, C>&&...)) Expr
    make(C&... children) {
  return std::make_unique<T>(std::move(children)...);
}
Expr makeStrExpression(Expr child) { return make<StrExpression>(child); }

Expr makeIriOrUriExpression(Expr child, SparqlExpression::Ptr baseIri) {
  return make<IriOrUriExpression>(child, baseIri);
}

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
Expr makeMergeRegexPatternAndFlagsExpression(Expr pattern, Expr flags) {
  return make<MergeRegexPatternAndFlagsExpression>(pattern, flags);
}
Expr makeReplaceExpression(Expr input, Expr pattern, Expr repl, Expr flags) {
  if (flags) {
    pattern = makeMergeRegexPatternAndFlagsExpression(std::move(pattern),
                                                      std::move(flags));
  }
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

Expr makeLangMatchesExpression(Expr child1, Expr child2) {
  return make<LangMatches>(child1, child2);
}

Expr makeMD5Expression(Expr child) { return make<MD5Expression>(child); }

Expr makeSHA1Expression(Expr child) { return make<SHA1Expression>(child); }

Expr makeSHA256Expression(Expr child) { return make<SHA256Expression>(child); }

Expr makeSHA384Expression(Expr child) { return make<SHA384Expression>(child); }

Expr makeSHA512Expression(Expr child) { return make<SHA512Expression>(child); }

Expr makeConvertToStringExpression(Expr child) {
  return std::make_unique<StrExpressionImpl>(std::move(child));
}

}  // namespace sparqlExpression

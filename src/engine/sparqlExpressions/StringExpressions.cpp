//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// This file only contains the basic string expressions. The other string
// expressions are split into several files (`HashExpressions.cpp`,
// `SubstringExpressions.cpp`, `StringMatchingExpressions.cpp`,
// `ReplaceExpression.cpp`, `ConcatExpression.cpp`, and
// `StrLangAndStrDtExpressions.cpp`), because compiling and statically
// analyzing all of them in a single translation unit requires a lot of memory.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <boost/url.hpp>
#endif

#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/StringExpressionsHelper.h"
#include "util/ParsedUri.h"
#include "util/StringUtils.h"

namespace sparqlExpression {
namespace detail::string_expressions {

// String functions.
struct StrImpl {
  IdOrLiteralOrIri operator()(std::optional<std::string> s) const {
    if (s.has_value()) {
      return LiteralOrIri{toLiteral(s.value())};
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

// IRI or URI
//
// 1. What's the correct behavior for non-strings, like `1` or `true`?
//
// @1: Only a `LiteralOrIri` or an `Id` from `Vocab`/`LocalVocab` is in
// consideration within the `IriOrUriValueGetter`, hence automatically
// ignores values like `1`, `true`, `Date` etc.

const Iri& extractIri(const IdOrLiteralOrIri& litOrIri) {
  AD_CORRECTNESS_CHECK(std::holds_alternative<LiteralOrIri>(litOrIri));
  const auto& baseIriOrUri = std::get<LiteralOrIri>(litOrIri);
  AD_CORRECTNESS_CHECK(baseIriOrUri.isIri());
  return baseIriOrUri.getIri();
}

struct ApplyBaseIfPresent {
  IdOrLiteralOrIri operator()(
      IdOrLiteralOrIri iri,
      const std::optional<qlever::util::ParsedUri>& base) const {
    if (std::holds_alternative<Id>(iri)) {
      AD_CORRECTNESS_CHECK(std::get<Id>(iri).isUndefined());
      return std::get<Id>(iri);
    }
    if (!base.has_value()) {
      return std::get<LiteralOrIri>(std::move(iri));
    }
    return LiteralOrIri{extractIri(iri).resolveAgainstBase(base.value())};
  }
};
using IriOrUriExpression =
    NARY<2, FV<ApplyBaseIfPresent, IriOrUriValueGetter, ParsedUriGetter>>;

// STRLEN
struct Strlen {
  Id operator()(std::string_view s) const {
    size_t length =
        ad_utility::getUTF8Prefix(s, std::numeric_limits<size_t>::max()).first;
    return Id::makeFromInt(length);
  }
};
using StrlenExpression = StringExpressionImpl<1, LiftStringFunction<Strlen>>;

// UCase and LCase
template <auto toLowerOrToUpper>
struct UpperOrLowerCaseImpl {
  IdOrLiteralOrIri operator()(
      std::optional<ad_utility::triple_component::Literal> input) const {
    if (!input.has_value()) {
      return Id::makeUndefined();
    }
    auto& literal = input.value();
    auto newContent = std::invoke(toLowerOrToUpper, literal.getContent());
    literal.replaceContent(newContent);
    return LiteralOrIri(std::move(literal));
  }
};

// `ad_utility::utf8ToLower` and `ad_utility::utf8ToUpper` have an optional
// locale argument, so their addresses cannot be used directly as the
// (single-argument) non-type template argument of `UpperOrLowerCaseImpl`. Wrap
// them in functions with the exact required signature.
// TODO<RobinTF> These use the locale-independent (root) case conversion, as
// `LCASE` and `UCASE` always have. Note that this can differ from the
// (locale-dependent) lowercasing the index uses for its vocabulary, which could
// in principle lead to missed lookups; revisit once a locale can be wired into
// the n-ary string expressions.
inline std::string utf8ToLowerRootLocale(std::string_view s) {
  return ad_utility::utf8ToLower(s);
}
inline std::string utf8ToUpperRootLocale(std::string_view s) {
  return ad_utility::utf8ToUpper(s);
}

using UppercaseExpression =
    LiteralExpressionImpl<1, UpperOrLowerCaseImpl<&utf8ToUpperRootLocale>>;
using LowercaseExpression =
    LiteralExpressionImpl<1, UpperOrLowerCaseImpl<&utf8ToLowerRootLocale>>;

// ENCODE_FOR_URI
struct EncodeForUriImpl {
  IdOrLiteralOrIri operator()(std::optional<std::string> input) const {
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
    if (!input.has_value()) {
      return Id::makeUndefined();
    } else {
      std::string_view value{input.value()};

      return toLiteral(
          boost::urls::encode(value, boost::urls::unreserved_chars));
    }
#else
    throw std::runtime_error(
        "EncodeForUri is not available in reduced feature set for C++17");
#endif
  }
};
using EncodeForUriExpression = StringExpressionImpl<1, EncodeForUriImpl>;

}  // namespace detail::string_expressions
using namespace detail::string_expressions;
using Expr = SparqlExpression::Ptr;

Expr makeStrExpression(Expr child) { return make<StrExpression>(child); }

Expr makeIriOrUriExpression(Expr child, SparqlExpression::Ptr baseIri) {
  return make<IriOrUriExpression>(child, baseIri);
}

Expr makeStrlenExpression(Expr child) { return make<StrlenExpression>(child); }

Expr makeLowercaseExpression(Expr child) {
  return make<LowercaseExpression>(child);
}

Expr makeUppercaseExpression(Expr child) {
  return make<UppercaseExpression>(child);
}

Expr makeEncodeForUriExpression(Expr child) {
  return make<EncodeForUriExpression>(child);
}

Expr makeConvertToStringExpression(Expr child) {
  return std::make_unique<StrExpressionImpl>(std::move(child));
}

}  // namespace sparqlExpression

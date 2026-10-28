// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// The `LANGMATCHES`, `STRLANG`, and `STRDT` expressions, which work on the
// language tags and datatypes of literals. They are split from
// `StringExpressions.cpp` to reduce the memory needed for compiling and
// statically analyzing a single translation unit.

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/StringExpressionsHelper.h"
#include "index/TripleComponentConversions.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "parser/RdfParser.h"
#include "util/StringUtils.h"

namespace sparqlExpression {
namespace detail::string_expressions {

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
    }
    // Try to encode literals with suitable datatype directly as a `ValueId`.
    try {
      std::string_view content = literal.value().getContent();
      // We do not need to encode numeric IRIs because the behavior of the
      // regular one is also valid.
      EncodedIriManager ev;
      auto tc =
          TurtleParser<TokenizerCtre>::literalAndDatatypeToTripleComponent(
              content, inputIri.value(), ev);
      auto id = toValueIdIfNotString(tc, &ev);
      if (id.has_value()) {
        return id.value();
      }
    } catch (const ParseException&) {
      // Parse failure for the given datatype, return a `LiteralOrIri`.
      // NOTE: This behavior differs from the parsing at index build time, where
      // invalid integers throw an exception.
    }
    literal.value().addDatatype(inputIri.value());
    return LiteralOrIri{std::move(literal.value())};
  }
};

using StrIriTagged = LiteralExpressionImpl<2, StrIriDtTag, IriValueGetter>;

}  // namespace detail::string_expressions
using namespace detail::string_expressions;
using Expr = SparqlExpression::Ptr;

Expr makeStrLangTagExpression(Expr child1, Expr child2) {
  return make<StrLangTagged>(child1, child2);
}

Expr makeStrIriDtExpression(Expr child1, Expr child2) {
  return make<StrIriTagged>(child1, child2);
}

Expr makeLangMatchesExpression(Expr child1, Expr child2) {
  return make<LangMatches>(child1, child2);
}

}  // namespace sparqlExpression

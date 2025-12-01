//  Copyright 2022-2024 University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de> (2022)
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de> (2024)

// Unit tests for the functionality from this file can be
// found in LanguageExpressionsTest.h

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "index/IndexImpl.h"

namespace sparqlExpression {
namespace detail::langImpl {

using Lit = ad_utility::triple_component::Literal;

//______________________________________________________________________________
// A value getter that returns the language tag as an ID if optimized, otherwise
// as a literal
struct LanguageTagAsIdOrLiteralGetter : Mixin<LanguageTagAsIdOrLiteralGetter> {
  using Mixin<LanguageTagAsIdOrLiteralGetter>::operator();

  IdOrLiteralOrIri operator()(ValueId id, const EvaluationContext* ctx) const {
    // For VocabIndex IDs with language tags, try to return the optimized ID
    if (id.getDatatype() == Datatype::VocabIndex) {
      uint32_t langTagIndex = id.getLangTagIndex();

      if (langTagIndex != LanguageTagManager::noLanguageTag &&
          langTagIndex != LanguageTagManager::unknownLanguageTag) {
        const auto& languageTagManager =
            ctx->_qec.getIndex().getImpl().languageTagManager();
        auto optIdBits = languageTagManager.getLanguageTagIdBits(langTagIndex);
        if (optIdBits.has_value()) {
          return Id::fromBits(optIdBits.value());
        }
      }
    }

    // Fall back to using the LanguageTagValueGetter to get the string
    LanguageTagValueGetter stringGetter;
    auto optLangTag = stringGetter(id, ctx);

    if (!optLangTag.has_value()) {
      return Id::makeUndefined();
    }

    const auto& langTag = optLangTag.value();
    if (langTag.empty()) {
      return LiteralOrIri{
          Lit::literalWithNormalizedContent(asNormalizedStringViewUnsafe(""))};
    }

    // Check if this is an optimized language
    const auto& languageTagManager =
        ctx->_qec.getIndex().getImpl().languageTagManager();
    uint32_t langTagIndex = languageTagManager.getLanguageTagIndex(langTag);

    if (langTagIndex != LanguageTagManager::unknownLanguageTag &&
        langTagIndex != LanguageTagManager::noLanguageTag) {
      auto optIdBits = languageTagManager.getLanguageTagIdBits(langTagIndex);
      if (optIdBits.has_value()) {
        return Id::fromBits(optIdBits.value());
      }
    }

    // Return the language tag as a literal
    return LiteralOrIri{Lit::literalWithNormalizedContent(
        asNormalizedStringViewUnsafe(langTag))};
  }

  IdOrLiteralOrIri operator()(const LiteralOrIri& litOrIri,
                              const EvaluationContext* ctx) const {
    if (litOrIri.isLiteral()) {
      if (litOrIri.hasLanguageTag()) {
        auto langTag =
            std::string(asStringViewUnsafe(litOrIri.getLanguageTag()));

        // Check if this is an optimized language
        const auto& languageTagManager =
            ctx->_qec.getIndex().getImpl().languageTagManager();
        uint32_t langTagIndex = languageTagManager.getLanguageTagIndex(langTag);

        if (langTagIndex != LanguageTagManager::unknownLanguageTag &&
            langTagIndex != LanguageTagManager::noLanguageTag) {
          auto optIdBits =
              languageTagManager.getLanguageTagIdBits(langTagIndex);
          if (optIdBits.has_value()) {
            return Id::fromBits(optIdBits.value());
          }
        }

        return LiteralOrIri{Lit::literalWithNormalizedContent(
            asNormalizedStringViewUnsafe(langTag))};
      }
      // Literal without language tag returns empty string
      return LiteralOrIri{
          Lit::literalWithNormalizedContent(asNormalizedStringViewUnsafe(""))};
    } else {
      return Id::makeUndefined();
    }
  }
};

//______________________________________________________________________________
struct IdentityOperation {
  IdOrLiteralOrIri operator()(IdOrLiteralOrIri value) const { return value; }
};

//______________________________________________________________________________
NARY_EXPRESSION(LangExpression, 1,
                FV<IdentityOperation, LanguageTagAsIdOrLiteralGetter>);

}  //  namespace detail::langImpl

//______________________________________________________________________________
// This function is a stand-alone helper used in `RelationalExpression.cpp`,
// if `getVariableFromLangExpression()` returns std::nullopt, no language filter
// will be created within. This happens when the provided pointer doesn't
// point to a `LangExpression`, or the given respective `LangExpression` doesn't
// hold a `Variable` as a child expression.
std::optional<Variable> getVariableFromLangExpression(
    const SparqlExpression* expPtr) {
  const auto* langExpr =
      dynamic_cast<const detail::langImpl::LangExpression*>(expPtr);
  if (!langExpr) {
    return std::nullopt;
  }

  auto children = langExpr->children();
  AD_CORRECTNESS_CHECK(children.size() == 1);
  return children[0]->getVariableOrNullopt();
}

//______________________________________________________________________________
SparqlExpression::Ptr makeLangExpression(SparqlExpression::Ptr child) {
  return std::make_unique<detail::langImpl::LangExpression>(std::move(child));
}

}  // namespace sparqlExpression

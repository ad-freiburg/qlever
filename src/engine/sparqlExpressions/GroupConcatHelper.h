//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_GROUPCONCATHELPER_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_GROUPCONCATHELPER_H

#include <optional>
#include <string>

#include "parser/Literal.h"
#include "parser/NormalizedString.h"
#include "util/Exception.h"

namespace sparqlExpression::detail {

// Merge the language tag of the source literal into the target aggregate. If
// one of the arguments doesn't hold a language tag or the language tags
// mismatch then `target` will be set to `std::nullopt`. Otherwise, the language
// tag of `target` will remain unchanged.
inline void mergeLanguageTags(
    std::optional<std::string>& target,
    const ad_utility::triple_component::Literal& source) {
  if (!target.has_value()) {
    return;
  }
  if (!source.hasLanguageTag() ||
      asStringViewUnsafe(source.getLanguageTag()) != target.value()) {
    target.reset();
  }
}

// Write the potential language tag of the source literal into the target. If
// the source literal doesn't have a language tag, then `target` will remain
// unchanged. It is assumed that this function is only called if `target` is not
// already set.
inline void pushLanguageTag(
    std::optional<std::string>& target,
    const std::optional<ad_utility::triple_component::Literal>& source) {
  AD_CONTRACT_CHECK(!target.has_value());
  if (source.has_value() && source.value().hasLanguageTag()) {
    target.emplace(asStringViewUnsafe(source.value().getLanguageTag()));
  }
}

// Combine a string and an optional language tag into a `LiteralOrIri` object.
inline LiteralOrIri stringWithOptionalLangTagToLiteral(
    const std::string& result, std::optional<std::string> langTag) {
  return ad_utility::triple_component::LiteralOrIri{
      ad_utility::triple_component::Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(result),
          langTag.has_value()
              ? std::optional<
                    std::variant<ad_utility::triple_component::Iri,
                                 std::string>>{std::move(langTag.value())}
              : std::nullopt)};
}
}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_GROUPCONCATHELPER_H

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

// _____________________________________________________________________________
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

// _____________________________________________________________________________
inline void pushLanguageTag(
    std::optional<std::string>& target,
    const std::optional<ad_utility::triple_component::Literal>& source) {
  AD_CONTRACT_CHECK(!target.has_value());
  if (source.has_value() && source.value().hasLanguageTag()) {
    target.emplace(asStringViewUnsafe(source.value().getLanguageTag()));
  }
}

// _____________________________________________________________________________
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

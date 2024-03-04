//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "parser/TripleComponent.h"

namespace ad_utility::testing {

// Create a valid `TripleComponent::Literal` that can then be stored in a
// `TripleComponent`. The contents of the literal are obtained by normalizing
// `literal` (which must be enclosed in double quotes) and the optional
// `langtagOrDatatype` (which must start with `@` or `^^`).
inline auto tripleComponentLiteral = [](const std::string& literal,
                                        std::string_view langtagOrDatatype =
                                            "") {
  if (langtagOrDatatype.starts_with("@")) {
    return TripleComponent::Literal::literalWithQuotes(
        literal, std::string(langtagOrDatatype));
  } else if (langtagOrDatatype.starts_with("^^")) {
    auto iri =
        ad_utility::triple_component::Iri::iriref(langtagOrDatatype.substr(2));
    return TripleComponent::Literal::literalWithQuotes(literal, std::move(iri));
  } else {
    AD_CONTRACT_CHECK(langtagOrDatatype.empty());
    return TripleComponent::Literal::literalWithQuotes(literal);
  }
};
auto iri = [](std::string_view s) { return TripleComponent::Iri::iriref(s); };
}  // namespace ad_utility::testing

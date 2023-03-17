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
inline auto tripleComponentLiteral =
    [](const std::string& literal, std::string_view langtagOrDatatype = "") {
      return TripleComponent::Literal{RdfEscaping::normalizeRDFLiteral(literal),
                                      std::string{langtagOrDatatype}};
    };
}  // namespace ad_utility::testing

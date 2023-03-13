//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include "parser/TripleComponent.h"

namespace ad_utility::testing {

inline auto tripleComponentLiteral =
    [](const std::string& s, std::string_view langtagOrDatatype = "") {
      return TripleComponent::Literal{RdfEscaping::normalizeRDFLiteral(s),
                                      std::string{langtagOrDatatype}};
    };
}

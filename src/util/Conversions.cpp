// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "util/Conversions.h"

#include "global/Constants.h"

namespace ad_utility {

// _________________________________________________________
triple_component::Iri convertLangtagToEntityUri(std::string_view tag) {
  return triple_component::Iri::fromIriref(makeQleverInternalIri("@", tag));
}

}  // namespace ad_utility

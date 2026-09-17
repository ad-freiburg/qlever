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

// _________________________________________________________
triple_component::Iri convertToLanguageTaggedPredicate(
    const triple_component::Iri& pred, std::string_view langtag) {
  return triple_component::Iri::fromLangtagAndIriref(
      langtag, pred.toStringRepresentation());
}

}  // namespace ad_utility

// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "util/Conversions.h"

#include <absl/strings/str_cat.h>

#include "global/Constants.h"
#include "parser/NormalizedString.h"

namespace ad_utility {

// _________________________________________________________
qlever::triple_component::Iri convertLangtagToEntityUri(std::string_view tag) {
  return qlever::triple_component::Iri::fromIriref(
      makeQleverInternalIri("@", tag));
}

// _________________________________________________________
qlever::triple_component::Iri convertToLanguageTaggedPredicate(
    const qlever::triple_component::Iri& pred, std::string_view langtag) {
  return qlever::triple_component::Iri::fromIriref(absl::StrCat(
      "@", langtag, "@<", asStringViewUnsafe(pred.getContent()), ">"));
}

}  // namespace ad_utility

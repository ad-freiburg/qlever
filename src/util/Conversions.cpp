// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "util/Conversions.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <ctre-unicode.hpp>

#include "../global/Constants.h"
#include "../parser/TokenizerCtre.h"
#include "./Exception.h"
#include "./StringUtils.h"

namespace ad_utility {

// _________________________________________________________
triple_component::Iri convertLangtagToEntityUri(std::string_view tag) {
  return triple_component::Iri::fromIriref(makeQleverInternalIri("@", tag));
}

// _________________________________________________________
std::string convertToLanguageTaggedPredicate(std::string_view pred,
                                             std::string_view langtag) {
  return absl::StrCat("@", langtag, "@", pred);
}

static std::string_view getPrimaryLanguage(std::string_view language) {
  return language.substr(0, language.find('-'));
}

// _________________________________________________________
triple_component::Iri convertToLanguageTaggedPredicate(
    const triple_component::Iri& pred, std::string_view langtag) {
  return triple_component::Iri::fromIriref(absl::StrCat(
      "@", langtag, "@<", asStringViewUnsafe(pred.getContent()), ">"));
}

// _________________________________________________________
std::string convertToLangmatchesTaggedPredicate(std::string_view pred,
                                                std::string_view langtag) {
  return absl::StrCat("@@", getPrimaryLanguage(langtag), "@@", pred);
}

// _________________________________________________________
triple_component::Iri convertToLangmatchesTaggedPredicate(
    const triple_component::Iri& pred, std::string_view langtag) {
  return triple_component::Iri::fromIriref(
      absl::StrCat("@@", getPrimaryLanguage(langtag), "@@<",
                   asStringViewUnsafe(pred.getContent()), ">"));
}

}  // namespace ad_utility

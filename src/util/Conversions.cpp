//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

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
triple_component::Iri convertLangtagToEntityUri(const string& tag) {
  return triple_component::Iri::iriref(makeInternalIri("@", tag));
}

// _________________________________________________________
std::string convertToLanguageTaggedPredicate(const string& pred,
                                             const string& langtag) {
  return "<ql:langtagged" + langtag + ":" + pred.substr(1, pred.size() - 2) +
         ">";
  // return '@' + langtag + '@' + pred;
}

// TODO<joka921> Cleanup and unify with the above thing.
triple_component::Iri convertToLanguageTaggedPredicate(
    const triple_component::Iri& pred, const std::string& langtag) {
  return triple_component::Iri::iriref(
      absl::StrCat("<ql:langtagged", langtag, ":",
                   asStringViewUnsafe(pred.getContent()), ">"));
}

}  // namespace ad_utility

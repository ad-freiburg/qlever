//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "util/Conversions.h"

#include <assert.h>
#include <ctre/ctre.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../global/Constants.h"
#include "../parser/TokenizerCtre.h"
#include "./Exception.h"
#include "./StringUtils.h"

namespace ad_utility {

// _________________________________________________________
string convertLangtagToEntityUri(const string& tag) {
  return INTERNAL_ENTITIES_URI_PREFIX + "@" + tag + ">";
}

// _________________________________________________________
std::optional<string> convertEntityUriToLangtag(const string& word) {
  static const string prefix = INTERNAL_ENTITIES_URI_PREFIX + "@";
  if (word.starts_with(prefix)) {
    return word.substr(prefix.size(), word.size() - prefix.size() - 1);
  } else {
    return std::nullopt;
  }
}
// _________________________________________________________
std::string convertToLanguageTaggedPredicate(const string& pred,
                                             const string& langtag) {
  return '@' + langtag + '@' + pred;
}

}  // namespace ad_utility

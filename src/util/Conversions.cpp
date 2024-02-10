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
string convertLangtagToEntityUri(const string& tag) {
  return makeInternalIri("@", tag);
}

// _________________________________________________________
std::string convertToLanguageTaggedPredicate(const string& pred,
                                             const string& langtag) {
  return '@' + langtag + '@' + pred;
}

}  // namespace ad_utility

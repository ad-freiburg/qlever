

//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ACCEPTHEADERPARSER_H
#define QLEVER_ACCEPTHEADERPARSER_H

#include "./MediaTypes.h"
#include <sstream>

namespace ad_utility {
class AcceptHeaderParser {
 public:

  /// Return the first media type in `candidates`, whose string representation is contained in `input`. Throw `std::runtime` error if none of the candidates was found, with an error message starting with `errorMessage`.
  /// Note that this is only a very rough approximation of parsing an accept header, ignoring syntax errors, wildcards.
  static MediaType findAnyMediaType(std::string_view input, const std::vector<MediaType>& candidates, std::string_view errorMessage);
};

}

#endif  // QLEVER_ACCEPTHEADERPARSER_H

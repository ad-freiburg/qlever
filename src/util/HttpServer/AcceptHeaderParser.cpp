

//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "AcceptHeaderParser.h"

using namespace ad_utility;

// __________________________________________________________________________
MediaType AcceptHeaderParser::findAnyMediaType(std::string_view input, const std::vector<MediaType>& candidates, std::string_view errorMessage) {
  for (const auto& candidate : candidates) {
    if (input.find(toString(candidate))) {
      return candidate;
    }
  }
  std::stringstream error;
  error << errorMessage << ". No supported media type found in \"" << input << "\". supported media types are: ";
  for (const auto& candidate: candidates) {
    error << toString(candidate) << ' ';
  }

  throw std::runtime_error{error.str()};
}
};

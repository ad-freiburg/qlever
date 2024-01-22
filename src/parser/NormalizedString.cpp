// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "NormalizedString.h"

// __________________________________________
NormalizedString fromStringUnsafe(std::string_view input) {
  NormalizedString normalizedString;
  normalizedString.resize(input.size());

  std::transform(input.begin(), input.end(), normalizedString.begin(),
                 [](char c) { return NormalizedChar{c}; });

  return normalizedString;
}

// __________________________________________
std::string_view asStringView(NormalizedStringView normalizedStringView) {
  return {reinterpret_cast<const char*>(normalizedStringView.data()),
          normalizedStringView.size()};
}

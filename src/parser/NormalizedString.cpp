// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "NormalizedString.h"

// __________________________________________
std::string_view asStringViewUnsafe(NormalizedStringView normalizedStringView) {
  return {reinterpret_cast<const char*>(normalizedStringView.data()),
          normalizedStringView.size()};
}

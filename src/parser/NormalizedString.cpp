//
// Created by beckermann on 1/19/24.
//

#include "NormalizedString.h"

#include <algorithm>
#include <iostream>

std::ostream& operator<<(std::ostream& str, NormalizedChar c) {
  str << c.c_;
  return str;
}

NormalizedString fromStringUnsafe(std::string_view input) {
  NormalizedString normalizedString;
  normalizedString.resize(input.size());

  std::transform(input.begin(), input.end(), normalizedString.begin(),
                 [](char c) { return NormalizedChar{c}; });

  return normalizedString;
}

NormalizedString normalizeFromLiteralContent(std::string_view literal) {
  // TODO remove and replace invalid characters
  return fromStringUnsafe(literal);
}

std::string_view asStringView(NormalizedStringView normalizedStringView) {
  return {reinterpret_cast<const char*>(normalizedStringView.data()),
          normalizedStringView.size()};
}

// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#ifndef QLEVER_SRC_PARSER_NORMALIZEDSTRING_H
#define QLEVER_SRC_PARSER_NORMALIZEDSTRING_H

#include <string>
#include <string_view>

#include "backports/three_way_comparison.h"
#include "util/GenericCharTraits.h"

struct NormalizedChar {
  char c_;

  QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(NormalizedChar, c_)
};

// `NormalizedChar` behaves like a `char`, so we use `GenericCharTraits` (see
// there for why we cannot rely on `std::char_traits` directly).
using NormalizedCharTraits = ad_utility::GenericCharTraits<NormalizedChar>;

// A bespoke string representation that ensures the content
// is correctly encoded and does not contain invalid characters
using NormalizedString =
    std::basic_string<NormalizedChar, NormalizedCharTraits>;

// A string view representation of above described normalized strings
using NormalizedStringView =
    std::basic_string_view<NormalizedChar, NormalizedCharTraits>;

// Returns the given NormalizedStringView as a string_view.
inline std::string_view asStringViewUnsafe(
    NormalizedStringView normalizedStringView) {
  return {reinterpret_cast<const char*>(normalizedStringView.data()),
          normalizedStringView.size()};
}
inline NormalizedStringView asNormalizedStringViewUnsafe(
    std::string_view input) {
  return {reinterpret_cast<const NormalizedChar*>(input.data()), input.size()};
}

#endif  // QLEVER_SRC_PARSER_NORMALIZEDSTRING_H

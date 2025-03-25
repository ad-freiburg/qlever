// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <string>
#include <string_view>

struct NormalizedChar {
  char c_;
  auto operator<=>(const NormalizedChar&) const = default;
};

// A bespoke string representation that ensures the content
// is correctly encoded and does not contain invalid characters
using NormalizedString = std::basic_string<NormalizedChar>;

// A string view representation of above described normalized strings
using NormalizedStringView = std::basic_string_view<NormalizedChar>;

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

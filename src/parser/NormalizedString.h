// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <string>
#include <string_view>

struct NormalizedChar {
  char c_;
};

// A bespoke string representation that ensures the content
// is correctly encoded and does not contain invalid characters
using NormalizedString = std::basic_string<NormalizedChar>;

// A string view representation of above described normalized strings
using NormalizedStringView = std::basic_string_view<NormalizedChar>;

// Creates a new NormalizedString object by just copying the contents of the
// input.
//  Warning: This function should only be used for testing as is to be removed
//           once the normalizeFromLiteralContent function is implemented
NormalizedString fromStringUnsafe(std::string_view input);

// Returns the given NormalizedStringView as a string_view.
std::string_view asStringView(NormalizedStringView normalizedStringView);

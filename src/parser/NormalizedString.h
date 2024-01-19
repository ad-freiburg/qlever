//
// Created by beckermann on 1/19/24.
//

#pragma once

#include <string>
#include <string_view>

struct NormalizedChar {
  char c_;
};

using NormalizedStringView = std::basic_string_view<NormalizedChar>;
using NormalizedString = std::basic_string<NormalizedChar>;

// Creates a new NormalizedString object by just copying the contents of the
// input.
//  Warning: This function should only be used for testing as is to be removed
//           once the normalizeFromLiteralContent function is implemented
NormalizedString fromStringUnsafe(std::string_view input);

// Normalizes the given literal and returns is as a new NormalizedString object
NormalizedString normalizeFromLiteralContent(std::string_view literal);

// Returns the given NormalizedStringView as a string_view.
std::string_view asStringView(NormalizedStringView normalizedStringView);

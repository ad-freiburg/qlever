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


NormalizedString fromStringUnsafe(std::string_view input);
NormalizedString normalizeFromLiteralContent(std::string_view literal);
std::string_view asStringView(NormalizedStringView normalizedStringView);

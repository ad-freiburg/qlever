// Copyright 2016, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace ad_utility {

//! Convert a language tag like "@en" to the corresponding entity uri
//! for the efficient language filter
std::string convertLangtagToEntityUri(const std::string& tag);
std::optional<std::string> convertEntityUriToLangtag(const std::string& word);
std::string convertToLanguageTaggedPredicate(const std::string& pred,
                                             const std::string& langtag);
}  // namespace ad_utility

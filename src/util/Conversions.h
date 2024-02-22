// Copyright 2016, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "parser/LiteralOrIri.h"
#include "util/StringUtils.h"

namespace ad_utility {

static constexpr std::string_view languageTaggedPredicatePrefix =
    "<ql:langtagged";
//! Convert a language tag like "@en" to the corresponding entity uri
//! for the efficient language filter
std::string convertLangtagToEntityUri(const std::string& tag);
std::string convertToLanguageTaggedPredicate(const std::string& pred,
                                             const std::string& langtag);
triple_component::Iri convertToLanguageTaggedPredicate(
    const triple_component::Iri& pred, const std::string& langtag);
}  // namespace ad_utility

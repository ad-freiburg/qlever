// Copyright 2016, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#ifndef QLEVER_SRC_UTIL_CONVERSIONS_H
#define QLEVER_SRC_UTIL_CONVERSIONS_H

#include <string_view>

#include "parser/LiteralOrIri.h"

namespace ad_utility {

constexpr std::string_view languageTaggedPredicatePrefix = "@";
//! Convert a language tag like "@en" to the corresponding entity uri
//! for the efficient language filter.
triple_component::Iri convertLangtagToEntityUri(std::string_view tag);
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONVERSIONS_H

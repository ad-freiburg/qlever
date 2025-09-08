// Copyright 2016, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#ifndef QLEVER_SRC_UTIL_CONVERSIONS_H
#define QLEVER_SRC_UTIL_CONVERSIONS_H

#include <string>
#include <string_view>

#include "parser/LiteralOrIri.h"

namespace ad_utility {

constexpr std::string_view languageTaggedPredicatePrefix = "@";
//! Convert a language tag like "@en" to the corresponding entity uri
//! for the efficient language filter.
// TODO<joka921> The overload that takes and returns `std::string` can be
// removed as soon as we also store strongly-typed IRIs in the predicates of the
// `SparqlTriple` class.
triple_component::Iri convertLangtagToEntityUri(const std::string& tag);
triple_component::Iri convertToLanguageTaggedPredicate(
    const triple_component::Iri& pred, const std::string& langtag);
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONVERSIONS_H

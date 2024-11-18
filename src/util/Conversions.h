// Copyright 2016, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

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
triple_component::Iri convertLangtagToEntityUri(std::string_view tag);
std::string convertToLanguageTaggedPredicate(std::string_view pred,
                                             std::string_view langtag);
triple_component::Iri convertToLanguageTaggedPredicate(
    const triple_component::Iri& pred, std::string_view langtag);

// TODO<joka921> Comment.
std::string convertToLangmatchesTaggedPredicate(std::string_view pred,
                                                std::string_view langtag);
triple_component::Iri convertToLangmatchesTaggedPredicate(
    const triple_component::Iri& pred, std::string_view langtag);
}  // namespace ad_utility

// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "parser/Iri.h"

#include <utility>

#include "util/StringUtils.h"

namespace ad_utility::triple_component {
// __________________________________________
Iri::Iri(NormalizedString iri) : iri_{std::move(iri)} {}

// __________________________________________
Iri::Iri(const Iri& prefix, NormalizedStringView suffix)
    : iri_{NormalizedString{prefix.getContent()} + suffix} {};

// __________________________________________
NormalizedStringView Iri::getContent() const { return iri_; }

// __________________________________________
Iri Iri::iriref(std::string_view stringWithBrackets) {
  return Iri{RdfEscaping::normalizeIriWithBrackets(stringWithBrackets)};
}

// __________________________________________
Iri Iri::prefixed(const Iri& prefix, std::string_view suffix) {
  return Iri{std::move(prefix),
             RdfEscaping::normalizeIriWithoutBrackets(suffix)};
}

}  // namespace ad_utility::triple_component

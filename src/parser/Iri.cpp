// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "Iri.h"

#include <utility>

// __________________________________________
Iri::Iri(NormalizedString iri) : iri_{std::move(iri)} {}

// __________________________________________
Iri::Iri(const Iri& prefix, const NormalizedString& suffix) {
  iri_ = NormalizedString{prefix.getContent()} + suffix;
}

// __________________________________________
NormalizedStringView Iri::getContent() const { return iri_; }

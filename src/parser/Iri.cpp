// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include <utility>

#include "Iri.h"

// __________________________________________
Iri::Iri(NormalizedString iri) : iri_{std::move(iri)} {}

// __________________________________________
NormalizedStringView Iri::getContent() const { return iri_; }

// __________________________________________
std::string Iri::toRdf() const { return "<" + asStringView(iri_) + ">"; }

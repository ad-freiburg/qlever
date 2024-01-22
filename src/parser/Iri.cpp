// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "Iri.h"

#include <utility>

Iri::Iri(NormalizedString iri) { this->iri = std::move(iri); }

NormalizedStringView Iri::getIri() const { return this->iri; }

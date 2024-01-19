// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "IriType.h"

#include <utility>

IriType::IriType(NormalizedString iri) { this->iri = std::move(iri); }

NormalizedStringView IriType::getIri() const { return this->iri; }

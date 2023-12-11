// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "IriType.h"

IriType::IriType(std::string iri) { this->iri = iri; }

std::string_view IriType::getIri() const { return this->iri; }

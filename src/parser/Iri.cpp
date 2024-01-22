// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "Iri.h"

#include <utility>

#include "NormalizedString.h"

Iri::Iri(NormalizedString iri) : iri_{std::move(iri)} {}

NormalizedStringView Iri::getContent() const { return iri_; }

std::string Iri::toRdf() const { return "<" + asStringView(iri_) + ">"; }

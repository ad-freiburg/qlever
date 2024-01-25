// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "Literal.h"

#include <utility>

// __________________________________________
Literal::Literal(NormalizedString content) : content_{std::move(content)} {}

// __________________________________________
Literal::Literal(NormalizedString content, Iri datatype)
    : content_{std::move(content)}, descriptor_{std::move(datatype)} {}

// __________________________________________
Literal::Literal(NormalizedString content, NormalizedString languageTag)
    : content_{std::move(content)}, descriptor_{std::move(languageTag)} {}

// __________________________________________
bool Literal::hasLanguageTag() const {
  return std::holds_alternative<NormalizedString>(descriptor_);
}

// __________________________________________
bool Literal::hasDatatype() const {
  return std::holds_alternative<Iri>(descriptor_);
}

// __________________________________________
NormalizedStringView Literal::getContent() const { return content_; }

// __________________________________________
Iri Literal::getDatatype() const {
  if (!hasDatatype()) {
    AD_THROW("The literal does not have an explicit datatype.");
  }
  return std::get<Iri>(descriptor_);
}

// __________________________________________
NormalizedStringView Literal::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return std::get<NormalizedString>(descriptor_);
}

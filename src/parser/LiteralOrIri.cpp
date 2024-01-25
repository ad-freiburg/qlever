// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "LiteralOrIri.h"

#include <algorithm>

// __________________________________________
LiteralOrIri::LiteralOrIri(Iri iri) : data_{std::move(iri)} {}

// __________________________________________
LiteralOrIri::LiteralOrIri(Literal literal) : data_{std::move(literal)} {}

// __________________________________________
bool LiteralOrIri::isIri() const { return std::holds_alternative<Iri>(data_); }

// __________________________________________
Iri& LiteralOrIri::getIri() {
  if (!isIri()) {
    AD_THROW(
        "LiteralOrIri object does not contain an Iri object and thus "
        "cannot return it");
  }
  return std::get<Iri>(data_);
}

// __________________________________________
NormalizedStringView LiteralOrIri::getIriContent() {
  return getIri().getContent();
}

// __________________________________________
bool LiteralOrIri::isLiteral() const {
  return std::holds_alternative<Literal>(data_);
}

// __________________________________________
Literal& LiteralOrIri::getLiteral() {
  if (!isLiteral()) {
    AD_THROW(
        "LiteralOrIri object does not contain an Literal object and "
        "thus cannot return it");
  }
  return std::get<Literal>(data_);
}

// __________________________________________
bool LiteralOrIri::hasLanguageTag() { return getLiteral().hasLanguageTag(); }

// __________________________________________
bool LiteralOrIri::hasDatatype() { return getLiteral().hasDatatype(); }

// __________________________________________
NormalizedStringView LiteralOrIri::getLiteralContent() {
  return getLiteral().getContent();
}

// __________________________________________
NormalizedStringView LiteralOrIri::getLanguageTag() {
  return getLiteral().getLanguageTag();
}

// __________________________________________
Iri LiteralOrIri::getDatatype() { return getLiteral().getDatatype(); }

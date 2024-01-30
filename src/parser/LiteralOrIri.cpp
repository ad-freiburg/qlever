// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "parser/LiteralOrIri.h"

#include <algorithm>
#include <utility>

// __________________________________________
LiteralOrIri::LiteralOrIri(Iri iri) : data_{std::move(iri)} {}

// __________________________________________
LiteralOrIri::LiteralOrIri(Literal literal) : data_{std::move(literal)} {}

// __________________________________________
bool LiteralOrIri::isIri() const { return std::holds_alternative<Iri>(data_); }

// __________________________________________
const Iri& LiteralOrIri::getIri() const {
  if (!isIri()) {
    AD_THROW(
        "LiteralOrIri object does not contain an Iri object and thus "
        "cannot return it");
  }
  return std::get<Iri>(data_);
}

// __________________________________________
NormalizedStringView LiteralOrIri::getIriContent() const {
  return getIri().getContent();
}

// __________________________________________
bool LiteralOrIri::isLiteral() const {
  return std::holds_alternative<Literal>(data_);
}

// __________________________________________
const LiteralOrIri::Literal& LiteralOrIri::getLiteral() const {
  if (!isLiteral()) {
    AD_THROW(
        "LiteralOrIri object does not contain an Literal object and "
        "thus cannot return it");
  }
  return std::get<Literal>(data_);
}

// __________________________________________
bool LiteralOrIri::hasLanguageTag() const {
  return getLiteral().hasLanguageTag();
}

// __________________________________________
bool LiteralOrIri::hasDatatype() const { return getLiteral().hasDatatype(); }

// __________________________________________
NormalizedStringView LiteralOrIri::getLiteralContent() const {
  return getLiteral().getContent();
}

// __________________________________________
NormalizedStringView LiteralOrIri::getLanguageTag() const {
  return getLiteral().getLanguageTag();
}

// __________________________________________
Iri LiteralOrIri::getDatatype() const { return getLiteral().getDatatype(); }

// __________________________________________
NormalizedStringView LiteralOrIri::getContent() const {
  if (isLiteral())
    return getLiteralContent();
  else if (isIri())
    return getIriContent();
  else
    AD_THROW("LiteralOrIri object contains neither Iri not Literal");
}

// __________________________________________
LiteralOrIri LiteralOrIri::literalWithQuotes(const string& contentWithQuotes) {
  return LiteralOrIri{Literal::literalWithQuotes(contentWithQuotes)};
}

// __________________________________________
LiteralOrIri LiteralOrIri::literalWithoutQuotes(
    const std::string& contentWithoutQuotes) {
  return LiteralOrIri{Literal::literalWithoutQuotes(contentWithoutQuotes)};
}

// __________________________________________
LiteralOrIri LiteralOrIri::literalWithQuotesWithDatatype(
    const std::string& contentWithQuotes, Iri datatype) {
  return LiteralOrIri{Literal::literalWithQuotesWithDatatype(
      contentWithQuotes, std::move(datatype))};
}

// __________________________________________
LiteralOrIri LiteralOrIri::literalWithoutQuotesWithDatatype(
    const std::string& contentWithoutQuotes, Iri datatype) {
  return LiteralOrIri{Literal::literalWithoutQuotesWithDatatype(
      contentWithoutQuotes, std::move(datatype))};
}

// __________________________________________
LiteralOrIri LiteralOrIri::literalWithQuotesWithLanguageTag(
    const std::string& contentWithQuotes, const std::string& languageTag) {
  return LiteralOrIri{Literal::literalWithQuotesWithLanguageTag(
      contentWithQuotes, languageTag)};
}

// __________________________________________
LiteralOrIri LiteralOrIri::literalWithoutQuotesWithLanguageTag(
    const std::string& contentWithoutQuotes, const std::string& languageTag) {
  return LiteralOrIri{Literal::literalWithoutQuotesWithLanguageTag(
      contentWithoutQuotes, languageTag)};
}

// __________________________________________
LiteralOrIri LiteralOrIri::iriref(const std::string& stringWithBrackets) {
  return LiteralOrIri{Iri::iriref(stringWithBrackets)};
}

// __________________________________________
LiteralOrIri LiteralOrIri::prefixedIri(const Iri& prefix,
                                       const std::string& suffix) {
  return LiteralOrIri{Iri::prefixed(prefix, suffix)};
}

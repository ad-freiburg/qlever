// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "parser/LiteralOrIri.h"

#include <utility>

#include "backports/algorithm.h"
#include "index/IndexImpl.h"

namespace ad_utility::triple_component {
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
const Literal& LiteralOrIri::getLiteral() const {
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
NormalizedStringView LiteralOrIri::getDatatype() const {
  return getLiteral().getDatatype();
}

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
LiteralOrIri LiteralOrIri::iriref(const std::string& stringWithBrackets) {
  return LiteralOrIri{Iri::fromIriref(stringWithBrackets)};
}

// __________________________________________
LiteralOrIri LiteralOrIri::prefixedIri(const Iri& prefix,
                                       std::string_view suffix) {
  return LiteralOrIri{Iri::fromPrefixAndSuffix(prefix, suffix)};
}

// __________________________________________
LiteralOrIri LiteralOrIri::literalWithQuotes(
    std::string_view rdfContentWithQuotes,
    std::optional<std::variant<Iri, string>> descriptor) {
  return LiteralOrIri(Literal::fromEscapedRdfLiteral(rdfContentWithQuotes,
                                                     std::move(descriptor)));
}

// __________________________________________
LiteralOrIri LiteralOrIri::literalWithoutQuotes(
    std::string_view rdfContentWithoutQuotes,
    std::optional<std::variant<Iri, string>> descriptor) {
  return LiteralOrIri(Literal::literalWithoutQuotes(rdfContentWithoutQuotes,
                                                    std::move(descriptor)));
}

// ___________________________________________
std::strong_ordering LiteralOrIri::operator<=>(const LiteralOrIri& rhs) const {
  int i = IndexImpl::staticGlobalSingletonComparator().compare(
      toStringRepresentation(), rhs.toStringRepresentation());
  if (i < 0) {
    return std::strong_ordering::less;
  } else if (i > 0) {
    return std::strong_ordering::greater;
  } else {
    return std::strong_ordering::equal;
  }
}
}  // namespace ad_utility::triple_component

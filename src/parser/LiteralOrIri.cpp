// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "parser/LiteralOrIri.h"

#include <utility>

#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "index/IndexImpl.h"

namespace ad_utility::triple_component {

// __________________________________________
template <bool isOwning>
bool BasicLiteralOrIri<isOwning>::isIri() const {
  return std::holds_alternative<BasicIri<isOwning>>(data_);
}

// __________________________________________
template <bool isOwning>
const BasicIri<isOwning>& BasicLiteralOrIri<isOwning>::getIri() const {
  if (!isIri()) {
    AD_THROW(
        "LiteralOrIri object does not contain an Iri object and thus "
        "cannot return it");
  }
  return std::get<BasicIri<isOwning>>(data_);
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteralOrIri<isOwning>::getIriContent() const {
  return getIri().getContent();
}

// __________________________________________
template <bool isOwning>
bool BasicLiteralOrIri<isOwning>::isLiteral() const {
  return std::holds_alternative<BasicLiteral<isOwning>>(data_);
}

// __________________________________________
template <bool isOwning>
const BasicLiteral<isOwning>& BasicLiteralOrIri<isOwning>::getLiteral() const {
  AD_CONTRACT_CHECK(isLiteral(),
                    "LiteralOrIri object does not contain a Literal object and "
                    "thus cannot return it");
  return std::get<BasicLiteral<isOwning>>(data_);
}

// __________________________________________
template <bool isOwning>
bool BasicLiteralOrIri<isOwning>::hasLanguageTag() const {
  return getLiteral().hasLanguageTag();
}

// __________________________________________
template <bool isOwning>
bool BasicLiteralOrIri<isOwning>::hasDatatype() const {
  return getLiteral().hasDatatype();
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteralOrIri<isOwning>::getLiteralContent() const {
  return getLiteral().getContent();
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteralOrIri<isOwning>::getLanguageTag() const {
  return getLiteral().getLanguageTag();
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteralOrIri<isOwning>::getDatatype() const {
  return getLiteral().getDatatype();
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteralOrIri<isOwning>::getContent() const {
  if (isLiteral())
    return getLiteralContent();
  else if (isIri())
    return getIriContent();
  else
    AD_THROW("LiteralOrIri object contains neither Iri not Literal");
}

// ___________________________________________
template <bool isOwning>
ql::strong_ordering BasicLiteralOrIri<isOwning>::compareThreeWay(
    const BasicLiteralOrIri& rhs) const {
  int i = IndexImpl::staticGlobalSingletonComparator().compare(
      toStringRepresentation(), rhs.toStringRepresentation(),
      LocaleManager::Level::TOTAL);
  if (i < 0) {
    return ql::strong_ordering::less;
  } else if (i > 0) {
    return ql::strong_ordering::greater;
  } else {
    return ql::strong_ordering::equal;
  }
}

template class BasicLiteralOrIri<true>;
template class BasicLiteralOrIri<false>;

// ____________________________________________________________________________
// LiteralOrIri (owning) method implementations.
// ____________________________________________________________________________

LiteralOrIri LiteralOrIri::fromStringRepresentation(std::string internal) {
  return BasicLiteralOrIri<true>::fromStringRepresentation(std::move(internal));
}

// ____________________________________________________________________________
Iri& LiteralOrIri::getIri() {
  if (!isIri()) {
    AD_CONTRACT_CHECK(isIri(),
                      "LiteralOrIri object does not contain an Iri object "
                      "and thus cannot return it");
  }
  return static_cast<Iri&>(std::get<BasicIri<true>>(data_));
}

// ____________________________________________________________________________
Literal& LiteralOrIri::getLiteral() {
  AD_CONTRACT_CHECK(isLiteral(),
                    "LiteralOrIri object does not contain a Literal object "
                    "and thus cannot return it");
  return static_cast<Literal&>(std::get<BasicLiteral<true>>(data_));
}

// ____________________________________________________________________________
LiteralOrIri LiteralOrIri::literalWithQuotes(
    std::string_view rdfContentWithQuotes,
    std::optional<std::variant<Iri, std::string>> descriptor) {
  return LiteralOrIri(Literal::fromEscapedRdfLiteral(rdfContentWithQuotes,
                                                     std::move(descriptor)));
}

// ____________________________________________________________________________
LiteralOrIri LiteralOrIri::literalWithoutQuotes(
    std::string_view rdfContentWithoutQuotes,
    std::optional<std::variant<Iri, std::string>> descriptor) {
  return LiteralOrIri(Literal::literalWithoutQuotes(rdfContentWithoutQuotes,
                                                    std::move(descriptor)));
}

// ____________________________________________________________________________
LiteralOrIri LiteralOrIri::iriref(const std::string& stringWithBrackets) {
  return LiteralOrIri{Iri::fromIriref(stringWithBrackets)};
}

// ____________________________________________________________________________
LiteralOrIri LiteralOrIri::prefixedIri(const Iri& prefix,
                                       std::string_view suffix) {
  return LiteralOrIri{Iri::fromPrefixAndSuffix(prefix, suffix)};
}

}  // namespace ad_utility::triple_component

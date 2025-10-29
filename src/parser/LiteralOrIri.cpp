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
LiteralOrIri::LiteralOrIri(Iri iri, const IndexImpl* index)
    : data_{std::move(iri)}, index_{index} {
  AD_CONTRACT_CHECK(index_ != nullptr,
                    "LiteralOrIri requires a non-null IndexImpl pointer");
}

// __________________________________________
LiteralOrIri::LiteralOrIri(Literal literal, const IndexImpl* index)
    : data_{std::move(literal)}, index_{index} {
  AD_CONTRACT_CHECK(index_ != nullptr,
                    "LiteralOrIri requires a non-null IndexImpl pointer");
}

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
Iri& LiteralOrIri::getIri() {
  if (!isIri()) {
    AD_CONTRACT_CHECK(isIri(),
                      "LiteralOrIri object does not contain an Iri object and "
                      "thus cannot return it");
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
  AD_CONTRACT_CHECK(isLiteral(),
                    "LiteralOrIri object does not contain a Literal object and "
                    "thus cannot return it");
  return std::get<Literal>(data_);
}

// __________________________________________
Literal& LiteralOrIri::getLiteral() {
  AD_CONTRACT_CHECK(isLiteral(),
                    "LiteralOrIri object does not contain a Literal object and "
                    "thus cannot return it");
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
LiteralOrIri LiteralOrIri::iriref(const std::string& stringWithBrackets,
                                  const IndexImpl* index) {
  AD_CONTRACT_CHECK(index != nullptr,
                    "LiteralOrIri requires a non-null IndexImpl pointer");
  return LiteralOrIri{Iri::fromIriref(stringWithBrackets), index};
}

// __________________________________________
LiteralOrIri LiteralOrIri::prefixedIri(const Iri& prefix,
                                       std::string_view suffix,
                                       const IndexImpl* index) {
  AD_CONTRACT_CHECK(index != nullptr,
                    "LiteralOrIri requires a non-null IndexImpl pointer");
  return LiteralOrIri{Iri::fromPrefixAndSuffix(prefix, suffix), index};
}

// __________________________________________
LiteralOrIri LiteralOrIri::literalWithQuotes(
    std::string_view rdfContentWithQuotes,
    const IndexImpl* index,
    std::optional<std::variant<Iri, std::string>> descriptor) {
  AD_CONTRACT_CHECK(index != nullptr,
                    "LiteralOrIri requires a non-null IndexImpl pointer");
  return LiteralOrIri(Literal::fromEscapedRdfLiteral(rdfContentWithQuotes,
                                                     std::move(descriptor)),
                     index);
}

// __________________________________________
LiteralOrIri LiteralOrIri::literalWithoutQuotes(
    std::string_view rdfContentWithoutQuotes,
    const IndexImpl* index,
    std::optional<std::variant<Iri, std::string>> descriptor) {
  AD_CONTRACT_CHECK(index != nullptr,
                    "LiteralOrIri requires a non-null IndexImpl pointer");
  return LiteralOrIri(Literal::literalWithoutQuotes(rdfContentWithoutQuotes,
                                                    std::move(descriptor)),
                     index);
}

// ___________________________________________
ql::strong_ordering LiteralOrIri::compareThreeWay(
    const LiteralOrIri& rhs) const {
  // Use the index from either this or rhs (they should be the same or one
  // might be null). Prefer this->index_ if available.
  const IndexImpl* index = index_ ? index_ : rhs.index_;
  AD_CONTRACT_CHECK(index != nullptr,
                    "LiteralOrIri must have an associated IndexImpl for "
                    "three-way comparison");
  int i = index->getVocab().getCaseComparator().compare(
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
}  // namespace ad_utility::triple_component

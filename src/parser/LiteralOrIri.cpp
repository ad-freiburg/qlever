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

}  // namespace ad_utility::triple_component

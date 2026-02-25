// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "rdfTypes/Iri.h"

#include <utility>

using namespace std::string_view_literals;

namespace ad_utility::triple_component {

// ____________________________________________________________________________
template <bool isOwning>
BasicIri<isOwning>::BasicIri(StorageType iri) : iri_{std::move(iri)} {}

// ____________________________________________________________________________
template <bool isOwning>
NormalizedStringView BasicIri<isOwning>::getContent() const {
  return asNormalizedStringViewUnsafe(iri_).substr(1, iri_.size() - 2);
}

// ____________________________________________________________________________
template <bool isOwning>
BasicIri<isOwning> BasicIri<isOwning>::fromStringRepresentation(StorageType s) {
  AD_CORRECTNESS_CHECK(ql::starts_with(s, "<") || ql::starts_with(s, "@"));
  return BasicIri{std::move(s)};
}

template class BasicIri<true>;
template class BasicIri<false>;

}  // namespace ad_utility::triple_component

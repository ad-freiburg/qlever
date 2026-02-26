// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "rdfTypes/Iri.h"

#include <absl/strings/str_cat.h>

#include <utility>

#include "rdfTypes/RdfEscaping.h"
#include "util/Log.h"

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

// ____________________________________________________________________________
// Iri (owning) method implementations.
// ____________________________________________________________________________

Iri Iri::fromStringRepresentation(std::string s) {
  return BasicIri<true>::fromStringRepresentation(std::move(s));
}

// ____________________________________________________________________________
Iri Iri::fromIriref(std::string_view stringWithBrackets) {
  auto first = stringWithBrackets.find('<');
  AD_CORRECTNESS_CHECK(first != std::string_view::npos);
  return Iri{
      absl::StrCat(stringWithBrackets.substr(0, first + 1),
                   asStringViewUnsafe(RdfEscaping::normalizeIriWithBrackets(
                       stringWithBrackets.substr(first))),
                   ">")};
}

// ____________________________________________________________________________
Iri Iri::fromIrirefWithoutBrackets(std::string_view stringWithoutBrackets) {
  AD_CORRECTNESS_CHECK(!ql::starts_with(stringWithoutBrackets, '<') &&
                       !ql::ends_with(stringWithoutBrackets, '>'));
  return Iri{absl::StrCat("<", stringWithoutBrackets, ">")};
}

// ____________________________________________________________________________
Iri Iri::fromPrefixAndSuffix(const Iri& prefix, std::string_view suffix) {
  auto suffixNormalized = RdfEscaping::unescapePrefixedIri(suffix);
  return Iri{absl::StrCat(
      "<", asStringViewUnsafe(prefix.getContent()),
      asStringViewUnsafe(asNormalizedStringViewUnsafe(suffixNormalized)), ">")};
}

// ____________________________________________________________________________
Iri Iri::fromIrirefConsiderBase(std::string_view iriStringWithBrackets,
                                const Iri& basePrefixForRelativeIris,
                                const Iri& basePrefixForAbsoluteIris) {
  auto iriSv = iriStringWithBrackets;
  AD_CORRECTNESS_CHECK(iriSv.size() >= 2);
  AD_CORRECTNESS_CHECK(iriSv[0] == '<' && iriSv[iriSv.size() - 1] == '>');
  if (iriSv.find("://") != std::string_view::npos ||
      basePrefixForAbsoluteIris.empty()) {
    return fromIriref(iriSv);
  } else if (iriSv[1] == '/') {
    AD_CORRECTNESS_CHECK(!basePrefixForAbsoluteIris.empty());
    return fromPrefixAndSuffix(basePrefixForAbsoluteIris,
                               iriSv.substr(2, iriSv.size() - 3));
  } else {
    AD_CORRECTNESS_CHECK(!basePrefixForRelativeIris.empty());
    return fromPrefixAndSuffix(basePrefixForRelativeIris,
                               iriSv.substr(1, iriSv.size() - 2));
  }
}

// ____________________________________________________________________________
Iri Iri::getBaseIri(bool domainOnly) const {
  AD_CORRECTNESS_CHECK(ql::starts_with(iri_, '<') && ql::ends_with(iri_, '>'),
                       iri_);
  size_t pos = iri_.find(schemePattern);
  if (pos == std::string::npos) {
    AD_LOG_WARN << "No scheme found in base IRI: \"" << iri_ << "\""
                << " (but we accept it anyway)" << std::endl;
    pos = 1;
  } else {
    pos += schemePattern.size();
  }
  pos = iri_.find('/', pos);
  if (pos == std::string::npos ||
      (!domainOnly && iri_[iri_.size() - 2] != '/')) {
    return fromIrirefWithoutBrackets(
        absl::StrCat(std::string_view(iri_).substr(1, iri_.size() - 2), "/"));
  }
  if (domainOnly) {
    return fromIrirefWithoutBrackets(std::string_view(iri_).substr(1, pos));
  }
  return *this;
}

}  // namespace ad_utility::triple_component

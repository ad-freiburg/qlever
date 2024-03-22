// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "parser/Iri.h"

#include <utility>

#include "parser/LiteralOrIri.h"
#include "util/StringUtils.h"

namespace ad_utility::triple_component {
// __________________________________________
Iri::Iri(std::string iri) : iri_{std::move(iri)} {}

// __________________________________________
Iri::Iri(const Iri& prefix, NormalizedStringView suffix)
    : iri_{absl::StrCat("<"sv, asStringViewUnsafe(prefix.getContent()),
                        asStringViewUnsafe(suffix), ">"sv)} {};

// __________________________________________
NormalizedStringView Iri::getContent() const {
  return asNormalizedStringViewUnsafe(iri_).substr(1, iri_.size() - 2);
}

// __________________________________________
Iri Iri::fromIriref(std::string_view stringWithBrackets) {
  auto first = stringWithBrackets.find('<');
  AD_CORRECTNESS_CHECK(first != std::string_view::npos);
  return Iri{
      absl::StrCat(stringWithBrackets.substr(0, first + 1),
                   asStringViewUnsafe(RdfEscaping::normalizeIriWithBrackets(
                       stringWithBrackets.substr(first))),
                   ">"sv)};
}

// __________________________________________
Iri Iri::fromPrefixAndSuffix(const Iri& prefix, std::string_view suffix) {
  auto suffixNormalized = RdfEscaping::unescapePrefixedIri(suffix);
  return Iri{prefix, asNormalizedStringViewUnsafe(suffixNormalized)};
}

// __________________________________________
Iri Iri::fromStringRepresentation(std::string s) {
  AD_CORRECTNESS_CHECK(s.starts_with("<") || s.starts_with("@"));
  return Iri{std::move(s)};
}

// __________________________________________
const std::string& Iri::toStringRepresentation() const { return iri_; }
// __________________________________________
std::string& Iri::toStringRepresentation() { return iri_; }

}  // namespace ad_utility::triple_component

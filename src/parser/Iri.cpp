// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "parser/Iri.h"

#include <utility>

#include "parser/LiteralOrIri.h"
#include "util/StringUtils.h"

namespace ad_utility::triple_component {
// __________________________________________
Iri::Iri(NormalizedString iri) : iri_{std::move(iri)} {}

// __________________________________________
Iri::Iri(const Iri& prefix, NormalizedStringView suffix)
    : iri_{NormalizedString{prefix.getContent()} + suffix} {};

// __________________________________________
NormalizedStringView Iri::getContent() const { return iri_; }

// __________________________________________
Iri Iri::iriref(std::string_view stringWithBrackets) {
  return Iri{RdfEscaping::normalizeIriWithBrackets(stringWithBrackets)};
}

// __________________________________________
Iri Iri::prefixed(const Iri& prefix, std::string_view suffix) {
  auto suffixNormalized = RdfEscaping::unescapePrefixedIri(suffix);
  return Iri{std::move(prefix), asNormalizedStringViewUnsafe(suffixNormalized)};
}

Iri Iri::fromInternalRepresentation(std::string_view s) {
  // TODO<joka921> check the tag.
  s.remove_prefix(1);
  s.remove_suffix(1);
  return Iri{NormalizedString{asNormalizedStringViewUnsafe(s)}};
}
std::string Iri::toInternalRepresentation() const {
  static_assert(iriPrefix == "<");
  return absl::StrCat(iriPrefix, asStringViewUnsafe(getContent()), ">");
}

}  // namespace ad_utility::triple_component

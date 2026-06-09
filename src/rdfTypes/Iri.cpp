// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
// 2024 Hannah Bast <bast@cs.uni-freiburg.de>
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "rdfTypes/Iri.h"

#include <absl/strings/str_cat.h>

#include <ctre-unicode.hpp>
#include <utility>

#include "rdfTypes/RdfEscaping.h"
#include "util/Log.h"

using namespace std::string_view_literals;

namespace ad_utility::triple_component {

namespace {
// CTRE regex (C++17-compatible) for QLever's internal IRI string
// representation: an optional internal `@langtag@` prefix followed by an
// angle-bracketed IRI whose content excludes control characters, spaces and the
// characters disallowed in an IRI reference. This is the same validation that
// the (now removed) `parser/data/Iri` constructor performed.
constexpr ctll::fixed_string iriValidationRegex =
    "(?:@[a-zA-Z]+(?:-(?:[a-zA-Z]|\\d)+)*@)?"
    "<[^<>\"{}|^\\\\`\\0- ]*>";
}  // namespace

// ____________________________________________________________________________
template <bool isOwning>
BasicIri<isOwning>::BasicIri(StorageType iri) : iri_{std::move(iri)} {
  // Validate that the stored string is a well-formed QLever-internal IRI
  // representation. This is the single choke point for all factories
  // (`fromStringRepresentation`, `fromIriref`, `fromIrirefWithoutBrackets`,
  // `fromPrefixAndSuffix`), so the check applies once the internal form is
  // finalized (in particular after `fromIriref` has normalized its input).
  AD_CONTRACT_CHECK(ctre::match<iriValidationRegex>(iri_));
}

// ____________________________________________________________________________
template <bool isOwning>
NormalizedStringView BasicIri<isOwning>::getContent() const {
  return asNormalizedStringViewUnsafe(iri_).substr(1, iri_.size() - 2);
}

// ____________________________________________________________________________
template <bool isOwning>
BasicIri<isOwning> BasicIri<isOwning>::fromStringRepresentation(StorageType s) {
  // The validity of the internal representation is checked in the constructor.
  return BasicIri{std::move(s)};
}

template class BasicIri<true>;
template class BasicIri<false>;

// ____________________________________________________________________________
// Iri (owning) method implementations.
// ____________________________________________________________________________

Iri Iri::fromStringRepresentation(std::string s) {
  return Iri{BasicIri<true>::fromStringRepresentation(std::move(s))};
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
    // Case 1: IRI with scheme (like `<http://...>`) or `BASE_IRI_FOR_TESTING`
    // (which is `<@>`, and no valid base IRI has length 3).
    return fromIriref(iriSv);
  } else if (iriSv[1] == '/') {
    // Case 2: Absolute IRI without scheme (like `</prosite/PS51927>`).
    AD_CORRECTNESS_CHECK(!basePrefixForAbsoluteIris.empty());
    return fromPrefixAndSuffix(basePrefixForAbsoluteIris,
                               iriSv.substr(2, iriSv.size() - 3));
  } else {
    // Case 3: Relative IRI (like `<UPI001AF4585D>`).
    AD_CORRECTNESS_CHECK(!basePrefixForRelativeIris.empty());
    return fromPrefixAndSuffix(basePrefixForRelativeIris,
                               iriSv.substr(1, iriSv.size() - 2));
  }
}

// ____________________________________________________________________________
Iri Iri::getBaseIri(bool domainOnly) const {
  AD_CORRECTNESS_CHECK(ql::starts_with(iri(), '<') && ql::ends_with(iri(), '>'),
                       iri());
  // Check if we have a scheme and find the first `/` after that (or the first
  // `/` at all if there is no scheme).
  size_t pos = iri().find(schemePattern);
  if (pos == std::string::npos) {
    AD_LOG_WARN << "No scheme found in base IRI: \"" << iri() << "\""
                << " (but we accept it anyway)" << std::endl;
    pos = 1;
  } else {
    pos += schemePattern.size();
  }
  pos = iri().find('/', pos);
  // Return the IRI with `/` appended in the following two cases: the IRI has
  // the empty path, or `domainOnly` is false and the final `/` is missing.
  if (pos == std::string::npos ||
      (!domainOnly && iri()[iri().size() - 2] != '/')) {
    return fromIrirefWithoutBrackets(
        absl::StrCat(std::string_view(iri()).substr(1, iri().size() - 2), "/"));
  }
  // If `domainOnly` is true, remove the path part.
  if (domainOnly) {
    return fromIrirefWithoutBrackets(std::string_view(iri()).substr(1, pos));
  }
  // Otherwise, return the IRI as is.
  return *this;
}

}  // namespace ad_utility::triple_component

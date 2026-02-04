// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "rdfTypes/Iri.h"

#include <absl/strings/str_cat.h>

#include <utility>

#include "backports/StartsWithAndEndsWith.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Log.h"
#include "util/StringUtils.h"

using namespace std::string_view_literals;

namespace ad_utility::triple_component {
// ____________________________________________________________________________
Iri::Iri(std::string iri) : iri_{std::move(iri)} {}

// ____________________________________________________________________________
Iri::Iri(const Iri& prefix, NormalizedStringView suffix)
    : iri_{absl::StrCat("<"sv, asStringViewUnsafe(prefix.getContent()),
                        asStringViewUnsafe(suffix), ">"sv)} {};

// ____________________________________________________________________________
NormalizedStringView Iri::getContent() const {
  return asNormalizedStringViewUnsafe(iri_).substr(1, iri_.size() - 2);
}

// ____________________________________________________________________________
Iri Iri::fromIriref(std::string_view stringWithBrackets) {
  auto first = stringWithBrackets.find('<');
  AD_CORRECTNESS_CHECK(first != std::string_view::npos);
  return Iri{
      absl::StrCat(stringWithBrackets.substr(0, first + 1),
                   asStringViewUnsafe(RdfEscaping::normalizeIriWithBrackets(
                       stringWithBrackets.substr(first))),
                   ">"sv)};
}

// ____________________________________________________________________________
Iri Iri::fromIrirefWithoutBrackets(std::string_view stringWithoutBrackets) {
  AD_CORRECTNESS_CHECK(!ql::starts_with(stringWithoutBrackets, '<') &&
                       !ql::ends_with(stringWithoutBrackets, '>'));
  return Iri{absl::StrCat("<"sv, stringWithoutBrackets, ">"sv)};
}

// ____________________________________________________________________________
Iri Iri::fromPrefixAndSuffix(const Iri& prefix, std::string_view suffix) {
  auto suffixNormalized = RdfEscaping::unescapePrefixedIri(suffix);
  return Iri{prefix, asNormalizedStringViewUnsafe(suffixNormalized)};
}

// ____________________________________________________________________________
Iri Iri::getBaseIri(bool domainOnly) const {
  AD_CORRECTNESS_CHECK(ql::starts_with(iri_, '<') && ql::ends_with(iri_, '>'),
                       iri_);
  // Check if we have a scheme and find the first `/` after that (or the first
  // `/` at all if there is no scheme).
  size_t pos = iri_.find(schemePattern);
  if (pos == std::string::npos) {
    AD_LOG_WARN << "No scheme found in base IRI: \"" << iri_ << "\""
                << " (but we accept it anyway)" << std::endl;
    pos = 1;
  } else {
    pos += schemePattern.size();
  }
  pos = iri_.find('/', pos);
  // Return the IRI with `/` appended in the following two cases: the IRI has
  // the empty path, or `domainOnly` is false and the final `/` is missing.
  if (pos == std::string::npos ||
      (!domainOnly && iri_[iri_.size() - 2] != '/')) {
    return fromIrirefWithoutBrackets(
        absl::StrCat(std::string_view(iri_).substr(1, iri_.size() - 2), "/"sv));
  }
  // If `domainOnly` is true, remove the path part.
  if (domainOnly) {
    return fromIrirefWithoutBrackets(std::string_view(iri_).substr(1, pos));
  }
  // Otherwise, return the IRI as is.
  return *this;
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
Iri Iri::fromStringRepresentation(std::string s) {
  AD_CORRECTNESS_CHECK(ql::starts_with(s, "<") || ql::starts_with(s, "@"));
  return Iri{std::move(s)};
}

// ____________________________________________________________________________
const std::string& Iri::toStringRepresentation() const& { return iri_; }

// ____________________________________________________________________________
std::string Iri::toStringRepresentation() && { return std::move(iri_); }

}  // namespace ad_utility::triple_component

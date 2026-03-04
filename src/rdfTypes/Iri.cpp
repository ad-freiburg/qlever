// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "rdfTypes/Iri.h"

#include <absl/cleanup/cleanup.h>
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
                                const UriUriA& baseUri) {
  auto iriSv = iriStringWithBrackets;
  AD_CORRECTNESS_CHECK(iriSv.size() >= 2);
  AD_CORRECTNESS_CHECK(iriSv[0] == '<' && iriSv[iriSv.size() - 1] == '>');
  UriUriA relativeIri;
  auto parseResult =
      uriParseSingleUriExA(&relativeIri, iriSv.data() + 1,
                           iriSv.data() + (iriSv.size() - 1), nullptr);
  absl::Cleanup cleanupRelativeIri{
      [&relativeIri]() { uriFreeUriMembersA(&relativeIri); }};
  AD_CONTRACT_CHECK(parseResult == URI_SUCCESS);
  UriUriA resolvedIri;
  auto resolveResult = uriAddBaseUriExA(&resolvedIri, &relativeIri, &baseUri,
                                        URI_RESOLVE_STRICTLY);
  absl::Cleanup cleanupResolvedIri{
      [&resolvedIri]() { uriFreeUriMembersA(&resolvedIri); }};
  AD_CONTRACT_CHECK(resolveResult == URI_SUCCESS);
  return fromUri(resolvedIri);
}

// ____________________________________________________________________________
Iri Iri::fromUri(const UriUriA& uri) {
  int charsRequired;
  auto printResult = uriToStringCharsRequiredA(&uri, &charsRequired);
  AD_CORRECTNESS_CHECK(printResult == URI_SUCCESS);
  std::string targetIri;
  targetIri.resize(charsRequired + 2);
  targetIri.at(0) = '<';
  printResult =
      uriToStringA(targetIri.data() + 1, &uri, charsRequired + 1, nullptr);
  AD_CORRECTNESS_CHECK(printResult == URI_SUCCESS);
  targetIri.back() = '>';
  return fromStringRepresentation(std::move(targetIri));
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

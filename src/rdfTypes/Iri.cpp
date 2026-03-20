// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
// 2024 Hannah Bast <bast@cs.uni-freiburg.de>
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "rdfTypes/Iri.h"

#include <absl/cleanup/cleanup.h>
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

}  // namespace ad_utility::triple_component

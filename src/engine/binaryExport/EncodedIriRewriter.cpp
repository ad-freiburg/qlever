// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "engine/binaryExport/EncodedIriRewriter.h"

#include <stdexcept>
#include <utility>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "engine/binaryExport/StringMapping.h"

namespace qlever::binary_export {

// _____________________________________________________________________________
EncodedIriRewriter::EncodedIriRewriter(
    const Index& index, std::vector<encodedIri::Pattern> remotePatterns)
    : index_{&index},
      remotePatterns_{std::move(remotePatterns)},
      prefixMapping_{
          getPrefixMapping(index.encodedIriManager(), remotePatterns_)} {}

// _____________________________________________________________________________
Id EncodedIriRewriter::operator()(Id id, LocalVocab& localVocab) {
  auto [prefixIdx, payload] =
      EncodedIriManager::splitIntoPrefixIdxAndPayload(id);
  if (prefixIdx >= prefixMapping_.size()) {
    throw std::runtime_error{
        "The result in QLever's binary export format contains an encoded IRI "
        "with an unknown pattern"};
  }
  // The common case: the pattern also exists locally (and is the one that the
  // local instance would use for the IRI), which requires no lookup.
  if (auto localPrefixIdx = prefixMapping_[prefixIdx]) {
    return EncodedIriManager::makeIdFromPrefixIdxAndPayload(
        localPrefixIdx.value(), payload);
  }
  // Otherwise the decoded IRI might be encoded with a different local pattern.
  auto [it, inserted] = decodedIriCache_.try_emplace(id.getBits(), Id{});
  if (inserted) {
    it->second = stringToLocalId(
        encodedIri::decodeToIri(remotePatterns_[prefixIdx], payload), *index_,
        localVocab);
  }
  return it->second;
}

// _____________________________________________________________________________
std::vector<std::optional<uint8_t>> EncodedIriRewriter::getPrefixMapping(
    const EncodedIriManager& localManager,
    const std::vector<encodedIri::Pattern>& remotePatterns) {
  const auto& localPatterns = localManager.patterns_;
  // Return true if an IRI might match both patterns, which requires that the
  // prefix of one of them is a prefix of the other one.
  auto mightMatchSameIri = [](const encodedIri::Pattern& a,
                              const encodedIri::Pattern& b) {
    return ql::starts_with(a.prefix_, b.prefix_) ||
           ql::starts_with(b.prefix_, a.prefix_);
  };
  std::vector<std::optional<uint8_t>> prefixMapping;
  prefixMapping.reserve(remotePatterns.size());
  for (const auto& pattern : remotePatterns) {
    auto patternIt = ql::ranges::find(localPatterns, pattern);
    // The local instance encodes an IRI with the first pattern that it
    // matches, so the equal local pattern can only be used directly if none
    // of the local patterns before it might match the same IRI.
    bool isFirstMatch = ql::ranges::none_of(
        localPatterns.begin(), patternIt, [&](const auto& earlier) {
          return mightMatchSameIri(earlier, pattern);
        });
    prefixMapping.push_back(
        patternIt == localPatterns.end() || !isFirstMatch
            ? std::nullopt
            : std::optional{static_cast<uint8_t>(
                  ql::ranges::distance(localPatterns.begin(), patternIt))});
  }
  return prefixMapping;
}

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

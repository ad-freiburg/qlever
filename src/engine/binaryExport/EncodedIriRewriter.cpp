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
  // The common case: the pattern also exists locally, which requires no
  // lookup.
  if (auto localPrefixIdx = prefixMapping_[prefixIdx]) {
    return EncodedIriManager::makeIdFromPrefixIdxAndPayload(
        localPrefixIdx.value(), payload);
  }
  // Otherwise the decoded IRI might be encoded with a different local pattern.
  auto [it, inserted] = decodedIris_.try_emplace(id.getBits(), Id{});
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
  std::vector<std::optional<uint8_t>> prefixMapping;
  prefixMapping.reserve(remotePatterns.size());
  for (const auto& pattern : remotePatterns) {
    auto patternIt = ql::ranges::find(localPatterns, pattern);
    prefixMapping.push_back(
        patternIt == localPatterns.end()
            ? std::nullopt
            : std::optional{static_cast<uint8_t>(
                  ql::ranges::distance(localPatterns.begin(), patternIt))});
  }
  return prefixMapping;
}

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BINARYEXPORT_ENCODEDIRIREWRITER_H
#define QLEVER_SRC_ENGINE_BINARYEXPORT_ENCODEDIRIREWRITER_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <cstdint>
#include <optional>
#include <vector>

#include "global/Id.h"
#include "index/Index.h"
#include "index/LocalVocab.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "util/HashMap.h"

namespace qlever::binary_export {

// Convert the encoded IRIs (IDs of type `EncodedVal`) of the exporting
// instance to IDs of the local instance. The exporter sends these IDs as they
// are, together with the patterns of its `EncodedIriManager` (see `Header` in
// `BinaryFormat.h`), which might differ from the local ones.
class EncodedIriRewriter {
  const Index* index_;
  std::vector<encodedIri::Pattern> remotePatterns_;
  // For each remote pattern the index of the equal local pattern, if there is
  // one, see `getPrefixMapping`.
  std::vector<std::optional<uint8_t>> prefixMapping_;
  // A cache of the IRIs that were decoded since the last call to
  // `invalidateCache`.
  ad_utility::HashMap<Id::T, Id> decodedIriCache_;

 public:
  EncodedIriRewriter(const Index& index,
                     std::vector<encodedIri::Pattern> remotePatterns);

  // If the pattern of the `id` also exists locally, only change the index of
  // the pattern. Otherwise decode the IRI and convert it to a local `Id` (see
  // `stringToLocalId`), which might add it to the `localVocab`. Each IRI is
  // decoded at most once until the next call to `invalidateCache`.
  Id operator()(Id id, LocalVocab& localVocab);

  // Clear the cache of decoded IRIs, which is required when the `localVocab`
  // that was passed to `operator()` is replaced, as the cached IDs might point
  // into it.
  void invalidateCache() { decodedIriCache_.clear(); }

  // Get the mapping from the indices of the `remotePatterns` to the indices of
  // the equal patterns of the `localManager` (`std::nullopt` for a remote
  // pattern that doesn't exist locally).
  static std::vector<std::optional<uint8_t>> getPrefixMapping(
      const EncodedIriManager& localManager,
      const std::vector<encodedIri::Pattern>& remotePatterns);
};

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_BINARYEXPORT_ENCODEDIRIREWRITER_H

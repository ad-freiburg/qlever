// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025        Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BINARY_EXPORT_STRINGMAPPING_H
#define QLEVER_SRC_ENGINE_BINARY_EXPORT_STRINGMAPPING_H

#include <string>
#include <vector>

#include "global/Id.h"
#include "index/LocalVocab.h"
#include "util/HashMap.h"

// Forward declaration
class Index;

namespace qlever::binary_export {
// A helper class for the efficient binary export, that collects the unique
// non-trivial IDs (IDs that point to literals or IRIs that are not encoded
// directly in the ID) for a batch of IDs. It assigns a unique index to each of
// those IDs and resolves it to its string right away, so the size of a batch
// is known.
// The counterpart on the importing side is the `IdRewriter` (see
// `IdRewriter.h`).
class StringMapping {
  const Index* index_;
  // Only required for the signature of `idToLiteralOrIri`. A `LocalVocabIndex`
  // directly points to its word, so the local vocab is never accessed. It is a
  // member, because constructing a `LocalVocab` allocates.
  LocalVocab dummyLocalVocab_;
  // Map the bits of an ID to its unique index wrt the current batch (the first
  // ID gets index 0, the second ID 1, and so on). Using the bits is cheaper
  // than `Id`, whose hash looks up a `LocalVocabIndex` in the vocabulary. The
  // same word with two different IDs is then sent twice, which is harmless.
  ad_utility::HashMap<Id::T, uint64_t> stringMapping_;
  // The string of the ID with index `i` is at position `i`.
  // TODO<joka921, RobinTF> Store the strings contiguously in a single buffer
  // (e.g. a monotonic buffer with `string_view`s or offsets) to avoid one
  // allocation per string.
  std::vector<std::string> strings_;
  // The total size of the strings in `strings_`.
  size_t numBytes_ = 0;

 public:
  explicit StringMapping(const Index& index) : index_{&index} {}

  // Return the strings of the current batch (ordered by their index) and start
  // a new batch. The indices of the new batch again start at 0, so an index
  // returned by `remapId` refers to a string of its batch only.
  std::vector<std::string> takeStrings();

  // Remap an `Id` to another `Id` which internally uses the `LocalVocab`
  // datatype, but instead of a pointer it uses the index provided by the
  // `stringMapping_`. If `remapId` was previously called for the same ID in
  // this batch, the same index will be used, otherwise the next free index.
  Id remapId(Id id);

  // The number of distinct IDs in the current batch.
  size_t size() const { return stringMapping_.size(); }

  // The total size of the strings of the current batch in bytes.
  size_t numBytes() const { return numBytes_; }

  // Const access to the string mapping.
  const auto& stringMappingForTesting() const { return stringMapping_; }
};
}  // namespace qlever::binary_export

#endif  // QLEVER_SRC_ENGINE_BINARY_EXPORT_STRINGMAPPING_H

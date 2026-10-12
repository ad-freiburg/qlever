// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_STRING_MAPPING_H
#define QLEVER_SRC_ENGINE_STRING_MAPPING_H

#include <string>
#include <vector>

#include "global/Id.h"
#include "util/HashMap.h"

// Forward declaration
class Index;

namespace qlever::binary_export {
// A helper class for the efficient binary export, that collects the unique
// non-trivial IDs (IDs that point to literals or IRIs that are not encoded
// directly in the ID) for a batch of IDs. It assigns a unique index to each of
// those IDs, and resolves each of them to the corresponding string as soon as
// it is seen for the first time (such that the size of the strings of a batch
// is known, and the IDs don't have to stay valid until the end of the batch).
// The counterpart on the importing side is the `IdRewriter` (see
// `BinaryExport.h`).
class StringMapping {
  // The index that is used to resolve the IDs to strings.
  const Index* index_;
  // Store the actual mapping from an ID to the unique index (wrt the current
  // batch). (The first newly inserted ID will get index 0, the second ID 1, and
  // so on.)
  // TODO<RobinTF, joka921> consider mapping from `Id::T` instead of `Id` to get
  // a cheaper lookup.
  ad_utility::HashMap<Id, uint64_t> stringMapping_;
  // The strings of the IDs of the current batch, the string of the ID with
  // the unique index `i` is stored at position `i`.
  std::vector<std::string> strings_;
  // The total size of the `strings_` in bytes.
  size_t numBytes_ = 0;

 public:
  explicit StringMapping(const Index& index) : index_{&index} {}

  // Return the strings of the IDs of the current batch (see above): For each
  // of the unique IDs, the string to which it is resolved via
  // `ql::exportIds::idToLiteralOrIri` is stored at position
  // `stringMapping_[ID]` in the result, so the vector is sorted by the order in
  // which `remapId` was first called for the IDs. Calling `takeStrings` also
  // clears the mapping, which means that the indices will be reused thereafter.
  std::vector<std::string> takeStrings();

  // Remap an `Id` to another `Id` which internally uses the `LocalVocab`
  // datatype, but instead of a pointer it uses the index provided by the
  // `stringMapping_`. If `remapId` was previously called for the same ID (since
  // the last call to `takeStrings`), the same result index will be used,
  // otherwise the next free index will be assigned and the ID is resolved to
  // its string.
  Id remapId(Id id);

  // Return the number of distinct `ID`s for which `remapId` has been called
  // since the last call to `takeStrings()`.
  size_t size() const { return stringMapping_.size(); }

  // Return the total size of the strings of these IDs in bytes.
  size_t numBytes() const { return numBytes_; }

  // Const access to the string mapping.
  const auto& stringMappingForTesting() const { return stringMapping_; }
};
}  // namespace qlever::binary_export

#endif  // QLEVER_SRC_ENGINE_STRING_MAPPING_H

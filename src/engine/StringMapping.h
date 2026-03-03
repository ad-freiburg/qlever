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
// those IDs, and at the end of a batch resolves all of the unique IDs to the
// corresponding strings.
class StringMapping {
  // Store the actual mapping from an ID to the unique index (wrt the current
  // batch). (The first newly inserted ID will get index 0, the second ID 1, and
  // so on.)
  // TODO<RobinTF, joka921> consider mapping from `Id::T` instead of `Id` to get
  // a cheaper lookup.
  ad_utility::HashMap<Id, uint64_t> stringMapping_;

 public:
  // Create a vector with `stringMapping_.size()` entries as follows: For each
  // of the unique `IDs` in the `stringMapping_` resolve the ID to a string via
  // `ExportQueryExecutionTrees::idToLiteralOrIri`. the result will be stored at
  // position `stringMapping_[ID]` in the result, so the vector will be sorted
  // by the order in which `remapId` was called for the IDs.
  // Calling `flush` also clears the `stringMapping_`, which means that the
  // indices will be reused thereafter.
  std::vector<std::string> flush(const Index& index);

  // Remap an `Id` to another `Id` which internally uses the `LocalVocab`
  // datatype, but instead of a pointer it uses the index provided by the
  // `stringMapping_`. If `remapId` was previously called for the same ID, the
  // same result index will be used, otherwise the next free index will be
  // assigned.
  Id remapId(Id id);

  // Return the number of distinct `ID`s for which `remapId` has been called
  // since the last call to `flush()`.
  size_t size() const { return stringMapping_.size(); }

  // Const access to the string mapping.
  const auto& stringMappingForTesting() const { return stringMapping_; }
};
}  // namespace qlever::binary_export

#endif  // QLEVER_SRC_ENGINE_STRING_MAPPING_H

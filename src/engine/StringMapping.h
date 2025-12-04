// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_STRING_MAPPING_H
#define QLEVER_SRC_ENGINE_STRING_MAPPING_H

#include <string>
#include <vector>

#include "global/Id.h"
#include "util/HashMap.h"

// Forward declaration
class Index;

namespace qlever::binary_export {
// Helper struct to temporarily store strings and aggregate them to avoid
// sending the same strings over and over again.
struct StringMapping {
  // Store the actual mapping from an id to the corresponding insertion order.
  // (The first newly inserted string will get id 0, the second id 1, and so
  // on.)
  // TODO<RobinTF, joka921> consider mapping from `Id::T` instead of `Id` to get
  // a cheaper lookup.
  ad_utility::HashMap<Id, uint64_t> stringMapping_;
  uint64_t numProcessedRows_ = 0;

  // Serialize the internal datastructure to a string and clear it.
  std::vector<std::string> flush(const Index& index);

  // Increment the row counter by one if the mapping contains elements.
  void nextRow() {
    if (!stringMapping_.empty()) {
      numProcessedRows_++;
    }
  }

  // Remap an `Id` to another `Id` which internally uses the `LocalVocab`
  // datatype, but instead of a pointer it uses the index provided by
  // `stringMapping_`, creating a new index if not already present.
  Id remapId(Id id);

  // Return true if the datastructure has grown large enough, or wasn't
  // cleared recently enough to avoid stalling the consumer.
  bool needsFlush() const {
    return numProcessedRows_ >= 100'000 || stringMapping_.size() >= 10'000;
  }
};
}  // namespace qlever::binary_export

#endif  // QLEVER_SRC_ENGINE_STRING_MAPPING_H

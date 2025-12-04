// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>

#include "engine/StringMapping.h"

#include "index/Index.h"

namespace qlever::binary_export {

// _____________________________________________________________________________
std::vector<std::string> StringMapping::flush(const Index& index) {
  numProcessedRows_ = 0;
  std::vector<std::string> sortedStrings;
  sortedStrings.resize(stringMapping_.size());
  for (auto& [oldId, newId] : stringMapping_) {
    auto type = oldId.getDatatype();
    if (type == Datatype::LocalVocabIndex) {
      sortedStrings[newId] =
          oldId.getLocalVocabIndex()->toStringRepresentation();
    } else {
      // TODO<joka921, RobinTF>, make the list exhaustive.
      AD_CONTRACT_CHECK(type == Datatype::VocabIndex);
      // TODO<joka921> Deduplicate on the level of IDs for the string mapping.
      sortedStrings[newId] = index.indexToString(oldId.getVocabIndex());
    }
  }
  stringMapping_.clear();
  return sortedStrings;
}

// _____________________________________________________________________________
Id StringMapping::remapId(Id id) {
  size_t distinctIndex = 0;
  if (stringMapping_.contains(id)) {
    distinctIndex = stringMapping_.at(id);
  } else {
    distinctIndex = stringMapping_[id] = stringMapping_.size();
  }
  // The shift is required to imitate the unused bits of a pointer.
  return Id::makeFromLocalVocabIndex(
      reinterpret_cast<LocalVocabIndex>(distinctIndex << Id::numDatatypeBits));
}

}  // namespace qlever::binary_export

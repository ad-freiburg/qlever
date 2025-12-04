// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>

#include "engine/StringMapping.h"
#include "engine/ExportQueryExecutionTrees.h"

#include "index/Index.h"

namespace qlever::binary_export {

// _____________________________________________________________________________
std::vector<std::string> StringMapping::flush(const Index& index) {
  LocalVocab dummy;
  numProcessedRows_ = 0;
  std::vector<std::string> sortedStrings;
  sortedStrings.resize(stringMapping_.size());
  for (auto& [oldId, newId] : stringMapping_) {
    auto literalOrIri =
        ExportQueryExecutionTrees::idToLiteralOrIri(index, oldId, dummy, true);
    AD_CORRECTNESS_CHECK(literalOrIri.has_value());
    sortedStrings[newId] =
        std::move(literalOrIri.value()).toStringRepresentation();
  }
  stringMapping_.clear();
  return sortedStrings;
}

// _____________________________________________________________________________
Id StringMapping::remapId(Id id) {
  static constexpr std::array allowedDatatypes{
    Datatype::VocabIndex, Datatype::LocalVocabIndex,
    Datatype::TextRecordIndex, Datatype::WordVocabIndex};
  AD_EXPENSIVE_CHECK(ad_utility::contains(allowedDatatypes, id.getDatatype()));
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

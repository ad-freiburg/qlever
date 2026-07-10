// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>

#include "engine/StringMapping.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "index/ExportIds.h"
#include "index/Index.h"

namespace qlever::binary_export {

// _____________________________________________________________________________
std::vector<std::string> StringMapping::flush(const Index& index) {
  LocalVocab dummy;
  std::vector<std::string> sortedStrings;
  sortedStrings.resize(stringMapping_.size());
  for (const auto& [oldId, newId] : stringMapping_) {
    auto literalOrIri =
        ql::exportIds::idToLiteralOrIri(index, oldId, dummy, true);
    AD_CORRECTNESS_CHECK(literalOrIri.has_value());
    sortedStrings[newId] =
        std::move(literalOrIri.value()).toStringRepresentation();
  }
  stringMapping_.clear();
  return sortedStrings;
}

// _____________________________________________________________________________
Id StringMapping::remapId(Id id) {
  using enum Datatype;
  // The datatypes that can be passed to a string mapping are exactly the
  // datatypes that semantically point to strings (so literals/IRIs that can't
  // be directly encoded into the ID). All other IDs have to be serialized by
  // different mechanism.
  static constexpr std::array allowedDatatypes{VocabIndex, LocalVocabIndex,
                                               TextRecordIndex, WordVocabIndex};
  AD_EXPENSIVE_CHECK(ad_utility::contains(allowedDatatypes, id.getDatatype()));

  // A static assertion that each datatype is either `trivial`, or `allowed`
  // (see above), or `BlankNodeIndex` (which also requires special handling and
  // remapping, but cannot be handled by the `StringMapping`.
  static constexpr auto checkDatatypes = []() {
    auto checkType = [](Datatype datatype) {
      return ad_utility::contains(allowedDatatypes, datatype) ||
             isDatatypeTrivial(datatype) || datatype == BlankNodeIndex ||
             datatype == EncodedVal;
    };
    for (size_t i = 0; i <= static_cast<size_t>(MaxValue); ++i) {
      if (!checkType(static_cast<Datatype>(i))) {
        return false;
      }
    }
    return true;
  };
  static_assert(checkDatatypes());
  size_t distinctIndex =
      stringMapping_.try_emplace(id, stringMapping_.size()).first->second;
  // `Id::makeFromLocalVocabIndex` stores the pointer verbatim, so we can
  // directly use the `distinctIndex` as a fake pointer.
  return Id::makeFromLocalVocabIndex(
      reinterpret_cast<::LocalVocabIndex>(distinctIndex));
}

}  // namespace qlever::binary_export

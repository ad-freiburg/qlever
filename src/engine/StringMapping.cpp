// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>

#include "engine/StringMapping.h"

#include <utility>

#include "engine/ExportQueryExecutionTrees.h"
#include "index/ExportIds.h"
#include "index/Index.h"

namespace qlever::binary_export {

// _____________________________________________________________________________
std::vector<std::string> StringMapping::takeStrings() {
  stringMapping_.clear();
  numBytes_ = 0;
  return std::exchange(strings_, {});
}

// _____________________________________________________________________________
Id StringMapping::remapId(Id id) {
  using enum Datatype;
  // The datatypes that can be passed to a string mapping are exactly the
  // datatypes that semantically point to strings (so literals/IRIs that can't
  // be directly encoded into the ID). All other IDs have to be serialized by
  // different mechanism.
  static constexpr std::array allowedDatatypes{VocabIndex, LocalVocabIndex,
                                               TextRecordIndex, WordVocabIndex,
                                               SecondaryVocabIndex};
  AD_EXPENSIVE_CHECK(ad_utility::contains(allowedDatatypes, id.getDatatype()));

  // A static assertion that each datatype is either `trivial`, or `allowed`
  // (see above), or `EncodedVal`, or `BlankNodeIndex`. The latter two are not
  // sent as strings: An `EncodedVal` is sent as is (together with the patterns
  // of its encoding), and a `BlankNodeIndex` is remapped by the importer.
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

  // A new ID gets the next free index and is resolved to its string right
  // away, so that `numBytes()` always accounts for all IDs of the batch.
  auto [it, inserted] =
      stringMapping_.try_emplace(id.getBits(), stringMapping_.size());
  size_t distinctIndex = it->second;
  if (inserted) {
    auto literalOrIri =
        ql::exportIds::idToLiteralOrIri(*index_, id, dummyLocalVocab_, true);
    AD_CORRECTNESS_CHECK(literalOrIri.has_value());
    strings_.push_back(
        std::move(literalOrIri.value()).toStringRepresentation());
    numBytes_ += strings_.back().size();
    AD_CORRECTNESS_CHECK(strings_.size() == stringMapping_.size());
  }

  // `Id::makeFromLocalVocabIndex` assumes that the last `numDatatypeBits` bits
  // are all zero and then performs a right shift. We have to shift the
  // `distinctIndex` left by the same amount to counter this effect.
  return Id::makeFromLocalVocabIndex(reinterpret_cast<::LocalVocabIndex>(
      distinctIndex << Id::numDatatypeBits));
}

}  // namespace qlever::binary_export

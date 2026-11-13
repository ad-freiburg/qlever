// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025        Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/binaryExport/StringMapping.h"

#include <stdexcept>
#include <utility>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "index/ExportIds.h"
#include "index/Index.h"
#include "index/TripleComponentConversions.h"
#include "parser/TripleComponent.h"

namespace qlever::binary_export {

// _____________________________________________________________________________
StringMapping::StringMapping(const Index& index)
    : index_{&index}, dummyLocalVocab_{index.getImpl().allocator()} {}

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

  return makeTransmittedId(distinctIndex);
}

// _____________________________________________________________________________
TransmittedStringRewriter::TransmittedStringRewriter(
    std::vector<std::string> strings, const Index& index,
    LocalVocab& localVocab) {
  ids_.reserve(strings.size());
  for (auto& string : strings) {
    ids_.push_back(stringToLocalId(std::move(string), index, localVocab));
  }
}

// _____________________________________________________________________________
Id TransmittedStringRewriter::operator()(Id id) const {
  auto index = StringMapping::getTransmittedIndex(id);
  if (index >= ids_.size()) {
    throw std::runtime_error{
        "The result in QLever's binary export format refers to a string that "
        "was not transmitted"};
  }
  return ids_[index];
}

// _____________________________________________________________________________
Id stringToLocalId(std::string representation, const Index& index,
                   LocalVocab& localVocab) {
  // The string was sent by another instance, so check its form before it is
  // parsed as an internal representation (which would otherwise fail an
  // internal assertion, or silently accept e.g. an IRI without the closing
  // `>`).
  bool isIri = (ql::starts_with(representation, '<') ||
                ql::starts_with(representation, '@')) &&
               ql::ends_with(representation, '>');
  bool isLiteral =
      ql::starts_with(representation, '"') && representation.rfind('"') > 0;
  if (!isIri && !isLiteral) {
    throw std::runtime_error{
        "The result in QLever's binary export format contains a malformed "
        "string"};
  }
  TripleComponent tripleComponent{
      ad_utility::triple_component::LiteralOrIri::fromStringRepresentation(
          std::move(representation))};
  return toValueId(std::move(tripleComponent), index.getImpl(), localVocab);
}

}  // namespace qlever::binary_export

// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "engine/binaryExport/IdRewriter.h"

#include <absl/strings/str_cat.h>

#include <utility>

#include "backports/algorithm.h"
#include "index/TripleComponentConversions.h"
#include "parser/TripleComponent.h"

namespace qlever::binary_export {

// Convert the string representation of a literal or IRI to a local `Id` in
// the same way as for any other input (see `toValueId`): an IRI that can be
// encoded with a local pattern becomes an `EncodedVal`, a word of the
// vocabularies of the local index the corresponding `VocabIndex` or
// `SecondaryVocabIndex`, and only the remaining words are added to `vocab`.
static Id stringToId(std::string representation,
                     const QueryExecutionContext& qec, LocalVocab& vocab) {
  TripleComponent tripleComponent{
      ad_utility::triple_component::LiteralOrIri::fromStringRepresentation(
          std::move(representation))};
  return toValueId(std::move(tripleComponent), qec.getIndex().getImpl(), vocab);
}

// _____________________________________________________________________________
IdRewriter::IdRewriter(const QueryExecutionContext& qec,
                       std::vector<encodedIri::Pattern> remotePatterns,
                       GeoPointEncodingEnum remoteGeoPointEncoding)
    : qec_{&qec},
      remotePatterns_{std::move(remotePatterns)},
      prefixMapping_{getPrefixMapping(qec, remotePatterns_)},
      remoteGeoPointEncoding_{remoteGeoPointEncoding} {}

// _____________________________________________________________________________
void IdRewriter::startBatch(std::vector<std::string> transmittedStrings) {
  transmittedIds_.clear();
  transmittedIds_.reserve(transmittedStrings.size());
  for (auto& string : transmittedStrings) {
    transmittedIds_.push_back(
        stringToId(std::move(string), *qec_, localVocab_));
  }
  decodedIris_.clear();
}

// _____________________________________________________________________________
void IdRewriter::rewriteRows(IdTable& table, size_t firstRow) {
  for (auto column : table.getColumns()) {
    for (Id& id : column.subspan(firstRow)) {
      id = (*this)(id);
    }
  }
}

// _____________________________________________________________________________
Id IdRewriter::operator()(Id id) {
  switch (id.getDatatype()) {
    case Datatype::EncodedVal:
      return rewriteEncodedVal(id);
    case Datatype::BlankNodeIndex:
      return rewriteBlankNode(id);
    case Datatype::LocalVocabIndex:
      return rewriteString(id);
    case Datatype::GeoPoint:
      return rewriteGeoPoint(id);
    default:
      // All other IDs that the exporter sends are self-contained, see
      // `toExportableId`.
      if (!id.isTrivial()) {
        throw std::runtime_error{absl::StrCat(
            "The result in QLever's binary export format contains an ID of "
            "the unexpected datatype ",
            toString(id.getDatatype()))};
      }
      return id;
  }
}

// _____________________________________________________________________________
Id IdRewriter::rewriteEncodedVal(Id id) {
  auto [prefixIdx, payload] =
      EncodedIriManager::splitIntoPrefixIdxAndPayload(id);
  if (auto it = prefixMapping_.find(prefixIdx); it != prefixMapping_.end()) {
    return EncodedIriManager::makeIdFromPrefixIdxAndPayload(it->second,
                                                            payload);
  }
  if (prefixIdx >= remotePatterns_.size()) {
    throw std::runtime_error{
        "The result in QLever's binary export format contains an encoded IRI "
        "with an unknown pattern"};
  }
  // The decoded IRI might be encoded with a different local pattern.
  auto [it, inserted] = decodedIris_.try_emplace(id.getBits(), Id{});
  if (inserted) {
    it->second =
        stringToId(encodedIri::decodeToIri(remotePatterns_[prefixIdx], payload),
                   *qec_, localVocab_);
  }
  return it->second;
}

// _____________________________________________________________________________
Id IdRewriter::rewriteBlankNode(Id id) {
  auto [it, inserted] = blankNodeMapping_.try_emplace(id.getBits(), Id{});
  if (inserted) {
    it->second = Id::makeFromBlankNodeIndex(blankNodeVocab_.getBlankNodeIndex(
        qec_->getIndex().getBlankNodeManager()));
  }
  return it->second;
}

// _____________________________________________________________________________
Id IdRewriter::rewriteString(Id id) const {
  // Undo the shift done by `StringMapping::remapId`.
  auto index =
      reinterpret_cast<size_t>(id.getLocalVocabIndex()) >> Id::numDatatypeBits;
  if (index >= transmittedIds_.size()) {
    throw std::runtime_error{
        "The result in QLever's binary export format refers to a string that "
        "was not transmitted"};
  }
  return transmittedIds_[index];
}

// _____________________________________________________________________________
Id IdRewriter::rewriteGeoPoint(Id id) const {
  if (remoteGeoPointEncoding_ == GeoPoint::encoding()) {
    return id;
  }
  // NOTE: Decoding the point to a `GeoPoint` and encoding it again would also
  // work, but would needlessly go through the (lossy) floating point
  // coordinates, so only the bits of the payload are converted.
  return Id::fromBits(
      Datatype::GeoPoint,
      GeoPoint::convertEncoding(id.getPayloadBits(), remoteGeoPointEncoding_,
                                GeoPoint::encoding()));
}

// _____________________________________________________________________________
LocalVocab IdRewriter::takeLocalVocab() {
  if (!blankNodeMapping_.empty()) {
    // NOTE: `mergeWith` marks the `blankNodeVocab_` as copied, which only
    // forbids adding words, but not blank nodes, to it.
    localVocab_.mergeWith(blankNodeVocab_);
  }
  // The IDs of the current batch might point into the old local vocab.
  transmittedIds_.clear();
  decodedIris_.clear();
  return std::exchange(localVocab_, LocalVocab{});
}

// _____________________________________________________________________________
ad_utility::HashMap<uint8_t, uint8_t> IdRewriter::getPrefixMapping(
    const QueryExecutionContext& qec,
    const std::vector<encodedIri::Pattern>& patterns) {
  ad_utility::HashMap<uint8_t, uint8_t> prefixMapping;
  const auto& localPatterns = qec.getIndex().encodedIriManager().patterns_;
  for (const auto& [index, pattern] : ::ranges::views::enumerate(patterns)) {
    auto patternIt = ql::ranges::find(localPatterns, pattern);
    if (patternIt != localPatterns.end()) {
      prefixMapping[index] = static_cast<uint8_t>(
          ql::ranges::distance(localPatterns.begin(), patternIt));
    }
  }
  return prefixMapping;
}

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

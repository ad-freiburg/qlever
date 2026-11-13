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

#include <stdexcept>
#include <utility>

namespace qlever::binary_export {

// _____________________________________________________________________________
IdRewriter::IdRewriter(const QueryExecutionContext& qec,
                       std::vector<encodedIri::Pattern> remotePatterns,
                       GeoPointEncodingEnum remoteGeoPointEncoding)
    : qec_{&qec},
      encodedIriRewriter_{qec.getIndex(), std::move(remotePatterns)},
      blankNodeRewriter_{qec.getIndex().getBlankNodeManager()},
      remoteGeoPointEncoding_{remoteGeoPointEncoding} {}

// _____________________________________________________________________________
void IdRewriter::rewriteBatch(IdTable& table, size_t firstRow,
                              std::vector<std::string> transmittedStrings) {
  AD_CONTRACT_CHECK(firstRow <= table.size());
  TransmittedStringRewriter stringRewriter{std::move(transmittedStrings),
                                           qec_->getIndex(), localVocab_};
  for (auto column : table.getColumns()) {
    for (IdRef id : column.subspan(firstRow)) {
      id = rewrite(id, stringRewriter);
    }
  }
}

// _____________________________________________________________________________
Id IdRewriter::rewrite(Id id, const TransmittedStringRewriter& stringRewriter) {
  switch (id.getDatatype()) {
    case Datatype::EncodedVal:
      return encodedIriRewriter_(id, localVocab_);
    case Datatype::BlankNodeIndex:
      return blankNodeRewriter_(id);
    case Datatype::LocalVocabIndex:
      return stringRewriter(id);
    case Datatype::GeoPoint:
      return rewriteGeoPoint(id, remoteGeoPointEncoding_);
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
Id IdRewriter::rewriteGeoPoint(Id id, GeoPointEncodingEnum remoteEncoding) {
  if (remoteEncoding == GeoPoint::encoding()) {
    return id;
  }
  // NOTE: Decoding the point to a `GeoPoint` and encoding it again would also
  // work, but would needlessly go through the (lossy) floating point
  // coordinates, so only the bits of the payload are converted.
  return Id::fromBits(
      Datatype::GeoPoint,
      GeoPoint::convertEncoding(id.getPayloadBits(), remoteEncoding,
                                GeoPoint::encoding()));
}

// _____________________________________________________________________________
LocalVocab IdRewriter::takeLocalVocab() {
  blankNodeRewriter_.addBlankNodesTo(localVocab_);
  // The cached IDs of the decoded IRIs might point into the old local vocab.
  encodedIriRewriter_.invalidateCache();
  return std::exchange(localVocab_, LocalVocab{});
}

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

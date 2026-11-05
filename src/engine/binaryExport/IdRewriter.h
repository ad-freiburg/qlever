// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_BINARYEXPORT_IDREWRITER_H
#define QLEVER_SRC_ENGINE_BINARYEXPORT_IDREWRITER_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <string>
#include <vector>

#include "engine/QueryExecutionContext.h"
#include "engine/binaryExport/BlankNodeRewriter.h"
#include "engine/binaryExport/EncodedIriRewriter.h"
#include "engine/binaryExport/StringMapping.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/LocalVocab.h"
#include "rdfTypes/GeoPoint.h"

namespace qlever::binary_export {

// Convert the IDs that the exporting instance sent (see `toExportableId` in
// `BinaryExport.cpp`) to IDs of the local instance. This is the counterpart of
// `toExportableId` on the importing side. All the IDs that depend on the
// instance are converted by the respective helper: the strings by
// `TransmittedStrings` (the counterpart of `StringMapping`), the encoded IRIs
// (whose patterns might differ) by the `EncodedIriRewriter`, the blank nodes
// by the `BlankNodeRewriter`, and the `GeoPoint`s (whose encoding might
// differ) by `rewriteGeoPoint`. The words that are not contained in the local
// index are added to the local vocab (see `takeLocalVocab`).
class IdRewriter {
  const QueryExecutionContext* qec_;
  EncodedIriRewriter encodedIriRewriter_;
  BlankNodeRewriter blankNodeRewriter_;
  GeoPointEncodingEnum remoteGeoPointEncoding_;
  // The entries of the rewritten IDs that are not contained in the local
  // index.
  LocalVocab localVocab_;

 public:
  IdRewriter(const QueryExecutionContext& qec,
             std::vector<encodedIri::Pattern> remotePatterns,
             GeoPointEncodingEnum remoteGeoPointEncoding);

  // Rewrite all the IDs of the `table`, starting at row `firstRow`. These rows
  // form one batch, the IDs of which refer to the `transmittedStrings` (see
  // `StringMapping::takeStrings`). Several batches can be rewritten before
  // the next call to `takeLocalVocab`.
  void rewriteBatch(IdTable& table, size_t firstRow,
                    std::vector<std::string> transmittedStrings);

  // Return the local vocab that contains the entries of the IDs that were
  // rewritten since the last call (including all the blank nodes so far, as
  // the next batches might contain the same blank nodes again). The local
  // vocab is then reset.
  LocalVocab takeLocalVocab();

  // Convert a `GeoPoint` from the `remoteEncoding` to the local encoding.
  static Id rewriteGeoPoint(Id id, GeoPointEncodingEnum remoteEncoding);

 private:
  // Rewrite a single `Id` of a batch with the `transmittedStrings`.
  Id rewrite(Id id, const TransmittedStrings& transmittedStrings);
};

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_BINARYEXPORT_IDREWRITER_H

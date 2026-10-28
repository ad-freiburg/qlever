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
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/LocalVocab.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "rdfTypes/GeoPoint.h"
#include "util/HashMap.h"

namespace qlever::binary_export {

// Convert the IDs that the exporting instance sent (see `toExportableId` in
// `BinaryExport.cpp`) to IDs of the local instance. This is the counterpart of
// `toExportableId` and `StringMapping` on the importing side. In addition to
// the strings, it also handles all the other IDs that depend on the instance:
// encoded IRIs (whose patterns might differ), blank nodes, and `GeoPoint`s
// (whose encoding might differ). The strings and the IRIs that can't be
// encoded with the same local pattern are converted like any other input (see
// `toValueId`), so they become encoded IRIs or IDs of the vocabularies of the
// local index if possible, and are added to the local vocab (see
// `takeLocalVocab`) otherwise.
class IdRewriter {
  const QueryExecutionContext* qec_;
  // The patterns of the encoded IRIs of the exporting instance, and the
  // mapping from their indices to the indices of the equal local patterns.
  std::vector<encodedIri::Pattern> remotePatterns_;
  ad_utility::HashMap<uint8_t, uint8_t> prefixMapping_;
  GeoPointEncodingEnum remoteGeoPointEncoding_;
  // The same remote blank node is always replaced by the same local one. The
  // local blank nodes are allocated in `blankNodeVocab_`, which is merged into
  // the `LocalVocab` of each batch (see `takeLocalVocab`) and only contains
  // blank nodes. That way, the batches don't keep each other's words alive.
  ad_utility::HashMap<Id::T, Id> blankNodeMapping_;
  LocalVocab blankNodeVocab_;
  // The entries of the current batch that are not contained in the local
  // index.
  LocalVocab localVocab_;
  // The local IDs of the strings of the current batch (see `startBatch`), and
  // of the encoded IRIs that were decoded in the current batch.
  std::vector<Id> transmittedIds_;
  ad_utility::HashMap<Id::T, Id> decodedIris_;

 public:
  IdRewriter(const QueryExecutionContext& qec,
             std::vector<encodedIri::Pattern> remotePatterns,
             GeoPointEncodingEnum remoteGeoPointEncoding);

  // Start a new batch, the IDs of which refer to the `transmittedStrings`. Each
  // of the strings is converted once, all the IDs that refer to it are then
  // replaced by the result of that conversion.
  void startBatch(std::vector<std::string> transmittedStrings);

  // Rewrite all the IDs of the `table`, starting at row `firstRow`.
  void rewriteRows(IdTable& table, size_t firstRow);

  // Rewrite a single `Id`, see the functions below.
  Id operator()(Id id);

  // Rewrite an `Id` of type `EncodedVal`. If its pattern also exists locally,
  // only the index of the pattern is changed, otherwise the IRI is decoded and
  // converted (at most once per batch).
  Id rewriteEncodedVal(Id id);

  // Replace a remote blank node by a local one (the same for all occurrences).
  Id rewriteBlankNode(Id id);

  // Replace an `Id` that refers to one of the transmitted strings of the
  // current batch (see `startBatch`).
  Id rewriteString(Id id) const;

  // Convert a `GeoPoint` from the remote to the local encoding.
  Id rewriteGeoPoint(Id id) const;

  // Return the local vocab that contains the entries of the rewritten IDs
  // (including all the blank nodes so far, as the next batches might contain
  // the same blank nodes again). The local vocab is then reset.
  LocalVocab takeLocalVocab();

  // Get the mapping from the indices of the remote `patterns` to the indices
  // of the equal local patterns of `qec`. Remote patterns that don't exist
  // locally are not contained in the mapping.
  static ad_utility::HashMap<uint8_t, uint8_t> getPrefixMapping(
      const QueryExecutionContext& qec,
      const std::vector<encodedIri::Pattern>& patterns);
};

}  // namespace qlever::binary_export

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_ENGINE_BINARYEXPORT_IDREWRITER_H

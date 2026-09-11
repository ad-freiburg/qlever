// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BLOBCONVERTER_BLOBCONVERTER_H
#define QLEVER_SRC_BLOBCONVERTER_BLOBCONVERTER_H

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "backports/span.h"
#include "blobConverter/LegacyBlobReader.h"
#include "blobConverter/LegacyDatatype.h"
#include "blobConverter/LegacyEncodedIriManager.h"
#include "global/Id.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "util/json.h"

// Convert a blob of the vocabulary and the named cached queries from the legacy
// format of the `demo-v1-c++17_unimodel` branch of the `qlever-bmw` fork (see
// `LegacyBlobReader.h`) to the current format (see
// `NamedCachedQueryBlobManager::serialize`). The following differences between
// the two formats are handled:
//
// 1. The container: the legacy blob is a ZSTD frame followed by the
//    uncompressed size, with the magic header `"QLVUBLOB"`; the current blob is
//    a single ZSTD frame with the magic header `"QLVRBLOB"`.
// 2. The index metadata: the `"index-format-version"` is set to the current
//    one, and the `"encoded-iri-prefixes"` are expressed as
//    `encodedIri::Pattern`s (see below).
// 3. The `Datatype` enum: the current format has an additional
//    `SecondaryVocabIndex`, so the datatype bits of all `Id`s from
//    `TextRecordIndex` on are shifted by one (see `LegacyDatatype.h`).
// 4. The encoded IRIs: the legacy format had hardcoded, BMW-specific encoding
//    schemes with their own bit layouts (see `LegacyEncodedIriManager.h`).
//    Each encoded IRI is decoded with the legacy scheme and re-encoded with the
//    equivalent `encodedIri::Pattern` of the current format.
// 5. The `NamedResultCache` has a magic byte and a format version in the
//    current format.
//
// The vocabulary itself (which has the same byte layout in both formats) is
// passed through unchanged, so the vocabulary indices of the `Id`s stay valid.
namespace qlever::blobConverter {

// The statistics of the conversion of one named cached query.
struct EntryStatistics {
  std::string name_;
  size_t numRows_ = 0;
  size_t numColumns_ = 0;
  std::vector<std::string> variables_;
  // The number of `Id`s of each `LegacyDatatype` in the result.
  std::array<size_t, numLegacyDatatypes> numIdsPerDatatype_{};
  // The number of encoded IRIs that were decoded with the legacy scheme and
  // re-encoded for the current format (the same as the number of `Id`s with
  // the datatype `EncodedVal`).
  size_t numReencodedIris_ = 0;
  bool hasGeoIndex_ = false;
};

// The statistics of the conversion of a complete blob.
struct ConversionStatistics {
  std::string vocabularyType_;
  size_t numVocabularyWords_ = 0;
  // The legacy encoded-IRI configuration and the patterns it was mapped to, as
  // JSON.
  nlohmann::json legacyEncodedIriConfig_;
  nlohmann::json currentEncodedIriConfig_;
  std::vector<EntryStatistics> entries_;

  // A human-readable summary.
  std::string toString() const;
};

// The result of a conversion.
struct ConversionResult {
  // The blob in the current format, ready to be loaded via
  // `Qlever::deserializeVocabAndNamedCacheFromCompressedBlob`.
  std::vector<char> blob_;
  // The index metadata that is stored in `blob_`.
  nlohmann::json metadata_;
  ConversionStatistics statistics_;
};

// Convert the bits of a legacy `Id` to the equivalent `Id` of the current
// format: shift the datatype where necessary, and re-encode encoded IRIs from
// the `legacyManager` to the `currentManager`. Throw a `std::runtime_error` if
// the `Id` cannot be converted: for a `LocalVocabIndex` (which cannot be part
// of a blob), for an unknown datatype, and for an encoded IRI that the
// `currentManager` cannot encode.
Id convertId(uint64_t legacyBits, const LegacyEncodedIriManager& legacyManager,
             const EncodedIriManager& currentManager);

// Convert the index metadata of a legacy blob to the metadata that is stored
// in the converted blob: the `"index-format-version"` becomes the current one,
// and the `"encoded-iri-prefixes"` are replaced by the JSON of the
// `currentManager`. All other keys are copied.
nlohmann::json convertMetadata(const nlohmann::json& legacyMetadata,
                               const EncodedIriManager& currentManager);

// Convert an already read legacy blob (see `LegacyBlobReader.h`).
ConversionResult convertLegacyBlob(const LegacyBlob& legacyBlob);

// Convert a legacy blob, given as the compressed bytes that were written by the
// legacy `qlever-bmw` fork (typically the contents of a `.dat` file).
ConversionResult convertLegacyBlob(ql::span<const char> legacyCompressedBlob);

}  // namespace qlever::blobConverter

#endif  // QLEVER_SRC_BLOBCONVERTER_BLOBCONVERTER_H

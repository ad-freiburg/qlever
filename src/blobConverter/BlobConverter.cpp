// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "blobConverter/BlobConverter.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <memory>
#include <stdexcept>

#include "engine/NamedResultCache.h"
#include "engine/NamedResultCacheSerializer.h"
#include "engine/idTable/IdTable.h"
#include "index/IndexFormatConverter.h"
#include "index/IndexFormatVersion.h"
#include "index/LocalVocab.h"
#include "rdfTypes/Variable.h"
#include "util/AllocatorWithLimit.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeString.h"

namespace qlever::blobConverter {

namespace {
// The key of the encoded-IRI configuration in the index metadata.
constexpr std::string_view encodedIriKey = "encoded-iri-prefixes";

// Convert one legacy named cache entry to a `NamedResultCache::Value`, and
// record the `statistics`.
NamedResultCache::Value convertEntry(
    const LegacyNamedCacheEntry& entry,
    const LegacyEncodedIriManager& legacyManager,
    const EncodedIriManager& currentManager, EntryStatistics& statistics) {
  if (!entry.blankNodeBlocks_.empty() || !entry.localVocabWords_.empty()) {
    throw std::runtime_error{absl::StrCat(
        "The cached result \"", entry.name_,
        "\" has a non-empty local vocabulary (", entry.localVocabWords_.size(),
        " words, ", entry.blankNodeBlocks_.size(),
        " blank node blocks), which the converter does not support")};
  }
  statistics.name_ = entry.name_;
  statistics.numRows_ = entry.numRows_;
  statistics.numColumns_ = entry.numColumns_;
  statistics.hasGeoIndex_ = entry.geoIndex_.has_value();

  IdTable table{entry.numColumns_, ad_utility::makeUnlimitedAllocator<Id>()};
  table.resize(entry.numRows_);
  for (size_t col = 0; col < entry.numColumns_; ++col) {
    auto column = table.getColumn(col);
    const auto& legacyColumn = entry.columns_.at(col);
    for (size_t row = 0; row < entry.numRows_; ++row) {
      uint64_t legacyBits = legacyColumn[row];
      auto datatype = legacyDatatypeBits(legacyBits);
      if (datatype < numLegacyDatatypes) {
        ++statistics.numIdsPerDatatype_[datatype];
      }
      column[row] = convertId(legacyBits, legacyManager, currentManager);
    }
  }
  statistics.numReencodedIris_ =
      statistics
          .numIdsPerDatatype_[static_cast<size_t>(LegacyDatatype::EncodedVal)];

  VariableToColumnMap variableToColumn;
  for (const auto& [name, columnInfo] : entry.variables_) {
    statistics.variables_.push_back(name);
    variableToColumn[Variable{name}] = columnInfo;
  }
  ql::ranges::sort(statistics.variables_);

  return NamedResultCache::Value{
      std::make_shared<const IdTable>(std::move(table)),
      std::move(variableToColumn),
      entry.resultSortedOn_,
      LocalVocab{},
      entry.cacheKey_,
      entry.geoIndex_};
}
}  // namespace

// _____________________________________________________________________________
std::string ConversionStatistics::toString() const {
  std::string result;
  absl::StrAppend(&result, "Vocabulary: ", numVocabularyWords_,
                  " words, type \"", vocabularyType_, "\"\n");
  absl::StrAppend(&result, "Legacy encoded-IRI configuration: ",
                  legacyEncodedIriConfig_.dump(), "\n");
  absl::StrAppend(&result, "Current encoded-IRI configuration: ",
                  currentEncodedIriConfig_.dump(), "\n");
  absl::StrAppend(&result, "Named cached queries: ", entries_.size(), "\n");
  for (const auto& entry : entries_) {
    absl::StrAppend(
        &result, "  \"", entry.name_, "\": ", entry.numRows_, " rows, ",
        entry.numColumns_,
        " columns, variables: ", absl::StrJoin(entry.variables_, " "),
        entry.hasGeoIndex_ ? ", with a cached geo index" : "", "\n");
    std::vector<std::string> datatypeCounts;
    for (size_t i = 0; i < numLegacyDatatypes; ++i) {
      if (entry.numIdsPerDatatype_[i] > 0) {
        datatypeCounts.push_back(absl::StrCat(
            qlever::blobConverter::toString(static_cast<LegacyDatatype>(i)),
            ": ", entry.numIdsPerDatatype_[i]));
      }
    }
    absl::StrAppend(&result, "    Ids per datatype: ",
                    absl::StrJoin(datatypeCounts, ", "), "\n");
    absl::StrAppend(&result, "    Re-encoded IRIs: ", entry.numReencodedIris_,
                    "\n");
  }
  return result;
}

// _____________________________________________________________________________
Id convertId(uint64_t legacyBits, const LegacyEncodedIriManager& legacyManager,
             const EncodedIriManager& currentManager) {
  // An encoded IRI is decoded with the legacy scheme and re-encoded with the
  // current patterns (which also sets the current datatype bits).
  if (legacyDatatypeBits(legacyBits) ==
      static_cast<uint64_t>(LegacyDatatype::EncodedVal)) {
    auto iri = legacyManager.toString(legacyDataBits(legacyBits));
    auto converted = currentManager.encode(iri);
    if (!converted.has_value()) {
      throw std::runtime_error{absl::StrCat(
          "The encoded IRI ", iri,
          " of the legacy blob cannot be encoded with the patterns of the "
          "current format")};
    }
    return converted.value();
  }
  // All other `Id`s keep their data bits, and only their datatype bits are
  // shifted where necessary. The legacy blob format has the same `Datatype`
  // enum as the previous index format, so the converter for that format
  // applies (it also rejects `LocalVocabIndex` and unknown datatypes).
  return qlever::indexFormatConverter::convertId(Id::fromBits(legacyBits));
}

// _____________________________________________________________________________
nlohmann::json convertMetadata(const nlohmann::json& legacyMetadata,
                               const EncodedIriManager& currentManager) {
  nlohmann::json metadata = legacyMetadata;
  metadata["index-format-version"] = qlever::indexFormatVersion;
  metadata[encodedIriKey] = currentManager;
  return metadata;
}

// _____________________________________________________________________________
ConversionResult convertLegacyBlob(const LegacyBlob& legacyBlob) {
  ConversionResult result;
  auto& statistics = result.statistics_;

  // Set up the encoded-IRI managers of both formats.
  const auto& metadata = legacyBlob.metadata_;
  LegacyEncodedIriManager legacyManager;
  if (metadata.contains(encodedIriKey)) {
    legacyManager = LegacyEncodedIriManager::fromJson(metadata[encodedIriKey]);
    statistics.legacyEncodedIriConfig_ = metadata[encodedIriKey];
  }
  auto currentManager = legacyManager.makeCurrentManager();
  statistics.currentEncodedIriConfig_ = currentManager;

  result.metadata_ = convertMetadata(metadata, currentManager);
  statistics.vocabularyType_ =
      static_cast<std::string>(result.metadata_["vocabulary-type"]);
  statistics.numVocabularyWords_ = legacyBlob.numWords();

  // Convert the named cached queries.
  NamedResultCache cache;
  for (const auto& entry : legacyBlob.entries_) {
    auto& entryStatistics = statistics.entries_.emplace_back();
    cache.store(entry.name_, convertEntry(entry, legacyManager, currentManager,
                                          entryStatistics));
  }

  // Write the blob in the current format (see
  // `NamedCachedQueryBlobManager::serialize`).
  ad_utility::serialization::AlignedByteBufferWriteSerializer serializer;
  NamedCachedQueryBlobManager::writeBlobHeader(serializer);
  serializer << result.metadata_.dump();
  std::visit([&serializer](const auto& vocab) { serializer << vocab; },
             legacyBlob.vocabulary_);
  cache.writeToSerializer(serializer);
  auto uncompressed = std::move(serializer).data();
  result.blob_ = NamedCachedQueryBlobManager::compressBlob(
      ql::span<const char>{uncompressed.data(), uncompressed.size()});
  return result;
}

// _____________________________________________________________________________
ConversionResult convertLegacyBlob(ql::span<const char> legacyCompressedBlob) {
  return convertLegacyBlob(readLegacyBlobFromCompressed(legacyCompressedBlob));
}

}  // namespace qlever::blobConverter

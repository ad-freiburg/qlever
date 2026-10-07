// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_LIBQLEVER_BLOBTESTHELPERS_H
#define QLEVER_TEST_LIBQLEVER_BLOBTESTHELPERS_H

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "../util/GTestHelpers.h"
#include "./QleverTestHelpers.h"
#include "backports/span.h"
#include "global/Id.h"
#include "index/vocabulary/SecondaryVocabulary.h"
#include "libqlever/BlobLayout.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/Qlever.h"
#include "util/File.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace blobTestHelpers {

using Manager = qlever::NamedCachedQueryBlobManager;
using Bytes = std::vector<char, Manager::BlobAllocator>;

// Write the `turtleContents` to a turtle file, build an index with an
// in-memory vocabulary from it, and return the `IndexBuilderConfig`. The
// basename is derived from the name of the running test. Persisted updates of
// a previous run of the test are deleted.
inline qlever::IndexBuilderConfig buildTestIndex(
    std::string_view turtleContents) {
  std::string basename = gtestCurrentTestName();
  // Updates that were persisted by a previous run of the test must not be
  // applied to the new index.
  ad_utility::deleteFile(absl::StrCat(basename, ".update-triples"), false);
  std::string sourceFilename = absl::StrCat(basename, ".ttl");
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << turtleContents;
  }
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  qlever::IndexBuilderConfig config;
  config.inputFiles_.push_back(
      {sourceFilename, qlever::Filetype::Turtle, std::nullopt});
  config.baseName_ = std::move(basename);
  config.vocabType_ = ad_utility::VocabularyType::InMemoryUncompressed;
  EXPECT_NO_THROW(qlever::Qlever::buildIndex(config));
  return config;
}

// Decompress `compressedBlob` into an aligned buffer (fail the test on error).
inline Bytes decompressOrFail(ql::span<const char> compressedBlob) {
  auto result = Manager::tryToDecompressBlob(compressedBlob, {});
  if (const auto* error = std::get_if<Manager::BlobError>(&result)) {
    ADD_FAILURE() << error->message_;
    return {};
  }
  return std::get<Bytes>(std::move(result));
}

// An uncompressed blob together with its layout.
struct ParsedBlob {
  Bytes bytes_;
  qlever::BlobLayout layout_;

  explicit ParsedBlob(ql::span<const char> compressedBlob)
      : bytes_{decompressOrFail(compressedBlob)},
        layout_{qlever::BlobLayout::parse(ql::span<const char>{bytes_})} {}
  ql::span<const char> span() const { return {bytes_.data(), bytes_.size()}; }
};

// The geo index of a hand-made entry (entries format version 2).
struct HandmadeGeo {
  std::optional<double> simplification_;
  std::vector<std::string> segments_;
  std::vector<uint64_t> rowToShape_;
};

// A hand-made named cache entry whose columns contain `Id`s given by the
// integers.
struct HandmadeEntry {
  std::string key_;
  std::vector<std::vector<int64_t>> columns_;
  std::vector<uint64_t> sortedOn_;
  std::optional<HandmadeGeo> geo_;
};

// Return the first `prefixSize` bytes of a (decompressed) blob, which consist
// of the header, the metadata, and the main vocabulary of a real blob.
inline std::vector<char> prefixOf(ql::span<const char> blob,
                                  uint64_t prefixSize) {
  return {blob.begin(), blob.begin() + prefixSize};
}

// Build a decompressed blob from the `prefix` (see `prefixOf`), an optional
// secondary vocabulary, and the hand-made `entries`. Everything is written
// with the same aligned serializer as in the real writer. If a secondary
// vocabulary is given, the blob version in the prefix is changed to 2.
inline std::vector<char> makeHandmadeBlob(
    std::vector<char> prefix, const SecondaryVocabulary* secondaryVocab,
    uint16_t entriesVersion, const std::vector<HandmadeEntry>& entries) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  uint16_t blobVersion = secondaryVocab != nullptr ? 2 : 1;
  std::memcpy(prefix.data() + 8, &blobVersion, sizeof(blobVersion));
  writer.serializeBytes(prefix.data(), prefix.size());
  if (secondaryVocab != nullptr) {
    writer << *secondaryVocab;
  }
  writer << uint8_t{0xC3};
  writer << entriesVersion;
  writer << entries.size();
  for (const auto& entry : entries) {
    writer << entry.key_;
    // The local vocab: no blank node blocks and no words.
    writer << size_t{0};
    writer << uint64_t{0};
    size_t numRows = entry.columns_.empty() ? 0 : entry.columns_[0].size();
    writer << numRows;
    writer << entry.columns_.size();
    for (const auto& column : entry.columns_) {
      std::vector<Id> ids;
      for (int64_t value : column) {
        ids.push_back(Id::makeFromInt(value));
      }
      writer << ids;
    }
    // The empty `VariableToColumnMap`.
    writer << size_t{0};
    writer << entry.sortedOn_;
    writer << std::string{"cache key of " + entry.key_};
    writer << entry.geo_.has_value();
    if (entry.geo_.has_value()) {
      writer << std::string{"?geo"};
      writer << uint8_t{entry.geo_->simplification_.has_value()};
      if (entry.geo_->simplification_.has_value()) {
        writer << entry.geo_->simplification_.value();
      }
      writer << uint64_t{entry.geo_->segments_.size()};
      for (const auto& segment : entry.geo_->segments_) {
        writer << segment;
      }
      writer << entry.geo_->rowToShape_;
    }
  }
  auto data = std::move(writer).data();
  return {data.begin(), data.end()};
}

}  // namespace blobTestHelpers

#endif  // QLEVER_TEST_LIBQLEVER_BLOBTESTHELPERS_H

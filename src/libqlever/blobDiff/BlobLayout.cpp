// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/blobDiff/BlobLayout.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <variant>

#include "libqlever/NamedCachedQueryBlobManager.h"
#include "util/Exception.h"
#include "util/json.h"

namespace qlever {

// _____________________________________________________________________________
BlobLayout BlobLayout::parse(ql::span<const char> decompressedBlob) {
  BlobLayout layout;
  layout.totalSize_ = decompressedBlob.size();
  ad_utility::serialization::LayoutReadSerializer reader{decompressedBlob};

  // The header (see `NamedCachedQueryBlobManager::writeBlobHeader`).
  auto version =
      NamedCachedQueryBlobManager::tryToSkipAndVerifyBlobHeader(reader);
  if (auto* error =
          std::get_if<NamedCachedQueryBlobManager::BlobError>(&version)) {
    AD_THROW(error->message_);
  }
  layout.blobVersion_ = std::get<uint16_t>(version);
  layout.header_ = {0, reader.position()};

  // The metadata JSON is a `std::string`: read its length and payload, so that
  // the range of the payload is known, and parse it (the vocabulary type is
  // needed to skip the vocabulary).
  layout.metadata_ = reader.readArray<char>();
  auto metadata =
      nlohmann::json::parse(decompressedBlob.begin() + layout.metadata_.begin_,
                            decompressedBlob.begin() + layout.metadata_.end_);

  // The main vocabulary, which is opaque to the layout.
  layout.vocabulary_ = reader.recordRange(
      [&] { NamedCachedQueryBlobManager::skipVocabulary(reader, metadata); });

  // The secondary vocabulary (only in the newer blob format).
  if (layout.blobVersion_ ==
      NamedCachedQueryBlobManager::formatVersionWithSecondaryVocab) {
    layout.secondaryVocab_ = SecondaryVocabulary::parseLayout(reader);
  }

  // The named cache entries, preceded by their header.
  auto entriesHeader = namedResultCacheSerializer::parseEntriesHeader(reader);
  layout.entriesHeader_ = entriesHeader.whole_;
  layout.entriesVersion_ = entriesHeader.version_;
  layout.entries_.begin_ = layout.entriesHeader_.begin_;
  layout.entryLayouts_.reserve(entriesHeader.numEntries_);
  for (size_t i = 0; i < entriesHeader.numEntries_; ++i) {
    layout.entryLayouts_.push_back(namedResultCacheSerializer::parseEntryLayout(
        reader, layout.entriesVersion_));
  }
  layout.entries_.end_ = reader.position();
  AD_CONTRACT_CHECK(reader.atEnd(),
                    "The blob has trailing bytes after the last entry");
  return layout;
}

// _____________________________________________________________________________
std::string BlobLayout::describe() const {
  std::string result;
  auto line = [&result](std::string_view name, uint64_t bytes) {
    absl::StrAppendFormat(&result, "%-48s %14d bytes\n", name, bytes);
  };
  absl::StrAppendFormat(&result,
                        "Blob version %d, entries version %d, %d entries\n",
                        blobVersion_, entriesVersion_, entryLayouts_.size());
  line("total", totalSize_);
  line("header", header_.size());
  line("metadata json", metadata_.size());
  line("main vocabulary", vocabulary_.size());
  if (secondaryVocab_.present_) {
    line(absl::StrCat("secondary vocabulary (",
                      secondaryVocab_.segments_.size(), " segments)"),
         secondaryVocab_.whole_.size());
    for (size_t i = 0; i < secondaryVocab_.segments_.size(); ++i) {
      line(absl::StrCat("  segment ", i), secondaryVocab_.segments_[i].size());
    }
    line("  segment offsets", secondaryVocab_.segmentOffsets_.size());
    line("  sorted indices", secondaryVocab_.sortedIndices_.size());
  }
  for (const auto& entry : entryLayouts_) {
    uint64_t columns = 0;
    for (const auto& column : entry.columnPayloads_) {
      columns += column.size();
    }
    line(absl::StrCat("entry \"", entry.key_, "\" (", entry.numRows_, "x",
                      entry.numColumns_, ")"),
         entry.whole_.size());
    line("  columns", columns);
    if (entry.hasGeoIndex_) {
      line(absl::StrCat("  geo index (", entry.geo_.segmentPayloads_.size(),
                        " segments)"),
           entry.geo_.whole_.size());
    }
  }
  return result;
}

}  // namespace qlever

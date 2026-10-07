// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/BlobLayout.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <array>
#include <variant>

#include "engine/VariableToColumnMap.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/VocabularyType.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "util/BlankNodeManager.h"
#include "util/Exception.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializePair.h"
#include "util/Serializer/SerializeVector.h"
#include "util/json.h"

namespace qlever {

namespace {
using Reader =
    ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                         ql::span<const char>>;
using ad_utility::serialization::zeroCopyDeserializeToSpan;

// The magic byte of the serialized named cache entries (see
// `namedResultCacheSerializer::detail::magicByte`) and the magic bytes of the
// serialized secondary vocabulary (see `SecondaryVocabulary`).
constexpr uint8_t entriesMagicByte = 0xC3;
constexpr std::array<char, 8> secondaryVocabMagic{'Q', 'L', 'S', 'E',
                                                  'C', 'V', 'O', 'C'};

// A helper that reads from a `Reader` and keeps track of byte offsets.
class Parser {
  ql::span<const char> blob_;
  Reader reader_;

 public:
  explicit Parser(ql::span<const char> blob) : blob_{blob}, reader_{blob} {}

  Reader& reader() { return reader_; }
  uint64_t position() const { return reader_.getCurrentPosition(); }

  // Read an array (`std::vector`, `std::string`, or `ql::span` format) of
  // trivially serializable `T`s and return the byte range of its payload.
  template <typename T>
  ByteRange readArray() {
    auto span = zeroCopyDeserializeToSpan<T>(reader_);
    uint64_t begin = static_cast<uint64_t>(
        reinterpret_cast<const char*>(span.data()) - blob_.data());
    return {begin, begin + span.size() * sizeof(T)};
  }

  // Same as `readArray`, but also return the contents.
  template <typename T>
  std::pair<ByteRange, std::vector<T>> readArrayWithContents() {
    auto range = readArray<T>();
    const T* ptr = reinterpret_cast<const T*>(blob_.data() + range.begin_);
    return {range, std::vector<T>(ptr, ptr + range.size() / sizeof(T))};
  }

  template <typename T>
  T readScalar() {
    T value{};
    reader_ >> value;
    return value;
  }
};

// Skip the main vocabulary, whose type is given by the `"vocabulary-type"` of
// the metadata JSON. The vocabulary is read zero-copy into a scratch object,
// which only advances the `reader`.
void skipVocabulary(Parser& parser, const nlohmann::json& metadata) {
  ad_utility::VocabularyType type{
      ad_utility::VocabularyType::Enum::OnDiskCompressed};
  if (auto it = metadata.find("vocabulary-type"); it != metadata.end()) {
    type = static_cast<ad_utility::VocabularyType>(*it);
  }
  PolymorphicVocabulary vocab;
  vocab.resetToType(type);
  vocab.applyToZeroCopyCapableVocabulary("Skipping", [&parser](auto& v) {
    using T = std::decay_t<decltype(v)>;
    [[maybe_unused]] T scratch = T::fromZeroCopyDeserializer(parser.reader());
  });
}

// Parse the serialized secondary vocabulary.
SecondaryVocabLayout parseSecondaryVocab(Parser& parser) {
  SecondaryVocabLayout result;
  result.present_ = true;
  result.whole_.begin_ = parser.position();
  auto magic = parser.readScalar<std::array<char, 8>>();
  AD_CONTRACT_CHECK(magic == secondaryVocabMagic,
                    "The blob does not contain a valid secondary vocabulary");
  auto version = parser.readScalar<uint16_t>();
  AD_CONTRACT_CHECK(version == 1, "Unsupported secondary vocabulary version ",
                    version);
  auto numSegments = parser.readScalar<size_t>();
  for (size_t i = 0; i < numSegments; ++i) {
    uint64_t begin = parser.position();
    parser.readArray<char>();
    parser.readArray<uint64_t>();
    result.segments_.push_back({begin, parser.position()});
  }
  result.segmentOffsets_ = parser.readArray<uint64_t>();
  result.sortedIndices_ = parser.readArray<uint64_t>();
  result.whole_.end_ = parser.position();
  return result;
}

// Parse the serialized geo index of an entry.
GeoIndexLayout parseGeoIndex(Parser& parser, uint16_t entriesVersion,
                             size_t numRows) {
  GeoIndexLayout geo;
  geo.whole_.begin_ = parser.position();
  // The geometry column.
  parser.readArray<char>();
  if (entriesVersion == 1) {
    // The S2 index and the hash map from shape ids to rows.
    parser.readArray<char>();
    auto numPairs = parser.readScalar<size_t>();
    for (size_t i = 0; i < numPairs; ++i) {
      parser.readScalar<std::pair<size_t, size_t>>();
    }
  } else {
    auto hasSimplification = parser.readScalar<uint8_t>();
    if (hasSimplification != 0) {
      parser.readScalar<double>();
    }
    auto numSegments = parser.readScalar<uint64_t>();
    for (uint64_t i = 0; i < numSegments; ++i) {
      geo.segmentPayloads_.push_back(parser.readArray<char>());
    }
    geo.rowToShape_ = parser.readArray<uint64_t>();
    AD_CONTRACT_CHECK(geo.rowToShape_.size() == numRows * sizeof(uint64_t),
                      "The geo index of an entry has the wrong number of rows");
  }
  geo.whole_.end_ = parser.position();
  return geo;
}

// Parse a single named cache entry.
EntryLayout parseEntry(Parser& parser, uint16_t entriesVersion) {
  EntryLayout entry;
  entry.whole_.begin_ = parser.position();
  parser.reader() >> entry.key_;
  // The local vocab: the owned blank node blocks, and the words (none in
  // blobs from current writers).
  parser.readScalar<std::vector<
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>>();
  auto numWords = parser.readScalar<uint64_t>();
  for (uint64_t i = 0; i < numWords; ++i) {
    parser.readScalar<uint64_t>();
    parser.readArray<char>();
  }
  entry.numRows_ = parser.readScalar<size_t>();
  entry.numColumns_ = parser.readScalar<size_t>();
  for (size_t i = 0; i < entry.numColumns_; ++i) {
    auto range = parser.readArray<uint64_t>();
    AD_CONTRACT_CHECK(range.size() == entry.numRows_ * sizeof(uint64_t),
                      "A column of a named cache entry has the wrong size");
    entry.columnPayloads_.push_back(range);
  }
  entry.varToColMap_.begin_ = parser.position();
  {
    VariableToColumnMap scratch;
    serializeDeterministically(parser.reader(), scratch);
  }
  entry.varToColMap_.end_ = parser.position();
  auto [sortedOnRange, sortedOn] = parser.readArrayWithContents<uint64_t>();
  entry.resultSortedOn_ = sortedOnRange;
  entry.sortedOnColumns_ = std::move(sortedOn);
  entry.cacheKey_ = parser.readArray<char>();
  entry.hasGeoIndex_ = parser.readScalar<bool>();
  if (entry.hasGeoIndex_) {
    entry.geo_ = parseGeoIndex(parser, entriesVersion, entry.numRows_);
  }
  entry.whole_.end_ = parser.position();
  return entry;
}
}  // namespace

// _____________________________________________________________________________
BlobLayout BlobLayout::parse(ql::span<const char> decompressedBlob) {
  BlobLayout layout;
  layout.totalSize_ = decompressedBlob.size();
  Parser parser{decompressedBlob};

  auto version = NamedCachedQueryBlobManager::tryToSkipAndVerifyBlobHeader(
      parser.reader());
  if (auto* error =
          std::get_if<NamedCachedQueryBlobManager::BlobError>(&version)) {
    AD_THROW(error->message_);
  }
  layout.blobVersion_ = std::get<uint16_t>(version);
  layout.header_ = {0, parser.position()};

  // The metadata JSON is a `std::string`: read its length and payload, so that
  // the range of the payload is known.
  layout.metadata_ = parser.readArray<char>();
  auto metadata =
      nlohmann::json::parse(decompressedBlob.begin() + layout.metadata_.begin_,
                            decompressedBlob.begin() + layout.metadata_.end_);

  layout.vocabulary_.begin_ = parser.position();
  skipVocabulary(parser, metadata);
  layout.vocabulary_.end_ = parser.position();

  if (layout.blobVersion_ ==
      NamedCachedQueryBlobManager::formatVersionWithSecondaryVocab) {
    layout.secondaryVocab_ = parseSecondaryVocab(parser);
  }

  layout.entriesHeader_.begin_ = parser.position();
  auto magic = parser.readScalar<uint8_t>();
  AD_CONTRACT_CHECK(magic == entriesMagicByte,
                    "The blob does not contain valid named cache entries");
  layout.entriesVersion_ = parser.readScalar<uint16_t>();
  AD_CONTRACT_CHECK(layout.entriesVersion_ == 1 || layout.entriesVersion_ == 2,
                    "Unsupported format version of the named cache entries: ",
                    layout.entriesVersion_);
  auto numEntries = parser.readScalar<size_t>();
  layout.entriesHeader_.end_ = parser.position();
  layout.entries_.begin_ = layout.entriesHeader_.begin_;
  for (size_t i = 0; i < numEntries; ++i) {
    layout.entryLayouts_.push_back(parseEntry(parser, layout.entriesVersion_));
  }
  layout.entries_.end_ = parser.position();
  AD_CONTRACT_CHECK(parser.position() == decompressedBlob.size(),
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

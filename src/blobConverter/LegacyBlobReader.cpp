// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "blobConverter/LegacyBlobReader.h"

#include <absl/strings/str_cat.h>
#include <zstd.h>

#include <boost/uuid/uuid.hpp>
#include <cstring>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "index/vocabulary/CompressionWrappers.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializePair.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"

namespace qlever::blobConverter {

namespace {
// The header of the uncompressed legacy blob.
constexpr std::string_view legacyMagicHeader = "QLVUBLOB";
constexpr uint32_t legacyBlobVersion = 1;

// The number of bytes of the uncompressed size that follows the ZSTD frame.
constexpr size_t sizeSuffixLength = sizeof(uint64_t);

// The reader for the decompressed legacy blob (see the comment in the header
// for why the current aligned reader can be used).
using Reader =
    ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                         ql::span<const char>>;

// The exception that is thrown for every input that the reader rejects
// deliberately (with a descriptive message). Unexpected exceptions from the
// serializer (for example on truncated input) are converted to this type by
// `rethrowAsLegacyBlobError`, so that a caller always gets a message that
// names the expected input.
struct LegacyBlobError : std::runtime_error {
  using std::runtime_error::runtime_error;
};

// Throw a `LegacyBlobError` with a message that names the legacy blob.
[[noreturn]] void throwNotALegacyBlob(std::string_view details) {
  throw LegacyBlobError{absl::StrCat(
      "The given input is not a blob in the legacy format of the `qlever-bmw` "
      "fork, or it is corrupted: ",
      details)};
}

// Run `function`. A `LegacyBlobError` passes through unchanged, any other
// exception (typically from the serializer, on truncated input) is rethrown as
// a `LegacyBlobError` with the original message as the details.
template <typename Function>
decltype(auto) rethrowAsLegacyBlobError(const Function& function) {
  try {
    return function();
  } catch (const LegacyBlobError&) {
    throw;
  } catch (const std::exception& e) {
    throwNotALegacyBlob(absl::StrCat("Details: ", e.what()));
  }
}

// The number of bytes of the `reader` that have not been consumed yet.
size_t numRemainingBytes(const Reader& reader) {
  return reader.data().size() - reader.getCurrentPosition();
}

// Throw if `count` elements of `elementSize` bytes each cannot possibly be
// stored in the remaining bytes of the `reader`. Call this before allocating
// anything whose size is controlled by the blob, so that a corrupt count leads
// to a descriptive error instead of a huge allocation. The `description` names
// the counted elements for the error message.
void checkCount(const Reader& reader, uint64_t count, size_t elementSize,
                std::string_view description) {
  if (count > numRemainingBytes(reader) / elementSize) {
    throwNotALegacyBlob(absl::StrCat("the number of ", description, " (", count,
                                     ") exceeds the size of the input"));
  }
}

// Read a value of type `T` (which must be trivially serializable) from the
// `reader`.
template <typename T>
T read(Reader& reader) {
  T value{};
  reader >> value;
  return value;
}

// Read a `std::string` (a `uint64_t` length followed by the characters),
// checking the length against the remaining bytes before allocating.
std::string readString(Reader& reader, std::string_view description) {
  auto size = read<uint64_t>(reader);
  checkCount(reader, size, 1, description);
  std::string result(size, '\0');
  reader.serializeBytes(result.data(), size);
  return result;
}

// Read a vector of trivially serializable values (a `uint64_t` count followed
// by the padding and the values, see `SerializeVector.h`), checking the count
// against the remaining bytes before allocating.
template <typename T>
std::vector<T> readVector(Reader& reader, std::string_view description) {
  auto count = read<uint64_t>(reader);
  checkCount(reader, count, sizeof(T), description);
  ad_utility::serialization::alignSerializerForType<T>(reader);
  auto bytes = reader.getSpanToBytes(count * sizeof(T));
  std::vector<T> result(count);
  std::memcpy(result.data(), bytes.data(), bytes.size());
  return result;
}

// Check the count of a serialized vector of `elementSize`-byte elements
// (aligned to `alignment`) that starts `offset` bytes after the current
// position of the `reader`, WITHOUT consuming anything. Return the offset
// (again relative to the current position) directly after that vector. This
// is used to check the counts inside a structure that is subsequently read via
// its own serialization function (which would allocate before checking).
size_t checkVectorAhead(const Reader& reader, size_t offset, size_t elementSize,
                        size_t alignment, std::string_view description) {
  size_t total = reader.data().size();
  size_t position = reader.getCurrentPosition() + offset;
  if (position > total || total - position < sizeof(uint64_t)) {
    throwNotALegacyBlob(
        absl::StrCat("the input ends before the number of ", description));
  }
  uint64_t count;
  std::memcpy(&count, reader.data().data() + position, sizeof(uint64_t));
  position += sizeof(uint64_t);
  position += (alignment - (position % alignment)) % alignment;
  if (position > total || count > (total - position) / elementSize) {
    throwNotALegacyBlob(absl::StrCat("the number of ", description, " (", count,
                                     ") exceeds the size of the input"));
  }
  return position + count * elementSize - reader.getCurrentPosition();
}

// Read a `SpatialJoinCachedIndex` (the variable name, the serialized S2 index,
// and the map from shape indices to rows). The parts are first read with
// bounds checks, and then handed to the serialization function of
// `SpatialJoinCachedIndex` (which would allocate before checking) via a small
// buffer.
SpatialJoinCachedIndex readGeoIndex(Reader& reader) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  writer << readString(reader, "characters of a geo index variable name");
  writer << readString(reader, "bytes of a serialized S2 index");
  auto numShapes = read<uint64_t>(reader);
  checkCount(reader, numShapes, 2 * sizeof(uint64_t), "shapes of a geo index");
  writer << numShapes;
  for (uint64_t i = 0; i < numShapes; ++i) {
    writer << read<std::pair<size_t, size_t>>(reader);
  }
  ad_utility::serialization::AlignedByteBufferReadSerializer geoReader{
      std::move(writer).data()};
  SpatialJoinCachedIndex geoIndex{
      SpatialJoinCachedIndex::TagForSerialization{}};
  geoReader >> geoIndex;
  return geoIndex;
}

// Read one entry of the legacy `NamedResultCache`.
LegacyNamedCacheEntry readEntry(Reader& reader) {
  using OwnedBlocksEntry =
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry;
  LegacyNamedCacheEntry entry;
  entry.name_ = readString(reader, "characters of a cached result name");
  // The `LocalVocab`: the owned blank node blocks (each a 16-byte UUID and a
  // vector of block indices), then the words, each preceded by the legacy `Id`
  // that referred to it.
  auto numBlocks = read<uint64_t>(reader);
  checkCount(reader, numBlocks, sizeof(boost::uuids::uuid) + sizeof(uint64_t),
             "blank node blocks");
  entry.blankNodeBlocks_.reserve(numBlocks);
  for (uint64_t i = 0; i < numBlocks; ++i) {
    OwnedBlocksEntry block;
    ad_utility::serialization::triviallySerialize(reader, block.uuid_);
    block.blockIndices_ =
        readVector<uint64_t>(reader, "blank node block indices");
    entry.blankNodeBlocks_.push_back(std::move(block));
  }
  auto numWords = read<uint64_t>(reader);
  checkCount(reader, numWords, 2 * sizeof(uint64_t), "local vocabulary words");
  entry.localVocabWords_.reserve(numWords);
  for (uint64_t i = 0; i < numWords; ++i) {
    auto id = read<uint64_t>(reader);
    auto word = readString(reader, "characters of a local vocabulary word");
    entry.localVocabWords_.emplace_back(id, std::move(word));
  }
  // The `IdTable`: the dimensions, then one span of `Id`s per column.
  entry.numRows_ = read<size_t>(reader);
  entry.numColumns_ = read<size_t>(reader);
  checkCount(reader, entry.numColumns_, sizeof(uint64_t), "columns");
  entry.columns_.reserve(entry.numColumns_);
  for (size_t i = 0; i < entry.numColumns_; ++i) {
    auto& column = entry.columns_.emplace_back(
        readVector<uint64_t>(reader, "rows of a column"));
    if (column.size() != entry.numRows_) {
      throwNotALegacyBlob(absl::StrCat("column ", i, " of the cached result \"",
                                       entry.name_, "\" has ", column.size(),
                                       " entries, but the result has ",
                                       entry.numRows_, " rows"));
    }
  }
  // The `VariableToColumnMap`.
  auto numVariables = read<size_t>(reader);
  checkCount(reader, numVariables,
             sizeof(uint64_t) + sizeof(ColumnIndexAndTypeInfo), "variables");
  entry.variables_.reserve(numVariables);
  for (size_t i = 0; i < numVariables; ++i) {
    auto name = readString(reader, "characters of a variable name");
    ColumnIndexAndTypeInfo columnInfo{0, ColumnIndexAndTypeInfo::AlwaysDefined};
    reader >> columnInfo;
    if (columnInfo.columnIndex_ >= entry.numColumns_) {
      throwNotALegacyBlob(absl::StrCat(
          "the variable ", name, " of the cached result \"", entry.name_,
          "\" refers to column ", columnInfo.columnIndex_,
          ", but the result only has ", entry.numColumns_, " columns"));
    }
    entry.variables_.emplace_back(std::move(name), columnInfo);
  }
  entry.resultSortedOn_ = readVector<ColumnIndex>(reader, "sort columns");
  entry.cacheKey_ = readString(reader, "characters of a cache key");
  if (read<bool>(reader)) {
    entry.geoIndex_ = readGeoIndex(reader);
  }
  return entry;
}
}  // namespace

// _____________________________________________________________________________
size_t LegacyBlob::numWords() const {
  return std::visit([](const auto& vocab) { return vocab.size(); },
                    vocabulary_);
}

// _____________________________________________________________________________
std::string LegacyBlob::word(uint64_t index) const {
  return std::visit(
      [index](const auto& vocab) { return std::string{vocab[index]}; },
      vocabulary_);
}

// _____________________________________________________________________________
const LegacyNamedCacheEntry* LegacyBlob::findEntry(
    std::string_view name) const {
  for (const auto& entry : entries_) {
    if (entry.name_ == name) {
      return &entry;
    }
  }
  return nullptr;
}

// _____________________________________________________________________________
DecompressedBuffer decompressLegacyBlob(ql::span<const char> compressedBlob) {
  if (compressedBlob.size() < sizeSuffixLength) {
    throwNotALegacyBlob("the input is too short");
  }
  uint64_t storedUncompressedSize;
  std::memcpy(&storedUncompressedSize,
              compressedBlob.data() + compressedBlob.size() - sizeSuffixLength,
              sizeSuffixLength);
  auto frame = compressedBlob.first(compressedBlob.size() - sizeSuffixLength);
  size_t uncompressedSize = 0;
  try {
    uncompressedSize =
        ZstdWrapper::getUncompressedSize(frame.data(), frame.size());
  } catch (const std::exception& e) {
    throwNotALegacyBlob(e.what());
  }
  if (uncompressedSize != storedUncompressedSize) {
    // A blob in the current format is a bare ZSTD frame (without a trailing
    // size), so in that case the first frame spans the complete input. Point
    // this out, because it is the most likely reason for this mismatch. NOTE:
    // `ZSTD_findFrameCompressedSize` only scans the block headers of the
    // frame, so it is cheap and does not decompress anything.
    size_t frameSize = ZSTD_findFrameCompressedSize(compressedBlob.data(),
                                                    compressedBlob.size());
    bool isBareZstdFrame =
        !ZSTD_isError(frameSize) && frameSize == compressedBlob.size();
    throwNotALegacyBlob(absl::StrCat(
        "the ZSTD frame stores an uncompressed size of ", uncompressedSize,
        " bytes, but the trailing size is ", storedUncompressedSize,
        isBareZstdFrame
            ? ". The input looks like a blob in the current format (a bare "
              "ZSTD frame), which needs no conversion"
            : ""));
  }
  DecompressedBuffer uncompressed(uncompressedSize);
  size_t actualSize = 0;
  try {
    actualSize = ZstdWrapper::decompressToBuffer(
        frame.data(), frame.size(), uncompressed.data(), uncompressed.size());
  } catch (const std::exception& e) {
    throwNotALegacyBlob(e.what());
  }
  if (actualSize != uncompressedSize) {
    throwNotALegacyBlob("the decompressed data has an unexpected size");
  }
  return uncompressed;
}

// _____________________________________________________________________________
LegacyBlob readLegacyBlob(ql::span<const char> decompressedBlob) {
  return rethrowAsLegacyBlobError([&decompressedBlob]() {
    Reader reader{decompressedBlob};
    // The magic header was written as a `std::string`, so it is preceded by
    // its length. Check the length explicitly first, so that arbitrary input
    // does not lead to a huge allocation.
    if (decompressedBlob.size() < sizeof(uint64_t) + legacyMagicHeader.size() +
                                      sizeof(legacyBlobVersion)) {
      throwNotALegacyBlob("the input is too short for the header");
    }
    if (read<uint64_t>(reader) != legacyMagicHeader.size()) {
      throwNotALegacyBlob("the magic header is missing");
    }
    std::string magicHeader(legacyMagicHeader.size(), '\0');
    reader.serializeBytes(magicHeader.data(), magicHeader.size());
    if (magicHeader != legacyMagicHeader) {
      throwNotALegacyBlob(absl::StrCat("expected the magic header \"",
                                       legacyMagicHeader, "\", but found \"",
                                       magicHeader, "\""));
    }
    auto version = read<uint32_t>(reader);
    if (version != legacyBlobVersion) {
      throwNotALegacyBlob(absl::StrCat("the blob has the format version ",
                                       version, ", but only version ",
                                       legacyBlobVersion, " is supported"));
    }

    LegacyBlob result;
    auto metadataString =
        readString(reader, "characters of the index metadata JSON");
    try {
      result.metadata_ = nlohmann::json::parse(metadataString);
    } catch (const std::exception& e) {
      throwNotALegacyBlob(
          absl::StrCat("the index metadata is not valid JSON: ", e.what()));
    }
    if (!result.metadata_.contains("vocabulary-type")) {
      throwNotALegacyBlob("the index metadata has no \"vocabulary-type\" key");
    }
    auto vocabularyType = ad_utility::VocabularyType::fromString(
        static_cast<std::string>(result.metadata_["vocabulary-type"]));
    using Enum = ad_utility::VocabularyType::Enum;
    bool isCompressed = vocabularyType.value() == Enum::InMemoryCompressed;
    if (!isCompressed && vocabularyType.value() != Enum::InMemoryUncompressed) {
      throw LegacyBlobError{absl::StrCat(
          "The legacy blob has the vocabulary type \"",
          vocabularyType.toString(),
          "\", but only the types \"in-memory-uncompressed\" and "
          "\"in-memory-compressed\" are supported by the converter")};
    }
    // The vocabularies are read via their own serialization functions, which
    // would allocate for a corrupt count before checking it (see
    // `SerializeVector.h`), so check all their counts first: the word data
    // and the offsets of the `VocabularyInMemory`, and the decoders of the
    // `CompressedVocabulary`.
    using Decoder =
        ad_utility::vocabulary::FsstSquaredCompressionWrapper::Decoder;
    auto offset = checkVectorAhead(reader, 0, 1, 1, "vocabulary bytes");
    offset = checkVectorAhead(reader, offset, sizeof(uint64_t),
                              alignof(uint64_t), "vocabulary offsets");
    if (isCompressed) {
      checkVectorAhead(reader, offset, sizeof(Decoder), alignof(Decoder),
                       "vocabulary decoders");
      reader >> result.vocabulary_
                    .emplace<CompressedVocabulary<VocabularyInMemory>>();
    } else {
      reader >> result.vocabulary_.emplace<VocabularyInMemory>();
    }

    auto numEntries = read<size_t>(reader);
    // An entry has at least a name, the two local vocabulary counts, and the
    // two dimensions.
    checkCount(reader, numEntries, 5 * sizeof(uint64_t),
               "named cached queries");
    result.entries_.reserve(numEntries);
    for (size_t i = 0; i < numEntries; ++i) {
      result.entries_.push_back(readEntry(reader));
    }
    if (numRemainingBytes(reader) != 0) {
      throwNotALegacyBlob(absl::StrCat(
          numRemainingBytes(reader),
          " unexpected trailing bytes after the named result cache"));
    }
    return result;
  });
}

// _____________________________________________________________________________
LegacyBlob readLegacyBlobFromCompressed(ql::span<const char> compressedBlob) {
  auto decompressed = decompressLegacyBlob(compressedBlob);
  return readLegacyBlob(decompressed);
}

}  // namespace qlever::blobConverter

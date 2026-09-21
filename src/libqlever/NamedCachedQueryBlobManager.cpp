// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/NamedCachedQueryBlobManager.h"

#include <absl/strings/str_cat.h>

#include <array>
#include <cstdint>
#include <string_view>
#include <type_traits>
#include <utility>

#include "backports/algorithm.h"
#include "engine/NamedResultCacheSerializer.h"
#include "index/IndexImpl.h"
#include "index/vocabulary/BuildFilteredVocabulary.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/SecondaryVocabulary.h"
#include "libqlever/Qlever.h"
#include "util/CompactStringVector.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/HashMap.h"
#include "util/Random.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/json.h"

namespace qlever {

namespace {
// The header that is written at the beginning of every blob (see
// `NamedCachedQueryBlobManager::writeBlobHeader` /
// `NamedCachedQueryBlobManager::skipAndVerifyBlobHeader`), to guard against
// loading a blob written by an incompatible version of QLever.
constexpr std::array<char, 8> blobMagicBytes{'Q', 'L', 'V', 'R',
                                             'B', 'L', 'O', 'B'};
constexpr uint16_t blobFormatVersion = 2;

// The number of bytes written by `writeBlobHeader`. Note that no alignment
// padding is inserted between the two members, because `blobMagicBytes` has an
// alignment of one and its size is a multiple of the alignment of
// `blobFormatVersion`.
constexpr size_t blobHeaderSize =
    sizeof(blobMagicBytes) + sizeof(blobFormatVersion);
static_assert(sizeof(blobMagicBytes) % alignof(uint16_t) == 0);

// The message that is reported for any input that is not a blob written by
// `NamedCachedQueryBlobManager::serialize`.
constexpr std::string_view blobNotReadableMessage =
    "The given blob was not written by "
    "`Qlever::serializeVocabAndNamedCacheToCompressedBlob`, or is corrupted";

// The message that is reported when the contents of a blob cannot be read, even
// though its header is valid.
constexpr std::string_view blobContentsNotReadableMessage =
    "Error while reading the contents of a blob written by "
    "`Qlever::serializeVocabAndNamedCacheToCompressedBlob`; the blob is "
    "probably corrupted";

// The precondition of `serialize`, see the comment at
// `NamedCachedQueryBlobManager::serialize`.
constexpr std::string_view noSecondaryVocabularyMessage =
    "A blob can only be created from an index without a secondary vocabulary. "
    "In particular, a `Qlever` instance that was itself loaded from a blob "
    "whose named cache entries contained new words cannot be used to create "
    "another blob";

// The alignment to which the beginning of every chunk, and hence also the
// beginning of every chunk payload, is padded (see
// `NamedCachedQueryBlobManager::parseBlobLayout` for the chunk format and for
// why this matters).
constexpr size_t chunkAlignment = alignof(std::max_align_t);
using BlobLayout = NamedCachedQueryBlobManager::BlobLayout;
static_assert(BlobLayout::chunkHeaderSize == chunkAlignment);
// The chunk header consists of the `uint64_t` size field plus the padding that
// aligns the payload, so the alignment has to leave room for that size field,
// and it has to be a multiple of its alignment.
static_assert(chunkAlignment >= sizeof(uint64_t));
static_assert(chunkAlignment % alignof(uint64_t) == 0);

// The serializer types that the blob format uses. Note that a blob is written
// and read with *aligned* serialization, which is what makes the zero-copy
// deserialization possible.
using BlobWriter = ad_utility::serialization::AlignedByteBufferWriteSerializer;
using BlobReader =
    ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                         ql::span<const char>>;

// Run `function` and, if it throws, rethrow with `message` prepended. That way,
// the rather cryptic low-level error messages (in particular those of ZSTD)
// never reach the user unadorned.
template <typename Function>
decltype(auto) rethrowWithContext(std::string_view message,
                                  const Function& function) {
  try {
    return function();
  } catch (const std::exception& e) {
    AD_THROW(absl::StrCat(message, ". Details: ", e.what()));
  }
}

// The positions that `endChunk` needs to complete a chunk that `beginChunk`
// has started.
struct ChunkHandle {
  // The position of the `uint64_t` size field of the chunk.
  size_t sizeFieldPosition_;
  // The position at which the payload of the chunk begins.
  size_t payloadPosition_;
};

// Pad the `writer` with zeros up to the next multiple of `chunkAlignment`.
void padToChunkAlignment(BlobWriter& writer) {
  ad_utility::serialization::alignSerializerForType<std::max_align_t>(writer);
}

// Begin a new chunk (see `NamedCachedQueryBlobManager::parseBlobLayout` for the
// format): pad to the chunk alignment, write a placeholder for the size of the
// payload, and pad again, so that the payload begins at a multiple of the chunk
// alignment. The size is only known once the payload has been written, and is
// patched by `endChunk`.
[[nodiscard]] ChunkHandle beginChunk(BlobWriter& writer) {
  padToChunkAlignment(writer);
  size_t sizeFieldPosition = writer.getCurrentPosition();
  writer << uint64_t{0};
  padToChunkAlignment(writer);
  return {sizeFieldPosition, writer.getCurrentPosition()};
}

// Complete the chunk that `beginChunk` returned the `handle` for, by patching
// its size field with the number of payload bytes that have been written since.
void endChunk(BlobWriter& writer, const ChunkHandle& handle) {
  uint64_t payloadSize = writer.getCurrentPosition() - handle.payloadPosition_;
  writer.overwriteBytes(handle.sizeFieldPosition_,
                        reinterpret_cast<const char*>(&payloadSize),
                        sizeof(payloadSize));
}

// Write one complete chunk whose payload is written by `writePayload`.
template <typename Function>
void writeChunk(BlobWriter& writer, const Function& writePayload) {
  auto handle = beginChunk(writer);
  writePayload(writer);
  endChunk(writer, handle);
}

// One chunk, as returned by `readChunk`.
struct Chunk {
  BlobLayout::Region region_;
  ql::span<const char> payload_;
};

// Read the next chunk from `reader` (which must be positioned at its
// beginning), and advance the `reader` past it. Note that the payload is a
// zero-copy view into the buffer of the `reader`, which therefore has to
// outlive the returned `Chunk` (and everything that is deserialized from it).
Chunk readChunk(BlobReader& reader) {
  ad_utility::serialization::alignSerializerForType<std::max_align_t>(reader);
  size_t regionBegin = reader.getCurrentPosition();
  uint64_t payloadSize = 0;
  reader >> payloadSize;
  ad_utility::serialization::alignSerializerForType<std::max_align_t>(reader);
  AD_CORRECTNESS_CHECK(reader.getCurrentPosition() ==
                       regionBegin + BlobLayout::chunkHeaderSize);
  auto payload = reader.getSpanToBytes(payloadSize);
  return {{regionBegin, reader.getCurrentPosition()}, payload};
}

// Return a serializer that reads the `payload` of a chunk. Note that the
// payload of a chunk begins at a multiple of `chunkAlignment` inside a buffer
// that is aligned to `chunkAlignment`, so the alignment that the constructor of
// the returned serializer requires is guaranteed, and the alignment padding
// inside the payload is at the same offsets as when the payload was written.
BlobReader makeSubReader(ql::span<const char> payload) {
  return BlobReader{payload};
}

// Read a single value of a trivially serializable type `T` that makes up the
// complete payload of the given `chunk`.
template <typename T>
T readScalarChunk(const Chunk& chunk) {
  auto subReader = makeSubReader(chunk.payload_);
  T value;
  subReader >> value;
  return value;
}

// Read the next chunk of `reader` (see `readChunk`) and return the result of
// `readPayload` applied to a sub-reader for its payload (see `makeSubReader`).
// This discards the chunk's region, so use `readChunk` directly instead when
// the region is also needed (as it is, for example, when building a
// `BlobLayout`). Note that this does not break the zero-copy semantics of the
// payload: the sub-reader that `readPayload` is applied to still spans a view
// into the buffer of the outer `reader`.
template <typename Function>
decltype(auto) readChunkPayload(BlobReader& reader,
                                const Function& readPayload) {
  auto chunk = readChunk(reader);
  auto subReader = makeSubReader(chunk.payload_);
  return readPayload(subReader);
}

// The `BlobReader` counterpart of `readScalarChunk(const Chunk&)`: read a
// single value of a trivially serializable type `T` that makes up the
// complete payload of the next chunk of `reader`, without keeping that
// chunk's region.
template <typename T>
T readScalarChunk(BlobReader& reader) {
  return readChunkPayload(reader, [](BlobReader& subReader) {
    T value;
    subReader >> value;
    return value;
  });
}

// Write one complete chunk whose payload is the single value `value` of a
// trivially serializable type `T` (see `writeChunk`).
template <typename T>
void writeScalarChunk(BlobWriter& writer, const T& value) {
  writeChunk(writer,
             [&value](BlobWriter& payloadWriter) { payloadWriter << value; });
}

// Write the index metadata JSON and the `vocabulary` of `indexImpl` (which has
// to be passed separately, see the NOTE below) as the first two chunks of a
// blob, omitting all vocabulary entries that match one of the
// `excludedEntryRegexes` (which must not be empty). The metadata JSON is
// written with its `"vocabulary-type"` set to the type of the filtered
// vocabulary, so that the reading side (which applies the metadata JSON before
// loading the vocabulary) sets up the matching vocabulary implementation.
//
// NOTE: This is a template (with the type of the vocabulary as its parameter),
// so that the `if constexpr` below actually discards the branch that does not
// apply. In a non-template function both branches would have to compile, which
// the call to `buildFilteredVocabulary` does not for a vocabulary that is not a
// `PolymorphicVocabulary`.
template <typename VocabularyImpl>
void writeMetadataAndFilteredVocabulary(
    BlobWriter& writer, const IndexImpl& indexImpl,
    const VocabularyImpl& vocabulary,
    const std::vector<std::string>& excludedEntryRegexes) {
  // The filtering is implemented for the `PolymorphicVocabulary`, which is the
  // vocabulary implementation that QLever is built with by default (see
  // `detail::UnderlyingVocabRdfsVocabulary`).
  if constexpr (std::is_same_v<VocabularyImpl, PolymorphicVocabulary>) {
    // Use a unique temporary basename (next to the index, because the
    // intermediate on-disk vocabulary can become as large as the vocabulary
    // itself), so that concurrent calls do not interfere with each other.
    ad_utility::UuidGenerator uuidGenerator;
    auto filtered = buildFilteredVocabulary(
        vocabulary, excludedEntryRegexes,
        absl::StrCat(indexImpl.getOnDiskBase(),
                     ".tmp-filtered-blob-vocabulary.", uuidGenerator()));
    nlohmann::json metadata = indexImpl.configurationJson();
    metadata["vocabulary-type"] = filtered.type_;
    writeChunk(writer, [&metadata](BlobWriter& payloadWriter) {
      payloadWriter << metadata.dump();
    });
    // NOTE: This writes exactly the same format that
    // `Vocabulary::writeAsZeroCopyBlob` writes (and that
    // `Vocabulary::loadFromZeroCopyDeserializer` reads back), because both use
    // the generic serialization of the active alternative of the
    // `PolymorphicVocabulary`, and no comparator is part of that format:
    // `filtered.vocabulary_` is a bare `PolymorphicVocabulary` that has no
    // wrapping `UnicodeVocabulary` to begin with, and
    // `Vocabulary::writeAsZeroCopyBlob` explicitly bypasses its own wrapping
    // `UnicodeVocabulary`.
    writeChunk(writer, [&filtered](BlobWriter& payloadWriter) {
      payloadWriter << filtered.vocabulary_;
    });
  } else {
    AD_THROW(
        "Excluding vocabulary entries from a blob is only supported for the "
        "polymorphic vocabulary, but QLever was compiled with "
        "`QLEVER_VOCAB_UNCOMPRESSED_IN_MEMORY`");
  }
}

// Assign the `Id`s of the secondary vocabulary of a blob (see
// `index/vocabulary/SecondaryVocabulary.h`) to the words of those local vocab
// entries that occur in the named cache entries that are written to that blob.
// Such a word is *new* in the sense that it is not part of the vocabulary of
// the index, which happens when SPARQL UPDATE operations were applied before
// the entry was pinned.
//
// The new words that `rewrite` encounters are appended in the order in which
// they are encountered, and together form the single segment that the blob adds
// to its secondary vocabulary (see `newSegment`).
class SecondaryVocabularyBuilder {
 private:
  // The index in the secondary vocabulary of the blob, for every new word that
  // has an index so far.
  ad_utility::HashMap<std::string, uint64_t> wordToIndex_;

  // The new words, in the order in which they were encountered.
  std::vector<std::string> newWords_;

 public:
  // Rewrite a single `Id` of a named cache entry. An `Id` that does not refer
  // to a local vocab entry is returned unchanged. For an `Id` that does, the
  // position of the word in the vocabularies of the index decides: if the word
  // is contained in the vocabulary of the main index (or is encodable as an
  // `EncodedVal`), then the `Id` of that position is used; else the word is
  // added to the secondary vocabulary of the blob (if it is not already there)
  // and the corresponding `Id` of type `Datatype::SecondaryVocabIndex` is
  // used.
  Id rewrite(Id id) {
    if (id.getDatatype() != Datatype::LocalVocabIndex) {
      return id;
    }
    const LocalVocabEntry& entry = *id.getLocalVocabIndex();
    auto [lowerBound, upperBound] = entry.positionInVocab();
    if (lowerBound != upperBound) {
      // The word is contained in the vocabulary of the main index, or is
      // encodable, in which case the position is exactly the `Id` of the word.
      return Id::fromBits(lowerBound.get());
    }
    std::string word = entry.toStringRepresentation();
    auto iterator = wordToIndex_.find(word);
    if (iterator == wordToIndex_.end()) {
      uint64_t index = wordToIndex_.size();
      iterator = wordToIndex_.emplace(word, index).first;
      newWords_.push_back(std::move(word));
    }
    return Id::makeFromSecondaryVocabIndex(
        SecondaryVocabIndex::make(iterator->second));
  }

  // Whether `rewrite` has encountered any new word, and hence whether a segment
  // of the secondary vocabulary has to be written at all.
  bool hasNewWords() const { return !newWords_.empty(); }

  // The segment with the new words that `rewrite` has encountered.
  CompactVectorOfStrings<char> newSegment() const {
    CompactVectorOfStrings<char> segment;
    segment.build(newWords_);
    return segment;
  }
};

// The columns of a named cache entry with all `Id`s rewritten (see
// `SecondaryVocabularyBuilder::rewrite`), together with the sort order that
// still holds for them.
struct RewrittenColumns {
  std::vector<std::vector<Id>> columns_;
  std::vector<ColumnIndex> resultSortedOn_;
};

// Rewrite all `Id`s of the given cache entry `value`, see
// `SecondaryVocabularyBuilder::rewrite`. A column in which at least one `Id`
// was rewritten is no longer guaranteed to be sorted (the internal order of
// the `Id`s of a secondary vocabulary is unrelated to the order of the local
// vocab entries that they replace), so the sort order is truncated before the
// first such column.
RewrittenColumns rewriteColumns(const NamedResultCache::Value& value,
                                SecondaryVocabularyBuilder& builder) {
  auto view = ExplicitIdTableOperation::viewOf(value.result_);
  RewrittenColumns result;
  std::vector<bool> columnWasRewritten;
  for (const auto& column : view.getColumns()) {
    std::vector<Id> rewrittenColumn;
    rewrittenColumn.reserve(column.size());
    bool wasRewritten = false;
    for (Id id : column) {
      Id rewrittenId = builder.rewrite(id);
      wasRewritten = wasRewritten || rewrittenId != id;
      rewrittenColumn.push_back(rewrittenId);
    }
    result.columns_.push_back(std::move(rewrittenColumn));
    columnWasRewritten.push_back(wasRewritten);
  }
  auto firstRewritten = ql::ranges::find_if(
      value.resultSortedOn_, [&columnWasRewritten](ColumnIndex column) {
        return column < columnWasRewritten.size() && columnWasRewritten[column];
      });
  result.resultSortedOn_.assign(value.resultSortedOn_.begin(), firstRewritten);
  return result;
}

// Collect the words of all local vocab entries that occur in the given cache
// entry `value` (and hence assign their `Id`s), without producing the
// rewritten columns. This is the first of the two passes over the cache
// entries that the writing of a blob needs: only once all words are known can
// the segment of the secondary vocabulary be written, and that segment
// precedes the entries in the blob. Doing it this way (instead of keeping the
// result of `rewriteColumns` for all entries) means that the rewritten columns
// of only one entry at a time have to be held in memory.
void collectNewWords(const NamedResultCache::Value& value,
                     SecondaryVocabularyBuilder& builder) {
  auto view = ExplicitIdTableOperation::viewOf(value.result_);
  for (const auto& column : view.getColumns()) {
    for (Id id : column) {
      if (id.getDatatype() == Datatype::LocalVocabIndex) {
        [[maybe_unused]] Id rewrittenId = builder.rewrite(id);
      }
    }
  }
}

// Run `collectNewWords` (the first pass over the cache entries, see there)
// over all of `entries`.
void collectAllNewWords(
    const std::vector<std::pair<
        NamedResultCache::Key, std::shared_ptr<const NamedResultCache::Value>>>&
        entries,
    SecondaryVocabularyBuilder& builder) {
  for (const auto& [key, value] : entries) {
    collectNewWords(*value, builder);
  }
}

// Write the payload of the chunk of one entry of the `NamedResultCache`: its
// `key`, followed by the `value` with all its `Id`s rewritten (see
// `rewriteColumns`). The words of the local vocab of the `value` are
// deliberately not written, because they are stored in the secondary
// vocabulary of the blob instead.
void writeEntryPayload(BlobWriter& writer, const std::string& key,
                       const NamedResultCache::Value& value,
                       SecondaryVocabularyBuilder& builder) {
  writer << key;
  auto rewritten = rewriteColumns(value, builder);
  namedResultCacheSerializer::writeValue(writer, value, rewritten.columns_,
                                         rewritten.resultSortedOn_,
                                         /*writeLocalVocabWords=*/false);
}

// Return a function that writes the payload of the chunk of the named cache
// entry `key`/`value` (see `writeEntryPayload`), suitable for `writeChunk`.
auto makeEntryPayloadWriter(const std::string& key,
                            const NamedResultCache::Value& value,
                            SecondaryVocabularyBuilder& builder) {
  return [&key, &value, &builder](BlobWriter& payloadWriter) {
    writeEntryPayload(payloadWriter, key, value, builder);
  };
}
}  // namespace

// _____________________________________________________________________________
void NamedCachedQueryBlobManager::writeBlobHeader(
    ad_utility::serialization::AlignedByteBufferWriteSerializer& serializer) {
  serializer << blobMagicBytes;
  serializer << blobFormatVersion;
}

// _____________________________________________________________________________
void NamedCachedQueryBlobManager::skipAndVerifyBlobHeader(
    ad_utility::serialization::ByteBufferReadSerializerT<
        true, ql::span<const char>>& serializer) {
  // Explicitly check that the header is complete, so that a truncated blob is
  // reported with the message below instead of with the rather cryptic message
  // of the serializer.
  AD_CONTRACT_CHECK(
      serializer.data().size() - serializer.getCurrentPosition() >=
          blobHeaderSize,
      blobNotReadableMessage);
  std::decay_t<decltype(blobMagicBytes)> magicBytes{};
  serializer >> magicBytes;
  AD_CONTRACT_CHECK(magicBytes == blobMagicBytes, blobNotReadableMessage);
  uint16_t version;
  serializer >> version;
  AD_CONTRACT_CHECK(
      version == blobFormatVersion,
      "The given blob was written by an incompatible version of QLever "
      "(format version ",
      version, ", expected ", blobFormatVersion, ")");
}

// _____________________________________________________________________________
std::vector<char> NamedCachedQueryBlobManager::compressBlob(
    ql::span<const char> uncompressedBlob) {
  return ZstdWrapper::compress(uncompressedBlob.data(),
                               uncompressedBlob.size());
}

// _____________________________________________________________________________
std::vector<char, NamedCachedQueryBlobManager::BlobAllocator>
NamedCachedQueryBlobManager::decompressBlob(
    ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  // Read the size of the uncompressed data from the ZSTD frame header (which
  // always stores it, because `compressBlob` uses the one-shot
  // `ZSTD_compress`). This also validates that `compressedBlob` starts with a
  // ZSTD frame at all, so that arbitrary garbage is rejected right here,
  // instead of being misinterpreted as an (arbitrarily large) size for the
  // allocation below.
  size_t uncompressedSize =
      rethrowWithContext(blobNotReadableMessage, [&compressedBlob]() {
        return ZstdWrapper::getUncompressedSize(compressedBlob.data(),
                                                compressedBlob.size());
      });

  // Decompress into a buffer that is 1. allocated via the caller-provided
  // `allocator`, 2. aligned to the maximal possible alignment (required for the
  // zero-copy deserialization), and 3. not needlessly zero-initialized before
  // the decompression overwrites it (see `BlobAllocator`).
  std::vector<char, BlobAllocator> uncompressed(
      uncompressedSize,
      BlobAllocator{ad_utility::AlignedAllocator<
          char, ql::pmr::polymorphic_allocator<char>>{allocator}});
  auto actualUncompressedSize = rethrowWithContext(
      blobNotReadableMessage, [&compressedBlob, &uncompressed]() {
        return ZstdWrapper::decompressToBuffer(
            compressedBlob.data(), compressedBlob.size(), uncompressed.data(),
            uncompressed.size());
      });
  AD_CORRECTNESS_CHECK(actualUncompressedSize == uncompressedSize);
  return uncompressed;
}

// _____________________________________________________________________________
NamedCachedQueryBlobManager::BlobLayout
NamedCachedQueryBlobManager::parseBlobLayout(
    ql::span<const char> uncompressedBlob) {
  BlobReader reader{uncompressedBlob};
  skipAndVerifyBlobHeader(reader);
  BlobLayout layout;
  layout.metadata_ = readChunk(reader).region_;
  layout.vocabulary_ = readChunk(reader).region_;

  auto segmentCountChunk = readChunk(reader);
  layout.segmentCount_ = segmentCountChunk.region_;
  auto numSegments = readScalarChunk<uint64_t>(segmentCountChunk);
  for (uint64_t i = 0; i < numSegments; ++i) {
    layout.segments_.push_back(readChunk(reader).region_);
  }

  auto entryCountChunk = readChunk(reader);
  layout.entryCount_ = entryCountChunk.region_;
  auto numEntries = readScalarChunk<uint64_t>(entryCountChunk);
  for (uint64_t i = 0; i < numEntries; ++i) {
    auto entryChunk = readChunk(reader);
    // The key of an entry is stored at the very beginning of its payload.
    auto subReader = makeSubReader(entryChunk.payload_);
    std::string key;
    subReader >> key;
    layout.entries_.push_back({std::move(key), entryChunk.region_});
  }
  return layout;
}

// _____________________________________________________________________________
std::vector<char> NamedCachedQueryBlobManager::serialize(
    const Qlever& qlever, const BlobSerializationConfig& config) const {
  auto indexAndViews = qlever.indexAndViewsSnapshot();
  const auto& indexImpl = indexAndViews->index_.getImpl();
  AD_CONTRACT_CHECK(indexImpl.secondaryVocab() == nullptr,
                    noSecondaryVocabularyMessage);

  // The first pass over the cache entries, which assigns the `Id`s of the
  // secondary vocabulary of the blob to all the words that only exist as local
  // vocab entries (see `collectNewWords`).
  auto entries = qlever.namedResultCache_.getAllEntries();
  SecondaryVocabularyBuilder vocabularyBuilder;
  collectAllNewWords(entries, vocabularyBuilder);

  // Serialize everything into an uncompressed, suitably aligned buffer. The
  // alignment (guaranteed by the `AlignedByteBufferWriteSerializer`) is
  // required so that the buffer can later be deserialized zero-copy (see
  // `deserialize`).
  BlobWriter writer;
  writeBlobHeader(writer);

  // Serialize the index metadata JSON together with the vocabulary, so that the
  // blob is self-contained and the loading side can set up the vocabulary
  // configuration without access to the on-disk index. Without excluded
  // entries, the metadata JSON and the vocabulary are written as they are;
  // with excluded entries, the vocabulary is filtered and the
  // `"vocabulary-type"` entry of the metadata JSON is rewritten to the type of
  // the filtered vocabulary (see `writeMetadataAndFilteredVocabulary`).
  if (config.excludedEntryRegexes_.empty()) {
    writeChunk(writer, [&indexImpl](BlobWriter& payloadWriter) {
      payloadWriter << indexImpl.configurationJson().dump();
    });
    writeChunk(writer, [&indexImpl](BlobWriter& payloadWriter) {
      indexImpl.writeVocabularyToZeroCopyBlob(payloadWriter);
    });
  } else {
    writeMetadataAndFilteredVocabulary(
        writer, indexImpl, indexImpl.getVocab().getUnderlyingVocabulary(),
        config.excludedEntryRegexes_);
  }

  // The secondary vocabulary of the blob, which consists of a single segment
  // with the new words (or of no segment at all, if there are none).
  uint64_t numSegments = vocabularyBuilder.hasNewWords() ? 1 : 0;
  writeScalarChunk(writer, numSegments);
  if (numSegments == 1) {
    auto segment = vocabularyBuilder.newSegment();
    writeChunk(writer, [&segment](BlobWriter& payloadWriter) {
      payloadWriter << segment;
    });
  }

  // The entries of the `NamedResultCache`, in the second pass over them.
  writeScalarChunk(writer, uint64_t{entries.size()});
  for (const auto& [key, value] : entries) {
    writeChunk(writer, makeEntryPayloadWriter(key, *value, vocabularyBuilder));
  }

  auto uncompressed = std::move(writer).data();
  return compressBlob(uncompressed);
}

// _____________________________________________________________________________
void NamedCachedQueryBlobManager::deserialize(
    Qlever& qlever, ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  AD_CONTRACT_CHECK(
      !deserializedBlobLifetimeExtender_.has_value(),
      "`deserializeVocabAndNamedCacheFromCompressedBlob` must not be called "
      "more than once on the same `Qlever` instance");

  // Decompress into `deserializedBlobLifetimeExtender_`, which is kept alive
  // for the lifetime of this manager because the vocabulary and named result
  // cache entries loaded below are zero-copy views directly into it.
  deserializedBlobLifetimeExtender_.emplace(
      decompressBlob(compressedBlob, allocator));

  // Use a serializer that only borrows a view of
  // `deserializedBlobLifetimeExtender_`, rather than one that owns/moves it, so
  // that the buffer stays owned by `deserializedBlobLifetimeExtender_` for the
  // rest of this manager's lifetime.
  BlobReader reader{
      ql::span<const char>{deserializedBlobLifetimeExtender_.value()}};

  skipAndVerifyBlobHeader(reader);

  auto indexAndViews = qlever.indexAndViewsSnapshot();
  auto& indexImpl = indexAndViews->index_.getImpl();
  // The header is valid, but the contents may still be corrupted. Wrap the
  // reading of the contents, so that the user gets a message that names the
  // expected input, instead of a low-level error from deep inside the
  // deserialization.
  rethrowWithContext(blobContentsNotReadableMessage, [&indexImpl, &reader,
                                                      &qlever,
                                                      &indexAndViews]() {
    // Read and apply the index metadata JSON before loading the vocabulary,
    // so that the vocabulary is set up with the correct configuration
    // (locale, comparator, etc.).
    std::string metadataJson =
        readChunkPayload(reader, [](BlobReader& subReader) {
          std::string json;
          subReader >> json;
          return json;
        });
    indexImpl.applyConfiguration(nlohmann::json::parse(metadataJson));

    readChunkPayload(reader, [&indexImpl](BlobReader& subReader) {
      indexImpl.loadVocabularyFromZeroCopyBlob(subReader);
    });

    // The segments of the secondary vocabulary, which have to be appended
    // in exactly the order in which they are stored, because the `Id` of a
    // word is its position in the concatenation of all segments.
    auto numSegments = readScalarChunk<uint64_t>(reader);
    SecondaryVocabulary secondaryVocabulary;
    for (uint64_t i = 0; i < numSegments; ++i) {
      secondaryVocabulary.appendSegment(
          readChunkPayload(reader, [](BlobReader& subReader) {
            return CompactVectorOfStrings<char>::fromZeroCopyDeserializer(
                subReader);
          }));
    }
    if (secondaryVocabulary.numWords() > 0) {
      indexImpl.setSecondaryVocab(std::make_shared<const SecondaryVocabulary>(
          std::move(secondaryVocabulary)));
    }

    // The entries of the `NamedResultCache`.
    auto numEntries = readScalarChunk<uint64_t>(reader);
    qlever.namedResultCache_.clear();
    for (uint64_t i = 0; i < numEntries; ++i) {
      auto [key, value] = readChunkPayload(
          reader, [&qlever, &indexAndViews](BlobReader& subReader) {
            NamedResultCache::Key key;
            subReader >> key;
            NamedResultCache::Value value;
            value.allocatorForSerialization_ = qlever.allocator_;
            value.contextForSerialization_ =
                &indexAndViews->index_.getLocalVocabContext();
            subReader >> value;
            return std::pair{std::move(key), std::move(value)};
          });
      qlever.namedResultCache_.store(key, std::move(value));
    }
  });
}

}  // namespace qlever

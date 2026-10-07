// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/NamedCachedQueryBlobManager.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <array>
#include <cstdint>
#include <string_view>
#include <type_traits>
#include <variant>

#include "backports/algorithm.h"
#include "engine/ExplicitIdTableOperation.h"
#include "engine/NamedResultCacheSerializer.h"
#include "index/IndexImpl.h"
#include "index/vocabulary/BuildFilteredVocabulary.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/SecondaryVocabulary.h"
#include "libqlever/CanonicalRowOrder.h"
#include "libqlever/NamedCacheSecondaryVocabRewriter.h"
#include "libqlever/Qlever.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Log.h"
#include "util/Random.h"
#include "util/json.h"

namespace qlever {

namespace {
// The header that is written at the beginning of every blob (see
// `NamedCachedQueryBlobManager::writeBlobHeader` /
// `NamedCachedQueryBlobManager::tryToSkipAndVerifyBlobHeader`), to guard
// against loading a blob written by an incompatible version of QLever.
constexpr std::array<char, 8> blobMagicBytes{'Q', 'L', 'V', 'R',
                                             'B', 'L', 'O', 'B'};
using Manager = NamedCachedQueryBlobManager;

// The number of bytes written by `writeBlobHeader`. Note that no alignment
// padding is inserted between the two members, because `blobMagicBytes` has an
// alignment of one and its size is a multiple of the alignment of the format
// version.
constexpr size_t blobHeaderSize =
    sizeof(blobMagicBytes) + sizeof(Manager::formatVersionWithSecondaryVocab);
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

// Write the index metadata JSON and the `vocabulary` of `indexImpl` (which has
// to be passed separately, see the NOTE below) to `serializer`, omitting all
// vocabulary entries that match one of the `excludedEntryRegexes` (which must
// not be empty). The metadata JSON is written with its `"vocabulary-type"` set
// to the type of the filtered vocabulary, so that the reading side (which
// applies the metadata JSON before loading the vocabulary) sets up the matching
// vocabulary implementation.
//
// NOTE: This is a template (with the type of the vocabulary as its parameter),
// so that the `if constexpr` below actually discards the branch that does not
// apply. In a non-template function both branches would have to compile, which
// the call to `buildFilteredVocabulary` does not for a vocabulary that is not a
// `PolymorphicVocabulary`.
template <typename VocabularyImpl>
void writeMetadataAndFilteredVocabulary(
    ad_utility::serialization::AlignedByteBufferWriteSerializer& serializer,
    const IndexImpl& indexImpl, const VocabularyImpl& vocabulary,
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
    serializer << metadata.dump();
    // NOTE: This writes exactly the same format that
    // `Vocabulary::writeAsZeroCopyBlob` writes (and that
    // `Vocabulary::loadFromZeroCopyDeserializer` reads back), because both use
    // the generic serialization of the active alternative of the
    // `PolymorphicVocabulary`, and no comparator is part of that format:
    // `filtered.vocabulary_` is a bare `PolymorphicVocabulary` that has no
    // wrapping `UnicodeVocabulary` to begin with, and
    // `Vocabulary::writeAsZeroCopyBlob` explicitly bypasses its own wrapping
    // `UnicodeVocabulary`.
    serializer << filtered.vocabulary_;
  } else {
    AD_THROW(
        "Excluding vocabulary entries from a blob is only supported for the "
        "polymorphic vocabulary, but QLever was compiled with "
        "`QLEVER_VOCAB_UNCOMPRESSED_IN_MEMORY`");
  }
}

// Return the extension of the geo index of the base entry with the same `key`
// as the entry `original` (whose canonicalized copy is `canonicalized`), or
// `std::nullopt` if the extension is not possible or if it has to be compacted
// (see `BlobSerializationConfig::IncrementalBase`), in which case the geo
// index of `canonicalized` (which was built from scratch) is to be used. The
// result is in canonical row order. The `index` is used to resolve the `Id`s
// of the table of `original`.
//
// NOTE: The new geometries have to be read from the table of `original`
// (including all its rows in the original order), because the table of
// `canonicalized` may contain `Id`s that only the secondary vocabulary of the
// blob can resolve.
std::optional<SpatialJoinCachedIndex> tryToExtendGeoIndex(
    const std::string& key, const NamedResultCache::Value& original,
    const namedCacheSecondaryVocab::CanonicalizedValue& canonicalized,
    const NamedResultCache::Entries& baseEntries,
    const BlobSerializationConfig::IncrementalBase& incremental,
    const Index& index) {
  const auto& canonicalValue = canonicalized.value_;
  const auto& newGeoIndex = original.cachedGeoIndex_.value();
  auto it = ql::ranges::lower_bound(
      baseEntries, key, {}, &NamedResultCache::Entries::value_type::first);
  if (it == baseEntries.end() || it->first != key) {
    return std::nullopt;
  }
  const auto& baseValue = *it->second;
  auto skip = [&key](std::string_view reason) {
    AD_LOG_INFO << "The geo index of the named result \"" << key
                << "\" is built from scratch, because " << reason << std::endl;
    return std::nullopt;
  };
  if (!baseValue.cachedGeoIndex_.has_value()) {
    return skip("the base entry has no geo index");
  }
  const auto& baseGeoIndex = baseValue.cachedGeoIndex_.value();
  auto baseView = ExplicitIdTableOperation::viewOf(baseValue.result_);
  auto newView = ExplicitIdTableOperation::viewOf(canonicalValue.result_);
  if (baseView.numColumns() != newView.numColumns() ||
      baseGeoIndex.getGeometryColumn() != newGeoIndex.getGeometryColumn() ||
      baseGeoIndex.simplificationErrorInMeters() !=
          newGeoIndex.simplificationErrorInMeters() ||
      baseValue.varToColMap_ != canonicalValue.varToColMap_ ||
      baseValue.resultSortedOn_ != canonicalValue.resultSortedOn_ ||
      baseGeoIndex.numRows() != baseView.numRows()) {
    return skip("the structure of the base entry differs");
  }
  if (!isInCanonicalOrder(baseView, baseValue.resultSortedOn_)) {
    return skip("the table of the base entry is not in canonical order");
  }

  // Translate the matching from the canonical rows of the new table to the
  // original rows of the table of `original`.
  auto baseRowOfNewRow =
      alignRows(baseView, newView, canonicalValue.resultSortedOn_);
  const auto& oldRowOfNewRow = canonicalized.oldRowOfNewRow_;
  std::vector<size_t> baseRowOfOriginalRow(baseRowOfNewRow.size(),
                                           SpatialJoinCachedIndex::NO_ROW);
  for (size_t newRow = 0; newRow < baseRowOfNewRow.size(); ++newRow) {
    baseRowOfOriginalRow[oldRowOfNewRow[newRow]] = baseRowOfNewRow[newRow];
  }
  ColumnIndex column =
      original.varToColMap_.at(newGeoIndex.getGeometryColumn()).columnIndex_;
  auto extended =
      SpatialJoinCachedIndex::extend(
          baseGeoIndex, baseRowOfOriginalRow,
          ExplicitIdTableOperation::viewOf(original.result_), column, index)
          .withPermutedRows(invertPermutation(oldRowOfNewRow));

  // Decide whether to keep the extension or to compact.
  size_t numDeadShapes = extended.numShapes() - extended.numLiveShapes();
  bool tooManySegments = extended.numSegments() > incremental.maxGeoSegments_;
  bool tooManyDeadShapes = extended.numShapes() > 0 &&
                           static_cast<double>(numDeadShapes) >
                               incremental.maxDeadShapeRatio_ *
                                   static_cast<double>(extended.numShapes());
  AD_LOG_INFO << "The geo index of the named result \"" << key << "\" has "
              << extended.numSegments() << " segments and " << numDeadShapes
              << " dead shapes out of " << extended.numShapes()
              << " when extended from the base; "
              << (tooManySegments || tooManyDeadShapes
                      ? "it is compacted (rebuilt from scratch)"
                      : "the extension is kept")
              << std::endl;
  if (tooManySegments || tooManyDeadShapes) {
    return std::nullopt;
  }
  return extended;
}
}  // namespace

// _____________________________________________________________________________
void NamedCachedQueryBlobManager::writeBlobHeader(
    ad_utility::serialization::AlignedByteBufferWriteSerializer& serializer,
    uint16_t formatVersion) {
  AD_CONTRACT_CHECK(formatVersion == formatVersionWithoutSecondaryVocab ||
                    formatVersion == formatVersionWithSecondaryVocab);
  serializer << blobMagicBytes;
  serializer << formatVersion;
}

// _____________________________________________________________________________
NamedCachedQueryBlobManager::FormatVersionOrError
NamedCachedQueryBlobManager::tryToSkipAndVerifyBlobHeader(
    ad_utility::serialization::ByteBufferReadSerializerT<
        true, ql::span<const char>>& serializer) {
  // Explicitly check that the header is complete, so that a truncated blob is
  // reported as `invalidMagicBytes` instead of making the reads below fail.
  if (serializer.data().size() - serializer.getCurrentPosition() <
      blobHeaderSize) {
    return BlobError{BlobErrorType::invalidMagicBytes,
                     std::string{blobNotReadableMessage}};
  }
  // The reads below cannot throw, because the header is known to be complete
  // (see above) and no alignment padding is inserted inside the header (see
  // `blobHeaderSize`) or before the format version (the position of the
  // `serializer` is even, see the precondition in the header file).
  std::decay_t<decltype(blobMagicBytes)> magicBytes{};
  serializer >> magicBytes;
  if (magicBytes != blobMagicBytes) {
    return BlobError{BlobErrorType::invalidMagicBytes,
                     std::string{blobNotReadableMessage}};
  }
  uint16_t version;
  serializer >> version;
  if (version != formatVersionWithoutSecondaryVocab &&
      version != formatVersionWithSecondaryVocab) {
    return BlobError{
        BlobErrorType::invalidVersion,
        absl::StrCat("The given blob was written by an incompatible version of "
                     "QLever (format version ",
                     version, ", expected ", formatVersionWithoutSecondaryVocab,
                     " or ", formatVersionWithSecondaryVocab, ")")};
  }
  return version;
}

// _____________________________________________________________________________
std::vector<char> NamedCachedQueryBlobManager::compressBlob(
    ql::span<const char> uncompressedBlob) {
  return ZstdWrapper::compress(uncompressedBlob.data(),
                               uncompressedBlob.size());
}

// _____________________________________________________________________________
NamedCachedQueryBlobManager::DecompressedBlobOrError
NamedCachedQueryBlobManager::tryToDecompressBlob(
    ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  // Use only the non-throwing functions of `ZstdWrapper`, so that a failure is
  // detected without any exception being thrown.
  auto toBlobError = [](const ZstdWrapper::Error& error) {
    return BlobError{
        BlobErrorType::notDecompressible,
        absl::StrCat(blobNotReadableMessage, ". Details: ", error.message_)};
  };
  // Read the size of the uncompressed data from the ZSTD frame header (which
  // always stores it, because `compressBlob` uses the one-shot
  // `ZSTD_compress`). This also validates that `compressedBlob` starts with a
  // ZSTD frame at all, so that arbitrary garbage is rejected right here,
  // instead of being misinterpreted as an (arbitrarily large) size for the
  // allocation below.
  auto uncompressedSize = ZstdWrapper::tryToGetUncompressedSize(
      compressedBlob.data(), compressedBlob.size());
  if (const auto* error = std::get_if<ZstdWrapper::Error>(&uncompressedSize)) {
    return toBlobError(*error);
  }

  // Decompress into a buffer that is 1. allocated via the caller-provided
  // `allocator`, 2. aligned to the maximal possible alignment (required for the
  // zero-copy deserialization), and 3. not needlessly zero-initialized before
  // the decompression overwrites it (see `BlobAllocator`).
  std::vector<char, BlobAllocator> uncompressed(
      std::get<size_t>(uncompressedSize),
      BlobAllocator{ad_utility::AlignedAllocator<
          char, ql::pmr::polymorphic_allocator<char>>{allocator}});
  auto actualUncompressedSize = ZstdWrapper::tryToDecompressToBuffer(
      compressedBlob.data(), compressedBlob.size(), uncompressed.data(),
      uncompressed.size());
  if (const auto* error =
          std::get_if<ZstdWrapper::Error>(&actualUncompressedSize)) {
    return toBlobError(*error);
  }
  AD_CORRECTNESS_CHECK(std::get<size_t>(actualUncompressedSize) ==
                       uncompressed.size());
  return uncompressed;
}

// _____________________________________________________________________________
std::vector<char> NamedCachedQueryBlobManager::serialize(
    const Qlever& qlever, const BlobSerializationConfig& config) const {
  // First serialize everything into an uncompressed, suitably aligned buffer.
  // The alignment (guaranteed by the `AlignedByteBufferWriteSerializer`) is
  // required so that the buffer can later be deserialized zero-copy (see
  // `deserialize`).
  ad_utility::serialization::AlignedByteBufferWriteSerializer serializer;

  auto indexAndViews = qlever.indexAndViewsSnapshot();
  const auto& indexImpl = indexAndViews->index_.getImpl();
  const auto& incremental = config.incremental_;
  std::shared_ptr<Qlever::IndexAndViews> baseIndexAndViews;
  NamedResultCache::Entries baseEntries;
  if (incremental.has_value()) {
    AD_CONTRACT_CHECK(incremental->base_ != nullptr,
                      "The base of an incremental blob must not be null");
    baseIndexAndViews = incremental->base_->indexAndViewsSnapshot();
    baseEntries =
        incremental->base_->namedResultCache_.getAllEntriesSortedByKey();
    AD_CONTRACT_CHECK(
        indexImpl.secondaryVocab() == nullptr,
        "An incremental blob can only be written from an instance without a "
        "secondary vocabulary, because the secondary vocabulary of the blob "
        "starts from the one of its base");
  }
  // The index whose secondary vocabulary is the starting point of the one of
  // the blob.
  const auto& vocabSourceIndexImpl =
      incremental.has_value() ? baseIndexAndViews->index_.getImpl() : indexImpl;

  // The secondary vocabulary of the blob consists of the one of the index (or
  // of the base, see above; if any, for example because the index was itself
  // loaded from a blob, so that the `Id`s of its words stay valid), extended by
  // the new words of the named cache entries (see
  // `NamedCacheSecondaryVocabRewriter.h`). Only if it is empty and no entry has
  // a geo index, the blob is written in the old format (blob version 1 and
  // entries version 1) without a secondary vocabulary, which can also be read
  // by older versions of QLever. Otherwise the new format is used (blob version
  // 2, which then contains a possibly empty secondary vocabulary, and entries
  // version 2, which is required for the segmented geo index).
  auto entries = qlever.namedResultCache_.getAllEntriesSortedByKey();
  SecondaryVocabulary secondaryVocab =
      vocabSourceIndexImpl.secondaryVocab() != nullptr
          ? vocabSourceIndexImpl.secondaryVocab()->clone()
          : SecondaryVocabulary{};
  namedCacheSecondaryVocab::addNewWordsToSecondaryVocab(entries,
                                                        secondaryVocab);
  bool hasGeoIndex = ql::ranges::any_of(entries, [](const auto& entry) {
    return entry.second->cachedGeoIndex_.has_value();
  });
  bool useNewFormat = secondaryVocab.numWords() > 0 || hasGeoIndex;
  writeBlobHeader(serializer, useNewFormat
                                  ? formatVersionWithSecondaryVocab
                                  : formatVersionWithoutSecondaryVocab);
  // Serialize the index metadata JSON together with the vocabulary, so that the
  // blob is self-contained and the loading side can set up the vocabulary
  // configuration without access to the on-disk index. Without excluded
  // entries, the metadata JSON and the vocabulary are written as they are;
  // with excluded entries, the vocabulary is filtered and the
  // `"vocabulary-type"` entry of the metadata JSON is rewritten to the type of
  // the filtered vocabulary (see `writeMetadataAndFilteredVocabulary`).
  if (config.excludedEntryRegexes_.empty()) {
    serializer << indexImpl.configurationJson().dump();
    indexImpl.writeVocabularyToZeroCopyBlob(serializer);
  } else {
    writeMetadataAndFilteredVocabulary(
        serializer, indexImpl, indexImpl.getVocab().getUnderlyingVocabulary(),
        config.excludedEntryRegexes_);
  }
  if (useNewFormat) {
    serializer << secondaryVocab;
  }
  const uint16_t entriesVersion =
      useNewFormat ? namedResultCacheSerializer::detail::formatVersion
                   : namedResultCacheSerializer::detail::legacyFormatVersion;

  // Write the named cache entries. Each entry is written as a canonicalized
  // copy (the entry in the named cache itself stays unchanged): its `Id`s of
  // type `LocalVocabIndex` are rewritten and its rows are in canonical order
  // (see `canonicalizeWithPermutation`). The words of the local vocab of such a
  // copy are not written, because they are no longer referenced. The entries
  // are written in the order of `entries`.
  size_t entryIndex = 0;
  namedResultCacheSerializer::writeEntries(
      serializer, entries,
      [&](auto& entrySerializer, const NamedResultCache::Value& value) {
        const auto& key = entries.at(entryIndex++).first;
        auto canonicalized =
            namedCacheSecondaryVocab::canonicalizeWithPermutation(
                value, secondaryVocab, qlever.allocator_,
                config.sortOnAllColumns_);
        if (incremental.has_value() && value.cachedGeoIndex_.has_value()) {
          auto extended =
              tryToExtendGeoIndex(key, value, canonicalized, baseEntries,
                                  *incremental, indexAndViews->index_);
          if (extended.has_value()) {
            canonicalized.value_.cachedGeoIndex_ = std::move(extended);
          }
        }
        const auto& canonicalValue = canonicalized.value_;
        namedResultCacheSerializer::writeValue(
            entrySerializer, canonicalValue,
            ExplicitIdTableOperation::viewOf(canonicalValue.result_)
                .getColumns(),
            canonicalValue.resultSortedOn_, /*writeLocalVocabWords=*/false,
            entriesVersion);
      },
      entriesVersion);
  auto uncompressed = std::move(serializer).data();

  return compressBlob(uncompressed);
}

// _____________________________________________________________________________
std::optional<NamedCachedQueryBlobManager::BlobError>
NamedCachedQueryBlobManager::tryToDeserialize(
    Qlever& qlever, ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  AD_CONTRACT_CHECK(
      !deserializedBlobLifetimeExtender_.has_value(),
      "`deserializeVocabAndNamedCacheFromCompressedBlob` must not be called "
      "more than once on the same `Qlever` instance");

  // Write the message of the `error` to the error log (so that it is not lost
  // if the caller only inspects the type of the error), and return the `error`.
  auto logAndReturn = [](BlobError error) -> std::optional<BlobError> {
    AD_LOG_ERROR << error.message_ << std::endl;
    return error;
  };

  // Decompress into `deserializedBlobLifetimeExtender_`, which is kept alive
  // for the lifetime of this manager because the vocabulary, the secondary
  // vocabulary, and the named result cache entries loaded below are zero-copy
  // views directly into it. Note that
  // moving the buffer into the member does not change the location of its
  // storage, so the views taken below stay valid.
  auto uncompressed = tryToDecompressBlob(compressedBlob, allocator);
  if (auto* error = std::get_if<BlobError>(&uncompressed)) {
    // Nothing of `qlever` has been touched yet, so it is left exactly as it
    // was.
    return logAndReturn(std::move(*error));
  }
  deserializedBlobLifetimeExtender_.emplace(
      std::get<std::vector<char, BlobAllocator>>(std::move(uncompressed)));
  // As long as nothing of `qlever` has been modified, release the buffer again
  // if the blob is rejected or cannot be read (with or without an exception).
  // That way such a blob leaves this manager (and hence `qlever`) exactly as it
  // was, and another blob can be loaded afterwards.
  absl::Cleanup releaseBuffer = [this] {
    deserializedBlobLifetimeExtender_.reset();
  };

  // Use a serializer that only borrows a view of
  // `deserializedBlobLifetimeExtender_`, rather than one that owns/moves it, so
  // that the buffer stays owned by `deserializedBlobLifetimeExtender_` for the
  // rest of this manager's lifetime.
  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      reader{ql::span<const char>{deserializedBlobLifetimeExtender_.value()}};

  auto formatVersionOrError = tryToSkipAndVerifyBlobHeader(reader);
  if (auto* error = std::get_if<BlobError>(&formatVersionOrError)) {
    return logAndReturn(std::move(*error));
  }
  const uint16_t formatVersion = std::get<uint16_t>(formatVersionOrError);

  auto indexAndViews = qlever.indexAndViewsSnapshot();
  auto& indexImpl = indexAndViews->index_.getImpl();
  // The header is valid, but the contents may still be corrupted. Wrap the
  // reading of the contents, so that the user gets a message that names the
  // expected input, instead of a low-level error from deep inside the
  // deserialization.
  auto metadata =
      rethrowWithContext(blobContentsNotReadableMessage, [&reader]() {
        std::string metadataJson;
        reader >> metadataJson;
        return nlohmann::json::parse(metadataJson);
      });

  // Check the index format version of the metadata JSON (without throwing),
  // before anything of `qlever` is modified, so that a blob written by a
  // version of QLever with an incompatible index format is rejected like an
  // incompatible header above.
  if (auto error = indexImpl.checkIndexFormatVersion(metadata);
      error.has_value()) {
    return logAndReturn(BlobError{
        BlobErrorType::incompatibleIndexFormat,
        absl::StrCat("The given blob was written by a version of QLever with "
                     "an incompatible index format. Details: ",
                     error.value())});
  }

  // A blob with a secondary vocabulary cannot be loaded into an index that
  // already has one, because the words of that one might be referenced by
  // `Id`s that are already in use.
  //
  // NOTE: This cannot happen via the public interface: the secondary
  // vocabulary of an index is only set below, and this function may be called
  // at most once per `Qlever` instance (see the check at its beginning).
  // Therefore, this is not reported as a `BlobError`.
  AD_CORRECTNESS_CHECK(
      formatVersion != formatVersionWithSecondaryVocab ||
          indexImpl.secondaryVocab() == nullptr,
      "A blob with a secondary vocabulary cannot be loaded into "
      "an index that already has one");

  // From here on, `qlever` is modified, and its vocabulary and named result
  // cache may hold views into the buffer, so the buffer must be kept alive even
  // if the reading below fails.
  std::move(releaseBuffer).Cancel();
  rethrowWithContext(blobContentsNotReadableMessage,
                     [&indexImpl, &reader, &qlever, &indexAndViews, &metadata,
                      formatVersion]() {
                       // Apply the index metadata JSON before loading the
                       // vocabulary, so that the vocabulary is set up with the
                       // correct configuration (locale, comparator, etc.).
                       indexImpl.applyConfiguration(metadata);
                       indexImpl.loadVocabularyFromZeroCopyBlob(reader);
                       // The named cache entries may contain `Id`s of the
                       // secondary vocabulary, which therefore has to be set up
                       // before they are used. The words of the secondary
                       // vocabulary are zero-copy views into the blob, just
                       // like the ones of the vocabulary.
                       if (formatVersion == formatVersionWithSecondaryVocab) {
                         auto secondaryVocab =
                             std::make_shared<SecondaryVocabulary>();
                         reader >> *secondaryVocab;
                         indexImpl.setSecondaryVocab(std::move(secondaryVocab));
                       }
                       qlever.namedResultCache_.readFromSerializer(
                           reader, qlever.allocator_,
                           indexAndViews->index_.getLocalVocabContext());
                     });
  return std::nullopt;
}

// _____________________________________________________________________________
void NamedCachedQueryBlobManager::deserialize(
    Qlever& qlever, ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  if (auto error = tryToDeserialize(qlever, compressedBlob, allocator);
      error.has_value()) {
    AD_THROW(std::move(error.value().message_));
  }
}

}  // namespace qlever

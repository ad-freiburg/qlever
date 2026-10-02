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
#include <cstring>
#include <string_view>
#include <type_traits>
#include <variant>

#include "index/IndexImpl.h"
#include "index/vocabulary/BuildFilteredVocabulary.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "libqlever/Qlever.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Log.h"
#include "util/Random.h"
#include "util/json.h"

namespace qlever {

namespace {
// The header that is written at the beginning of every blob (see
// `NamedCachedQueryBlobManager::writeBlobHeader` /
// `NamedCachedQueryBlobManager::skipAndVerifyBlobHeader`), to guard against
// loading a blob written by an incompatible version of QLever.
constexpr std::array<char, 8> blobMagicBytes{'Q', 'L', 'V', 'R',
                                             'B', 'L', 'O', 'B'};
constexpr uint16_t blobFormatVersion = 1;

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

// Read the format version from the blob header that starts at `headerStart` in
// `data`. Only used to include the version in the error messages (see
// `blobHeaderErrorMessage` below), after the header has already been checked
// for completeness.
uint16_t readFormatVersionFromHeader(ql::span<const char> data,
                                     size_t headerStart) {
  AD_CORRECTNESS_CHECK(data.size() >= headerStart + blobHeaderSize);
  uint16_t version;
  std::memcpy(&version, data.data() + headerStart + sizeof(blobMagicBytes),
              sizeof(version));
  return version;
}

// Return the message that describes a non-`ok` blob status. If
// `foundFormatVersion` is set, it is named in the message for `invalidVersion`;
// it is not always available, because the buffer that holds the header may
// already have been released when the message is built.
std::string blobErrorMessage(NamedCachedQueryBlobManager::BlobStatus status,
                             std::optional<uint16_t> foundFormatVersion) {
  using enum NamedCachedQueryBlobManager::BlobStatus;
  switch (status) {
    case notDecompressible:
    case invalidMagicBytes:
      return std::string{blobNotReadableMessage};
    case invalidVersion:
      return absl::StrCat(
          "The given blob was written by an incompatible version of QLever (",
          foundFormatVersion.has_value()
              ? absl::StrCat("format version ", foundFormatVersion.value())
              : std::string{"incompatible blob format version"},
          ", expected ", blobFormatVersion, ")");
    case incompatibleIndexFormat:
      return std::string{
          "The given blob was written by a version of QLever with an "
          "incompatible index format"};
    case ok:
      break;
  }
  AD_FAIL();
}

// Return the message for the non-`ok` `status` that
// `NamedCachedQueryBlobManager::tryToSkipAndVerifyBlobHeader` reported for the
// blob header that starts at `headerStart` in `data`. For an incompatible
// format version, the message names the version that was found. It can still be
// read from `data`, because in that case the header is complete.
std::string blobHeaderErrorMessage(
    NamedCachedQueryBlobManager::BlobStatus status, ql::span<const char> data,
    size_t headerStart) {
  std::optional<uint16_t> foundFormatVersion;
  if (status == NamedCachedQueryBlobManager::BlobStatus::invalidVersion) {
    foundFormatVersion = readFormatVersionFromHeader(data, headerStart);
  }
  return blobErrorMessage(status, foundFormatVersion);
}

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

// The implementation of `NamedCachedQueryBlobManager::tryToDecompressBlob`
// (see there). Use only the non-throwing functions of `ZstdWrapper`, so that a
// failure is detected without any exception being thrown.
NamedCachedQueryBlobManager::DecompressedBlobOrError decompressBlobImpl(
    ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  using BlobAllocator = NamedCachedQueryBlobManager::BlobAllocator;
  // Read the size of the uncompressed data from the ZSTD frame header (which
  // always stores it, because `compressBlob` uses the one-shot
  // `ZSTD_compress`). This also validates that `compressedBlob` starts with a
  // ZSTD frame at all, so that arbitrary garbage is rejected right here,
  // instead of being misinterpreted as an (arbitrarily large) size for the
  // allocation below.
  auto uncompressedSize = ZstdWrapper::tryToGetUncompressedSize(
      compressedBlob.data(), compressedBlob.size());
  if (const auto* error = std::get_if<ZstdWrapper::Error>(&uncompressedSize)) {
    return *error;
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
    return *error;
  }
  AD_CORRECTNESS_CHECK(std::get<size_t>(actualUncompressedSize) ==
                       uncompressed.size());
  return uncompressed;
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
}  // namespace

// _____________________________________________________________________________
void NamedCachedQueryBlobManager::writeBlobHeader(
    ad_utility::serialization::AlignedByteBufferWriteSerializer& serializer) {
  serializer << blobMagicBytes;
  serializer << blobFormatVersion;
}

// _____________________________________________________________________________
NamedCachedQueryBlobManager::BlobStatus
NamedCachedQueryBlobManager::tryToSkipAndVerifyBlobHeader(
    ad_utility::serialization::ByteBufferReadSerializerT<
        true, ql::span<const char>>& serializer) noexcept {
  using enum BlobStatus;
  // Explicitly check that the header is complete, so that a truncated blob is
  // reported as `invalidMagicBytes` instead of making the reads below fail.
  if (serializer.data().size() - serializer.getCurrentPosition() <
      blobHeaderSize) {
    return invalidMagicBytes;
  }
  // The reads below cannot throw, because the header is known to be complete
  // (see above) and no alignment padding is inserted inside the header (see
  // `blobHeaderSize`).
  std::decay_t<decltype(blobMagicBytes)> magicBytes{};
  serializer >> magicBytes;
  if (magicBytes != blobMagicBytes) {
    return invalidMagicBytes;
  }
  uint16_t version;
  serializer >> version;
  if (version != blobFormatVersion) {
    return invalidVersion;
  }
  return ok;
}

// _____________________________________________________________________________
void NamedCachedQueryBlobManager::skipAndVerifyBlobHeader(
    ad_utility::serialization::ByteBufferReadSerializerT<
        true, ql::span<const char>>& serializer) {
  // Remember where the header starts, so that the incompatible format version
  // can be read again for the error message below.
  size_t headerStart = serializer.getCurrentPosition();
  auto status = tryToSkipAndVerifyBlobHeader(serializer);
  if (status == BlobStatus::ok) {
    return;
  }
  AD_THROW(blobHeaderErrorMessage(status, serializer.data(), headerStart));
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
  return decompressBlobImpl(compressedBlob, allocator);
}

// _____________________________________________________________________________
std::vector<char, NamedCachedQueryBlobManager::BlobAllocator>
NamedCachedQueryBlobManager::decompressBlob(
    ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  auto uncompressed = tryToDecompressBlob(compressedBlob, allocator);
  if (const auto* error = std::get_if<ZstdWrapper::Error>(&uncompressed)) {
    AD_THROW(
        absl::StrCat(blobNotReadableMessage, ". Details: ", error->message_));
  }
  return std::get<std::vector<char, BlobAllocator>>(std::move(uncompressed));
}

// _____________________________________________________________________________
std::vector<char> NamedCachedQueryBlobManager::serialize(
    const Qlever& qlever, const BlobSerializationConfig& config) const {
  // First serialize everything into an uncompressed, suitably aligned buffer.
  // The alignment (guaranteed by the `AlignedByteBufferWriteSerializer`) is
  // required so that the buffer can later be deserialized zero-copy (see
  // `deserialize`).
  ad_utility::serialization::AlignedByteBufferWriteSerializer serializer;
  writeBlobHeader(serializer);

  auto indexAndViews = qlever.indexAndViewsSnapshot();
  const auto& indexImpl = indexAndViews->index_.getImpl();
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
  qlever.namedResultCache_.writeToSerializer(serializer);
  auto uncompressed = std::move(serializer).data();

  return compressBlob(uncompressed);
}

// _____________________________________________________________________________
NamedCachedQueryBlobManager::BlobStatus
NamedCachedQueryBlobManager::tryToDeserialize(
    Qlever& qlever, ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  AD_CONTRACT_CHECK(
      !deserializedBlobLifetimeExtender_.has_value(),
      "`deserializeVocabAndNamedCacheFromCompressedBlob` must not be called "
      "more than once on the same `Qlever` instance");

  // Decompress into `deserializedBlobLifetimeExtender_`, which is kept alive
  // for the lifetime of this manager because the vocabulary and named result
  // cache entries loaded below are zero-copy views directly into it. Note that
  // moving the buffer into the member does not change the location of its
  // storage, so the views taken below stay valid.
  //
  // For each failure, the details (which are not part of the returned status)
  // are written to the error log, so that they are not lost.
  auto uncompressed = tryToDecompressBlob(compressedBlob, allocator);
  if (const auto* error = std::get_if<ZstdWrapper::Error>(&uncompressed)) {
    // Nothing of `qlever` has been touched yet, so it is left exactly as it
    // was.
    AD_LOG_ERROR << blobNotReadableMessage << ". Details: " << error->message_
                 << std::endl;
    return BlobStatus::notDecompressible;
  }
  deserializedBlobLifetimeExtender_.emplace(
      std::get<std::vector<char, BlobAllocator>>(std::move(uncompressed)));

  // Use a serializer that only borrows a view of
  // `deserializedBlobLifetimeExtender_`, rather than one that owns/moves it, so
  // that the buffer stays owned by `deserializedBlobLifetimeExtender_` for the
  // rest of this manager's lifetime.
  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      reader{ql::span<const char>{deserializedBlobLifetimeExtender_.value()}};

  auto headerStatus = tryToSkipAndVerifyBlobHeader(reader);
  if (headerStatus != BlobStatus::ok) {
    AD_LOG_ERROR << blobHeaderErrorMessage(
                        headerStatus, deserializedBlobLifetimeExtender_.value(),
                        0)
                 << std::endl;
    // Nothing of `qlever` has been touched yet either, so release the buffer
    // again. That way a rejected blob leaves this manager (and hence `qlever`)
    // exactly as it was, and another blob can be loaded afterwards.
    deserializedBlobLifetimeExtender_.reset();
    return headerStatus;
  }

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
    AD_LOG_ERROR << blobErrorMessage(BlobStatus::incompatibleIndexFormat,
                                     std::nullopt)
                 << ". Details: " << error.value() << std::endl;
    deserializedBlobLifetimeExtender_.reset();
    return BlobStatus::incompatibleIndexFormat;
  }

  rethrowWithContext(
      blobContentsNotReadableMessage,
      [&indexImpl, &reader, &qlever, &indexAndViews, &metadata]() {
        // Apply the index metadata JSON before loading the vocabulary, so that
        // the vocabulary is set up with the correct configuration (locale,
        // comparator, etc.).
        indexImpl.applyConfiguration(metadata);
        indexImpl.loadVocabularyFromZeroCopyBlob(reader);
        qlever.namedResultCache_.readFromSerializer(
            reader, qlever.allocator_,
            indexAndViews->index_.getLocalVocabContext());
      });
  return BlobStatus::ok;
}

// _____________________________________________________________________________
void NamedCachedQueryBlobManager::deserialize(
    Qlever& qlever, ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  auto status = tryToDeserialize(qlever, compressedBlob, allocator);
  if (status != BlobStatus::ok) {
    // NOTE: The message can only name the category of the failure, because
    // `tryToDeserialize` reports only a status. The details (the underlying
    // ZSTD error, the format version that was found, or the incompatibility of
    // the index format) have already been written to the error log by
    // `tryToDeserialize`.
    AD_THROW(blobErrorMessage(status, std::nullopt));
  }
}

}  // namespace qlever

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/NamedCachedQueryBlobManager.h"

#include <absl/strings/str_cat.h>

#include <string_view>

#include "index/IndexImpl.h"
#include "libqlever/Qlever.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"

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
std::vector<char> NamedCachedQueryBlobManager::serialize(
    const Qlever& qlever) const {
  // First serialize everything into an uncompressed, suitably aligned buffer.
  // The alignment (guaranteed by the `AlignedByteBufferWriteSerializer`) is
  // required so that the buffer can later be deserialized zero-copy (see
  // `deserialize`).
  ad_utility::serialization::AlignedByteBufferWriteSerializer serializer;
  writeBlobHeader(serializer);

  auto indexAndViews = qlever.indexAndViewsSnapshot();
  const auto& indexImpl = indexAndViews->index_.getImpl();
  // Serialize the index metadata JSON, so that the blob is self-contained and
  // the loading side can set up the vocabulary configuration without access to
  // the on-disk index.
  serializer << indexImpl.configurationJson().dump();
  indexImpl.writeVocabularyToZeroCopyBlob(serializer);
  qlever.namedResultCache_.writeToSerializer(serializer);
  auto uncompressed = std::move(serializer).data();

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
  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      reader{ql::span<const char>{deserializedBlobLifetimeExtender_.value()}};

  skipAndVerifyBlobHeader(reader);

  auto indexAndViews = qlever.indexAndViewsSnapshot();
  auto& indexImpl = indexAndViews->index_.getImpl();
  // The header is valid, but the contents may still be corrupted. Wrap the
  // reading of the contents, so that the user gets a message that names the
  // expected input, instead of a low-level error from deep inside the
  // deserialization.
  rethrowWithContext(
      blobContentsNotReadableMessage,
      [&indexImpl, &reader, &qlever, &indexAndViews]() {
        // Read and apply the index metadata JSON before loading the vocabulary,
        // so that the vocabulary is set up with the correct configuration
        // (locale, comparator, etc.).
        std::string metadataJson;
        reader >> metadataJson;
        indexImpl.applyConfiguration(nlohmann::json::parse(metadataJson));
        indexImpl.loadVocabularyFromZeroCopyBlob(reader);
        qlever.namedResultCache_.readFromSerializer(
            reader, qlever.allocator_,
            indexAndViews->index_.getLocalVocabContext());
      });
}

}  // namespace qlever

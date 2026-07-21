// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/NamedCachedQueryBlobManager.h"

#include <cstring>

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
  std::decay_t<decltype(blobMagicBytes)> magicBytes{};
  serializer >> magicBytes;
  AD_CONTRACT_CHECK(
      magicBytes == blobMagicBytes,
      "The given blob was not written by "
      "`Qlever::serializeVocabAndNamedCacheToCompressedBlob`, or is corrupted");
  uint16_t version;
  serializer >> version;
  AD_CONTRACT_CHECK(
      version == blobFormatVersion,
      "The given blob was written by an incompatible version of QLever "
      "(format version ",
      version, ", expected ", blobFormatVersion, ")");
}

// _____________________________________________________________________________
std::vector<char>
NamedCachedQueryBlobManager::compressBlobAndAddTrailingSizeInfo(
    ql::span<const char> uncompressedBlob) {
  std::vector<char> compressed =
      ZstdWrapper::compress(uncompressedBlob.data(), uncompressedBlob.size());
  uint64_t uncompressedSize = uncompressedBlob.size();
  const char* sizeBytes = reinterpret_cast<const char*>(&uncompressedSize);
  compressed.insert(compressed.end(), sizeBytes,
                    sizeBytes + sizeof(uncompressedSize));
  return compressed;
}

// _____________________________________________________________________________
std::vector<char, NamedCachedQueryBlobManager::BlobAllocator>
NamedCachedQueryBlobManager::decompressBlobWithTrailingSizeInfo(
    ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  // The size of the uncompressed buffer is stored as a trailing `uint64_t` at
  // the very end of the whole compressed block (see
  // `compressBlobAndAddTrailingSizeInfo`). Read it off first, then strip it
  // from the span that is passed to the decompression.
  uint64_t originalUncompressedSize;
  AD_CONTRACT_CHECK(compressedBlob.size() >= sizeof(originalUncompressedSize));
  std::memcpy(&originalUncompressedSize,
              compressedBlob.data() + compressedBlob.size() -
                  sizeof(originalUncompressedSize),
              sizeof(originalUncompressedSize));
  compressedBlob = compressedBlob.subspan(
      0, compressedBlob.size() - sizeof(originalUncompressedSize));

  // Decompress into a buffer that is 1. allocated via the caller-provided
  // `allocator`, 2. aligned to the maximal possible alignment (required for the
  // zero-copy deserialization), and 3. not needlessly zero-initialized before
  // the decompression overwrites it (see `BlobAllocator`).
  std::vector<char, BlobAllocator> uncompressed(
      originalUncompressedSize,
      BlobAllocator{ad_utility::AlignedAllocator<
          char, ql::pmr::polymorphic_allocator<char>>{allocator}});
  auto actualUncompressedSize = ZstdWrapper::decompressToBuffer(
      compressedBlob.data(), compressedBlob.size(), uncompressed.data(),
      uncompressed.size());
  AD_CORRECTNESS_CHECK(actualUncompressedSize == originalUncompressedSize);
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

  return compressBlobAndAddTrailingSizeInfo(uncompressed);
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
      decompressBlobWithTrailingSizeInfo(compressedBlob, allocator));

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
  // Read and apply the index metadata JSON before loading the vocabulary, so
  // that the vocabulary is set up with the correct configuration (locale,
  // comparator, etc.).
  std::string metadataJson;
  reader >> metadataJson;
  indexImpl.applyConfiguration(nlohmann::json::parse(metadataJson));
  indexImpl.loadVocabularyFromZeroCopyBlob(reader);
  qlever.namedResultCache_.readFromSerializer(reader, qlever.allocator_,
                                              indexAndViews->index_);
}

}  // namespace qlever

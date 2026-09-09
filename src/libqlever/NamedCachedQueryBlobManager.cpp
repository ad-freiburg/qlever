// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/NamedCachedQueryBlobManager.h"

#include <absl/strings/str_cat.h>

#include <cstring>
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

// Read the format version from the blob header that starts at `headerStart` in
// `data`. Only used to include the version in the error message of
// `NamedCachedQueryBlobManager::skipAndVerifyBlobHeader`, after the header has
// already been checked for completeness.
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
  using Status = NamedCachedQueryBlobManager::BlobStatus;
  switch (status) {
    case Status::notDecompressible:
    case Status::invalidMagicBytes:
      return std::string{blobNotReadableMessage};
    case Status::invalidVersion:
      return absl::StrCat(
          "The given blob was written by an incompatible version of QLever (",
          foundFormatVersion.has_value()
              ? absl::StrCat("format version ", foundFormatVersion.value())
              : std::string{"incompatible blob format version"},
          ", expected ", blobFormatVersion, ")");
    case Status::ok:
      break;
  }
  AD_FAIL();
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

// Run `function`, which must be one of the ZSTD calls below, and return
// `nullopt` if it reports an error. Only the `std::runtime_error`s that
// `ZstdWrapper` throws are caught; all other exceptions (in particular
// `std::bad_alloc`, and the `ad_utility::Exception`s of the `AD_..._CHECK`
// macros) are propagated. `errorDetails` is set to the message of the ZSTD
// error and is left untouched on success.
template <typename Function>
std::optional<std::invoke_result_t<const Function&>> runZstdCall(
    const Function& function, std::string& errorDetails) {
  try {
    return function();
  } catch (const std::runtime_error& e) {
    errorDetails = e.what();
    return std::nullopt;
  }
}

// The common implementation of `NamedCachedQueryBlobManager::decompressBlob`
// and `NamedCachedQueryBlobManager::tryToDecompressBlob`: decompress
// `compressedBlob`, and return `nullopt` (with `errorDetails` set to the
// message of the underlying ZSTD error) if that fails.
std::optional<std::vector<char, NamedCachedQueryBlobManager::BlobAllocator>>
decompressBlobOrErrorDetails(ql::span<const char> compressedBlob,
                             ql::pmr::polymorphic_allocator<char> allocator,
                             std::string& errorDetails) {
  using BlobAllocator = NamedCachedQueryBlobManager::BlobAllocator;
  // Read the size of the uncompressed data from the ZSTD frame header (which
  // always stores it, because `compressBlob` uses the one-shot
  // `ZSTD_compress`). This also validates that `compressedBlob` starts with a
  // ZSTD frame at all, so that arbitrary garbage is rejected right here,
  // instead of being misinterpreted as an (arbitrarily large) size for the
  // allocation below.
  auto uncompressedSize = runZstdCall(
      [&compressedBlob]() {
        return ZstdWrapper::getUncompressedSize(compressedBlob.data(),
                                                compressedBlob.size());
      },
      errorDetails);
  if (!uncompressedSize.has_value()) {
    return std::nullopt;
  }

  // Decompress into a buffer that is 1. allocated via the caller-provided
  // `allocator`, 2. aligned to the maximal possible alignment (required for the
  // zero-copy deserialization), and 3. not needlessly zero-initialized before
  // the decompression overwrites it (see `BlobAllocator`).
  std::vector<char, BlobAllocator> uncompressed(
      uncompressedSize.value(),
      BlobAllocator{ad_utility::AlignedAllocator<
          char, ql::pmr::polymorphic_allocator<char>>{allocator}});
  auto actualUncompressedSize = runZstdCall(
      [&compressedBlob, &uncompressed]() {
        return ZstdWrapper::decompressToBuffer(
            compressedBlob.data(), compressedBlob.size(), uncompressed.data(),
            uncompressed.size());
      },
      errorDetails);
  if (!actualUncompressedSize.has_value()) {
    return std::nullopt;
  }
  AD_CORRECTNESS_CHECK(actualUncompressedSize.value() ==
                       uncompressedSize.value());
  return uncompressed;
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
  using Status = BlobStatus;
  // Explicitly check that the header is complete, so that a truncated blob is
  // reported as `invalidMagicBytes` instead of making the reads below fail.
  if (serializer.data().size() - serializer.getCurrentPosition() <
      blobHeaderSize) {
    return Status::invalidMagicBytes;
  }
  // The reads below cannot throw, because the header is known to be complete
  // (see above) and no alignment padding is inserted inside the header (see
  // `blobHeaderSize`). The `catch` is only there to make the guarantee that
  // this function never throws independent of the implementation details of
  // the serializer.
  try {
    std::decay_t<decltype(blobMagicBytes)> magicBytes{};
    serializer >> magicBytes;
    if (magicBytes != blobMagicBytes) {
      return Status::invalidMagicBytes;
    }
    uint16_t version;
    serializer >> version;
    if (version != blobFormatVersion) {
      return Status::invalidVersion;
    }
    return Status::ok;
  } catch (...) {
    return Status::invalidMagicBytes;
  }
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
  // For an incompatible format version, name the version that was found in the
  // message. It can still be read from the buffer, because in that case the
  // header is complete.
  std::optional<uint16_t> foundFormatVersion;
  if (status == BlobStatus::invalidVersion) {
    foundFormatVersion =
        readFormatVersionFromHeader(serializer.data(), headerStart);
  }
  AD_THROW(blobErrorMessage(status, foundFormatVersion));
}

// _____________________________________________________________________________
std::vector<char> NamedCachedQueryBlobManager::compressBlob(
    ql::span<const char> uncompressedBlob) {
  return ZstdWrapper::compress(uncompressedBlob.data(),
                               uncompressedBlob.size());
}

// _____________________________________________________________________________
std::optional<std::vector<char, NamedCachedQueryBlobManager::BlobAllocator>>
NamedCachedQueryBlobManager::tryToDecompressBlob(
    ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  std::string ignoredErrorDetails;
  return decompressBlobOrErrorDetails(compressedBlob, allocator,
                                      ignoredErrorDetails);
}

// _____________________________________________________________________________
std::vector<char, NamedCachedQueryBlobManager::BlobAllocator>
NamedCachedQueryBlobManager::decompressBlob(
    ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  std::string errorDetails;
  auto uncompressed =
      decompressBlobOrErrorDetails(compressedBlob, allocator, errorDetails);
  if (!uncompressed.has_value()) {
    AD_THROW(absl::StrCat(blobNotReadableMessage, ". Details: ", errorDetails));
  }
  return std::move(uncompressed).value();
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
  auto uncompressed = tryToDecompressBlob(compressedBlob, allocator);
  if (!uncompressed.has_value()) {
    // Nothing of `qlever` has been touched yet, so it is left exactly as it
    // was.
    return BlobStatus::notDecompressible;
  }
  deserializedBlobLifetimeExtender_.emplace(std::move(uncompressed).value());

  // Use a serializer that only borrows a view of
  // `deserializedBlobLifetimeExtender_`, rather than one that owns/moves it, so
  // that the buffer stays owned by `deserializedBlobLifetimeExtender_` for the
  // rest of this manager's lifetime.
  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      reader{ql::span<const char>{deserializedBlobLifetimeExtender_.value()}};

  auto headerStatus = tryToSkipAndVerifyBlobHeader(reader);
  if (headerStatus != BlobStatus::ok) {
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
  return BlobStatus::ok;
}

// _____________________________________________________________________________
void NamedCachedQueryBlobManager::deserialize(
    Qlever& qlever, ql::span<const char> compressedBlob,
    ql::pmr::polymorphic_allocator<char> allocator) {
  auto status = tryToDeserialize(qlever, compressedBlob, allocator);
  if (status != BlobStatus::ok) {
    // NOTE: The message can only name the category of the failure, not its
    // details (the underlying ZSTD error, or the format version that was
    // found), because `tryToDeserialize` reports only a status and has already
    // released the buffer that held the header. Use `decompressBlob` and
    // `skipAndVerifyBlobHeader` directly to get those details.
    AD_THROW(blobErrorMessage(status, std::nullopt));
  }
}

}  // namespace qlever

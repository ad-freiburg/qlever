// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/blobDiff/BlobDiff.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include "backports/algorithm.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/CryptographicHashUtils.h"
#include "util/Exception.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/TransparentFunctors.h"

namespace qlever::blobDiff {

namespace {
using Manager = NamedCachedQueryBlobManager;
// The decompressed base as returned by `Manager::tryToDecompressBlob`.
using DecompressedBase = std::vector<char, Manager::BlobAllocator>;
using ad_utility::serialization::ByteBufferReadSerializerT;
using ad_utility::serialization::ByteBufferWriteSerializer;
using ErrorType = BlobDiffErrorType;

constexpr std::array<char, 8> diffFileMagic{'Q', 'L', 'V', 'R',
                                            'B', 'D', 'I', 'F'};
constexpr uint16_t diffFileVersion = 2;

// The size of the header of a diff file in bytes (see the file format in
// `BlobDiff.h`).
constexpr size_t headerSize = diffFileMagic.size() + sizeof(diffFileVersion) +
                              3 * sizeof(uint64_t) +
                              2 * sizeof(BlobDiffHeader::Checksum);

// Create a `BlobDiffError` of the given `type` with a message that is the
// concatenation of the `messageParts`.
template <typename... Parts>
BlobDiffError makeError(BlobDiffErrorType type, const Parts&... messageParts) {
  return BlobDiffError{type, absl::StrCat(messageParts...)};
}

// Create the `BlobDiffError` for a diff that does not belong to the given base.
BlobDiffError wrongBaseError() {
  return makeError(
      ErrorType::wrongBase,
      "The blob diff does not belong to the given base blob (the size or the "
      "checksum of the base does not match). A diff can only be applied to "
      "exactly the blob from which it was computed.");
}
}  // namespace

// _____________________________________________________________________________
BlobDiffHeader::Checksum blobChecksum(ql::span<const char> bytes) {
  auto digest =
      ad_utility::HashSha256{}(std::string_view{bytes.data(), bytes.size()});
  BlobDiffHeader::Checksum result{};
  AD_CORRECTNESS_CHECK(digest.size() == result.size());
  ql::ranges::transform(digest, result.begin(), ad_utility::staticCast<char>);
  return result;
}

// _____________________________________________________________________________
std::vector<char> computeBlobDiff(ql::span<const char> base,
                                  ql::span<const char> target,
                                  int compressionLevel) {
  auto payload =
      ZstdWrapper::compressWithReference(base, target, compressionLevel);
  ByteBufferWriteSerializer writer;
  writer.reserve(headerSize + payload.size());
  writer << diffFileMagic;
  writer << diffFileVersion;
  writer << static_cast<uint64_t>(base.size());
  writer << blobChecksum(base);
  writer << static_cast<uint64_t>(target.size());
  writer << blobChecksum(target);
  writer << static_cast<uint64_t>(payload.size());
  AD_CORRECTNESS_CHECK(writer.getCurrentPosition() == headerSize);
  writer.serializeBytes(payload.data(), payload.size());
  return std::move(writer).data();
}

// _____________________________________________________________________________
BlobDiffHeaderOrError readBlobDiffHeader(ql::span<const char> diffFile,
                                         ad_utility::MemorySize maxTargetSize) {
  if (diffFile.size() < headerSize) {
    return makeError(ErrorType::invalidMagicBytes,
                     "The given file is too small to be a blob diff file");
  }
  ByteBufferReadSerializerT<false, ql::span<const char>> reader{diffFile};
  std::array<char, 8> magic{};
  reader >> magic;
  if (magic != diffFileMagic) {
    return makeError(ErrorType::invalidMagicBytes,
                     "The given file is not a blob diff file");
  }
  uint16_t version = 0;
  reader >> version;
  if (version != diffFileVersion) {
    return makeError(ErrorType::invalidVersion,
                     "The blob diff file has the unsupported format version ",
                     version, " (the supported version is ", diffFileVersion,
                     ")");
  }
  BlobDiffHeader header;
  reader >> header.baseSize_;
  reader >> header.baseChecksum_;
  reader >> header.targetSize_;
  reader >> header.targetChecksum_;
  reader >> header.payloadSize_;
  AD_CORRECTNESS_CHECK(reader.getCurrentPosition() == headerSize);
  if (header.payloadSize_ != diffFile.size() - headerSize) {
    return makeError(
        ErrorType::invalidPayloadSize,
        "The blob diff file is truncated or has trailing bytes (the header "
        "announces a payload of ",
        header.payloadSize_, " bytes, but ", diffFile.size() - headerSize,
        " bytes follow the header)");
  }
  // The size of the target is later used to allocate the target, so it is
  // validated here. First, it has to match the size that is stored in the
  // header of the ZSTD frame of the payload.
  auto payload = diffFile.subspan(headerSize);
  auto frameSize =
      ZstdWrapper::tryToGetUncompressedSize(payload.data(), payload.size());
  if (auto* error = std::get_if<ZstdWrapper::Error>(&frameSize)) {
    return makeError(ErrorType::corruptedPayload,
                     "The payload of the blob diff file is not a valid ZSTD "
                     "frame (the file is corrupted): ",
                     error->message_);
  }
  if (std::get<size_t>(frameSize) != header.targetSize_) {
    return makeError(
        ErrorType::invalidTargetSize,
        "The blob diff file announces a target of ", header.targetSize_,
        " bytes, but its payload encodes a target of ",
        std::get<size_t>(frameSize), " bytes (the file is corrupted)");
  }
  // Second, both sizes might be corrupted consistently, so check that the size
  // does not exceed the limit of the caller.
  //
  // NOTE: There is no useful bound in terms of the size of the payload or of
  // the base, because highly repetitive data compresses to almost nothing even
  // without a base.
  if (header.targetSize_ > maxTargetSize.getBytes()) {
    return makeError(ErrorType::invalidTargetSize,
                     "The blob diff file announces a target of ",
                     header.targetSize_,
                     " bytes, which exceeds the maximum target size of ",
                     maxTargetSize.asString());
  }
  return header;
}

// _____________________________________________________________________________
DecompressedBlobOrError applyBlobDiffToDecompressedBase(
    ql::span<const char> base, ql::span<const char> diffFile,
    ad_utility::MemorySize maxTargetSize) {
  auto headerOrError = readBlobDiffHeader(diffFile, maxTargetSize);
  if (auto* error = std::get_if<BlobDiffError>(&headerOrError)) {
    return std::move(*error);
  }
  const auto& header = std::get<BlobDiffHeader>(headerOrError);
  // The size is checked first, because it is much cheaper than the checksum.
  if (header.baseSize_ != base.size() ||
      header.baseChecksum_ != blobChecksum(base)) {
    return wrongBaseError();
  }
  auto payload = diffFile.subspan(headerSize);
  // The target is not zero-initialized, because it is completely overwritten
  // on success, and discarded otherwise.
  DecompressedBlob target(header.targetSize_);
  auto size = ZstdWrapper::tryToDecompressWithReference(
      base, payload, target.data(), target.size());
  if (auto* error = std::get_if<ZstdWrapper::Error>(&size)) {
    return makeError(ErrorType::corruptedPayload,
                     "The payload of the blob diff file cannot be "
                     "decompressed (the file is corrupted): ",
                     error->message_);
  }
  if (std::get<size_t>(size) != target.size() ||
      blobChecksum(target) != header.targetChecksum_) {
    return makeError(ErrorType::corruptedPayload,
                     "Applying the blob diff did not yield the expected result "
                     "(the checksum of the result does not match), the diff "
                     "file is corrupted");
  }
  return target;
}

// _____________________________________________________________________________
BlobOrError applyBlobDiffToCompressedBase(
    ql::span<const char> compressedBase, ql::span<const char> diffFile,
    ad_utility::MemorySize maxTargetSize) {
  // Run the cheap checks before the (possibly large) base is decompressed:
  // First validate the header of the diff, then compare the size of the
  // decompressed base, which is stored in the header of its ZSTD frame, with
  // the size that the diff expects. The checksum of the base is only checked
  // after the decompression (in `applyBlobDiffToDecompressedBase`, which also
  // parses the header again, which is cheap).
  auto headerOrError = readBlobDiffHeader(diffFile, maxTargetSize);
  if (auto* error = std::get_if<BlobDiffError>(&headerOrError)) {
    return std::move(*error);
  }
  auto baseSize = ZstdWrapper::tryToGetUncompressedSize(compressedBase.data(),
                                                        compressedBase.size());
  if (auto* error = std::get_if<ZstdWrapper::Error>(&baseSize)) {
    return makeError(ErrorType::baseNotDecompressible,
                     "The base blob cannot be decompressed: ", error->message_);
  }
  if (std::get<size_t>(baseSize) !=
      std::get<BlobDiffHeader>(headerOrError).baseSize_) {
    return wrongBaseError();
  }
  auto base = Manager::tryToDecompressBlob(compressedBase, {});
  if (auto* error = std::get_if<Manager::BlobError>(&base)) {
    return makeError(ErrorType::baseNotDecompressible,
                     "The base blob cannot be decompressed: ", error->message_);
  }
  auto target = applyBlobDiffToDecompressedBase(
      ql::span<const char>{std::get<DecompressedBase>(base)}, diffFile,
      maxTargetSize);
  if (auto* error = std::get_if<BlobDiffError>(&target)) {
    return std::move(*error);
  }
  return Manager::compressBlob(
      ql::span<const char>{std::get<DecompressedBlob>(target)});
}

// _____________________________________________________________________________
std::string describeBlobDiff(ql::span<const char> diffFile) {
  // Only the header is described, nothing is allocated, so any target size is
  // fine.
  auto headerOrError =
      readBlobDiffHeader(diffFile, ad_utility::MemorySize::max());
  if (auto* error = std::get_if<BlobDiffError>(&headerOrError)) {
    throw std::runtime_error{error->message_};
  }
  const auto& header = std::get<BlobDiffHeader>(headerOrError);
  auto percent = [](uint64_t part, uint64_t whole) {
    return whole == 0
               ? 0.0
               : 100.0 * static_cast<double>(part) / static_cast<double>(whole);
  };
  return absl::StrFormat(
      "Blob diff of %d bytes: base %d bytes, target %d bytes (both "
      "decompressed), payload %d bytes (%.2f%% of the decompressed target)",
      diffFile.size(), header.baseSize_, header.targetSize_,
      header.payloadSize_, percent(header.payloadSize_, header.targetSize_));
}

}  // namespace qlever::blobDiff

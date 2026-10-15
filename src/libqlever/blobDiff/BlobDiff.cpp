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

#include <cstring>

#include "backports/algorithm.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/CryptographicHashUtils.h"
#include "util/Exception.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/TransparentFunctors.h"

namespace qlever {

namespace {
using Manager = NamedCachedQueryBlobManager;
using ad_utility::serialization::ByteBufferReadSerializerT;
using ad_utility::serialization::ByteBufferWriteSerializer;

constexpr std::array<char, 8> diffFileMagic{'Q', 'L', 'V', 'R',
                                            'B', 'D', 'I', 'F'};
constexpr uint16_t diffFileVersion = 2;

// The size of the header of a diff file in bytes (see the file format in
// `BlobDiff.h`).
constexpr size_t headerSize = diffFileMagic.size() + sizeof(diffFileVersion) +
                              3 * sizeof(uint64_t) +
                              2 * sizeof(BlobDiffHeader::Checksum);

// Decompress a compressed blob, or throw with a message that starts with
// `what`.
std::vector<char, Manager::BlobAllocator> decompressOrThrow(
    ql::span<const char> compressedBlob, std::string_view what) {
  auto result = Manager::tryToDecompressBlob(compressedBlob, {});
  if (auto* error = std::get_if<Manager::BlobError>(&result)) {
    throw std::runtime_error{
        absl::StrCat(what, " cannot be decompressed: ", error->message_)};
  }
  return std::get<0>(std::move(result));
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
  auto writeArray = [&writer](const auto& array) {
    writer.serializeBytes(array.data(), array.size());
  };
  writeArray(diffFileMagic);
  writer << diffFileVersion;
  writer << static_cast<uint64_t>(base.size());
  writeArray(blobChecksum(base));
  writer << static_cast<uint64_t>(target.size());
  writeArray(blobChecksum(target));
  writer << static_cast<uint64_t>(payload.size());
  AD_CORRECTNESS_CHECK(writer.getCurrentPosition() == headerSize);
  writer.serializeBytes(payload.data(), payload.size());
  return std::move(writer).data();
}

// _____________________________________________________________________________
BlobDiffHeader readBlobDiffHeader(ql::span<const char> diffFile) {
  AD_CONTRACT_CHECK(diffFile.size() >= headerSize,
                    "The given file is too small to be a blob diff file");
  ByteBufferReadSerializerT<false, ql::span<const char>> reader{diffFile};
  auto readArray = [&reader](auto& array) {
    reader.serializeBytes(array.data(), array.size());
  };
  std::array<char, 8> magic{};
  readArray(magic);
  AD_CONTRACT_CHECK(magic == diffFileMagic,
                    "The given file is not a blob diff file");
  uint16_t version = 0;
  reader >> version;
  AD_CONTRACT_CHECK(version == diffFileVersion,
                    "The blob diff file has the unsupported format version ",
                    version, " (the supported version is ", diffFileVersion,
                    ")");
  BlobDiffHeader header;
  reader >> header.baseSize_;
  readArray(header.baseChecksum_);
  reader >> header.targetSize_;
  readArray(header.targetChecksum_);
  reader >> header.payloadSize_;
  AD_CORRECTNESS_CHECK(reader.getCurrentPosition() == headerSize);
  AD_CONTRACT_CHECK(
      header.payloadSize_ == diffFile.size() - headerSize,
      "The blob diff file is truncated or has trailing bytes (the header "
      "announces a payload of ",
      header.payloadSize_, " bytes, but ", diffFile.size() - headerSize,
      " bytes follow the header)");
  return header;
}

// _____________________________________________________________________________
std::vector<char> applyBlobDiffToDecompressed(ql::span<const char> base,
                                              ql::span<const char> diffFile) {
  auto header = readBlobDiffHeader(diffFile);
  // The size is checked first, because it is much cheaper than the checksum.
  AD_CONTRACT_CHECK(
      header.baseSize_ == base.size() &&
          header.baseChecksum_ == blobChecksum(base),
      "The blob diff does not belong to the given base blob (the size or the "
      "checksum of the base does not match). Note that a diff has to be "
      "applied to exactly the blob of the previous week.");
  auto payload = diffFile.subspan(headerSize);
  std::vector<char> target(header.targetSize_);
  auto size = ZstdWrapper::tryToDecompressWithReference(
      base, payload, target.data(), target.size());
  if (auto* error = std::get_if<ZstdWrapper::Error>(&size)) {
    throw std::runtime_error{
        absl::StrCat("The payload of the blob diff file cannot be "
                     "decompressed (the file is corrupted): ",
                     error->message_)};
  }
  AD_CONTRACT_CHECK(std::get<size_t>(size) == target.size() &&
                        blobChecksum(target) == header.targetChecksum_,
                    "Applying the blob diff did not yield the expected result "
                    "(the checksum of the result does not match), the diff "
                    "file is corrupted");
  return target;
}

// _____________________________________________________________________________
std::vector<char> applyBlobDiff(ql::span<const char> compressedBase,
                                ql::span<const char> diffFile) {
  auto base = decompressOrThrow(compressedBase, "The base blob");
  auto target =
      applyBlobDiffToDecompressed(ql::span<const char>{base}, diffFile);
  return Manager::compressBlob(ql::span<const char>{target});
}

// _____________________________________________________________________________
std::string describeBlobDiff(ql::span<const char> diffFile) {
  auto header = readBlobDiffHeader(diffFile);
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

}  // namespace qlever

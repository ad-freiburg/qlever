// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/BlobCompression.h"

#include <zstd.h>

#include <array>

#include "backports/algorithm.h"
#include "util/Log.h"
#include "util/compressionUsingZstd/ZstdFrameFormat.h"
#include "util/compressionUsingZstd/ZstdStablePrefix.h"
#include "util/compressionUsingZstd/ZstdWrapper.h"
#include "util/serializer/ByteBufferSerializer.h"
#include "util/serializer/SerializeArrayOrTuple.h"

namespace qlever::blobCompression {

// The serialization of a `BlobIndex` (also of its `BlobEncoding`).
AD_SERIALIZE_FUNCTION(BlobEncoding) {
  static_assert(sizeof(arg.compressionLevel_) == sizeof(int32_t));
  serializer | arg.compressionLevel_;
  serializer | arg.stablePrefixSize_;
}
AD_SERIALIZE_FUNCTION(BlobIndex) {
  serializer | arg.encoding_;
  serializer | arg.stablePrefixBlocksEnd_;
}

namespace {

namespace zstd = ad_utility::zstd;
using namespace ad_utility::serialization;

// The skippable frame with the `BlobIndex` has the magic variant `0xB` (one of
// the 16 magic numbers of skippable frames). Its content consists of the
// magic bytes, the version, and the serialized `BlobIndex`.
constexpr uint32_t indexMagicVariant = 0xB;
constexpr std::array<char, 8> indexMagic{'Q', 'L', 'V', 'R',
                                         'B', 'I', 'D', 'X'};
constexpr uint16_t indexVersion = 1;
constexpr size_t indexContentSize = indexMagic.size() + sizeof(indexVersion) +
                                    sizeof(int32_t) + 2 * sizeof(uint64_t);

// Append the skippable frame with the `index` to `out`.
void appendIndexFrame(std::vector<char>& out, const BlobIndex& index) {
  ByteBufferWriteSerializer content;
  content << indexMagic;
  content << indexVersion;
  content << index;
  AD_CORRECTNESS_CHECK(content.data().size() == indexContentSize);
  zstd::appendSkippableFrame(out, indexMagicVariant, content.data());
}

// Return the blocks of the stable prefix of the `reusableBlob` if they can be
// used for the stable `prefix` of another blob: the `reusableBlob` has a
// stable prefix of the same size and with the same content, and its blocks are
// consistent with its `BlobIndex`.
std::optional<ql::span<const char>> reusableStablePrefix(
    const ReusableBlob& reusableBlob, ql::span<const char> prefix) {
  auto index = readBlobIndex(reusableBlob.compressed_);
  if (!index.has_value() ||
      index->encoding_.stablePrefixSize_ != prefix.size() ||
      reusableBlob.decompressed_.size() < prefix.size() ||
      !ql::ranges::equal(reusableBlob.decompressed_.first(prefix.size()),
                         prefix)) {
    return std::nullopt;
  }
  return zstd::leadingNonLastBlocks(reusableBlob.compressed_,
                                    index->stablePrefixBlocksEnd_);
}

// Return the blocks of the stable `prefix`, either copied from the
// `reusableBlob` or freshly encoded (then they are stored in `storage`). Return
// `std::nullopt` if the prefix cannot be encoded.
std::optional<ql::span<const char>> stablePrefixBlocks(
    ql::span<const char> prefix,
    const std::optional<ReusableBlob>& reusableBlob,
    std::vector<char>& storage) {
  if (reusableBlob.has_value()) {
    if (auto blocks = reusableStablePrefix(reusableBlob.value(), prefix)) {
      return blocks;
    }
  }
  auto blocks = zstd::encodeStablePrefix(prefix, stablePrefixCompressionLevel);
  if (!blocks.has_value()) {
    AD_LOG_WARN << "The stable prefix of a blob (" << prefix.size()
                << " bytes) could not be encoded, the blob is written "
                   "without it"
                << std::endl;
    return std::nullopt;
  }
  storage = std::move(blocks.value());
  return ql::span<const char>{storage};
}

}  // namespace

// _____________________________________________________________________________
std::vector<char> compressBlob(
    ql::span<const char> uncompressedBlob, const BlobEncoding& encoding,
    const std::optional<ReusableBlob>& reusableBlob) {
  const uint64_t prefixSize = encoding.stablePrefixSize_;
  std::optional<ql::span<const char>> prefixBlocks;
  std::vector<char> encodedPrefixBlocks;
  if (prefixSize >= minStablePrefixSize &&
      prefixSize < uncompressedBlob.size()) {
    prefixBlocks = stablePrefixBlocks(uncompressedBlob.first(prefixSize),
                                      reusableBlob, encodedPrefixBlocks);
  }

  constexpr size_t indexFrameSize =
      zstd::skippableFrameHeaderSize + indexContentSize;
  BlobIndex index;
  index.encoding_.compressionLevel_ = encoding.compressionLevel_;
  std::vector<char> result;
  if (!prefixBlocks.has_value()) {
    // A single regular frame.
    auto frame = ZstdWrapper::compressToUninitializedBuffer(
        uncompressedBlob, encoding.compressionLevel_);
    result.reserve(frame.size() + indexFrameSize);
    result.insert(result.end(), frame.begin(), frame.end());
  } else {
    // The blocks of the stable prefix, followed by the blocks of a fresh
    // compression of the remainder (see `ZstdStablePrefix.h`).
    auto remainder = ZstdWrapper::compressToUninitializedBuffer(
        uncompressedBlob.subspan(prefixSize), encoding.compressionLevel_);
    auto remainderBlocks = zstd::blocksOfFrame(remainder).blocks_;
    result.reserve(zstd::singleSegmentFrameHeaderSize + prefixBlocks->size() +
                   remainderBlocks.size() + indexFrameSize);
    zstd::appendSingleSegmentFrameHeader(result, uncompressedBlob.size());
    result.insert(result.end(), prefixBlocks->begin(), prefixBlocks->end());
    index.encoding_.stablePrefixSize_ = prefixSize;
    index.stablePrefixBlocksEnd_ = result.size();
    result.insert(result.end(), remainderBlocks.begin(), remainderBlocks.end());
  }
  appendIndexFrame(result, index);
  return result;
}

// _____________________________________________________________________________
std::optional<BlobIndex> readBlobIndex(ql::span<const char> compressedBlob) {
  auto content = zstd::readTrailingSkippableFrame(
      compressedBlob, indexMagicVariant, indexContentSize);
  if (!content.has_value()) {
    return std::nullopt;
  }
  // The content has the fixed size `indexContentSize`, so the deserialization
  // cannot run out of bytes.
  ByteBufferReadSerializerT reader{content.value()};
  std::array<char, indexMagic.size()> magic;
  uint16_t version;
  BlobIndex result;
  reader >> magic;
  reader >> version;
  if (magic != indexMagic || version != indexVersion) {
    return std::nullopt;
  }
  reader >> result;

  // The index has to directly follow the (only) frame of the blob, and the
  // stable prefix has to be within that frame.
  const size_t frameEnd =
      compressedBlob.size() - zstd::skippableFrameHeaderSize - indexContentSize;
  auto frame = compressedBlob.first(frameEnd);
  if (frame.empty() ||
      ZSTD_findFrameCompressedSize(frame.data(), frame.size()) != frameEnd) {
    return std::nullopt;
  }
  if (result.encoding_.stablePrefixSize_ == 0) {
    if (result.stablePrefixBlocksEnd_ != 0) {
      return std::nullopt;
    }
  } else {
    auto contentSize = ZSTD_getFrameContentSize(frame.data(), frame.size());
    if (contentSize == ZSTD_CONTENTSIZE_UNKNOWN ||
        contentSize == ZSTD_CONTENTSIZE_ERROR ||
        result.encoding_.stablePrefixSize_ >= contentSize ||
        result.stablePrefixBlocksEnd_ >= frameEnd) {
      return std::nullopt;
    }
  }
  return result;
}

}  // namespace qlever::blobCompression

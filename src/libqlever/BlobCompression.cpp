// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// The experimental API of ZSTD (`ZSTD_frameHeaderSize`, and the sequence API
// for the stable prefix) has to be enabled before `zstd.h` is included for the
// first time. `ZSTD_generateSequences` is deprecated in ZSTD 1.5.7, but still
// the only way to obtain the sequences of a regular compression.
#define ZSTD_STATIC_LINKING_ONLY
#define ZSTD_DISABLE_DEPRECATE_WARNINGS
#include "libqlever/BlobCompression.h"

#include <zstd.h>

#include <array>
#include <cstring>
#include <memory>
#include <random>
#include <string>

#include "backports/algorithm.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/UninitializedAllocator.h"

namespace qlever::blobCompression {

namespace {

// The magic number of the skippable frame with the `BlobIndex` (one of the 16
// magic numbers of skippable frames), and the magic bytes and the version of
// its content.
constexpr uint32_t indexFrameMagic = ZSTD_MAGIC_SKIPPABLE_START + 0xB;
constexpr std::array<char, 8> indexMagic{'Q', 'L', 'V', 'R',
                                         'B', 'I', 'D', 'X'};
constexpr uint16_t indexVersion = 1;
// The content of the skippable frame: the magic bytes, the version, the
// compression level, the size of the stable prefix, and the end of its blocks.
constexpr size_t indexContentSize = indexMagic.size() + sizeof(uint16_t) +
                                    sizeof(int32_t) + 2 * sizeof(uint64_t);
// The size of the complete skippable frame (magic number, size, content).
constexpr size_t indexFrameSize = 2 * sizeof(uint32_t) + indexContentSize;
// The size of the frame header that is written by `writeFrameHeader`.
constexpr size_t frameHeaderSize = sizeof(uint32_t) + 1 + sizeof(uint64_t);

// Append `value` to `out` in little endian (the native byte order on all
// platforms that QLever currently supports).
template <typename T>
void append(std::vector<char>& out, T value) {
  static_assert(std::is_trivially_copyable_v<T>);
  const char* bytes = reinterpret_cast<const char*>(&value);
  out.insert(out.end(), bytes, bytes + sizeof(T));
}

// Read a `T` at `position` of `bytes` (which has to be in bounds).
template <typename T>
T read(ql::span<const char> bytes, size_t position) {
  static_assert(std::is_trivially_copyable_v<T>);
  AD_CORRECTNESS_CHECK(position + sizeof(T) <= bytes.size());
  T value;
  std::memcpy(&value, bytes.data() + position, sizeof(T));
  return value;
}

// The header of a block of a ZSTD frame (see the ZSTD format specification).
struct BlockHeader {
  static constexpr size_t size = 3;
  static constexpr uint32_t compressedBlock = 2;
  bool isLast_;
  uint32_t type_;
  uint32_t blockSize_;
  // The number of bytes of the block including its header.
  size_t totalSize() const {
    // A block of type 1 (RLE) only stores a single byte.
    return size + (type_ == 1 ? 1 : blockSize_);
  }
};

// Read the block header at `position` of `bytes`. Return `std::nullopt` if the
// header or the block itself exceeds `end`, or has the reserved type.
std::optional<BlockHeader> readBlockHeader(ql::span<const char> bytes,
                                           size_t position, size_t end) {
  if (position + BlockHeader::size > end) {
    return std::nullopt;
  }
  auto byte = [&bytes, position](size_t i) {
    return static_cast<uint32_t>(static_cast<uint8_t>(bytes[position + i]));
  };
  uint32_t value = byte(0) | byte(1) << 8 | byte(2) << 16;
  BlockHeader header{(value & 1) != 0, (value >> 1) & 3, value >> 3};
  if (header.type_ == 3 || position + header.totalSize() > end) {
    return std::nullopt;
  }
  return header;
}

// The blocks of a single frame that was written by the ZSTD compressor
// (without the frame header, and without a checksum, which is never written
// here). `lastHeader_` is the offset of the header of the last block (relative
// to `blocks_`).
struct FrameBlocks {
  ql::span<const char> blocks_;
  size_t lastHeader_;
};
FrameBlocks blocksOfFrame(ql::span<const char> frame) {
  size_t begin =
      ZstdWrapper::checkError(ZSTD_frameHeaderSize(frame.data(), frame.size()));
  ZSTD_frameHeader header;
  AD_CORRECTNESS_CHECK(
      ZSTD_getFrameHeader(&header, frame.data(), frame.size()) == 0);
  AD_CORRECTNESS_CHECK(header.checksumFlag == 0);
  size_t position = begin;
  size_t lastHeader = begin;
  bool foundLast = false;
  while (!foundLast) {
    auto block = readBlockHeader(frame, position, frame.size());
    AD_CORRECTNESS_CHECK(block.has_value());
    lastHeader = position;
    foundLast = block->isLast_;
    position += block->totalSize();
  }
  AD_CORRECTNESS_CHECK(position == frame.size());
  return {frame.subspan(begin), lastHeader - begin};
}

// Write the header of a frame with the given `contentSize`. The frame is a
// "single segment" frame (its window is the whole content), so that the blocks
// of the stable prefix and the blocks of the remainder, which were written by
// different compressors with different windows, can be combined.
//
// NOTE: A streaming decompressor needs a buffer for the whole window, so it
// has to allow a window of the size of the blob (see `ZSTD_d_windowLogMax`).
// The one-shot decompression of `tryToDecompressBlob` is not affected.
void writeFrameHeader(std::vector<char>& out, uint64_t contentSize) {
  append(out, static_cast<uint32_t>(ZSTD_MAGICNUMBER));
  // The frame header descriptor: 8 bytes for the content size (`3 << 6`), and
  // the single segment flag (`1 << 5`); no checksum, no dictionary.
  out.push_back(static_cast<char>((3 << 6) | (1 << 5)));
  append(out, contentSize);
  AD_CORRECTNESS_CHECK(out.size() == frameHeaderSize);
}

// Append the skippable frame with the `index` to `out`.
void appendIndexFrame(std::vector<char>& out, const BlobIndex& index) {
  append(out, indexFrameMagic);
  append(out, static_cast<uint32_t>(indexContentSize));
  out.insert(out.end(), indexMagic.begin(), indexMagic.end());
  append(out, indexVersion);
  append(out, static_cast<int32_t>(index.encoding_.compressionLevel_));
  append(out, index.encoding_.stablePrefixSize_);
  append(out, index.stablePrefixBlocksEnd_);
}

// A buffer whose bytes are not zero-initialized.
using Buffer = std::vector<char, ad_utility::default_init_allocator<char>>;

// Compress `data` with a fresh ZSTD compressor at the `compressionLevel` and
// return the frame.
//
// NOTE: The buffer for the worst-case size of the frame is not
// zero-initialized, so only the pages that are actually written are touched
// (the frame is typically much smaller than the worst case). This avoids a lot
// of page faults for large blobs.
Buffer compressPlain(ql::span<const char> data, int compressionLevel) {
  Buffer frame(ZSTD_compressBound(data.size()));
  frame.resize(ZstdWrapper::checkError(ZSTD_compress(
      frame.data(), frame.size(), data.data(), data.size(), compressionLevel)));
  return frame;
}

// Return a deterministic sequence of bytes that consists of random literals
// and runs with the periods 1, 4, and 8, such that its compression uses all
// three repeat offsets (see `checkStablePrefix`).
std::string makeProbe(uint32_t seed) {
  std::mt19937 generator{seed};
  std::string probe;
  for (size_t i = 0; i < 2000; ++i) {
    auto kind = generator() % 4;
    if (kind == 0 || probe.size() < 8) {
      for (size_t j = 0; j < 5; ++j) {
        probe.push_back(static_cast<char>(generator()));
      }
      continue;
    }
    size_t period = kind == 1 ? 1 : (kind == 2 ? 4 : 8);
    size_t length = 6 + generator() % 10;
    for (size_t j = 0; j < length; ++j) {
      probe.push_back(probe[probe.size() - period]);
    }
  }
  return probe;
}

// Check that the encoded stable `prefixBlocks` of `prefix` leave the decoder
// in the state of a frame start, by appending the blocks of fresh compressions
// of probes that use all three repeat offsets, and decompressing the result.
bool checkStablePrefix(ql::span<const char> prefix,
                       const std::vector<char>& prefixBlocks) {
  for (uint32_t seed : {1u, 2u}) {
    std::string probe = makeProbe(seed);
    std::vector<char> frame;
    writeFrameHeader(frame, prefix.size() + probe.size());
    frame.insert(frame.end(), prefixBlocks.begin(), prefixBlocks.end());
    auto probeFrame = compressPlain(
        ql::span<const char>{probe.data(), probe.size()}, seed == 1 ? 1 : 3);
    auto probeBlocks = blocksOfFrame(probeFrame).blocks_;
    frame.insert(frame.end(), probeBlocks.begin(), probeBlocks.end());
    std::vector<char> decompressed(prefix.size() + probe.size());
    size_t size = ZSTD_decompress(decompressed.data(), decompressed.size(),
                                  frame.data(), frame.size());
    if (ZSTD_isError(size) || size != decompressed.size() ||
        std::memcmp(decompressed.data(), prefix.data(), prefix.size()) != 0 ||
        std::memcmp(decompressed.data() + prefix.size(), probe.data(),
                    probe.size()) != 0) {
      return false;
    }
  }
  return true;
}

#if ZSTD_VERSION_NUMBER >= 10505
// Append the `sequences` to `result`, followed by `trailingLiterals`
// literals, and insert block delimiters such that no block is larger than the
// maximal block size of ZSTD (literals are split between blocks, matches are
// not). The `blockSize` is the size of the current (unfinished) block of
// `result`. The result ends with a block delimiter.
void appendInBlocks(std::vector<ZSTD_Sequence>& result,
                    std::vector<ZSTD_Sequence> sequences,
                    size_t trailingLiterals, size_t blockSize) {
  constexpr size_t maxBlockSize = ZSTD_BLOCKSIZE_MAX;
  auto endBlock = [&result, &blockSize](size_t literals) {
    ZSTD_Sequence delimiter{};
    delimiter.litLength = static_cast<unsigned>(literals);
    result.push_back(delimiter);
    blockSize = 0;
  };
  for (auto sequence : sequences) {
    while (blockSize + sequence.litLength + sequence.matchLength >
           maxBlockSize) {
      size_t literals =
          std::min<size_t>(sequence.litLength, maxBlockSize - blockSize);
      sequence.litLength -= static_cast<unsigned>(literals);
      endBlock(literals);
    }
    result.push_back(sequence);
    blockSize += sequence.litLength + sequence.matchLength;
  }
  do {
    size_t literals = std::min(trailingLiterals, maxBlockSize - blockSize);
    trailingLiterals -= literals;
    endBlock(literals);
  } while (trailingLiterals > 0);
}

// Encode `prefix` as a stable prefix (see `BlobCompression.h`) and return its
// blocks (without a frame header, and with the last-block flag cleared).
// Return `std::nullopt` if this is not possible, because no suitable matches
// for the end of the prefix are found (or only far from its end), or if the
// result does not pass the `checkStablePrefix` check.
std::optional<std::vector<char>> encodeStablePrefix(
    ql::span<const char> prefix) {
  const char* data = prefix.data();
  const size_t size = prefix.size();
  if (size < minStablePrefixSize) {
    return std::nullopt;
  }
  // 1. Find the positions of three matches of length 3 with the offsets 8, 4,
  // and 1 (in this order), with at least one literal before each of them, as
  // late as possible. The repeat offsets after these three matches are
  // `{1, 4, 8}`, no matter how the matches are encoded and what the repeat
  // offsets were before (see the repeat offset rules of the ZSTD format). All
  // following bytes are encoded as literals (without matches), so they are
  // compressed worse, which is why they must not be too many.
  constexpr size_t matchLength = 3;
  // Return the largest position `< limit` of a valid match with the `offset`
  // that is preceded by at least one literal.
  auto findMatchBefore = [data](size_t limit,
                                size_t offset) -> std::optional<size_t> {
    for (size_t position = limit; position > offset + 1;) {
      --position;
      if (std::memcmp(data + position, data + position - offset, matchLength) ==
          0) {
        return position;
      }
    }
    return std::nullopt;
  };
  auto match1 = findMatchBefore(size - matchLength + 1, 1);
  if (!match1.has_value() || match1.value() <= matchLength) {
    return std::nullopt;
  }
  const size_t literalTail = size - (match1.value() + matchLength);
  if (literalTail > std::max<size_t>(256 * 1024, size / 10)) {
    return std::nullopt;
  }
  auto match4 = findMatchBefore(match1.value() - matchLength, 4);
  if (!match4.has_value() || match4.value() <= matchLength) {
    return std::nullopt;
  }
  auto match8 = findMatchBefore(match4.value() - matchLength, 8);
  if (!match8.has_value()) {
    return std::nullopt;
  }

  // 2. The sequences of a regular compression of `[0, lastStart)` (in
  // complete blocks), and of `[lastStart, match8 - 1)`, which is followed by
  // the three matches and the trailing literals. These are split into blocks
  // by `appendInBlocks`.
  //
  // NOTE: A block that contains one of the three matches must be stored as a
  // compressed block. A block that is stored uncompressed would lose its
  // matches, and thereby their effect on the repeat offsets, which
  // `checkStablePrefix` detects.
  constexpr size_t lastSliceSize = 64 * 1024;
  const size_t head = match8.value() - 1;
  const size_t lastStart = head > lastSliceSize ? head - lastSliceSize : 0;
  std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context{
      ZSTD_createCCtx(), &ZSTD_freeCCtx};
  AD_CORRECTNESS_CHECK(context != nullptr);
  auto setParameter = [&context](ZSTD_cParameter parameter, int value) {
    ZstdWrapper::checkError(
        ZSTD_CCtx_setParameter(context.get(), parameter, value));
  };
  // Return the sequences of `[from, to)`, which end with a block delimiter.
  auto generateSequences = [&](size_t from, size_t to) {
    ZSTD_CCtx_reset(context.get(), ZSTD_reset_session_and_parameters);
    setParameter(ZSTD_c_compressionLevel, stablePrefixCompressionLevel);
    std::vector<ZSTD_Sequence> sequences(ZSTD_sequenceBound(to - from));
    size_t numSequences = ZstdWrapper::checkError(
        ZSTD_generateSequences(context.get(), sequences.data(),
                               sequences.size(), data + from, to - from));
    sequences.resize(numSequences);
    AD_CORRECTNESS_CHECK(!sequences.empty() &&
                         sequences.back().matchLength == 0);
    return sequences;
  };
  std::vector<ZSTD_Sequence> sequences;
  if (lastStart > 0) {
    sequences = generateSequences(0, lastStart);
  }
  // The sequences of the last slice without its block delimiters. The
  // literals of a delimiter become literals of the next sequence.
  std::vector<ZSTD_Sequence> tail;
  unsigned literals = 0;
  for (auto sequence : generateSequences(lastStart, head)) {
    sequence.litLength += literals;
    literals = 0;
    if (sequence.matchLength == 0) {
      literals = sequence.litLength;
    } else {
      tail.push_back(sequence);
    }
  }
  // The literal at `head` precedes the first match.
  auto addMatch = [&tail](unsigned offset, size_t literalLength) {
    ZSTD_Sequence sequence{};
    sequence.offset = offset;
    sequence.litLength = static_cast<unsigned>(literalLength);
    sequence.matchLength = matchLength;
    tail.push_back(sequence);
  };
  addMatch(8, literals + 1);
  addMatch(4, match4.value() - (match8.value() + matchLength));
  addMatch(1, match1.value() - (match4.value() + matchLength));
  appendInBlocks(sequences, std::move(tail), literalTail, 0);

  // 3. Compress the sequences.
  ZSTD_CCtx_reset(context.get(), ZSTD_reset_session_and_parameters);
  setParameter(ZSTD_c_compressionLevel, stablePrefixCompressionLevel);
  setParameter(ZSTD_c_blockDelimiters, ZSTD_sf_explicitBlockDelimiters);
  setParameter(ZSTD_c_validateSequences, 1);
  // The matches at the end have the minimal length.
  setParameter(ZSTD_c_minMatch, matchLength);
  std::vector<char> frame(ZSTD_compressBound(size));
  size_t frameSize =
      ZSTD_compressSequences(context.get(), frame.data(), frame.size(),
                             sequences.data(), sequences.size(), data, size);
  if (ZSTD_isError(frameSize)) {
    return std::nullopt;
  }
  frame.resize(frameSize);

  // 4. Extract the blocks, clear the last-block flag of the last one (the
  // blocks of the remainder follow), and check the result.
  auto [blocks, lastHeader] = blocksOfFrame(frame);
  std::vector<char> result{blocks.begin(), blocks.end()};
  result[lastHeader] = static_cast<char>(result[lastHeader] & ~1);
  if (!checkStablePrefix(prefix, result)) {
    return std::nullopt;
  }
  return result;
}
#else
// Without the sequence API (see `BlobCompression.h`), no stable prefix can be
// encoded.
std::optional<std::vector<char>> encodeStablePrefix(ql::span<const char>) {
  return std::nullopt;
}
#endif

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
      std::memcmp(reusableBlob.decompressed_.data(), prefix.data(),
                  prefix.size()) != 0) {
    return std::nullopt;
  }
  const auto& compressed = reusableBlob.compressed_;
  size_t begin = ZSTD_frameHeaderSize(compressed.data(), compressed.size());
  size_t end = index->stablePrefixBlocksEnd_;
  if (ZSTD_isError(begin) || begin >= end) {
    return std::nullopt;
  }
  // The blocks have to end exactly at `end`, and none of them may be the last
  // block of the frame.
  size_t position = begin;
  while (position < end) {
    auto block = readBlockHeader(compressed, position, end);
    if (!block.has_value() || block->isLast_) {
      return std::nullopt;
    }
    position += block->totalSize();
  }
  return compressed.subspan(begin, end - begin);
}

}  // namespace

// _____________________________________________________________________________
std::vector<char> compressBlob(
    ql::span<const char> uncompressedBlob, const BlobEncoding& encoding,
    const std::optional<ReusableBlob>& reusableBlob) {
  const uint64_t prefixSize = encoding.stablePrefixSize_;
  // The blocks of the stable prefix, either copied from the `reusableBlob`, or
  // encoded (then they are stored in `encodedPrefixBlocks`).
  std::optional<ql::span<const char>> prefixBlocks;
  std::vector<char> encodedPrefixBlocks;
  if (prefixSize >= minStablePrefixSize &&
      prefixSize < uncompressedBlob.size()) {
    auto prefix = uncompressedBlob.first(prefixSize);
    if (reusableBlob.has_value()) {
      prefixBlocks = reusableStablePrefix(reusableBlob.value(), prefix);
    }
    if (!prefixBlocks.has_value()) {
      if (auto blocks = encodeStablePrefix(prefix)) {
        encodedPrefixBlocks = std::move(blocks.value());
        prefixBlocks = ql::span<const char>{encodedPrefixBlocks};
      } else {
        AD_LOG_WARN << "The stable prefix of a blob (" << prefixSize
                    << " bytes) could not be encoded, the blob is written "
                       "without it"
                    << std::endl;
      }
    }
  }

  BlobIndex index;
  index.encoding_.compressionLevel_ = encoding.compressionLevel_;
  std::vector<char> result;
  if (!prefixBlocks.has_value()) {
    // A single regular frame.
    auto frame = compressPlain(uncompressedBlob, encoding.compressionLevel_);
    result.reserve(frame.size() + indexFrameSize);
    result.insert(result.end(), frame.begin(), frame.end());
  } else {
    // The blocks of the stable prefix, followed by the blocks of a fresh
    // compression of the remainder (see `BlobCompression.h`).
    auto remainder = compressPlain(uncompressedBlob.subspan(prefixSize),
                                   encoding.compressionLevel_);
    auto remainderBlocks = blocksOfFrame(remainder).blocks_;
    result.reserve(frameHeaderSize + prefixBlocks->size() +
                   remainderBlocks.size() + indexFrameSize);
    writeFrameHeader(result, uncompressedBlob.size());
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
  if (compressedBlob.size() <= indexFrameSize) {
    return std::nullopt;
  }
  const size_t frameEnd = compressedBlob.size() - indexFrameSize;
  auto index = compressedBlob.subspan(frameEnd);
  if (read<uint32_t>(index, 0) != indexFrameMagic ||
      read<uint32_t>(index, 4) != indexContentSize ||
      std::memcmp(index.data() + 8, indexMagic.data(), indexMagic.size()) !=
          0 ||
      read<uint16_t>(index, 8 + indexMagic.size()) != indexVersion) {
    return std::nullopt;
  }
  size_t position = 8 + indexMagic.size() + sizeof(uint16_t);
  BlobIndex result;
  result.encoding_.compressionLevel_ = read<int32_t>(index, position);
  position += sizeof(int32_t);
  result.encoding_.stablePrefixSize_ = read<uint64_t>(index, position);
  position += sizeof(uint64_t);
  result.stablePrefixBlocksEnd_ = read<uint64_t>(index, position);

  // The index has to directly follow the (only) frame of the blob, and the
  // stable prefix has to be within that frame.
  auto frame = compressedBlob.first(frameEnd);
  if (ZSTD_findFrameCompressedSize(frame.data(), frame.size()) != frameEnd) {
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

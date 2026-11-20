// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/compressionUsingZstd/ZstdIndependentUnit.h"

#include <zstd.h>

#include <algorithm>
#include <cstring>
#include <random>
#include <string>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/UninitializedAllocator.h"
#include "util/compressionUsingZstd/ZstdFrameBlocks.h"
#include "util/compressionUsingZstd/ZstdHandWrittenBlocks.h"

namespace ad_utility::zstd {

namespace {

// The length of the three final matches (the minimal match length of ZSTD).
constexpr size_t matchLength = 3;

// Return the maximal number of bytes at the end of a unit of `size` bytes that
// are stored uncompressed (everything after the start of the first of the
// three final matches, except for the matches themselves).
size_t maxUncompressedTail(size_t size) {
  return std::max<size_t>(64 * 1024, size / 16);
}

// The start positions of the three final matches with the offsets 8, 4, and 1.
struct FinalMatches {
  size_t match8_;
  size_t match4_;
  size_t match1_;
};

// Find the three final matches (see `ZstdIndependentUnit.h`) in `data`, as
// late as possible. They must not overlap, and must start at most
// `maxUncompressedTail` bytes before the end of `data`. Return `std::nullopt`
// if there are no such matches.
std::optional<FinalMatches> findFinalMatches(ql::span<const char> data) {
  const size_t maxTail = maxUncompressedTail(data.size());
  const size_t lowerBound = data.size() > maxTail ? data.size() - maxTail : 0;
  // Return the largest position `p` of a match with the `offset` that ends at
  // or before `limit` (`p + matchLength <= limit`).
  auto findMatchEndingBefore =
      [data, lowerBound](size_t limit, size_t offset) -> std::optional<size_t> {
    const size_t smallest = std::max(lowerBound, offset);
    for (size_t end = limit; end >= smallest + matchLength; --end) {
      const size_t position = end - matchLength;
      if (std::memcmp(data.data() + position, data.data() + position - offset,
                      matchLength) == 0) {
        return position;
      }
    }
    return std::nullopt;
  };
  auto match1 = findMatchEndingBefore(data.size(), 1);
  if (!match1.has_value()) {
    return std::nullopt;
  }
  auto match4 = findMatchEndingBefore(match1.value(), 4);
  if (!match4.has_value()) {
    return std::nullopt;
  }
  auto match8 = findMatchEndingBefore(match4.value(), 8);
  if (!match8.has_value()) {
    return std::nullopt;
  }
  return FinalMatches{match8.value(), match4.value(), match1.value()};
}

// Compress `data` with a fresh ZSTD compressor and append the blocks to
// `out`. The last-block flag of the last block is set iff `isLast` is true.
// Return an error if ZSTD fails to compress.
Expected<void> appendCompressedBlocks(std::vector<char>& out,
                                      ql::span<const char> data,
                                      int compressionLevel,
                                      bool isLast = false) {
  // The buffer for the worst-case size of the frame is not zero-initialized,
  // so only the pages that are actually written are touched.
  std::vector<char, ad_utility::default_init_allocator<char>> frame(
      ZSTD_compressBound(data.size()));
  const size_t frameSize = ZSTD_compress(
      frame.data(), frame.size(), data.data(), data.size(), compressionLevel);
  if (ZSTD_isError(frameSize)) {
    return ZstdError{std::string{"ZSTD compression failed: "} +
                     ZSTD_getErrorName(frameSize)};
  }
  frame.resize(frameSize);
  // The frame was just written by ZSTD, so it has to be valid.
  auto frameBlocks = blocksOfFrame(frame);
  AD_CORRECTNESS_CHECK(frameBlocks.hasValue(),
                       [&frameBlocks] { return frameBlocks.error().message_; });
  auto [blocks, lastHeader] = frameBlocks.value();
  const size_t begin = out.size();
  out.insert(out.end(), blocks.begin(), blocks.end());
  AD_CORRECTNESS_CHECK(
      setLastBlockFlag(ql::span<char>{out}.subspan(begin), lastHeader, isLast)
          .hasValue());
  return {};
}

// Return a deterministic sequence of bytes that consists of random literals
// and runs with the periods 1, 4, and 8, such that its compression uses all
// three repeat offsets (see `checkEndsInStartState`).
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

// Return true iff the hand-written `tailBlocks`, which encode `data` from
// `matches.match8_` on, decode correctly and leave the decoder in the start
// state. For this purpose, the 8 bytes before `match8_` (which the first match
// refers to) are stored as a raw block, followed by the `tailBlocks`, followed
// by the blocks of fresh compressions of probes that use all three repeat
// offsets. This only takes time proportional to the size of the tail. Return
// an error if ZSTD fails to compress the probes.
Expected<bool> checkEndsInStartState(ql::span<const char> data,
                                     const FinalMatches& matches,
                                     ql::span<const char> tailBlocks) {
  auto expected = data.subspan(matches.match8_ - 8);
  for (uint32_t seed : {1u, 2u}) {
    std::string probe = makeProbe(seed);
    std::vector<char> frame;
    SingleSegmentFrame::appendHeader(frame, expected.size() + probe.size());
    appendRawBlocks(frame, expected.first(8));
    frame.insert(frame.end(), tailBlocks.begin(), tailBlocks.end());
    auto probeBlocks =
        appendCompressedBlocks(frame, probe, seed == 1 ? 1 : 3, true);
    if (!probeBlocks.hasValue()) {
      return std::move(probeBlocks).error();
    }
    std::vector<char> decompressed(expected.size() + probe.size());
    size_t size = ZSTD_decompress(decompressed.data(), decompressed.size(),
                                  frame.data(), frame.size());
    auto output = ql::span<const char>{decompressed};
    if (ZSTD_isError(size) || size != decompressed.size() ||
        !ql::ranges::equal(output.first(expected.size()), expected) ||
        !ql::ranges::equal(output.subspan(expected.size()), probe)) {
      return false;
    }
  }
  return true;
}

// Return a unit that stores `data` uncompressed in raw blocks. Raw blocks
// neither use nor change the repeat offsets or the entropy tables of the
// decoder, so the unit ends in the start state. Empty `data` is stored as a
// single empty raw block, because a unit needs at least one block.
CompressedUnit rawUnit(ql::span<const char> data) {
  CompressedUnit result{{}, true};
  if (data.empty()) {
    BlockHeader emptyBlock{false, BlockHeader::Type::Raw, 0};
    AD_CORRECTNESS_CHECK(emptyBlock.append(result.blocks_).hasValue());
  } else {
    appendRawBlocks(result.blocks_, data);
  }
  return result;
}

// Return true iff the `rawUnit` of data, whose ordinary compression has
// `compressedSize` bytes, is at most a little larger than this compression.
bool rawIsCheap(size_t rawSize, size_t compressedSize) {
  return rawSize <= compressedSize + compressedSize / 32 + 64;
}

}  // namespace

// _____________________________________________________________________________
Expected<CompressedUnit> compressIndependentUnit(ql::span<const char> data,
                                                 int compressionLevel,
                                                 bool endInStartState) {
  CompressedUnit regular;
  if (!endInStartState) {
    auto compressed =
        appendCompressedBlocks(regular.blocks_, data, compressionLevel);
    if (!compressed.hasValue()) {
      return std::move(compressed).error();
    }
    return regular;
  }
  if (auto matches = findFinalMatches(data); matches.has_value()) {
    const auto [match8, match4, match1] = matches.value();
    // Everything before the first match is compressed regularly, the rest is
    // written by hand (see `ZstdIndependentUnit.h`).
    CompressedUnit result;
    auto& blocks = result.blocks_;
    auto compressed =
        appendCompressedBlocks(blocks, data.first(match8), compressionLevel);
    if (!compressed.hasValue()) {
      return std::move(compressed).error();
    }
    const size_t tailBegin = blocks.size();
    // The offsets 8, 4, and 1 are always valid for a hand-written match.
    auto appendMatch = [&blocks](uint32_t offset) {
      AD_CORRECTNESS_CHECK(appendSingleMatchBlock(blocks, offset).hasValue());
    };
    appendMatch(8);
    appendRawBlocks(blocks, data.subspan(match8 + matchLength,
                                         match4 - (match8 + matchLength)));
    appendMatch(4);
    appendRawBlocks(blocks, data.subspan(match4 + matchLength,
                                         match1 - (match4 + matchLength)));
    appendMatch(1);
    appendRawBlocks(blocks, data.subspan(match1 + matchLength));
    auto endsInStartState = checkEndsInStartState(
        data, matches.value(), ql::span<const char>{blocks}.subspan(tailBegin));
    if (!endsInStartState.hasValue()) {
      return std::move(endsInStartState).error();
    }
    if (endsInStartState.value()) {
      result.endsInStartState_ = true;
      return result;
    }
    // Otherwise, the installed version of ZSTD decodes the hand-written
    // blocks differently than expected; continue as if there were no matches.
  }
  // Without the final matches, the data is stored uncompressed if this is
  // cheap (for example for short, empty, or random data), else it is
  // compressed regularly (and does not end in the start state).
  auto compressed =
      appendCompressedBlocks(regular.blocks_, data, compressionLevel);
  if (!compressed.hasValue()) {
    return std::move(compressed).error();
  }
  auto raw = rawUnit(data);
  if (rawIsCheap(raw.blocks_.size(), regular.blocks_.size())) {
    return raw;
  }
  return regular;
}

}  // namespace ad_utility::zstd

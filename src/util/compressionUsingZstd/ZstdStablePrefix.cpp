// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// The sequence API of ZSTD is experimental and has to be enabled before
// `zstd.h` is included for the first time. `ZSTD_generateSequences` is
// deprecated in ZSTD 1.5.7, but still the only way to obtain the sequences of a
// regular compression.
#define ZSTD_STATIC_LINKING_ONLY
#define ZSTD_DISABLE_DEPRECATE_WARNINGS
#include "util/compressionUsingZstd/ZstdStablePrefix.h"

#include <zstd.h>

#include <cstring>
#include <memory>
#include <random>
#include <string>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/compressionUsingZstd/ZstdFrameFormat.h"
#include "util/compressionUsingZstd/ZstdWrapper.h"

namespace ad_utility::zstd {

namespace {

// Return a deterministic sequence of bytes that consists of random literals
// and runs with the periods 1, 4, and 8, such that its compression uses all
// three repeat offsets (see `isStablePrefix`).
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

#if ZSTD_VERSION_NUMBER >= 10505
// The length of the three matches at the end of a stable prefix (the minimal
// match length of ZSTD).
constexpr size_t matchLength = 3;

// The positions of the three matches at the end of a stable prefix, see
// `findFinalMatches`.
struct FinalMatches {
  size_t match8_;
  size_t match4_;
  size_t match1_;
};

// Find the positions of three matches of length `matchLength` with the offsets
// 8, 4, and 1 (in this order) in `data`, with at least one literal before each
// of them, as late as possible. The repeat offsets after these three matches
// are `{1, 4, 8}`, no matter how the matches are encoded and what the repeat
// offsets were before (see the repeat offset rules of the ZSTD format). All
// following bytes are encoded as literals (without matches), so they are
// compressed worse, which is why they must not be too many. Return
// `std::nullopt` if no such matches exist (close enough to the end).
std::optional<FinalMatches> findFinalMatches(ql::span<const char> data) {
  // Return the largest position `< limit` of a valid match with the `offset`
  // that is preceded by at least one literal.
  auto findMatchBefore = [data](size_t limit,
                                size_t offset) -> std::optional<size_t> {
    for (size_t position = limit; position > offset + 1;) {
      --position;
      if (std::memcmp(data.data() + position, data.data() + position - offset,
                      matchLength) == 0) {
        return position;
      }
    }
    return std::nullopt;
  };
  const size_t size = data.size();
  if (size < matchLength) {
    return std::nullopt;
  }
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
  return FinalMatches{match8.value(), match4.value(), match1.value()};
}

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

// A ZSTD compression context with a fixed compression level for the sequence
// API.
class SequenceCompressor {
 private:
  std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context_{
      ZSTD_createCCtx(), &ZSTD_freeCCtx};
  int compressionLevel_;

 public:
  explicit SequenceCompressor(int compressionLevel)
      : compressionLevel_{compressionLevel} {
    AD_CORRECTNESS_CHECK(context_ != nullptr);
  }

  // Return the sequences of a regular compression of `data`, which end with a
  // block delimiter.
  std::vector<ZSTD_Sequence> generateSequences(ql::span<const char> data) {
    reset();
    std::vector<ZSTD_Sequence> sequences(ZSTD_sequenceBound(data.size()));
    size_t numSequences = ZstdWrapper::checkError(
        ZSTD_generateSequences(context_.get(), sequences.data(),
                               sequences.size(), data.data(), data.size()));
    sequences.resize(numSequences);
    AD_CORRECTNESS_CHECK(!sequences.empty() &&
                         sequences.back().matchLength == 0);
    return sequences;
  }

  // Compress `data` as the given `sequences` with explicit block delimiters
  // and return the frame, or `std::nullopt` if ZSTD rejects the sequences.
  std::optional<std::vector<char>> compressSequences(
      const std::vector<ZSTD_Sequence>& sequences, ql::span<const char> data) {
    reset();
    setParameter(ZSTD_c_blockDelimiters, ZSTD_sf_explicitBlockDelimiters);
    setParameter(ZSTD_c_validateSequences, 1);
    // The matches at the end have the minimal length.
    setParameter(ZSTD_c_minMatch, matchLength);
    std::vector<char> frame(ZSTD_compressBound(data.size()));
    size_t frameSize = ZSTD_compressSequences(
        context_.get(), frame.data(), frame.size(), sequences.data(),
        sequences.size(), data.data(), data.size());
    if (ZSTD_isError(frameSize)) {
      return std::nullopt;
    }
    frame.resize(frameSize);
    return frame;
  }

 private:
  void setParameter(ZSTD_cParameter parameter, int value) {
    ZstdWrapper::checkError(
        ZSTD_CCtx_setParameter(context_.get(), parameter, value));
  }

  void reset() {
    ZSTD_CCtx_reset(context_.get(), ZSTD_reset_session_and_parameters);
    setParameter(ZSTD_c_compressionLevel, compressionLevel_);
  }
};

// The sequences of a regular compression of the part of a stable prefix that
// precedes its three final matches.
struct HeadSequences {
  // The sequences of complete blocks, each block ends with a delimiter.
  std::vector<ZSTD_Sequence> completeBlocks_;
  // The sequences of the last (incomplete) block, without delimiters.
  std::vector<ZSTD_Sequence> lastBlock_;
  // The literals at the end of the last block, which become literals of the
  // first final match.
  size_t trailingLiterals_ = 0;
};

// Return the sequences of a regular compression of `head`. Only the last
// `lastSliceSize` bytes are compressed separately (and are not split into
// blocks), so that the rest consists of complete blocks.
//
// NOTE: A block that contains one of the three final matches must be stored as
// a compressed block. A block that is stored uncompressed would lose its
// matches, and thereby their effect on the repeat offsets, which
// `isStablePrefix` detects.
HeadSequences sequencesOfHead(SequenceCompressor& compressor,
                              ql::span<const char> head) {
  constexpr size_t lastSliceSize = 64 * 1024;
  const size_t lastStart =
      head.size() > lastSliceSize ? head.size() - lastSliceSize : 0;
  HeadSequences result;
  if (lastStart > 0) {
    result.completeBlocks_ =
        compressor.generateSequences(head.first(lastStart));
  }
  // The literals of a delimiter become literals of the next sequence.
  size_t literals = 0;
  for (auto sequence : compressor.generateSequences(head.subspan(lastStart))) {
    sequence.litLength += static_cast<unsigned>(literals);
    literals = 0;
    if (sequence.matchLength == 0) {
      literals = sequence.litLength;
    } else {
      result.lastBlock_.push_back(sequence);
    }
  }
  result.trailingLiterals_ = literals;
  return result;
}
#endif

}  // namespace

// _____________________________________________________________________________
bool stablePrefixEncodingIsSupported() { return ZSTD_VERSION_NUMBER >= 10505; }

// _____________________________________________________________________________
std::optional<std::vector<char>> encodeStablePrefix(
    [[maybe_unused]] ql::span<const char> prefix,
    [[maybe_unused]] int compressionLevel) {
#if ZSTD_VERSION_NUMBER >= 10505
  // 1. Find the three final matches.
  auto matches = findFinalMatches(prefix);
  if (!matches.has_value()) {
    return std::nullopt;
  }
  const auto [match8, match4, match1] = matches.value();

  // 2. The sequences of a regular compression of everything before the final
  // matches (the byte right before `match8` is a literal), followed by the
  // three matches and the trailing literals.
  SequenceCompressor compressor{compressionLevel};
  auto head = sequencesOfHead(compressor, prefix.first(match8 - 1));
  auto addMatch = [&head](unsigned offset, size_t literalLength) {
    ZSTD_Sequence sequence{};
    sequence.offset = offset;
    sequence.litLength = static_cast<unsigned>(literalLength);
    sequence.matchLength = matchLength;
    head.lastBlock_.push_back(sequence);
  };
  addMatch(8, head.trailingLiterals_ + 1);
  addMatch(4, match4 - (match8 + matchLength));
  addMatch(1, match1 - (match4 + matchLength));
  auto sequences = std::move(head.completeBlocks_);
  appendInBlocks(sequences, std::move(head.lastBlock_),
                 prefix.size() - (match1 + matchLength), 0);

  // 3. Compress the sequences.
  auto frame = compressor.compressSequences(sequences, prefix);
  if (!frame.has_value()) {
    return std::nullopt;
  }

  // 4. Extract the blocks, clear the last-block flag of the last one (the
  // blocks of the remainder follow), and check the result.
  auto [blocks, lastHeader] = blocksOfFrame(frame.value());
  std::vector<char> result{blocks.begin(), blocks.end()};
  clearLastBlockFlag(result, lastHeader);
  if (!isStablePrefix(prefix, result)) {
    return std::nullopt;
  }
  return result;
#else
  return std::nullopt;
#endif
}

// _____________________________________________________________________________
bool isStablePrefix(ql::span<const char> prefix,
                    ql::span<const char> prefixBlocks) {
  for (uint32_t seed : {1u, 2u}) {
    std::string probe = makeProbe(seed);
    std::vector<char> frame;
    appendSingleSegmentFrameHeader(frame, prefix.size() + probe.size());
    frame.insert(frame.end(), prefixBlocks.begin(), prefixBlocks.end());
    auto probeFrame =
        ZstdWrapper::compressToUninitializedBuffer(probe, seed == 1 ? 1 : 3);
    auto probeBlocks = blocksOfFrame(probeFrame).blocks_;
    frame.insert(frame.end(), probeBlocks.begin(), probeBlocks.end());
    std::vector<char> decompressed(prefix.size() + probe.size());
    size_t size = ZSTD_decompress(decompressed.data(), decompressed.size(),
                                  frame.data(), frame.size());
    if (ZSTD_isError(size) || size != decompressed.size() ||
        !ql::ranges::equal(
            ql::span<const char>{decompressed}.first(prefix.size()), prefix) ||
        !ql::ranges::equal(
            ql::span<const char>{decompressed}.subspan(prefix.size()), probe)) {
      return false;
    }
  }
  return true;
}

}  // namespace ad_utility::zstd

// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <absl/numeric/bits.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "backports/span.h"
#include "global/Id.h"
#include "util/BitUtils.h"
#include "util/Exception.h"

namespace ql::index::compression {

// Frame-of-reference (FOR) bit packing of up to `BLOCK_SIZE` `Id`s: every `Id`
// is stored as its offset from the smallest `Id` of the block (compared by
// their bits), using as many bits per value as the largest offset needs. For
// sorted `Id`s with small gaps this needs far fewer than 64 bits per value;
// for any input the round trip is exact. There are no exceptions or patches
// (the "P" of PFOR), so a single outlier widens the whole block.
class PforDeltaBitPacking {
 public:
  static constexpr size_t BLOCK_SIZE = 64;

  struct CompressedBlock {
    Id baseValue_{Id::makeUndefined()};
    // Number of bits per value, in [0, 64]. 0 means all values are equal.
    uint8_t bitWidth_ = 0;
    // Number of packed values, in [1, BLOCK_SIZE].
    uint8_t numValues_ = 0;
    std::vector<uint64_t> packedWords_;
  };

 private:
  // Position of the first bit of value `i` in the packed words.
  struct BitPosition {
    size_t word_;
    size_t offset_;
  };
  static BitPosition bitPosition(size_t i, uint8_t bitWidth) {
    size_t bit = i * bitWidth;
    return {bit / 64, bit % 64};
  }

 public:
  // Compress `inputIds`, which must contain between 1 and `BLOCK_SIZE` `Id`s.
  static CompressedBlock compressBlock(ql::span<const Id> inputIds) {
    AD_CONTRACT_CHECK(!inputIds.empty() && inputIds.size() <= BLOCK_SIZE,
                      "`compressBlock` needs between 1 and 64 `Id`s");
    uint64_t base = std::numeric_limits<uint64_t>::max();
    uint64_t max = 0;
    for (Id id : inputIds) {
      base = std::min(base, id.getBits());
      max = std::max(max, id.getBits());
    }
    CompressedBlock block;
    block.baseValue_ = Id::fromBits(base);
    block.numValues_ = static_cast<uint8_t>(inputIds.size());
    block.bitWidth_ = static_cast<uint8_t>(absl::bit_width(max - base));
    const uint8_t width = block.bitWidth_;
    block.packedWords_.resize((inputIds.size() * width + 63) / 64, 0);
    if (width == 0) {
      return block;
    }

    for (size_t i = 0; i < inputIds.size(); ++i) {
      uint64_t offset = inputIds[i].getBits() - base;
      auto [word, shift] = bitPosition(i, width);
      block.packedWords_[word] |= offset << shift;
      // The value continues in the next word.
      if (shift + width > 64) {
        block.packedWords_[word + 1] |= offset >> (64 - shift);
      }
    }
    return block;
  }

  // Write the `block.numValues_` `Id`s of `block` to the start of `outputIds`,
  // which must be at least that large.
  static void decompressBlock(const CompressedBlock& block,
                              ql::span<Id> outputIds) {
    const size_t numValues = block.numValues_;
    const uint8_t width = block.bitWidth_;
    AD_CONTRACT_CHECK(outputIds.size() >= numValues,
                      "Output too small for the compressed block");
    AD_CONTRACT_CHECK(
        numValues >= 1 && numValues <= BLOCK_SIZE && width <= 64 &&
            block.packedWords_.size() == (numValues * width + 63) / 64,
        "Invalid `CompressedBlock`");
    const uint64_t base = block.baseValue_.getBits();
    const uint64_t mask = ad_utility::bitMaskForLowerBits(width);
    for (size_t i = 0; i < numValues; ++i) {
      uint64_t offset = 0;
      if (width > 0) {
        auto [word, shift] = bitPosition(i, width);
        offset = block.packedWords_[word] >> shift;
        if (shift + width > 64) {
          offset |= block.packedWords_[word + 1] << (64 - shift);
        }
      }
      outputIds[i] = Id::fromBits(base + (offset & mask));
    }
  }
};

}  // namespace ql::index::compression

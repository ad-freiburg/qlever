// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/compressionUsingZstd/ZstdHandWrittenBlocks.h"

#include <absl/numeric/bits.h>
#include <zstd.h>

#include <algorithm>

#include "util/Exception.h"
#include "util/compressionUsingZstd/ZstdFrameFormat.h"

namespace ad_utility::zstd {

// _____________________________________________________________________________
void appendRawBlocks(std::vector<char>& out, ql::span<const char> literals) {
  while (!literals.empty()) {
    auto chunk =
        literals.first(std::min<size_t>(literals.size(), ZSTD_BLOCKSIZE_MAX));
    BlockHeader{false, BlockHeader::Type::Raw,
                static_cast<uint32_t>(chunk.size())}
        .append(out);
    out.insert(out.end(), chunk.begin(), chunk.end());
    literals = literals.subspan(chunk.size());
  }
}

// _____________________________________________________________________________
void appendSingleMatchBlock(std::vector<char>& out, uint32_t offset) {
  AD_CONTRACT_CHECK(offset >= 1 && offset < (1u << 30));
  // An explicit offset is stored as the "offset value" `offset + 3` (the
  // values 1 to 3 denote repeat offsets). The offset code is the number of
  // extra bits, and the value is `(1 << code) + extraBits`.
  const uint32_t offsetValue = offset + 3;
  const uint32_t offsetCode = absl::bit_width(offsetValue) - 1;
  const uint32_t extraBits = offsetValue - (1u << offsetCode);

  std::vector<char> content;
  // The literals section: a raw literals block (type 0) with a 1-byte header
  // and a size of 0.
  content.push_back(0);
  // The sequences section: one sequence, followed by the compression modes of
  // the literal lengths, the offsets, and the match lengths (in the bits 7-6,
  // 5-4, and 3-2), all of which are "RLE" (mode 1).
  content.push_back(1);
  content.push_back(static_cast<char>(1 << 6 | 1 << 4 | 1 << 2));
  // The single symbol of each of the three RLE tables (in the same order): the
  // literal length 0 (code 0), the offset code, and the match length 3 (code
  // 0, match length codes start at 3).
  content.push_back(0);
  content.push_back(static_cast<char>(offsetCode));
  content.push_back(0);
  // The bitstream, which the decoder reads backwards, starting at the highest
  // set bit (the end mark): the initial states of the three tables (0 bits
  // each for RLE), then the extra bits of the offset, the match length (0
  // bits), and the literal length (0 bits). The encoder therefore writes, from
  // the lowest bit upwards: the extra bits of the offset, then the end mark.
  uint64_t bitstream = extraBits | (uint64_t{1} << offsetCode);
  const size_t numBytes = (offsetCode + 1 + 7) / 8;
  for (size_t i = 0; i < numBytes; ++i) {
    content.push_back(static_cast<char>((bitstream >> (8 * i)) & 0xFF));
  }

  BlockHeader{false, BlockHeader::Type::Compressed,
              static_cast<uint32_t>(content.size())}
      .append(out);
  out.insert(out.end(), content.begin(), content.end());
}

}  // namespace ad_utility::zstd

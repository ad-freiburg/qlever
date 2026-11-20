// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDLITTLEENDIAN_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDLITTLEENDIAN_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "util/Exception.h"

// Read and write the little-endian integers of the ZSTD frame format (see
// `ZstdFrameHeader.h`): the magic numbers, the content size, the block headers
// (3 bytes), and the sizes of skippable frames. The bytes are assembled and
// taken apart explicitly, so the result does not depend on the byte order of
// the platform.
namespace ad_utility::zstd::detail {

// Return the unsigned integer that is stored in the first `NumBytes` bytes of
// `bytes` in little-endian order (the least significant byte first). `bytes`
// has to contain at least `NumBytes` bytes.
template <size_t NumBytes>
uint64_t readLittleEndian(ql::span<const char> bytes) {
  static_assert(NumBytes >= 1 && NumBytes <= 8);
  AD_CORRECTNESS_CHECK(bytes.size() >= NumBytes);
  uint64_t result = 0;
  for (size_t i = 0; i < NumBytes; ++i) {
    // Convert via `uint8_t`, because `char` may be signed, and a plain
    // conversion would sign-extend the bytes >= 128.
    result |= uint64_t{static_cast<uint8_t>(bytes[i])} << (8 * i);
  }
  return result;
}

// Append the lowest `NumBytes` bytes of `value` to `out` in little-endian order
// (the least significant byte first). `value` has to fit into `NumBytes`
// bytes.
template <size_t NumBytes>
void appendLittleEndian(std::vector<char>& out, uint64_t value) {
  static_assert(NumBytes >= 1 && NumBytes <= 8);
  if constexpr (NumBytes < 8) {
    AD_CORRECTNESS_CHECK(value >> (8 * NumBytes) == 0);
  }
  for (size_t i = 0; i < NumBytes; ++i) {
    out.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
  }
}

}  // namespace ad_utility::zstd::detail

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDLITTLEENDIAN_H

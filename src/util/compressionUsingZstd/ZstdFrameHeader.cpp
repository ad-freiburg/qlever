// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/compressionUsingZstd/ZstdFrameHeader.h"

#include <zstd.h>

#include <array>
#include <cstdint>
#include <string>

#include "util/compressionUsingZstd/ZstdLittleEndian.h"

namespace ad_utility::zstd {

// _____________________________________________________________________________
Expected<FrameHeader> parseFrameHeader(ql::span<const char> bytes) {
  using detail::readLittleEndian;
  // The header starts with the magic number (4 bytes) and the frame header
  // descriptor (1 byte), which are always present.
  if (bytes.size() < 5) {
    return ZstdError{
        "A ZSTD frame header must have at least 5 bytes, but only " +
        std::to_string(bytes.size()) + " bytes are given"};
  }
  if (readLittleEndian<4>(bytes) != ZSTD_MAGICNUMBER) {
    return ZstdError{
        "The data does not start with the magic number of a ZSTD frame"};
  }
  // The bits of the frame header descriptor (bit 7 is the most significant
  // bit):
  //
  //   bits 7-6: content size flag (encodes the size of the content size).
  //   bit  5  : single segment flag.
  //   bit  4  : unused, may have any value.
  //   bit  3  : reserved, must be zero.
  //   bit  2  : checksum flag.
  //   bits 1-0: dictionary ID flag (encodes the size of the dictionary ID).
  const uint64_t descriptor = readLittleEndian<1>(bytes.subspan(4));
  const uint64_t contentSizeFlag = descriptor >> 6;
  const bool singleSegment = (descriptor >> 5) & 1;
  const bool reservedBit = (descriptor >> 3) & 1;
  const bool hasChecksum = (descriptor >> 2) & 1;
  const uint64_t dictionaryIdFlag = descriptor & 3;
  if (reservedBit) {
    return ZstdError{"The reserved bit of a ZSTD frame header is set"};
  }
  // The two-bit flags encode the sizes of the dictionary ID and the content
  // size fields, which follow the descriptor (in this order, after the
  // optional window descriptor).
  constexpr std::array<size_t, 4> dictionaryIdSizes{0, 1, 2, 4};
  constexpr std::array<size_t, 4> contentSizeSizes{0, 2, 4, 8};
  // A single segment frame needs the content size (it is the window size), so
  // with a content size flag of 0 it still stores the content size in a single
  // byte.
  const size_t contentSizeSize = contentSizeFlag == 0 && singleSegment
                                     ? 1
                                     : contentSizeSizes[contentSizeFlag];
  // The window descriptor (1 byte) is omitted in a single segment frame,
  // because its window is the whole content.
  const size_t headerSize = 5 + (singleSegment ? 0 : 1) +
                            dictionaryIdSizes[dictionaryIdFlag] +
                            contentSizeSize;
  if (bytes.size() < headerSize) {
    return ZstdError{"The ZSTD frame header is truncated: it needs " +
                     std::to_string(headerSize) + " bytes, but only " +
                     std::to_string(bytes.size()) + " bytes are given"};
  }
  return FrameHeader{headerSize, hasChecksum};
}

}  // namespace ad_utility::zstd

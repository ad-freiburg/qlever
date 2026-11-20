// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/compressionUsingZstd/ZstdSkippableFrame.h"

#include <zstd.h>

#include <limits>
#include <string>

#include "util/compressionUsingZstd/ZstdLittleEndian.h"

namespace ad_utility::zstd {

namespace {
// The size of the trailing total frame size.
constexpr size_t trailingSizeSize = 4;

// Return the magic number of a skippable frame with the `magicVariant`.
uint32_t magicNumber(SkippableMagicVariant magicVariant) {
  return ZSTD_MAGIC_SKIPPABLE_START + magicVariant.value();
}
}  // namespace

// _____________________________________________________________________________
Expected<void> appendTrailingSkippableFrame(std::vector<char>& out,
                                            SkippableMagicVariant magicVariant,
                                            ql::span<const char> content) {
  using detail::appendLittleEndian;
  // The total size of the frame: the header, the `content`, and the trailing
  // total size. It is stored as a 4-byte integer, so it has to fit into one.
  const uint64_t frameSize =
      skippableFrameHeaderSize + uint64_t{content.size()} + trailingSizeSize;
  if (frameSize > std::numeric_limits<uint32_t>::max()) {
    return ZstdError{"The content of a skippable ZSTD frame is too large (" +
                     std::to_string(content.size()) + " bytes)"};
  }
  // Write the four fields from the diagram in `ZstdSkippableFrame.h`. The
  // content size that ZSTD sees includes the trailing total size.
  appendLittleEndian<4>(out, magicNumber(magicVariant));
  appendLittleEndian<4>(out, frameSize - skippableFrameHeaderSize);
  out.insert(out.end(), content.begin(), content.end());
  appendLittleEndian<trailingSizeSize>(out, frameSize);
  return {};
}

// _____________________________________________________________________________
Expected<ql::span<const char>> readTrailingSkippableFrame(
    ql::span<const char> bytes, SkippableMagicVariant magicVariant) {
  using detail::readLittleEndian;
  // The smallest possible frame has an empty `content`.
  constexpr size_t minFrameSize = skippableFrameHeaderSize + trailingSizeSize;
  if (bytes.size() < minFrameSize) {
    return ZstdError{"The data is too short to end with a skippable frame"};
  }
  // The last 4 bytes are the total size of the frame, which tells us where the
  // frame starts. Arbitrary data may end in arbitrary 4 bytes, so the size has
  // to be checked before it is used.
  const uint64_t frameSize =
      readLittleEndian<trailingSizeSize>(bytes.last(trailingSizeSize));
  if (frameSize < minFrameSize || frameSize > bytes.size()) {
    return ZstdError{
        "The data does not end with a skippable frame: invalid "
        "trailing frame size " +
        std::to_string(frameSize)};
  }
  // The frame starts with the expected magic number and with a content size
  // that is consistent with the total size. Both together make it very
  // unlikely that data which does not end in such a frame is accepted.
  auto frame = bytes.last(frameSize);
  if (readLittleEndian<4>(frame) != magicNumber(magicVariant)) {
    return ZstdError{
        "The data does not end with a skippable frame with the magic variant " +
        std::to_string(magicVariant.value())};
  }
  if (readLittleEndian<4>(frame.subspan(4)) !=
      frameSize - skippableFrameHeaderSize) {
    return ZstdError{
        "The content size of a skippable frame does not match its total size"};
  }
  // The `content` is everything between the header and the trailing size.
  return frame.subspan(skippableFrameHeaderSize, frameSize - minFrameSize);
}

}  // namespace ad_utility::zstd

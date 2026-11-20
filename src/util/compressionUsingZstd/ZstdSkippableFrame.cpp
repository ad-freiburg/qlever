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

#include "util/Exception.h"
#include "util/compressionUsingZstd/ZstdLittleEndian.h"

namespace ad_utility::zstd {

// _____________________________________________________________________________
void appendTrailingSkippableFrame(std::vector<char>& out, uint32_t magicVariant,
                                  ql::span<const char> content) {
  AD_CONTRACT_CHECK(magicVariant < 16);
  // The total size of the frame: the header, the `content`, and the trailing
  // total size. It is stored as a 4-byte integer, so it has to fit into one.
  const uint64_t frameSize =
      skippableFrameHeaderSize + content.size() + sizeof(uint32_t);
  AD_CONTRACT_CHECK(frameSize <= std::numeric_limits<uint32_t>::max());
  // Write the four fields from the diagram in `ZstdSkippableFrame.h`. The
  // content size that ZSTD sees includes the trailing total size.
  auto serializer = detail::appendingSerializer(out);
  serializer << static_cast<uint32_t>(ZSTD_MAGIC_SKIPPABLE_START +
                                      magicVariant);
  serializer << static_cast<uint32_t>(frameSize - skippableFrameHeaderSize);
  out.insert(out.end(), content.begin(), content.end());
  serializer << static_cast<uint32_t>(frameSize);
}

// _____________________________________________________________________________
std::optional<ql::span<const char>> readTrailingSkippableFrame(
    ql::span<const char> bytes, uint32_t magicVariant) {
  using detail::readValue;
  // The smallest possible frame has an empty `content`.
  constexpr size_t minFrameSize = skippableFrameHeaderSize + sizeof(uint32_t);
  if (bytes.size() < minFrameSize) {
    return std::nullopt;
  }
  // The last 4 bytes are the total size of the frame, which tells us where the
  // frame starts. Arbitrary data may end in arbitrary 4 bytes, so the size has
  // to be checked before it is used.
  const size_t frameSize = readValue<uint32_t>(bytes.last(sizeof(uint32_t)));
  if (frameSize < minFrameSize || frameSize > bytes.size()) {
    return std::nullopt;
  }
  // The frame starts with the expected magic number and with a content size
  // that is consistent with the total size. Both together make it very
  // unlikely that data which does not end in such a frame is accepted.
  auto frame = bytes.last(frameSize);
  if (readValue<uint32_t>(frame) != ZSTD_MAGIC_SKIPPABLE_START + magicVariant ||
      readValue<uint32_t>(frame.subspan(sizeof(uint32_t))) !=
          frameSize - skippableFrameHeaderSize) {
    return std::nullopt;
  }
  // The `content` is everything between the header and the trailing size.
  return frame.subspan(skippableFrameHeaderSize, frameSize - minFrameSize);
}

}  // namespace ad_utility::zstd

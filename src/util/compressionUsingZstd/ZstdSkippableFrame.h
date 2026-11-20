// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDSKIPPABLEFRAME_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDSKIPPABLEFRAME_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "backports/span.h"
#include "util/compressionUsingZstd/ZstdExpected.h"

// Helpers for SKIPPABLE FRAMES, which every ZSTD decompressor ignores, and
// which can therefore store metadata next to ZSTD-compressed data. For the
// general terminology see the comment at the top of `ZstdFrameHeader.h`.
//
// A skippable frame consists of a magic number (one of the 16 values
// `ZSTD_MAGIC_SKIPPABLE_START + magicVariant` with `magicVariant` in
// `[0, 16)`), the size of its content (a 4-byte little-endian integer), and the
// content, which ZSTD does not interpret.
//
// A TRAILING skippable frame, as written by `appendTrailingSkippableFrame`, is
// a skippable frame whose content ends with the size of the complete skippable
// frame (again a 4-byte little-endian integer). This makes it possible to find
// the frame from the END of the data, without knowing where it starts:
//
//   ┌────────────────────┬─────────────────┬───────────┬───────────────────┐
//   │ magic number       │ content size    │ `content` │ total frame size  │
//   │ (4 bytes)          │ (4 bytes)       │           │ (4 bytes)         │
//   └────────────────────┴─────────────────┴───────────┴───────────────────┘
//   ╰──── skippable frame header ─────────╯╰─── ZSTD's content size ───────╯
//   ╰────────────────────────── total frame size ─────────────────────────╯
//
// ZSTD's content size of the skippable frame includes the trailing 4 bytes,
// such that ZSTD skips them together with the frame. None of the functions
// throws for invalid input; errors are reported via `Expected` (see
// `ZstdExpected.h`).
namespace ad_utility::zstd {

// The size of the header of a skippable frame (its magic number and the size
// of its content).
constexpr size_t skippableFrameHeaderSize = 8;

// One of the 16 magic variants of a skippable frame (see above). The value is
// checked at compile time, so an invalid magic variant cannot be constructed.
class SkippableMagicVariant {
 private:
  uint32_t value_;

 public:
  // Return the magic variant `Value`, which has to be in `[0, 16)`.
  template <uint32_t Value>
  static constexpr SkippableMagicVariant make() {
    static_assert(Value < 16, "There are only 16 magic variants");
    return SkippableMagicVariant{Value};
  }

  // Return the magic variant as an integer in `[0, 16)`.
  constexpr uint32_t value() const { return value_; }

 private:
  constexpr explicit SkippableMagicVariant(uint32_t value) : value_{value} {}
};

// Append a trailing skippable frame with the `magicVariant` and the `content`
// to `out` (see above). Return an error (and append nothing) if the frame
// would be larger than 4 GiB, which is the limit of the format.
Expected<void> appendTrailingSkippableFrame(std::vector<char>& out,
                                            SkippableMagicVariant magicVariant,
                                            ql::span<const char> content);

// If `bytes` ends with a skippable frame that was written by
// `appendTrailingSkippableFrame` with the `magicVariant`, then return its
// `content`, else an error.
Expected<ql::span<const char>> readTrailingSkippableFrame(
    ql::span<const char> bytes, SkippableMagicVariant magicVariant);

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDSKIPPABLEFRAME_H

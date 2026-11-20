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
#include <optional>
#include <vector>

#include "backports/span.h"

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
// such that ZSTD skips them together with the frame.
namespace ad_utility::zstd {

// The size of the header of a skippable frame (its magic number and the size
// of its content).
constexpr size_t skippableFrameHeaderSize = 8;

// Append a trailing skippable frame with the `magicVariant` (in `[0, 16)`) and
// the `content` to `out` (see above).
void appendTrailingSkippableFrame(std::vector<char>& out, uint32_t magicVariant,
                                  ql::span<const char> content);

// If `bytes` ends with a skippable frame that was written by
// `appendTrailingSkippableFrame` with the `magicVariant`, then return its
// `content`, else `std::nullopt`. Do not throw for any input.
std::optional<ql::span<const char>> readTrailingSkippableFrame(
    ql::span<const char> bytes, uint32_t magicVariant);

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_ZSTDSKIPPABLEFRAME_H

// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_SEGMENTEDZSTDFRAME_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_SEGMENTEDZSTDFRAME_H

#include <array>
#include <cstdint>
#include <optional>
#include <vector>

#include "backports/span.h"
#include "util/UninitializedAllocator.h"
#include "util/serializer/SerializeArrayOrTuple.h"
#include "util/serializer/Serializer.h"

// A SEGMENTED ZSTD FRAME is an ordinary ZSTD frame (which every ZSTD
// decompressor reads) whose content is divided into SEGMENTS, followed by a
// skippable frame (which every ZSTD decompressor ignores) with a
// `SegmentIndex`:
//
//   ┌────────┬────────────────┬────────────────┬─────┬──────────────────────┐
//   │ frame  │ unit 0         │ unit 1         │ ... │ skippable frame with │
//   │ header │ (segments 0,1) │ (segment 2)    │     │ the `SegmentIndex`   │
//   └────────┴────────────────┴────────────────┴─────┴──────────────────────┘
//
// The segments are chosen by the writer, typically along the natural sections
// of the data. Their compressed blocks form INDEPENDENT UNITS (see
// `ZstdIndependentUnit.h`): each unit can be decompressed on its own, and the
// compressed blocks of a unit can be copied verbatim into another segmented
// frame. Normally each segment is a unit of its own. A segment is merged with
// the following segment(s) into one unit only if its compression cannot end in
// the start state of the decoder (which is rare, see `ZstdIndependentUnit.h`;
// short, empty, and random segments are stored uncompressed instead). The index
// stores the size and the SHA-256 hash of the content of each segment, so it
// can be determined without decompression whether a unit of one frame can be
// reused for another frame with partially the same content.
//
// Everything only uses the stable API of ZSTD that exists in all supported
// versions.
namespace ad_utility::zstd {

// The SHA-256 hash of the content of a segment.
using ContentHash = std::array<char, 32>;

// Return the `ContentHash` of `content`.
ContentHash hashOfContent(ql::span<const char> content);

// A segment of a segmented frame (see above).
struct SegmentInfo {
  // The size of the (decompressed) content of the segment.
  uint64_t size_ = 0;
  ContentHash hash_{};

  bool operator==(const SegmentInfo& other) const {
    return size_ == other.size_ && hash_ == other.hash_;
  }
  bool operator!=(const SegmentInfo& other) const { return !(*this == other); }
};

// The serialization of a `SegmentInfo` (40 bytes).
AD_SERIALIZE_FUNCTION(SegmentInfo) {
  serializer | arg.size_;
  serializer | arg.hash_;
}

// An independent unit of a segmented frame (see above).
struct UnitInfo {
  // The number of segments of the unit. The units consist of consecutive
  // segments, in order.
  uint64_t numSegments_ = 0;
  // The size of the compressed blocks of the unit. The blocks of the units
  // follow each other directly after the frame header.
  uint64_t compressedSize_ = 0;
  // True iff the decoder is in the start state after the unit, so that any
  // unit may follow it (see `ZstdIndependentUnit.h`). This can only be false
  // for the last unit of a frame (if its content does not allow it).
  bool endsInStartState_ = false;
};

// The index of a segmented frame.
struct SegmentIndex {
  std::vector<SegmentInfo> segments_;
  std::vector<UnitInfo> units_;
};

// A buffer whose bytes are not zero-initialized on allocation.
using UninitializedBuffer =
    std::vector<char, ad_utility::default_init_allocator<char>>;

// A view of a segmented frame together with its parsed and validated index.
class SegmentedFrame {
 private:
  ql::span<const char> compressed_;
  SegmentIndex index_;
  // The decompressed offset at which each segment begins, followed by the
  // total size of the content.
  std::vector<uint64_t> segmentBegins_;
  // The offset in `compressed_` at which the blocks of each unit begin,
  // followed by the end of the blocks of the last unit.
  std::vector<uint64_t> unitBegins_;
  // The index of the first segment of each unit, followed by the number of
  // segments.
  std::vector<size_t> unitFirstSegments_;
  // The offset of the header of the last block of each unit (relative to the
  // start of the unit).
  std::vector<size_t> unitLastBlockHeaders_;

 public:
  // Parse the index of the segmented frame `compressed` (which must stay
  // valid as long as the result is used) and check that it is consistent with
  // the frame. Return `std::nullopt` if `compressed` is not a valid segmented
  // frame (for example, an ordinary ZSTD frame without an index). Do not throw
  // for any input.
  static std::optional<SegmentedFrame> read(ql::span<const char> compressed);

  const SegmentIndex& index() const { return index_; }
  size_t numSegments() const { return index_.segments_.size(); }
  size_t numUnits() const { return index_.units_.size(); }
  const SegmentInfo& segment(size_t i) const { return index_.segments_.at(i); }
  const UnitInfo& unit(size_t u) const { return index_.units_.at(u); }

  // The decompressed offset of segment `i` (or the total size of the content
  // for `i == numSegments()`).
  uint64_t segmentBegin(size_t i) const { return segmentBegins_.at(i); }

  // The index of the first segment of unit `u` (or `numSegments()` for
  // `u == numUnits()`).
  size_t firstSegmentOfUnit(size_t u) const { return unitFirstSegments_.at(u); }

  // The index of the unit that contains segment `i`.
  size_t unitOfSegment(size_t i) const;

  // The compressed blocks of unit `u` (with the last-block flag as it is in the
  // frame), and the offset of the header of its last block within them.
  ql::span<const char> unitBlocks(size_t u) const;
  size_t unitLastBlockHeader(size_t u) const {
    return unitLastBlockHeaders_.at(u);
  }

  // Decompress unit `u` (only that unit). Return `std::nullopt` if its blocks
  // are corrupted. Do not throw, except if the result cannot be allocated.
  std::optional<UninitializedBuffer> decompressUnit(size_t u) const;
};

// Build a segmented frame segment by segment, either from the content of a
// segment (which is compressed) or by copying a unit of another segmented
// frame (which is not decompressed).
class SegmentedFrameBuilder {
 private:
  int compressionLevel_;
  // The frame header (whose content size is written by `finish`), followed by
  // the blocks of the units so far.
  std::vector<char> frame_;
  SegmentIndex index_;
  uint64_t contentSize_ = 0;
  // The offset in `frame_` of the header of the last block so far.
  std::optional<size_t> lastBlockHeader_;
  // False after a unit was copied that does not end in the start state.
  bool canAppend_ = true;
  // The segments that were added, but are not compressed yet: The most recent
  // segment is only compressed when the next segment is added (or by
  // `finish`), because only the last unit of a frame may fail to end in the
  // start state. If a unit cannot end in the start state, the following
  // segments are added to the same unit.
  std::vector<ql::span<const char>> pendingParts_;
  std::vector<SegmentInfo> pendingSegments_;
  // The storage for the pending parts that are owned by the builder (see
  // `copyUnit`).
  std::vector<UninitializedBuffer> ownedParts_;

 public:
  // The fresh units are compressed with the given ZSTD `compressionLevel`.
  explicit SegmentedFrameBuilder(int compressionLevel);

  // Append a segment with the given `content`, which has to stay valid until
  // `finish` is called. If the `hash` of the content is known, it can be
  // passed to avoid computing it again.
  void addSegment(ql::span<const char> content,
                  std::optional<ContentHash> hash = std::nullopt);

  // Append the segments of unit `u` of the `base` frame by copying its
  // compressed blocks. If that unit does not end in the start state, then
  // nothing can be appended afterwards. In the rare case that the previous
  // segment cannot end in the start state, the unit is decompressed instead and
  // added to the unit of the previous segment. Throw if that decompression
  // fails.
  void copyUnit(const SegmentedFrame& base, size_t u);

  // Return the complete segmented frame.
  std::vector<char> finish() &&;

 private:
  // Compress the pending segments as one unit that ends in the start state.
  // If that is not possible, then nothing happens and false is returned,
  // unless `isLast` is true, in which case the unit is compressed regularly.
  bool tryToCompressPending(bool isLast);

  // Append the `blocks` of a unit, the header of whose last block is at
  // `lastBlockHeader`, with the given `info` and `segments`.
  void appendUnit(ql::span<const char> blocks, size_t lastBlockHeader,
                  UnitInfo info, ql::span<const SegmentInfo> segments);
};

// Compress `data` as a segmented frame with the ZSTD `compressionLevel`. The
// segments have the given `segmentSizes`, whose sum must be `data.size()`. If a
// `reusableBase` frame is given, then each of its units whose segments have the
// same content as consecutive segments of `data` is copied instead of
// compressing these segments again (a unit that does not end in the start
// state only at the end of the new frame). The result is deterministic for the
// same version of ZSTD, and copying a unit yields the same result as
// compressing it again.
std::vector<char> compressSegmented(
    ql::span<const char> data, ql::span<const uint64_t> segmentSizes,
    int compressionLevel, const SegmentedFrame* reusableBase = nullptr);

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_SEGMENTEDZSTDFRAME_H

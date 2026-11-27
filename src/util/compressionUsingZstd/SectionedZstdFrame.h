// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_SECTIONEDZSTDFRAME_H
#define QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_SECTIONEDZSTDFRAME_H

#include <array>
#include <cstdint>
#include <optional>
#include <vector>

#include "backports/span.h"
#include "util/UninitializedAllocator.h"
#include "util/compressionUsingZstd/ZstdExpected.h"
#include "util/serializer/SerializeArrayOrTuple.h"
#include "util/serializer/Serializer.h"

// A SECTIONED ZSTD FRAME is an ordinary ZSTD frame (which every ZSTD
// decompressor reads) whose content is divided into SECTIONS, followed by a
// skippable frame (which every ZSTD decompressor ignores) with a
// `SectionIndex`:
//
//   ┌────────┬────────────────┬────────────────┬─────┬──────────────────────┐
//   │ frame  │ unit 0         │ unit 1         │ ... │ skippable frame with │
//   │ header │ (sections 0,1) │ (section 2)    │     │ the `SectionIndex`   │
//   └────────┴────────────────┴────────────────┴─────┴──────────────────────┘
//
// The sections are chosen by the writer, typically along the natural parts of
// the data. Their compressed blocks form INDEPENDENT UNITS (see
// `ZstdIndependentUnit.h`): each unit can be decompressed on its own, and the
// compressed blocks of a unit can be copied verbatim into another sectioned
// frame. Normally each section is a unit of its own. A section is merged with
// the following section(s) into one unit only if its compression cannot end in
// the start state of the decoder (which is rare, see `ZstdIndependentUnit.h`;
// short, empty, and random sections are stored uncompressed instead). The index
// stores the size and the SHA-256 hash of the content of each section, so it
// can be determined without decompression whether a unit of one frame can be
// reused for another frame with partially the same content.
//
// Everything only uses the stable API of ZSTD that exists in all supported
// versions. Nothing throws for invalid input or if ZSTD fails; errors are
// reported via `Expected` (see `ZstdExpected.h`).
namespace ad_utility::zstd {

// The SHA-256 hash of the content of a section.
using ContentHash = std::array<char, 32>;

// Return the `ContentHash` of `content`.
ContentHash hashOfContent(ql::span<const char> content);

// A section of a sectioned frame (see above).
struct SectionInfo {
  // The size of the (decompressed) content of the section.
  uint64_t size_ = 0;
  ContentHash hash_{};

  bool operator==(const SectionInfo& other) const {
    return size_ == other.size_ && hash_ == other.hash_;
  }
  bool operator!=(const SectionInfo& other) const { return !(*this == other); }
};

// The serialization of a `SectionInfo` (40 bytes).
AD_SERIALIZE_FUNCTION(SectionInfo) {
  serializer | arg.size_;
  serializer | arg.hash_;
}

// An independent unit of a sectioned frame (see above).
struct UnitInfo {
  // The number of sections of the unit. The units consist of consecutive
  // sections, in order.
  uint64_t numSections_ = 0;
  // The size of the compressed blocks of the unit. The blocks of the units
  // follow each other directly after the frame header.
  uint64_t compressedSize_ = 0;
  // True iff the decoder is in the start state after the unit, so that any
  // unit may follow it (see `ZstdIndependentUnit.h`). This can only be false
  // for the last unit of a frame (if its content does not allow it).
  bool endsInStartState_ = false;
};

// The index of a sectioned frame.
struct SectionIndex {
  std::vector<SectionInfo> sections_;
  std::vector<UnitInfo> units_;
};

// A buffer whose bytes are not zero-initialized on allocation.
using UninitializedBuffer =
    std::vector<char, ad_utility::default_init_allocator<char>>;

// A view of a sectioned frame together with its parsed and validated index.
class SectionedFrame {
 private:
  ql::span<const char> compressed_;
  SectionIndex index_;
  // The decompressed offset at which each section begins, followed by the
  // total size of the content.
  std::vector<uint64_t> sectionBegins_;
  // The offset in `compressed_` at which the blocks of each unit begin,
  // followed by the end of the blocks of the last unit.
  std::vector<uint64_t> unitBegins_;
  // The index of the first section of each unit, followed by the number of
  // sections.
  std::vector<size_t> unitFirstSections_;
  // The offset of the header of the last block of each unit (relative to the
  // start of the unit).
  std::vector<size_t> unitLastBlockHeaders_;

 public:
  // Parse the index of the sectioned frame `compressed` (which must stay
  // valid as long as the result is used) and check that it is consistent with
  // the frame. Return an error if `compressed` is not a valid sectioned frame
  // (for example, an ordinary ZSTD frame without an index).
  static Expected<SectionedFrame> read(ql::span<const char> compressed);

  const SectionIndex& index() const { return index_; }
  size_t numSections() const { return index_.sections_.size(); }
  size_t numUnits() const { return index_.units_.size(); }
  const SectionInfo& section(size_t i) const { return index_.sections_.at(i); }
  const UnitInfo& unit(size_t u) const { return index_.units_.at(u); }

  // The decompressed offset of section `i` (or the total size of the content
  // for `i == numSections()`).
  uint64_t sectionBegin(size_t i) const { return sectionBegins_.at(i); }

  // The index of the first section of unit `u` (or `numSections()` for
  // `u == numUnits()`).
  size_t firstSectionOfUnit(size_t u) const { return unitFirstSections_.at(u); }

  // The index of the unit that contains section `i`. Return an error if there
  // is no section `i`.
  Expected<size_t> unitOfSection(size_t i) const;

  // The compressed blocks of unit `u` (with the last-block flag as it is in the
  // frame), and the offset of the header of its last block within them.
  ql::span<const char> unitBlocks(size_t u) const;
  size_t unitLastBlockHeader(size_t u) const {
    return unitLastBlockHeaders_.at(u);
  }

  // Decompress unit `u` (only that unit). Return an error if its blocks are
  // corrupted.
  Expected<UninitializedBuffer> decompressUnit(size_t u) const;
};

// Build a sectioned frame section by section, either from the content of a
// section (which is compressed) or by copying a unit of another sectioned
// frame (which is not decompressed).
class SectionedFrameBuilder {
 private:
  int compressionLevel_;
  // The frame header (whose content size is written by `finish`), followed by
  // the blocks of the units so far.
  std::vector<char> frame_;
  SectionIndex index_;
  uint64_t contentSize_ = 0;
  // The offset in `frame_` of the header of the last block so far.
  std::optional<size_t> lastBlockHeader_;
  // False after a unit was copied that does not end in the start state.
  bool canAppend_ = true;
  // The sections that were added, but are not compressed yet: The most recent
  // section is only compressed when the next section is added (or by
  // `finish`), because only the last unit of a frame may fail to end in the
  // start state. If a unit cannot end in the start state, the following
  // sections are added to the same unit.
  std::vector<ql::span<const char>> pendingParts_;
  std::vector<SectionInfo> pendingSections_;
  // The storage for the pending parts that are owned by the builder (see
  // `copyUnit`).
  std::vector<UninitializedBuffer> ownedParts_;

 public:
  // The fresh units are compressed with the given ZSTD `compressionLevel`.
  explicit SectionedFrameBuilder(int compressionLevel);

  // Append a section with the given `content`, which has to stay valid until
  // `finish` is called. If the `hash` of the content is known, it can be
  // passed to avoid computing it again. Return an error if nothing can be
  // appended anymore (see `copyUnit`), or if ZSTD fails to compress.
  Expected<void> addSection(ql::span<const char> content,
                            std::optional<ContentHash> hash = std::nullopt);

  // Append the sections of unit `u` of the `base` frame by copying its
  // compressed blocks. If that unit does not end in the start state, then
  // nothing can be appended afterwards. In the rare case that the previous
  // section cannot end in the start state, the unit is decompressed instead and
  // added to the unit of the previous section. Return an error if nothing can
  // be appended anymore, if that decompression fails, or if ZSTD fails to
  // compress.
  Expected<void> copyUnit(const SectionedFrame& base, size_t u);

  // Return the complete sectioned frame, or an error if ZSTD fails to compress.
  Expected<std::vector<char>> finish() &&;

 private:
  // Compress the pending sections as one unit that ends in the start state.
  // If that is not possible, then nothing happens and false is returned,
  // unless `isLast` is true, in which case the unit is compressed regularly.
  // Return an error if ZSTD fails to compress.
  Expected<bool> tryToCompressPending(bool isLast);

  // Append the `blocks` of a unit, the header of whose last block is at
  // `lastBlockHeader`, with the given `info` and `sections`.
  void appendUnit(ql::span<const char> blocks, size_t lastBlockHeader,
                  UnitInfo info, ql::span<const SectionInfo> sections);
};

// Compress `data` as a sectioned frame with the ZSTD `compressionLevel`. The
// sections have the given `sectionSizes`, whose sum must be `data.size()`. If a
// `reusableBase` frame is given, then each of its units whose sections have the
// same content as consecutive sections of `data` is copied instead of
// compressing these sections again (a unit that does not end in the start
// state only at the end of the new frame). The result is deterministic for the
// same version of ZSTD, and copying a unit yields the same result as
// compressing it again. Return an error if the `sectionSizes` do not add up to
// `data.size()`, or if ZSTD fails to compress.
Expected<std::vector<char>> compressSectioned(
    ql::span<const char> data, ql::span<const uint64_t> sectionSizes,
    int compressionLevel, const SectionedFrame* reusableBase = nullptr);

}  // namespace ad_utility::zstd

#endif  // QLEVER_SRC_UTIL_COMPRESSIONUSINGZSTD_SECTIONEDZSTDFRAME_H

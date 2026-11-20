// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/compressionUsingZstd/SegmentedZstdFrame.h"

#include <zstd.h>

#include <map>
#include <string_view>

#include "backports/algorithm.h"
#include "util/CryptographicHashUtils.h"
#include "util/Exception.h"
#include "util/compressionUsingZstd/ZstdFrameBlocks.h"
#include "util/compressionUsingZstd/ZstdFrameHeader.h"
#include "util/compressionUsingZstd/ZstdIndependentUnit.h"
#include "util/compressionUsingZstd/ZstdSkippableFrame.h"
#include "util/serializer/ByteBufferSerializer.h"

namespace ad_utility::zstd {

// The serialization of a `UnitInfo` (for the `SegmentInfo` see the header).
AD_SERIALIZE_FUNCTION(UnitInfo) {
  serializer | arg.numSegments_;
  serializer | arg.compressedSize_;
  // The flag is stored as a byte that has to be 0 or 1.
  uint8_t flag = arg.endsInStartState_;
  serializer | flag;
  if constexpr (ad_utility::serialization::ReadSerializer<S>) {
    if (flag > 1) {
      throw ad_utility::serialization::SerializationException{
          "Invalid flag in the index of a segmented ZSTD frame"};
    }
    arg.endsInStartState_ = flag == 1;
  }
}

namespace {

using namespace ad_utility::serialization;

// The `SegmentIndex` is stored in a skippable frame with this magic variant
// (one of 16). Its content is:
//   8 bytes   the magic bytes `QLVRZSEG`
//   uint16_t  the version of the index format
//   uint64_t  the number of segments, and the number of units
//   the segments (40 bytes each), and the units (17 bytes each)
constexpr uint32_t indexMagicVariant = 0xB;
constexpr std::array<char, 8> indexMagic{'Q', 'L', 'V', 'R',
                                         'Z', 'S', 'E', 'G'};
constexpr uint16_t indexVersion = 1;
constexpr size_t indexHeaderSize =
    indexMagic.size() + sizeof(indexVersion) + 2 * sizeof(uint64_t);
constexpr size_t serializedSegmentSize = sizeof(uint64_t) + sizeof(ContentHash);
constexpr size_t serializedUnitSize = 2 * sizeof(uint64_t) + sizeof(uint8_t);

// Append the skippable frame with the `index` to `out`.
void appendIndexFrame(std::vector<char>& out, const SegmentIndex& index) {
  ByteBufferWriteSerializer writer;
  writer << indexMagic;
  writer << indexVersion;
  writer << static_cast<uint64_t>(index.segments_.size());
  writer << static_cast<uint64_t>(index.units_.size());
  for (const auto& segment : index.segments_) {
    writer << segment;
  }
  for (const auto& unit : index.units_) {
    writer << unit;
  }
  appendTrailingSkippableFrame(out, indexMagicVariant, writer.data());
}

// Parse the content of the skippable frame with the index. Return
// `std::nullopt` if it is malformed. Do not throw for any input.
std::optional<SegmentIndex> parseIndex(ql::span<const char> content) {
  if (content.size() < indexHeaderSize) {
    return std::nullopt;
  }
  ByteBufferReadSerializerT reader{content};
  std::array<char, indexMagic.size()> magic;
  uint16_t version;
  uint64_t numSegments;
  uint64_t numUnits;
  reader >> magic;
  reader >> version;
  reader >> numSegments;
  reader >> numUnits;
  // Check the size before anything is allocated (the numbers might be huge).
  const size_t remaining = content.size() - indexHeaderSize;
  if (magic != indexMagic || version != indexVersion ||
      numSegments > remaining / serializedSegmentSize ||
      numUnits > remaining / serializedUnitSize ||
      numSegments * serializedSegmentSize + numUnits * serializedUnitSize !=
          remaining) {
    return std::nullopt;
  }
  SegmentIndex index;
  index.segments_.resize(numSegments);
  index.units_.resize(numUnits);
  try {
    for (auto& segment : index.segments_) {
      reader >> segment;
    }
    for (auto& unit : index.units_) {
      reader >> unit;
    }
  } catch (const SerializationException&) {
    return std::nullopt;
  }
  return index;
}

// Add `value` to `sum`. Return false if this overflows.
bool addWithoutOverflow(uint64_t& sum, uint64_t value) {
  if (sum + value < sum) {
    return false;
  }
  sum += value;
  return true;
}

}  // namespace

// _____________________________________________________________________________
ContentHash hashOfContent(ql::span<const char> content) {
  auto digest = HashSha256{}(std::string_view{content.data(), content.size()});
  ContentHash result;
  AD_CORRECTNESS_CHECK(digest.size() == result.size());
  ql::ranges::copy(digest, result.begin());
  return result;
}

// _____________________________________________________________________________
std::optional<SegmentedFrame> SegmentedFrame::read(
    ql::span<const char> compressed) {
  auto indexContent = readTrailingSkippableFrame(compressed, indexMagicVariant);
  if (!indexContent.has_value()) {
    return std::nullopt;
  }
  auto index = parseIndex(indexContent.value());
  if (!index.has_value() || index->units_.empty()) {
    return std::nullopt;
  }
  SegmentedFrame result;
  result.index_ = std::move(index.value());
  const auto& segments = result.index_.segments_;
  const auto& units = result.index_.units_;

  // The frame before the index.
  const size_t frameEnd = compressed.size() - indexContent->size() -
                          skippableFrameHeaderSize - sizeof(uint32_t);
  auto frame = compressed.first(frameEnd);
  auto header = parseFrameHeader(frame);
  if (!header.has_value() || header->hasChecksum_ ||
      ZSTD_findFrameCompressedSize(frame.data(), frame.size()) != frameEnd) {
    return std::nullopt;
  }
  result.compressed_ = frame;

  // The offsets of the segments.
  uint64_t contentSize = 0;
  for (const auto& segment : segments) {
    result.segmentBegins_.push_back(contentSize);
    if (!addWithoutOverflow(contentSize, segment.size_)) {
      return std::nullopt;
    }
  }
  result.segmentBegins_.push_back(contentSize);
  if (ZSTD_getFrameContentSize(frame.data(), frame.size()) != contentSize) {
    return std::nullopt;
  }

  // The units and their blocks.
  uint64_t blockBegin = header->headerSize_;
  uint64_t firstSegment = 0;
  for (size_t u = 0; u < units.size(); ++u) {
    const auto& unit = units[u];
    result.unitBegins_.push_back(blockBegin);
    result.unitFirstSegments_.push_back(firstSegment);
    // All units except the last have to end in the start state.
    const bool isLastUnit = u + 1 == units.size();
    if (!addWithoutOverflow(firstSegment, unit.numSegments_) ||
        firstSegment > segments.size() ||
        (!isLastUnit && !unit.endsInStartState_) || unit.compressedSize_ == 0 ||
        unit.compressedSize_ > frameEnd - blockBegin) {
      return std::nullopt;
    }
    auto blocks = frame.subspan(blockBegin, unit.compressedSize_);
    blockBegin += unit.compressedSize_;
    // Only the last block of the last unit is the last block of the frame.
    auto lastHeader = lastBlockHeaderOffset(blocks);
    if (!lastHeader.has_value() ||
        BlockHeader::read(blocks.subspan(lastHeader.value()))->isLast_ !=
            isLastUnit) {
      return std::nullopt;
    }
    result.unitLastBlockHeaders_.push_back(lastHeader.value());
  }
  result.unitBegins_.push_back(blockBegin);
  result.unitFirstSegments_.push_back(firstSegment);
  if (blockBegin != frameEnd || firstSegment != segments.size()) {
    return std::nullopt;
  }
  return result;
}

// _____________________________________________________________________________
size_t SegmentedFrame::unitOfSegment(size_t i) const {
  AD_CONTRACT_CHECK(i < numSegments());
  // The last unit whose first segment is at most `i`.
  auto it = ql::ranges::upper_bound(unitFirstSegments_.begin(),
                                    unitFirstSegments_.end() - 1, i);
  return static_cast<size_t>(it - unitFirstSegments_.begin()) - 1;
}

// _____________________________________________________________________________
ql::span<const char> SegmentedFrame::unitBlocks(size_t u) const {
  return compressed_.subspan(unitBegins_.at(u),
                             unitBegins_.at(u + 1) - unitBegins_.at(u));
}

// _____________________________________________________________________________
std::optional<UninitializedBuffer> SegmentedFrame::decompressUnit(
    size_t u) const {
  const uint64_t size = segmentBegin(firstSegmentOfUnit(u + 1)) -
                        segmentBegin(firstSegmentOfUnit(u));
  UninitializedBuffer result(size);
  if (!SingleSegmentFrame::decompressBlocks(unitBlocks(u), size, result)) {
    return std::nullopt;
  }
  return result;
}

// _____________________________________________________________________________
SegmentedFrameBuilder::SegmentedFrameBuilder(int compressionLevel)
    : compressionLevel_{compressionLevel} {
  // The content size is not known yet, it is written by `finish`.
  SingleSegmentFrame::appendHeader(frame_, 0);
}

// _____________________________________________________________________________
void SegmentedFrameBuilder::addSegment(ql::span<const char> content,
                                       std::optional<ContentHash> hash) {
  AD_CONTRACT_CHECK(canAppend_);
  // If the pending segments cannot end in the start state, they stay pending
  // and the new segment is added to their unit.
  tryToCompressPending(false);
  pendingParts_.push_back(content);
  pendingSegments_.push_back(
      SegmentInfo{content.size(),
                  hash.has_value() ? hash.value() : hashOfContent(content)});
}

// _____________________________________________________________________________
void SegmentedFrameBuilder::copyUnit(const SegmentedFrame& base, size_t u) {
  AD_CONTRACT_CHECK(canAppend_);
  auto segments = ql::span<const SegmentInfo>{base.index().segments_}.subspan(
      base.firstSegmentOfUnit(u), base.unit(u).numSegments_);
  if (!tryToCompressPending(false)) {
    // The unit cannot follow the pending segments, so it is decompressed and
    // added to their unit.
    auto content = base.decompressUnit(u);
    if (!content.has_value()) {
      throw std::runtime_error{
          "A unit of a segmented ZSTD frame cannot be decompressed, the frame "
          "is corrupted"};
    }
    ownedParts_.push_back(std::move(content.value()));
    pendingParts_.push_back(ownedParts_.back());
    pendingSegments_.insert(pendingSegments_.end(), segments.begin(),
                            segments.end());
    return;
  }
  appendUnit(base.unitBlocks(u), base.unitLastBlockHeader(u), base.unit(u),
             segments);
  canAppend_ = base.unit(u).endsInStartState_;
}

// _____________________________________________________________________________
bool SegmentedFrameBuilder::tryToCompressPending(bool isLast) {
  // NOTE: The last unit is also ended in the start state if possible, such
  // that the compressed form of a unit only depends on its content: it can then
  // be copied to any position, and copying it gives the same result as
  // compressing it again.
  if (pendingSegments_.empty()) {
    return true;
  }
  // Concatenate the pending parts if there is more than one.
  std::vector<char> concatenation;
  ql::span<const char> content = pendingParts_.front();
  if (pendingParts_.size() > 1) {
    for (auto part : pendingParts_) {
      concatenation.insert(concatenation.end(), part.begin(), part.end());
    }
    content = concatenation;
  }
  auto unit = compressIndependentUnit(content, compressionLevel_, true);
  if (!unit.has_value()) {
    if (!isLast) {
      return false;
    }
    unit = compressIndependentUnit(content, compressionLevel_, false);
  }
  auto lastHeader = lastBlockHeaderOffset(unit->blocks_);
  AD_CORRECTNESS_CHECK(lastHeader.has_value());
  appendUnit(unit->blocks_, lastHeader.value(),
             UnitInfo{pendingSegments_.size(), unit->blocks_.size(),
                      unit->endsInStartState_},
             pendingSegments_);
  pendingParts_.clear();
  pendingSegments_.clear();
  ownedParts_.clear();
  return true;
}

// _____________________________________________________________________________
void SegmentedFrameBuilder::appendUnit(ql::span<const char> blocks,
                                       size_t lastBlockHeader, UnitInfo info,
                                       ql::span<const SegmentInfo> segments) {
  const size_t begin = frame_.size();
  frame_.insert(frame_.end(), blocks.begin(), blocks.end());
  lastBlockHeader_ = begin + lastBlockHeader;
  // The unit might have been the last unit of another frame.
  setLastBlockFlag(frame_, lastBlockHeader_.value(), false);
  index_.units_.push_back(info);
  for (const auto& segment : segments) {
    index_.segments_.push_back(segment);
    contentSize_ += segment.size_;
  }
}

// _____________________________________________________________________________
std::vector<char> SegmentedFrameBuilder::finish() && {
  AD_CORRECTNESS_CHECK(tryToCompressPending(true));
  if (index_.units_.empty()) {
    // A frame needs at least one block, so a frame without segments consists
    // of a single empty unit.
    auto unit = compressIndependentUnit({}, compressionLevel_, false);
    appendUnit(unit->blocks_, lastBlockHeaderOffset(unit->blocks_).value(),
               UnitInfo{0, unit->blocks_.size(), false}, {});
  }
  setLastBlockFlag(frame_, lastBlockHeader_.value(), true);
  std::vector<char> header;
  SingleSegmentFrame::appendHeader(header, contentSize_);
  ql::ranges::copy(header, frame_.begin());
  appendIndexFrame(frame_, index_);
  return std::move(frame_);
}

// _____________________________________________________________________________
std::vector<char> compressSegmented(ql::span<const char> data,
                                    ql::span<const uint64_t> segmentSizes,
                                    int compressionLevel,
                                    const SegmentedFrame* reusableBase) {
  // The segments of `data`.
  std::vector<ql::span<const char>> contents;
  std::vector<SegmentInfo> segments;
  uint64_t begin = 0;
  for (uint64_t size : segmentSizes) {
    AD_CONTRACT_CHECK(size <= data.size() - begin);
    contents.push_back(data.subspan(begin, size));
    segments.push_back(SegmentInfo{size, hashOfContent(contents.back())});
    begin += size;
  }
  AD_CONTRACT_CHECK(begin == data.size(),
                    "The segment sizes must add up to the size of the data");

  // The units of the `reusableBase` by their first segment.
  std::map<std::pair<uint64_t, ContentHash>, std::vector<size_t>> baseUnits;
  if (reusableBase != nullptr) {
    for (size_t u = 0; u < reusableBase->numUnits(); ++u) {
      if (reusableBase->unit(u).numSegments_ > 0) {
        const auto& first =
            reusableBase->segment(reusableBase->firstSegmentOfUnit(u));
        baseUnits[{first.size_, first.hash_}].push_back(u);
      }
    }
  }
  // Return true iff unit `u` of the `reusableBase` can be copied for the
  // segments starting at `i`.
  auto isReusable = [&segments, reusableBase](size_t i, size_t u) {
    const auto& unit = reusableBase->unit(u);
    const size_t first = reusableBase->firstSegmentOfUnit(u);
    if (unit.numSegments_ > segments.size() - i ||
        (!unit.endsInStartState_ && i + unit.numSegments_ != segments.size())) {
      return false;
    }
    for (size_t k = 0; k < unit.numSegments_; ++k) {
      if (segments[i + k] != reusableBase->segment(first + k)) {
        return false;
      }
    }
    return true;
  };

  SegmentedFrameBuilder builder{compressionLevel};
  for (size_t i = 0; i < segments.size();) {
    auto candidates = baseUnits.find({segments[i].size_, segments[i].hash_});
    std::optional<size_t> reusableUnit;
    if (candidates != baseUnits.end()) {
      for (size_t u : candidates->second) {
        if (isReusable(i, u)) {
          reusableUnit = u;
          break;
        }
      }
    }
    if (reusableUnit.has_value()) {
      builder.copyUnit(*reusableBase, reusableUnit.value());
      i += reusableBase->unit(reusableUnit.value()).numSegments_;
    } else {
      builder.addSegment(contents[i], segments[i].hash_);
      ++i;
    }
  }
  return std::move(builder).finish();
}

}  // namespace ad_utility::zstd
